// Licensed under the Apache License, Version 2.0.

//go:build cgo

package chargewire

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// The C library's allocator gets a recycler: freed bodies come back for the
// next ones, small allocations pass through, and it keeps at most recycleLimit.
func TestRecycler(t *testing.T) {
	m := mallocator.NewMallocator()
	r := bodyAllocator(m).(*recycler)
	if bodyAllocator(m) != r {
		t.Fatal("one recycler per allocator")
	}
	if bodyAllocator(memory.DefaultAllocator) != memory.DefaultAllocator {
		t.Fatal("only the C allocator is wrapped")
	}
	a := r.Allocate(3<<20 + 5)
	if len(a) != 3<<20+5 {
		t.Fatalf("len %d", len(a))
	}
	a[0], a[len(a)-1] = 1, 2
	r.Free(a)
	b := r.Allocate(3<<20 + 100) // the same 4 MiB class: the same block
	if &a[0] != &b[0] {
		t.Fatal("a freed block was not reused")
	}
	small := r.Allocate(100)
	r.Free(small)
	grown := r.Reallocate(5<<20, b)
	if grown[0] != 1 {
		t.Fatal("Reallocate lost the contents")
	}
	r.Free(grown)
	var held [][]byte
	for range recycleLimit/(8<<20) + 4 {
		held = append(held, r.Allocate(8<<20))
	}
	for _, h := range held {
		r.Free(h)
	}
	r.mu.Lock()
	cached, owned := r.cached, len(r.owned)
	r.mu.Unlock()
	if cached > recycleLimit || owned != 0 {
		t.Fatalf("cached %d bytes (limit %d), %d blocks still marked handed out", cached, recycleLimit, owned)
	}
}

// Decoding through recycled memory leaves no stale bytes: the same stream
// read twice through the C allocator equals Arrow's reading, both times.
func TestFramedReaderWithRecycledBodies(t *testing.T) {
	rec := mixedBatch(t, 50_000)
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithLZ4(), ipc.WithMinSpaceSavings(0.5))
	for range 3 {
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	alloc := mallocator.NewMallocator()
	// Dirty the recycler first, so reused blocks hold garbage.
	r := bodyAllocator(alloc).(*recycler)
	var dirty [][]byte
	for class := 1; class <= 16; class++ {
		for range 3 {
			d := r.Allocate(class << 20)
			for i := range d {
				d[i] = 0xAB
			}
			dirty = append(dirty, d)
		}
	}
	for _, d := range dirty {
		r.Free(d)
	}
	for round := range 2 {
		reader, err := newIPCReader(bytes.NewReader(buf.Bytes()), alloc)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for reader.Next() {
			if !array.RecordEqual(reader.RecordBatch(), rec) {
				t.Fatalf("round %d batch %d differs", round, n)
			}
			// Every buffer's padding up to 8 bytes is zero, as Arrow writes it.
			for c := range rec.NumCols() {
				for _, b := range reader.RecordBatch().Column(int(c)).Data().Buffers() {
					if b == nil || b.Len() == 0 {
						continue
					}
					tail := b.Buf()[b.Len():min(cap(b.Buf()), (b.Len()+7)&^7)]
					for _, x := range tail {
						if x != 0 {
							t.Fatalf("round %d: stale padding after column %d", round, c)
						}
					}
				}
			}
			n++
		}
		if reader.Err() != nil || n != 3 {
			t.Fatalf("round %d: %d batches, %v", round, n, reader.Err())
		}
		reader.Release()
	}
}
