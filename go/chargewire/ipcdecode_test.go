// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/pierrec/lz4/v4"
)

// readAllBatches reads a whole stream with read, returning its batches.
func readAllBatches(t *testing.T, r *ipc.Reader) []arrow.RecordBatch {
	t.Helper()
	var out []arrow.RecordBatch
	for r.Next() {
		rec := r.RecordBatch()
		rec.Retain()
		out = append(out, rec)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("reading: %v", err)
	}
	return out
}

// sameAsArrow checks the driver's reader returns exactly what Arrow's own
// reader does for a stream, and frees everything it allocated.
func sameAsArrow(t *testing.T, data []byte) int {
	t.Helper()
	want, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer want.Release()
	wantBatches := readAllBatches(t, want)

	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	got, err := newIPCReader(bytes.NewReader(data), alloc)
	if err != nil {
		t.Fatal(err)
	}
	gotBatches := readAllBatches(t, got)
	if !got.Schema().Equal(want.Schema()) {
		t.Fatalf("schema %s, want %s", got.Schema(), want.Schema())
	}
	if len(gotBatches) != len(wantBatches) {
		t.Fatalf("%d batches, want %d", len(gotBatches), len(wantBatches))
	}
	for i := range gotBatches {
		if !array.RecordEqual(gotBatches[i], wantBatches[i]) {
			for c := range gotBatches[i].NumCols() {
				if !array.Equal(gotBatches[i].Column(int(c)), wantBatches[i].Column(int(c))) {
					t.Fatalf("batch %d column %d (%s) differs", i, c, gotBatches[i].ColumnName(int(c)))
				}
			}
			t.Fatalf("batch %d differs", i)
		}
	}
	for i := range gotBatches {
		gotBatches[i].Release()
		wantBatches[i].Release()
	}
	got.Release()
	return len(gotBatches)
}

// Streams Arrow C++ compressed (testdata/make_compressed_ipc.py): LZ4 frames
// of linked blocks, some stored as they are, and zstd; nested, view, decimal
// and all-null columns; an empty batch.
func TestFramedReaderReadsArrowCppStreams(t *testing.T) {
	for _, name := range []string{"cpp_lz4.arrows.gz", "cpp_zstd.arrows.gz"} {
		t.Run(name, func(t *testing.T) {
			f, err := os.Open(filepath.Join("testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			z, err := gzip.NewReader(f)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(z)
			if err != nil {
				t.Fatal(err)
			}
			if n := sameAsArrow(t, data); n != 3 {
				t.Fatalf("%d batches, want 3", n)
			}
		})
	}
}

func mixedBatch(t *testing.T, rows int) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rand", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "sv", Type: arrow.BinaryTypes.StringView, Nullable: true},
		{Name: "l", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	rng := rand.New(rand.NewPCG(1, 2))
	for r := range rows {
		b.Field(0).(*array.Int64Builder).Append(int64(r))
		if r%9 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.Float64Builder).Append(rng.Float64())
		}
		if r%7 == 0 {
			b.Field(2).AppendNull()
			b.Field(3).AppendNull()
		} else {
			b.Field(2).(*array.StringBuilder).Append("row " + strings.Repeat("x", r%17))
			b.Field(3).(*array.StringViewBuilder).Append("a view longer than twelve bytes " + strings.Repeat("y", r%5))
		}
		lb := b.Field(4).(*array.ListBuilder)
		if r%3 == 0 {
			lb.AppendNull()
		} else {
			lb.Append(true)
			lb.ValueBuilder().(*array.Int32Builder).AppendValues([]int32{int32(r), int32(-r)}, nil)
		}
	}
	return b.NewRecordBatch()
}

// Streams the Go writer compressed, with and without buffers sent as they
// are (length -1) inside compressed messages.
func TestFramedReaderReadsGoStreams(t *testing.T) {
	rec := mixedBatch(t, 50_000)
	defer rec.Release()
	for _, tc := range []struct {
		name string
		opts []ipc.Option
	}{
		{"none", nil},
		{"lz4", []ipc.Option{ipc.WithLZ4()}},
		{"zstd", []ipc.Option{ipc.WithZstd()}},
		{"lz4, most buffers sent as they are", []ipc.Option{ipc.WithLZ4(), ipc.WithMinSpaceSavings(0.99)}},
		{"zstd, some buffers sent as they are", []ipc.Option{ipc.WithZstd(), ipc.WithMinSpaceSavings(0.1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := ipc.NewWriter(&buf, append([]ipc.Option{ipc.WithSchema(rec.Schema())}, tc.opts...)...)
			for range 3 {
				if err := w.Write(rec); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if n := sameAsArrow(t, buf.Bytes()); n != 3 {
				t.Fatalf("%d batches", n)
			}
		})
	}
}

// A compressed stream cut anywhere is incomplete, never a shorter result.
func TestFramedReaderCutCompressedStream(t *testing.T) {
	rec := mixedBatch(t, 20_000)
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(rec.Schema()), ipc.WithLZ4())
	for range 2 {
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	for _, cut := range []int{len(data) - 8, len(data) - 9, len(data) / 2, 100} {
		r, err := newIPCReader(bytes.NewReader(data[:cut]), memory.DefaultAllocator)
		if err != nil {
			if !errors.Is(err, errIncompleteStream) {
				t.Errorf("cut at %d: %v", cut, err)
			}
			continue
		}
		for r.Next() {
		}
		if !errors.Is(r.Err(), errIncompleteStream) {
			t.Errorf("cut at %d of %d: %v, want an incomplete stream", cut, len(data), r.Err())
		}
		r.Release()
	}
}

// The LZ4 frame decoder handles every frame option a writer may set.
func TestDecodeLZ4Frames(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	data := make([]byte, 300_000)
	for i := range data {
		if i%1000 < 600 {
			data[i] = byte(i / 1000) // compressible runs
		} else {
			data[i] = byte(rng.Uint32()) // blocks that don't compress
		}
	}
	frame := func(opts ...lz4.Option) []byte {
		var buf bytes.Buffer
		w := lz4.NewWriter(&buf)
		if err := w.Apply(opts...); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		return buf.Bytes()
	}
	plain := frame(lz4.BlockSizeOption(lz4.Block64Kb), lz4.ChecksumOption(false))
	cases := map[string][]byte{
		"64 KB blocks":                         plain,
		"4 MB blocks":                          frame(lz4.BlockSizeOption(lz4.Block4Mb)),
		"checksums and size":                   frame(lz4.BlockChecksumOption(true), lz4.ChecksumOption(true), lz4.SizeOption(uint64(len(data)))),
		"two frames":                           append(append([]byte{}, plain[:len(plain):len(plain)]...), frame(lz4.ChecksumOption(true))...),
		"a skippable frame first":              append([]byte{0x50, 0x2a, 0x4d, 0x18, 3, 0, 0, 0, 1, 2, 3}, plain...),
		"compression level 9 (long matches)":   frame(lz4.CompressionLevelOption(lz4.Level9)),
		"independent blocks, concurrent write": frame(lz4.ConcurrencyOption(4), lz4.BlockSizeOption(lz4.Block64Kb)),
	}
	for name, src := range cases {
		want := data
		if name == "two frames" {
			want = append(append([]byte{}, data...), data...)
		}
		dst := make([]byte, len(want))
		if err := decodeLZ4Frames(src, dst); err != nil {
			t.Errorf("%s: %v", name, err)
		} else if !bytes.Equal(dst, want) {
			t.Errorf("%s: decoded bytes differ", name)
		}
	}
	// Refusals: a wrong size either way, truncation, a bad magic number.
	for name, tc := range map[string]struct {
		src  []byte
		size int
	}{
		"too short a buffer": {plain, len(data) - 1},
		"too long a buffer":  {plain, len(data) + 1},
		"truncated":          {plain[:len(plain)/2], len(data)},
		"no end mark":        {plain[:len(plain)-4], len(data)},
		"not lz4":            {[]byte("not an lz4 frame at all"), 10},
	} {
		if err := decodeLZ4Frames(tc.src, make([]byte, tc.size)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
