// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

// Result bodies are tens of megabytes each, allocated for every batch and freed
// once the application has the batch. The C library's allocator gets each one
// from calloc, which maps fresh zeroed pages that fault in on first touch: on a
// fast link that cost a large share of the client's CPU. A recycler keeps
// freed bodies for the next ones instead (up to recycleLimit bytes).

const (
	recycleFrom  = 1 << 20   // smaller allocations go straight to the allocator
	recycleClass = 1 << 20   // sizes are rounded up to a multiple of this
	recycleLimit = 512 << 20 // the most bytes kept for reuse
)

// recycler is a memory.Allocator that reuses freed large blocks of another.
// Reused memory is not zeroed: callers must write every byte they hand on.
type recycler struct {
	inner memory.Allocator

	mu     sync.Mutex
	free   map[int][][]byte   // by capacity
	owned  map[uintptr][]byte // handed out: first byte -> the whole block
	cached int
}

var recyclers sync.Map // *mallocator.Mallocator -> *recycler

// bodyAllocator is what message bodies are allocated from: a recycler over
// the C library's allocator, alloc itself otherwise (a Go allocator reuses
// memory already; a checked one in tests must see every allocation).
func bodyAllocator(alloc memory.Allocator) memory.Allocator {
	m, ok := alloc.(*mallocator.Mallocator)
	if !ok {
		return alloc
	}
	r, _ := recyclers.LoadOrStore(m, &recycler{inner: m, free: map[int][][]byte{}, owned: map[uintptr][]byte{}})
	return r.(*recycler)
}

func (r *recycler) Allocate(size int) []byte {
	if size < recycleFrom {
		return r.inner.Allocate(size)
	}
	class := (size + recycleClass - 1) / recycleClass * recycleClass
	r.mu.Lock()
	var block []byte
	if list := r.free[class]; len(list) > 0 {
		block = list[len(list)-1]
		r.free[class] = list[:len(list)-1]
		r.cached -= class
	}
	r.mu.Unlock()
	if block == nil {
		block = r.inner.Allocate(class)
	}
	r.mu.Lock()
	r.owned[uintptr(unsafe.Pointer(&block[0]))] = block
	r.mu.Unlock()
	return block[:size:size]
}

func (r *recycler) Free(b []byte) {
	if cap(b) == 0 {
		return
	}
	key := uintptr(unsafe.Pointer(&b[:1][0]))
	r.mu.Lock()
	block, ok := r.owned[key]
	if ok {
		delete(r.owned, key)
		if r.cached+len(block) <= recycleLimit {
			r.free[len(block)] = append(r.free[len(block)], block)
			r.cached += len(block)
			block = nil
		}
	}
	r.mu.Unlock()
	switch {
	case !ok:
		r.inner.Free(b)
	case block != nil:
		r.inner.Free(block)
	}
}

func (r *recycler) Reallocate(size int, b []byte) []byte {
	out := r.Allocate(size)
	copy(out, b)
	r.Free(b)
	return out
}
