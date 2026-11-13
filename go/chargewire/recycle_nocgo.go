// Licensed under the Apache License, Version 2.0.

//go:build !cgo

package chargewire

import "github.com/apache/arrow-go/v18/arrow/memory"

// bodyAllocator is what message bodies are allocated from. A build without cgo
// (CGO_ENABLED=0, as a cross-compiled or statically linked program is built)
// has no C library allocator to recycle for: arrow-go's mallocator exists only
// with cgo, and a Go allocator reuses memory already. recycle.go is the cgo
// build's.
func bodyAllocator(alloc memory.Allocator) memory.Allocator { return alloc }
