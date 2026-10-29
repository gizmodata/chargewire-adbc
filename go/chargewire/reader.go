// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase/gadgets"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// errIncompleteStream is an Arrow stream that ended without its end-of-stream
// marker: the server stopped sending it, or the connection dropped (§6.6).
var errIncompleteStream = errors.New("the Arrow stream ended without its end-of-stream marker")

// maxResumes bounds how often one result download is resumed.
const maxResumes = 5

// settleWait is how long a broken unretained download waits for its
// statement to reach a final state, to report the statement's own error.
const settleWait = 2 * time.Second

func ioError(format string, args ...any) error {
	return adbc.Error{Code: adbc.StatusIO, Msg: fmt.Sprintf("[%s] "+format, append([]any{driverName}, args...)...)}
}

// eosMarker is the end-of-stream marker that ends a complete Arrow stream.
var eosMarker = []byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}

// newIPCReader reads an Arrow IPC stream, treating a stream without its
// end-of-stream marker as an error, and decompressing compressed record
// batches in parallel (framedReader).
func newIPCReader(body io.Reader, alloc memory.Allocator) (*ipc.Reader, error) {
	return ipc.NewReaderFromMessageReader(newFramedReader(body, alloc), ipc.WithAllocator(alloc))
}

// readSchema decodes a schema embedded in JSON: an IPC stream with only the
// schema message (§2.2).
func readSchema(data []byte, alloc memory.Allocator) (*arrow.Schema, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("decoding an Arrow schema: %w", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// resultPart is one partition of a result (§6.2).
type resultPart struct {
	index    int
	location *url.URL
}

// resultReader streams a statement's result: its partitions in order, each
// an Arrow stream. When a stream breaks off it asks the server why; a result
// the server retained is resumed from the next batch (§6.5), a failed
// statement reports the statement's error (§6.6).
type resultReader struct {
	refs    atomic.Int64
	ctx     context.Context
	alloc   memory.Allocator
	session *protocol.Session
	ref     protocol.StatementRef
	codec   string // the Arrow buffer compression asked for, or ""
	schema  *arrow.Schema

	parts   []resultPart
	part    int
	body    io.ReadCloser
	stream  *ipc.Reader
	batches int64 // record batches read from the current part
	resumes int

	rec  arrow.RecordBatch
	err  error
	done bool

	// cancelled reports whether the application cancelled the statement,
	// so a cut stream is reported as that.
	cancelled func() bool
	// onEnd runs once, when the result is read to the end, fails, or is
	// released early; finished says it was read to the end.
	onEnd func(finished bool)
	ended bool
}

type resultReaderConfig struct {
	ctx       context.Context
	alloc     memory.Allocator
	session   *protocol.Session
	ref       protocol.StatementRef
	parts     []resultPart
	codec     string
	cancelled func() bool
	onEnd     func(finished bool)
}

// newResultReader opens a result. first, when set, is the body of the
// response that already carries partition 0 (the streamed path, §6.1).
func newResultReader(cfg resultReaderConfig, first io.ReadCloser) (*resultReader, error) {
	r := &resultReader{
		ctx: cfg.ctx, alloc: cfg.alloc, session: cfg.session, ref: cfg.ref, parts: cfg.parts,
		codec: cfg.codec, cancelled: cfg.cancelled, onEnd: cfg.onEnd,
	}
	r.refs.Store(1)
	if first != nil {
		if err := r.attach(first); err != nil {
			if recovered := r.recover(err); recovered != nil {
				r.end(false)
				return nil, recovered
			}
		}
	} else if len(r.parts) > 0 {
		if err := r.open(); err != nil {
			r.end(false)
			return nil, err
		}
	}
	if r.schema == nil {
		r.end(false)
		return nil, wrapError(errors.New("the result has no partitions"), "reading the result")
	}
	return r, nil
}

// attach starts reading a part's stream from body.
func (r *resultReader) attach(body io.ReadCloser) error {
	stream, err := newIPCReader(body, r.alloc)
	if err != nil {
		body.Close()
		return err
	}
	if r.schema == nil {
		r.schema = stream.Schema()
	} else if !r.schema.Equal(stream.Schema()) {
		stream.Release()
		body.Close()
		return fmt.Errorf("partition %d has schema %s, but the result's schema is %s", r.parts[r.part].index, stream.Schema(), r.schema)
	}
	r.body, r.stream = body, stream
	return nil
}

// open requests the current part from the batch after the last one read.
func (r *resultReader) open() error {
	part := r.parts[r.part]
	for attempt := 0; ; attempt++ {
		resp, err := r.session.Result(r.ctx, protocol.ResultRequest{
			Ref: r.ref, Partition: part.index, FromBatch: r.batches, Location: part.location, Compression: r.codec,
		})
		if protocol.HasCode(err, protocol.CodeResultNotReady) && attempt < maxResumes {
			// No schema yet: wait for the statement to make progress (§6.4).
			if _, err := r.session.Status(r.ctx, r.ref, settleWait); err != nil {
				return wrapError(err, "waiting for the result")
			}
			continue
		}
		if err != nil {
			return wrapError(err, "reading the result")
		}
		if err := r.attach(resp.Body); err != nil {
			return wrapError(err, "reading the result")
		}
		return nil
	}
}

func (r *resultReader) closeStream() {
	if r.stream != nil {
		r.stream.Release()
		r.stream = nil
	}
	if r.body != nil {
		r.body.Close()
		r.body = nil
	}
}

// recover handles a stream that broke off: it reports the statement's error,
// or resumes a retained result where it stopped.
func (r *resultReader) recover(streamErr error) error {
	if r.cancelled() {
		return adbc.Error{Code: adbc.StatusCancelled, Msg: fmt.Sprintf("[%s] the statement was cancelled", driverName)}
	}
	if r.ctx.Err() != nil {
		return wrapError(r.ctx.Err(), "reading the result")
	}
	doc, err := r.session.Status(r.ctx, r.ref, 0)
	if err != nil {
		return wrapError(fmt.Errorf("the result stream broke off (%v), and the statement's status could not be read: %w", streamErr, err), "reading the result")
	}
	retained := doc.Result != nil && doc.Result.Retained
	if !retained && !doc.State.Terminal() {
		// An unretained result can't be resumed; wait briefly for the
		// statement to settle so its own error can be reported.
		if settled, err := r.session.Status(r.ctx, r.ref, settleWait); err == nil {
			doc = settled
		}
	}
	switch {
	case doc.State == protocol.StateFailed:
		return wrapError(statementError(doc), "the statement failed")
	case doc.State == protocol.StateCancelled && doc.CancelReason != protocol.CancelReasonClient:
		return wrapError(statementError(doc), "the statement was cancelled")
	case !retained:
		return ioError("reading the result: the result stream broke off (%v); the server did not retain the result, so it "+
			"cannot be resumed (set %s=true or %s=true to make downloads resumable)", streamErr, OptionRetainResult, OptionAsync)
	case r.resumes >= maxResumes:
		return ioError("reading the result: the result stream broke off %d times; last: %v", r.resumes, streamErr)
	}
	r.resumes++
	if err := gadgets.SleepCtx(r.ctx, min(time.Duration(r.resumes)*250*time.Millisecond, 2*time.Second)); err != nil {
		return wrapError(err, "reading the result")
	}
	return r.open()
}

func (r *resultReader) Retain() { r.refs.Add(1) }

func (r *resultReader) Release() {
	if r.refs.Add(-1) == 0 {
		r.closeStream()
		r.end(r.done && r.err == nil)
	}
}

func (r *resultReader) end(finished bool) {
	if r.ended {
		return
	}
	r.ended = true
	if r.onEnd != nil {
		r.onEnd(finished)
	}
}

func (r *resultReader) Schema() *arrow.Schema { return r.schema }

func (r *resultReader) Next() bool {
	r.rec = nil
	if r.done || r.err != nil {
		return false
	}
	for {
		if r.stream == nil {
			if r.part >= len(r.parts) {
				r.done = true
				r.end(true)
				return false
			}
			if err := r.open(); err != nil {
				return r.fail(err)
			}
		}
		if r.stream.Next() {
			r.rec = r.stream.RecordBatch()
			r.batches++
			return true
		}
		err := r.stream.Err()
		r.closeStream()
		if err == nil {
			r.part++
			r.batches = 0
			continue
		}
		if err := r.recover(err); err != nil {
			return r.fail(err)
		}
	}
}

func (r *resultReader) fail(err error) bool {
	r.err = err
	r.closeStream()
	r.end(false)
	return false
}

func (r *resultReader) RecordBatch() arrow.RecordBatch { return r.rec }

// Record is the deprecated name of RecordBatch.
func (r *resultReader) Record() arrow.RecordBatch { return r.rec }

func (r *resultReader) Err() error { return r.err }
