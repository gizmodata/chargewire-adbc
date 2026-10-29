// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/gizmodata/chargewire-adbc/go/internal/protocol"
)

// partUploadReplayLimit is how much of each part is kept in memory so that a
// part whose answer is lost can be sent again: a smaller part is retried, a
// larger one streams. Parts are sent at once, so this bounds memory per part.
const partUploadReplayLimit = 4 << 20

// lookahead reads up to n batches ahead of a reader, to decide whether an
// upload is large enough to split, and then hands them out first.
type lookahead struct {
	source array.RecordReader
	ahead  []arrow.RecordBatch
	done   bool // the source ended while reading ahead
}

// readAheadRows reads batches from source until they hold at least n rows or
// the source ends (done).
func readAheadRows(source array.RecordReader, n int64) (*lookahead, error) {
	l := &lookahead{source: source}
	for l.rows() < n {
		if !source.Next() {
			l.done = true
			return l, source.Err()
		}
		batch := source.RecordBatch()
		batch.Retain()
		l.ahead = append(l.ahead, batch)
	}
	return l, nil
}

// rows counts the rows read ahead.
func (l *lookahead) rows() int64 {
	var n int64
	for _, batch := range l.ahead {
		n += batch.NumRows()
	}
	return n
}

// sharedBatches is one upload's batches, taken one at a time by the readers
// of every part. Each batch goes to exactly one part.
type sharedBatches struct {
	mu     sync.Mutex
	schema *arrow.Schema
	l      *lookahead
	err    error
}

// next returns the next batch, retained for the caller, or nil at the end.
// uploadSliceRows is the most rows one uploaded batch carries: larger batches go as
// zero-copy slices of this size. The server inserts each batch on one of its threads,
// and DuckDB compresses a thread's rows in row groups of 122,880 on that thread, so with
// large batches a few threads that took one more than the others decide when the insert
// ends: SF1 lineitem (49 batches of 122,880 rows over 32 threads) took 0.35-0.58 s on a
// LAN, in steps of one row group's compression; in 16,384-row slices, 0.35 s every time.
const uploadSliceRows = 16384

func (s *sharedBatches) next() (arrow.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if len(s.l.ahead) == 0 {
		if s.l.done || !s.l.source.Next() {
			s.l.done = true
			s.err = s.l.source.Err()
			return nil, s.err
		}
		batch := s.l.source.RecordBatch()
		batch.Retain()
		s.l.ahead = append(s.l.ahead, batch)
	}
	batch := s.l.ahead[0]
	s.l.ahead = s.l.ahead[1:]
	if rows := batch.NumRows(); rows > uploadSliceRows {
		first := batch.NewSlice(0, uploadSliceRows)
		rest := batch.NewSlice(uploadSliceRows, rows)
		batch.Release()
		s.l.ahead = append([]arrow.RecordBatch{rest}, s.l.ahead...)
		return first, nil
	}
	return batch, nil
}

// release drops what was read ahead and never handed out.
func (s *sharedBatches) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, batch := range s.l.ahead {
		batch.Release()
	}
	s.l.ahead = nil
}

// partReader is one part's view of the shared batches.
type partReader struct {
	shared  *sharedBatches
	current arrow.RecordBatch
	err     error
	refs    atomic.Int64
}

func newPartReader(shared *sharedBatches) *partReader {
	r := &partReader{shared: shared}
	r.refs.Store(1)
	return r
}

func (r *partReader) Retain() { r.refs.Add(1) }
func (r *partReader) Release() {
	if r.refs.Add(-1) == 0 && r.current != nil {
		r.current.Release()
		r.current = nil
	}
}
func (r *partReader) Schema() *arrow.Schema { return r.shared.schema }
func (r *partReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.err != nil {
		return false
	}
	r.current, r.err = r.shared.next()
	return r.current != nil
}
func (r *partReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *partReader) Record() arrow.RecordBatch      { return r.current }
func (r *partReader) Err() error                     { return r.err }

// ingestInParts sends the rows as a partitioned ingest (§8.3): open, send
// every part at once, each over its own connection and taking the next batch
// from the shared reader, then commit. When anything fails the ingest is
// aborted, so nothing is written.
func (st *statementImpl) ingestInParts(ctx context.Context, ir protocol.IngestRequest, shared *sharedBatches, parts int) (int64, error) {
	session := st.session()
	handle, err := session.OpenIngest(ctx, ir)
	if err != nil {
		return -1, wrapError(err, "opening a partitioned ingest into "+ir.Table)
	}
	abort := func() {
		abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()
		_ = session.AbortIngest(abortCtx, handle)
	}

	partsCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	var wg sync.WaitGroup
	errs := make([]error, parts)
	for part := range parts {
		wg.Go(func() {
			rows := newPartReader(shared)
			defer rows.Release()
			up, err := encodeUploadWithLimit(rows, st.alloc, st.codec(), partUploadReplayLimit)
			if err == nil {
				_, err = session.IngestPart(partsCtx, handle, part, up.body)
				err = up.finish(err)
			}
			if err != nil {
				errs[part] = err
				stop(err) // the other parts stop sending
			}
		})
	}
	wg.Wait()
	if err := firstCause(errs); err != nil {
		abort()
		return -1, wrapError(err, "bulk ingest into "+ir.Table)
	}
	n, err := session.CommitIngest(ctx, handle, parts)
	if err != nil {
		abort()
		return -1, wrapError(err, "committing the bulk ingest into "+ir.Table)
	}
	return n, nil
}

// firstCause picks the error that explains a failed partitioned ingest: a
// part's own failure rather than the cancellation it caused in the others.
func firstCause(errs []error) error {
	var cancelled error
	for _, err := range errs {
		switch {
		case err == nil:
		case errors.Is(err, context.Canceled):
			if cancelled == nil {
				cancelled = err
			}
		default:
			return err
		}
	}
	return cancelled
}
