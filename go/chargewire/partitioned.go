// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gizmodata/chargewire-adbc/go/v2/internal/protocol"
)

// partitionBuffer is how many batches each partition's download may hold
// ahead of the reader.
const partitionBuffer = 2

// causeWait is how long a cancellation the application did not ask for
// waits for the partition that caused it to report its own error.
const causeWait = 2 * time.Second

// splitHeaders reads how a streamed result was split (§6.10): the partition
// count k and, when k is more than 1, the order. A result split in more than
// one partition must say how in both headers: a missing or unknown order is a
// protocol error, never guessed.
func splitHeaders(resp *http.Response) (int, string, error) {
	value := resp.Header.Get("Chargewire-Result-Partitions")
	if value == "" {
		return 1, "", nil
	}
	k, err := strconv.Atoi(value)
	if err != nil || k < 1 || k > maxResultPartitions {
		return 0, "", protocol.ProtocolError("the result's Chargewire-Result-Partitions is %q, not a partition count from 1 to %d",
			value, maxResultPartitions)
	}
	if k == 1 {
		return 1, "", nil
	}
	order := resp.Header.Get("Chargewire-Result-Order")
	if !knownPartitionOrder(order) {
		return 0, "", protocol.ProtocolError("the result is split into %d partitions, but its Chargewire-Result-Order is %q, "+
			"not %q or %q", k, order, protocol.PartitionOrderInterleaved, protocol.PartitionOrderAny)
	}
	return k, order, nil
}

// knownPartitionOrder reports an order of version 1 (§6.10). A later version
// adds orders only for clients that ask for them (§12).
func knownPartitionOrder(order string) bool {
	return order == protocol.PartitionOrderInterleaved || order == protocol.PartitionOrderAny
}

// partitionedReader reads a result split into partitions (§6.10): every
// partition at once, each over its own connection with its own resultReader
// (which resumes a retained partition and reports a failed statement's
// error), merged into one stream. "interleaved" partitions are merged round
// robin, which is the result's order; "any" partitions in whatever order
// their batches arrive. The result is complete only when every partition
// ended with its end-of-stream marker; the first partition that fails fails
// the whole result and stops the others. One exception: a cancellation the
// application did not ask for is usually the server's reaction to another
// partition's failure (its connection broke, so the server stopped the
// statement). The other partitions then keep reading, dropping rows, for up
// to causeWait, and the error of the one that caused it wins.
type partitionedReader struct {
	refs   atomic.Int64
	schema *arrow.Schema
	order  string

	ctx    context.Context
	cancel context.CancelFunc
	first  io.ReadCloser // partition 0's body from the submit, closed to stop it

	feeds  []chan arrow.RecordBatch // one per partition (interleaved)
	merged chan arrow.RecordBatch   // all partitions (any)
	wg     sync.WaitGroup

	failMu    sync.Mutex
	failure   error
	stopped   bool
	cancelled func() bool // the application cancelled the statement

	next int // interleaved: the partition the next batch comes from
	rec  arrow.RecordBatch
	err  error
	done bool

	onEnd func(finished bool)
	ended bool
}

// newPartitionedReader starts reading every partition in cfg.parts. first,
// when set, is the body that already carries partition 0 (the streamed path).
func newPartitionedReader(cfg resultReaderConfig, order string, first io.ReadCloser) (*partitionedReader, error) {
	ctx, cancel := context.WithCancel(cfg.ctx)
	p := &partitionedReader{order: order, ctx: ctx, cancel: cancel, first: first, onEnd: cfg.onEnd,
		cancelled: cfg.cancelled}
	p.refs.Store(1)
	sub := cfg
	sub.ctx, sub.onEnd = ctx, nil
	open := func(i int, body io.ReadCloser) (*resultReader, error) {
		part := sub
		part.parts = []resultPart{cfg.parts[i]}
		return newResultReader(part, body)
	}

	// Partition 0 first: its schema is the result's.
	zero, err := open(0, first)
	if err != nil {
		cancel()
		p.end(false)
		return nil, err
	}
	p.schema = zero.Schema()
	k := len(cfg.parts)
	if order == protocol.PartitionOrderInterleaved {
		p.feeds = make([]chan arrow.RecordBatch, k)
		for i := range p.feeds {
			p.feeds[i] = make(chan arrow.RecordBatch, partitionBuffer)
		}
	} else {
		p.merged = make(chan arrow.RecordBatch, partitionBuffer*k)
	}
	p.wg.Add(k)
	go p.pump(0, zero)
	for i := 1; i < k; i++ {
		go func(i int) {
			r, err := open(i, nil)
			if err != nil {
				p.fail(err)
				p.closeFeed(i)
				p.wg.Done()
				return
			}
			if !r.Schema().Equal(p.schema) {
				r.Release()
				p.fail(wrapError(fmt.Errorf("partition %d has schema %s, but the result's schema is %s", i, r.Schema(), p.schema), "reading the result"))
				p.closeFeed(i)
				p.wg.Done()
				return
			}
			p.pump(i, r)
		}(i)
	}
	if p.merged != nil {
		go func() {
			p.wg.Wait()
			close(p.merged)
		}()
	}
	return p, nil
}

// pump moves one partition's batches to the merge.
func (p *partitionedReader) pump(i int, r *resultReader) {
	defer p.wg.Done()
	defer r.Release()
	defer p.closeFeed(i)
	feed := p.merged
	if feed == nil {
		feed = p.feeds[i]
	}
	for r.Next() {
		rec := r.RecordBatch()
		rec.Retain()
		select {
		case feed <- rec:
		case <-p.ctx.Done():
			rec.Release()
			return
		}
	}
	if err := r.Err(); err != nil {
		p.fail(err)
	}
}

func (p *partitionedReader) closeFeed(i int) {
	if p.feeds != nil {
		close(p.feeds[i])
	}
}

// fail records the first failure and stops every partition, unless it is a
// consequential cancellation, which waits for its cause (see the type).
func (p *partitionedReader) fail(err error) {
	p.failMu.Lock()
	stop := false
	switch {
	case p.failure == nil:
		p.failure = err
		if p.consequential(err) {
			time.AfterFunc(causeWait, p.stopAll)
		} else {
			stop = true
		}
	case p.consequential(p.failure) && !protocol.HasCode(err, protocol.CodeStatementCancelled):
		p.failure = err
		stop = true
	}
	p.failMu.Unlock()
	if stop {
		p.stopAll()
	}
}

// consequential reports a cancellation the application did not ask for.
func (p *partitionedReader) consequential(err error) bool {
	return protocol.HasCode(err, protocol.CodeStatementCancelled) && (p.cancelled == nil || !p.cancelled())
}

func (p *partitionedReader) currentFailure() error {
	p.failMu.Lock()
	defer p.failMu.Unlock()
	return p.failure
}

// stopAll stops every partition's download.
func (p *partitionedReader) stopAll() {
	p.failMu.Lock()
	already := p.stopped
	p.stopped = true
	p.failMu.Unlock()
	if already {
		return
	}
	p.cancel()
	if p.first != nil {
		p.first.Close() // partition 0's body belongs to the submit's request
	}
}

func (p *partitionedReader) Schema() *arrow.Schema { return p.schema }

func (p *partitionedReader) Next() bool {
	if p.rec != nil {
		p.rec.Release()
		p.rec = nil
	}
	if p.done || p.err != nil {
		return false
	}
	if p.merged != nil {
		select {
		case rec, ok := <-p.merged:
			if ok {
				p.rec = rec
				return true
			}
			return p.finish()
		case <-p.ctx.Done():
			return p.stop(nil)
		}
	}
	select {
	case rec, ok := <-p.feeds[p.next]:
		if ok {
			p.rec = rec
			p.next = (p.next + 1) % len(p.feeds)
			return true
		}
		if p.currentFailure() != nil {
			return p.finish() // that partition failed: the result ends with the failure
		}
		// Round robin: once a partition has no next batch, none has (§6.10).
		for _, feed := range p.feeds {
			for rec := range feed {
				rec.Release()
				p.fail(wrapError(errors.New("a partition has more batches than an interleaved result allows"), "reading the result"))
			}
		}
		return p.finish()
	case <-p.ctx.Done():
		return p.stop(nil)
	}
}

// finish ends a result whose merge ran out: complete only if no partition failed.
func (p *partitionedReader) finish() bool {
	if p.currentFailure() != nil {
		// The other partitions may still be reading (a consequential cancellation waits
		// for its cause): drop what they send until they end.
		p.drainAll()
	}
	p.wg.Wait()
	if err := p.currentFailure(); err != nil {
		return p.stop(err)
	}
	p.done = true
	p.end(true)
	return false
}

// drainAll releases every batch the partitions send until each has ended.
func (p *partitionedReader) drainAll() {
	var drained sync.WaitGroup
	drain := func(feed chan arrow.RecordBatch) {
		drained.Go(func() {
			for rec := range feed {
				rec.Release()
			}
		})
	}
	if p.merged != nil {
		drain(p.merged)
	}
	for _, feed := range p.feeds {
		drain(feed)
	}
	drained.Wait()
}

// stop ends the result as failed: with err, or with the error of the
// partition that failed first (its download reports the statement's own
// error), once every download has stopped.
func (p *partitionedReader) stop(err error) bool {
	p.shutdown()
	if err == nil {
		err = p.currentFailure()
	}
	if err == nil {
		err = wrapError(context.Canceled, "reading the result")
	}
	p.err = err
	p.end(false)
	return false
}

// shutdown stops every download and waits for them.
func (p *partitionedReader) shutdown() {
	p.stopAll()
	p.drainAll()
	p.wg.Wait()
}

func (p *partitionedReader) end(finished bool) {
	if p.ended {
		return
	}
	p.ended = true
	if p.onEnd != nil {
		p.onEnd(finished)
	}
}

func (p *partitionedReader) Retain() { p.refs.Add(1) }

func (p *partitionedReader) Release() {
	if p.refs.Add(-1) == 0 {
		if p.rec != nil {
			p.rec.Release()
			p.rec = nil
		}
		if !p.ended {
			p.shutdown()
			p.end(p.done && p.err == nil)
		}
	}
}

func (p *partitionedReader) RecordBatch() arrow.RecordBatch { return p.rec }

// Record is the deprecated name of RecordBatch.
func (p *partitionedReader) Record() arrow.RecordBatch { return p.rec }

func (p *partitionedReader) Err() error { return p.err }
