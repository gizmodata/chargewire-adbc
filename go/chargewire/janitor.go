// Licensed under the Apache License, Version 2.0.

package chargewire

import (
	"context"
	"log/slog"
	"time"
)

// janitorTimeout bounds one cleanup request.
const janitorTimeout = 10 * time.Second

// janitor sends best-effort cleanup requests (free a finished statement,
// stop an abandoned one, drop a prepared statement) off the caller's path, so
// that releasing a result or closing a statement never waits on the network.
// Tasks run one at a time, each with its own timeout. Whatever is still
// queued when the connection closes is dropped: ending the session frees it
// all on the server.
type janitor struct {
	tasks  chan janitorTask
	ctx    context.Context
	stop   context.CancelFunc
	done   chan struct{}
	logger *slog.Logger
}

type janitorTask struct {
	name string
	run  func(context.Context) error
}

func newJanitor(logger *slog.Logger) *janitor {
	ctx, stop := context.WithCancel(context.Background())
	j := &janitor{tasks: make(chan janitorTask, 256), ctx: ctx, stop: stop, done: make(chan struct{}), logger: logger}
	go j.loop()
	return j
}

func (j *janitor) loop() {
	defer close(j.done)
	for {
		select {
		case <-j.ctx.Done():
			return
		case task := <-j.tasks:
			ctx, cancel := context.WithTimeout(j.ctx, janitorTimeout)
			if err := task.run(ctx); err != nil && j.ctx.Err() == nil {
				j.logger.Debug("cleanup request failed", "task", task.name, "error", err)
			}
			cancel()
		}
	}
}

// submit queues a task. When the queue is full the task is dropped (and its
// resource freed when the session ends).
func (j *janitor) submit(name string, run func(context.Context) error) {
	if j.ctx.Err() != nil {
		return
	}
	select {
	case j.tasks <- janitorTask{name: name, run: run}:
	default:
		j.logger.Debug("cleanup queue full; dropping", "task", name)
	}
}

// close stops the janitor and waits for a running task to end.
func (j *janitor) close() {
	j.stop()
	<-j.done
}
