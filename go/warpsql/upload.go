// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"bytes"
	"errors"
	"io"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gizmodata/warp-sql-adbc/go/internal/protocol"
)

// uploadReplayLimit is the largest Arrow upload (parameters or ingest rows)
// kept in memory. A body held in memory can be sent again with the same
// Idempotency-Key when its answer is lost (§6.7); a larger one streams as it
// is encoded and is sent once.
const uploadReplayLimit = 16 << 20

// errUploadEnded stops an encoder whose request is over.
var errUploadEnded = errors.New("the upload request ended")

// upload is an Arrow IPC request body being encoded from a record reader.
type upload struct {
	body    *protocol.Body
	encoded chan error  // the encoder's result
	abort   func(error) // stops a streaming encoder; nil when the body is in memory
	release func()      // releases the rows once encoded, when set
}

// spillWriter keeps what is written in memory up to a limit, then hands the
// rest to a pipe that the request reads from.
type spillWriter struct {
	limit   int
	buf     bytes.Buffer
	pipe    *io.PipeWriter
	spilled chan *io.PipeReader
}

func (w *spillWriter) Write(p []byte) (int, error) {
	if w.pipe == nil {
		if w.buf.Len()+len(p) <= w.limit {
			return w.buf.Write(p)
		}
		reader, writer := io.Pipe()
		w.pipe = writer
		w.spilled <- reader
	}
	return w.pipe.Write(p)
}

// encodeUpload encodes rows as an Arrow IPC stream, with zstd buffer
// compression when asked (§8.2). It returns once the body is complete in
// memory, or once it outgrows the limit and must stream.
func encodeUpload(rows array.RecordReader, alloc memory.Allocator, zstd bool) (*upload, error) {
	w := &spillWriter{limit: uploadReplayLimit, spilled: make(chan *io.PipeReader, 1)}
	encoded := make(chan error, 1)
	go func() {
		err := writeIPC(w, rows, alloc, zstd)
		if w.pipe != nil {
			w.pipe.CloseWithError(err) // nil: the request reads EOF after the end-of-stream marker
		}
		encoded <- err
	}()
	select {
	case err := <-encoded:
		if err != nil {
			return nil, err
		}
		done := make(chan error, 1)
		done <- nil
		return &upload{body: protocol.BytesBody(protocol.MediaTypeArrowStream, w.buf.Bytes()), encoded: done}, nil
	case pipe := <-w.spilled:
		body := io.MultiReader(bytes.NewReader(w.buf.Bytes()), pipe)
		return &upload{
			body:    protocol.StreamBody(protocol.MediaTypeArrowStream, body),
			encoded: encoded,
			abort:   func(err error) { pipe.CloseWithError(err) },
		}, nil
	}
}

func writeIPC(w io.Writer, rows array.RecordReader, alloc memory.Allocator, zstd bool) error {
	options := []ipc.Option{ipc.WithSchema(rows.Schema()), ipc.WithAllocator(alloc)}
	if zstd {
		options = append(options, ipc.WithZstd())
	}
	writer := ipc.NewWriter(w, options...)
	for rows.Next() {
		if err := writer.Write(rows.RecordBatch()); err != nil {
			writer.Close()
			return err
		}
	}
	if err := rows.Err(); err != nil {
		writer.Close()
		return err
	}
	return writer.Close()
}

// finish waits for the encoder after the request that carried the body is
// over, and returns the error that explains the outcome: a failure reading
// the rows beats the request error it caused.
func (u *upload) finish(requestErr error) error {
	if u.abort != nil {
		cause := requestErr
		if cause == nil {
			cause = errUploadEnded
		}
		u.abort(cause)
	}
	encodeErr := <-u.encoded
	if u.release != nil {
		u.release()
	}
	if encodeErr != nil && !errors.Is(encodeErr, errUploadEnded) && !errors.Is(encodeErr, requestErr) {
		return encodeErr
	}
	return requestErr
}
