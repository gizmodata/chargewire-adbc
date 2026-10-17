// Licensed under the Apache License, Version 2.0.

package protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

// Body is a request body. A body held in memory can be sent again, so a
// request carrying it can be retried after a lost response; a streamed body
// is read once, and its request is never retried.
type Body struct {
	contentType string
	data        []byte
	stream      io.Reader
}

// JSONBody encodes v as a JSON request body.
func JSONBody(v any) (*Body, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &Body{contentType: MediaTypeJSON, data: data}, nil
}

// BytesBody is a body held in memory.
func BytesBody(contentType string, data []byte) *Body {
	return &Body{contentType: contentType, data: data}
}

// StreamBody is a body read from r as it is sent (chunked).
func StreamBody(contentType string, r io.Reader) *Body {
	return &Body{contentType: contentType, stream: r}
}

// Replayable reports whether the body can be sent more than once.
func (b *Body) Replayable() bool {
	return b == nil || b.stream == nil
}

// Bytes is the body of a replayable Body.
func (b *Body) Bytes() []byte {
	return b.data
}

// open returns the body for one attempt and its length (-1 when streamed).
func (b *Body) open() (io.Reader, int64) {
	if b.stream != nil {
		return b.stream, -1
	}
	return bytes.NewReader(b.data), int64(len(b.data))
}
