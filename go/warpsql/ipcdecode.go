// Licensed under the Apache License, Version 2.0.

package warpsql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// The driver reads Arrow IPC streams through framedReader rather than
// Arrow's own message reader, for two reasons:
//
//   - It knows where the stream ends: only the end-of-stream marker ends it,
//     so a stream cut between two messages is reported as incomplete (§6.6).
//   - A record batch whose buffers are compressed (§8.2) is decompressed here,
//     every buffer at once on its own goroutine, straight into one new body,
//     and handed on as an uncompressed message. Arrow's reader decompresses
//     one buffer after another through a streaming decoder it makes for every
//     batch.

// Arrow IPC framing and metadata (Message.fbs, Schema.fbs).
const (
	ipcContinuation = 0xFFFFFFFF

	headerRecordBatch = 3 // MessageHeader.RecordBatch

	codecLZ4FrameID = 0 // CompressionType.LZ4_FRAME
	codecZstdID     = 1 // CompressionType.ZSTD

	// bodyAlignment is where each decompressed buffer starts in the new body.
	bodyAlignment = 64
	// uncompressedMarker in a compressed buffer's length prefix: the buffer
	// was sent as it is.
	uncompressedMarker = -1
)

// decodeSlots bounds the buffers being decompressed at once, over every
// stream the process reads.
var decodeSlots = make(chan struct{}, max(runtime.GOMAXPROCS(0), 2))

// zstdDecoder decodes whole zstd frames; DecodeAll may run concurrently.
var zstdDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(1<<36))
})

// framedReader is an ipc.MessageReader over one Arrow IPC stream. A goroutine
// reads the stream's messages ahead (prefetchDepth), so the connection keeps
// draining while a batch is decompressed and consumed. Like Arrow's own
// reader, it keeps the message it returned last (the reader retains what it
// keeps longer) and releases it on the next call, or on Release.
type framedReader struct {
	r     io.Reader
	alloc memory.Allocator
	refs  atomic.Int64
	last  *ipc.Message

	start  sync.Once
	frames chan frame
	stop   chan struct{}
	ended  bool // the terminal frame was taken
}

// prefetchDepth is how many messages are read ahead of the consumer.
const prefetchDepth = 2

// frame is one message as read: its metadata and body, or how the stream
// ended (io.EOF after the end-of-stream marker, or an error).
type frame struct {
	meta []byte
	msg  fbMessage
	body *memory.Buffer
	err  error
}

func (fr frame) release() {
	if fr.body != nil {
		fr.body.Release()
	}
}

func newFramedReader(r io.Reader, alloc memory.Allocator) *framedReader {
	f := &framedReader{r: r, alloc: bodyAllocator(alloc), frames: make(chan frame, prefetchDepth), stop: make(chan struct{})}
	f.refs.Store(1)
	return f
}

func (f *framedReader) Retain() { f.refs.Add(1) }

func (f *framedReader) Release() {
	if f.refs.Add(-1) != 0 {
		return
	}
	if f.last != nil {
		f.last.Release()
		f.last = nil
	}
	close(f.stop)
	f.drain()
}

// drain releases the frames read ahead that nobody will take. The reading
// goroutine drains too when it stops, so a frame it sends late isn't lost.
func (f *framedReader) drain() {
	for {
		select {
		case fr := <-f.frames:
			fr.release()
		default:
			return
		}
	}
}

func (f *framedReader) prefetch() {
	for {
		select {
		case <-f.stop:
			f.drain()
			return
		default:
		}
		fr := f.readFrame()
		select {
		case f.frames <- fr:
		case <-f.stop:
			fr.release()
			f.drain()
			return
		}
		if fr.err != nil {
			return // the end, or a failure: the consumer takes it next
		}
	}
}

// Message returns the next message, io.EOF after the end-of-stream marker,
// or errIncompleteStream when the stream stops without it.
func (f *framedReader) Message() (*ipc.Message, error) {
	if f.last != nil {
		f.last.Release()
		f.last = nil
	}
	if f.ended {
		return nil, io.EOF
	}
	f.start.Do(func() { go f.prefetch() })
	fr := <-f.frames
	if fr.err != nil {
		f.ended = true
		return nil, fr.err
	}
	msg, err := f.decode(fr)
	f.last = msg
	return msg, err
}

// readFull reads len(p) bytes; a stream that ends first is incomplete.
func (f *framedReader) readFull(p []byte) error {
	if _, err := io.ReadFull(f.r, p); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return errIncompleteStream
		}
		return err
	}
	return nil
}

// readFrame reads the next message's framing, metadata and body.
func (f *framedReader) readFrame() frame {
	var prefix [4]byte
	if err := f.readFull(prefix[:]); err != nil {
		return frame{err: err}
	}
	length := binary.LittleEndian.Uint32(prefix[:])
	if length == ipcContinuation {
		if err := f.readFull(prefix[:]); err != nil {
			return frame{err: err}
		}
		length = binary.LittleEndian.Uint32(prefix[:])
	}
	if length == 0 {
		return frame{err: io.EOF} // the end-of-stream marker (or its pre-0.15 form)
	}
	if length > 1<<30 {
		return frame{err: fmt.Errorf("an Arrow message with %d bytes of metadata", length)}
	}
	meta := make([]byte, length)
	if err := f.readFull(meta); err != nil {
		return frame{err: err}
	}
	msg, err := parseMessage(meta)
	if err != nil {
		return frame{err: err}
	}
	if msg.bodyLength < 0 || msg.bodyLength > 1<<40 {
		return frame{err: fmt.Errorf("an Arrow message with a body of %d bytes", msg.bodyLength)}
	}
	body := memory.NewResizableBuffer(f.alloc)
	body.Resize(int(msg.bodyLength))
	if err := f.readFull(body.Bytes()); err != nil {
		body.Release()
		return frame{err: err}
	}
	return frame{meta: meta, msg: msg, body: body}
}

// decode turns a frame into the message Arrow's reader gets: a compressed
// record batch decompressed, anything else as it is.
func (f *framedReader) decode(fr frame) (*ipc.Message, error) {
	defer fr.body.Release()
	if fr.msg.headerType == headerRecordBatch {
		if batch, ok := fr.msg.recordBatch(); ok && batch.compressed {
			newMeta, newBody, err := decompressBatch(fr.msg, batch, fr.body, f.alloc)
			if err != nil {
				return nil, fmt.Errorf("decompressing an Arrow record batch: %w", err)
			}
			defer newBody.Release()
			return ipc.NewMessage(memory.NewBufferBytes(newMeta), newBody), nil
		}
	}
	return ipc.NewMessage(memory.NewBufferBytes(fr.meta), fr.body), nil
}

// fbMessage is the part of a Message table the reader needs.
type fbMessage struct {
	tab            flatbuffers.Table
	version        int16
	headerType     byte
	bodyLength     int64
	customMetadata flatbuffers.UOffsetT // 0: none
}

func parseMessage(meta []byte) (msg fbMessage, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unreadable Arrow message metadata: %v", r)
		}
	}()
	if len(meta) < 8 {
		return msg, errors.New("Arrow message metadata too short")
	}
	root := flatbuffers.GetUOffsetT(meta)
	msg.tab = flatbuffers.Table{Bytes: meta, Pos: root}
	if o := flatbuffers.UOffsetT(msg.tab.Offset(4)); o != 0 {
		msg.version = msg.tab.GetInt16(o + msg.tab.Pos)
	}
	if o := flatbuffers.UOffsetT(msg.tab.Offset(6)); o != 0 {
		msg.headerType = msg.tab.GetByte(o + msg.tab.Pos)
	}
	if o := flatbuffers.UOffsetT(msg.tab.Offset(10)); o != 0 {
		msg.bodyLength = msg.tab.GetInt64(o + msg.tab.Pos)
	}
	if o := flatbuffers.UOffsetT(msg.tab.Offset(12)); o != 0 {
		msg.customMetadata = o
	}
	return msg, nil
}

// fbRecordBatch is a RecordBatch table's fields.
type fbRecordBatch struct {
	length     int64
	nodes      []byte // FieldNode structs, 16 bytes each, as stored
	buffers    [][2]int64
	compressed bool
	codec      byte
	variadic   []int64
}

func (msg fbMessage) recordBatch() (rb fbRecordBatch, ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	var header flatbuffers.Table
	o := flatbuffers.UOffsetT(msg.tab.Offset(8))
	if o == 0 {
		return rb, false
	}
	msg.tab.Union(&header, o)
	if o := flatbuffers.UOffsetT(header.Offset(4)); o != 0 {
		rb.length = header.GetInt64(o + header.Pos)
	}
	if o := flatbuffers.UOffsetT(header.Offset(6)); o != 0 {
		start, n := header.Vector(o), header.VectorLen(o)
		rb.nodes = header.Bytes[start : start+flatbuffers.UOffsetT(16*n)]
	}
	if o := flatbuffers.UOffsetT(header.Offset(8)); o != 0 {
		start, n := header.Vector(o), header.VectorLen(o)
		rb.buffers = make([][2]int64, n)
		for i := range n {
			at := start + flatbuffers.UOffsetT(16*i)
			rb.buffers[i] = [2]int64{header.GetInt64(at), header.GetInt64(at + 8)}
		}
	}
	if o := flatbuffers.UOffsetT(header.Offset(10)); o != 0 {
		var compression flatbuffers.Table
		compression.Bytes = header.Bytes
		compression.Pos = header.Indirect(o + header.Pos)
		rb.compressed = true
		rb.codec = codecLZ4FrameID // the field's default
		if c := flatbuffers.UOffsetT(compression.Offset(4)); c != 0 {
			rb.codec = compression.GetByte(c + compression.Pos)
		}
		if m := flatbuffers.UOffsetT(compression.Offset(6)); m != 0 && compression.GetByte(m+compression.Pos) != 0 {
			return rb, false // not BodyCompressionMethod.BUFFER: leave it to Arrow
		}
	}
	if o := flatbuffers.UOffsetT(header.Offset(12)); o != 0 {
		start, n := header.Vector(o), header.VectorLen(o)
		rb.variadic = make([]int64, n)
		for i := range n {
			rb.variadic[i] = header.GetInt64(start + flatbuffers.UOffsetT(8*i))
		}
	}
	return rb, true
}

// decompressBatch decompresses every buffer of a record batch into one new
// body, and returns that body with metadata describing it uncompressed.
func decompressBatch(msg fbMessage, rb fbRecordBatch, body *memory.Buffer, alloc memory.Allocator) ([]byte, *memory.Buffer, error) {
	if rb.codec != codecLZ4FrameID && rb.codec != codecZstdID {
		return nil, nil, fmt.Errorf("unknown Arrow compression codec %d", rb.codec)
	}
	src := body.Bytes()
	type piece struct {
		in      []byte // compressed, or the raw bytes of a buffer sent as it is
		size    int64  // its uncompressed size
		raw     bool
		outOffs int64
	}
	pieces := make([]piece, len(rb.buffers))
	newBuffers := make([][2]int64, len(rb.buffers))
	var total int64
	for i, b := range rb.buffers {
		offset, length := b[0], b[1]
		if offset < 0 || length < 0 || offset+length > int64(len(src)) {
			return nil, nil, fmt.Errorf("buffer %d (%d bytes at %d) is outside the %d-byte body", i, length, offset, len(src))
		}
		total = (total + bodyAlignment - 1) &^ (bodyAlignment - 1)
		if length == 0 {
			newBuffers[i] = [2]int64{total, 0}
			continue
		}
		if length < 8 {
			return nil, nil, fmt.Errorf("compressed buffer %d has no length prefix", i)
		}
		in := src[offset+8 : offset+length]
		size := int64(binary.LittleEndian.Uint64(src[offset:]))
		p := piece{in: in, size: size, outOffs: total}
		if size == uncompressedMarker {
			p.raw, p.size = true, int64(len(in))
		} else if size < 0 || size > 1<<40 {
			return nil, nil, fmt.Errorf("compressed buffer %d claims %d bytes", i, size)
		}
		pieces[i] = p
		newBuffers[i] = [2]int64{total, p.size}
		total += p.size
	}
	total = (total + 7) &^ 7
	out := memory.NewResizableBuffer(alloc)
	out.Resize(int(total))
	dst := out.Bytes()
	// The allocator may reuse memory: zero what no buffer covers (alignment gaps,
	// the end), so no stale bytes reach the application.
	covered := int64(0)
	for _, b := range newBuffers {
		clear(dst[covered:b[0]])
		covered = max(covered, b[0]+b[1])
	}
	clear(dst[covered:])

	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for i := range pieces {
		p := pieces[i]
		if p.in == nil && !p.raw {
			continue
		}
		into := dst[p.outOffs : p.outOffs+p.size : p.outOffs+p.size]
		if p.raw || p.size < 64<<10 {
			// Small buffers aren't worth a goroutine.
			if err := decodeBuffer(rb.codec, p.raw, p.in, into); err != nil {
				out.Release()
				return nil, nil, fmt.Errorf("buffer %d: %w", i, err)
			}
			continue
		}
		decodeSlots <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-decodeSlots; wg.Done() }()
			if err := decodeBuffer(rb.codec, false, p.in, into); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("buffer %d: %w", i, err)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		out.Release()
		return nil, nil, firstErr
	}
	return uncompressedMeta(msg, rb, newBuffers, total), out, nil
}

// decodeBuffer fills dst, exactly, from one buffer's bytes.
func decodeBuffer(codec byte, raw bool, in, dst []byte) error {
	if raw {
		copy(dst, in)
		return nil
	}
	switch codec {
	case codecLZ4FrameID:
		return decodeLZ4Frames(in, dst)
	default:
		dec, err := zstdDecoder()
		if err != nil {
			return err
		}
		// A full slice expression: the decoder must not use the capacity past
		// dst, which is the next buffer's.
		got, err := dec.DecodeAll(in, dst[:0:len(dst)])
		if err != nil {
			return err
		}
		if len(got) != len(dst) || (len(dst) > 0 && &got[0] != &dst[0]) {
			return fmt.Errorf("zstd: %d bytes decoded, want %d", len(got), len(dst))
		}
		return nil
	}
}

// LZ4 frame format (lz4 frame_format.md).
const (
	lz4FrameMagic     = 0x184D2204
	lz4SkippableMagic = 0x184D2A50 // through 0x184D2A5F
	lz4History        = 64 << 10   // how far back a linked block may refer
)

// decodeLZ4Frames decodes one or more LZ4 frames from src into dst, which
// must be filled exactly. Linked blocks refer to the output before them,
// which is the preceding part of dst.
func decodeLZ4Frames(src, dst []byte) error {
	pos := 0
	for len(src) > 0 {
		if len(src) < 4 {
			return errors.New("lz4: truncated frame")
		}
		magic := binary.LittleEndian.Uint32(src)
		if magic&0xFFFFFFF0 == lz4SkippableMagic {
			if len(src) < 8 {
				return errors.New("lz4: truncated skippable frame")
			}
			skip := int(binary.LittleEndian.Uint32(src[4:]))
			if 8+skip > len(src) {
				return errors.New("lz4: truncated skippable frame")
			}
			src = src[8+skip:]
			continue
		}
		if magic != lz4FrameMagic {
			return fmt.Errorf("lz4: not an LZ4 frame (magic %#x)", magic)
		}
		if len(src) < 7 {
			return errors.New("lz4: truncated frame header")
		}
		flg := src[4]
		if flg>>6 != 1 {
			return fmt.Errorf("lz4: frame version %d", flg>>6)
		}
		independent := flg&0x20 != 0
		blockChecksum := flg&0x10 != 0
		contentSize := flg&0x08 != 0
		contentChecksum := flg&0x04 != 0
		dictID := flg&0x01 != 0
		header := 7 // magic, FLG, BD, HC
		if contentSize {
			header += 8
		}
		if dictID {
			return errors.New("lz4: frames with a dictionary are not supported")
		}
		if len(src) < header {
			return errors.New("lz4: truncated frame header")
		}
		src = src[header:]
		frameStart := pos
		for {
			if len(src) < 4 {
				return errors.New("lz4: truncated block")
			}
			word := binary.LittleEndian.Uint32(src)
			src = src[4:]
			if word == 0 {
				break // end mark
			}
			size := int(word & 0x7FFFFFFF)
			if size > len(src) {
				return errors.New("lz4: truncated block")
			}
			block := src[:size]
			src = src[size:]
			if blockChecksum {
				if len(src) < 4 {
					return errors.New("lz4: truncated block checksum")
				}
				src = src[4:]
			}
			if word&0x80000000 != 0 {
				if pos+size > len(dst) {
					return errors.New("lz4: more data than the buffer's size")
				}
				copy(dst[pos:], block)
				pos += size
				continue
			}
			var dict []byte
			if !independent && pos > frameStart {
				dict = dst[max(frameStart, pos-lz4History):pos]
			}
			n, err := lz4.UncompressBlockWithDict(block, dst[pos:], dict)
			if err != nil {
				return fmt.Errorf("lz4: %w", err)
			}
			pos += n
		}
		if contentChecksum {
			if len(src) < 4 {
				return errors.New("lz4: truncated content checksum")
			}
			src = src[4:]
		}
	}
	if pos != len(dst) {
		return fmt.Errorf("lz4: %d bytes decoded, want %d", pos, len(dst))
	}
	return nil
}

// uncompressedMeta is the Message metadata for the decompressed batch: the
// same nodes and variadic counts, the new buffer layout, no compression.
func uncompressedMeta(msg fbMessage, rb fbRecordBatch, buffers [][2]int64, bodyLength int64) []byte {
	b := flatbuffers.NewBuilder(256 + len(rb.nodes) + 16*len(buffers))

	var custom flatbuffers.UOffsetT
	if msg.customMetadata != 0 {
		custom = copyKeyValues(b, msg.tab, msg.customMetadata)
	}

	nodeCount := len(rb.nodes) / 16
	b.StartVector(16, nodeCount, 8)
	for i := nodeCount - 1; i >= 0; i-- {
		b.Prep(8, 16)
		b.PrependInt64(int64(binary.LittleEndian.Uint64(rb.nodes[16*i+8:])))
		b.PrependInt64(int64(binary.LittleEndian.Uint64(rb.nodes[16*i:])))
	}
	nodes := b.EndVector(nodeCount)

	b.StartVector(16, len(buffers), 8)
	for i := len(buffers) - 1; i >= 0; i-- {
		b.Prep(8, 16)
		b.PrependInt64(buffers[i][1])
		b.PrependInt64(buffers[i][0])
	}
	bufs := b.EndVector(len(buffers))

	var variadic flatbuffers.UOffsetT
	if rb.variadic != nil {
		b.StartVector(8, len(rb.variadic), 8)
		for i := len(rb.variadic) - 1; i >= 0; i-- {
			b.PrependInt64(rb.variadic[i])
		}
		variadic = b.EndVector(len(rb.variadic))
	}

	b.StartObject(5) // RecordBatch
	b.PrependInt64Slot(0, rb.length, 0)
	b.PrependUOffsetTSlot(1, nodes, 0)
	b.PrependUOffsetTSlot(2, bufs, 0)
	if variadic != 0 {
		b.PrependUOffsetTSlot(4, variadic, 0)
	}
	header := b.EndObject()

	b.StartObject(5) // Message
	b.PrependInt64Slot(3, bodyLength, 0)
	if custom != 0 {
		b.PrependUOffsetTSlot(4, custom, 0)
	}
	b.PrependUOffsetTSlot(2, header, 0)
	b.PrependInt16Slot(0, msg.version, 0)
	b.PrependByteSlot(1, headerRecordBatch, 0)
	b.Finish(b.EndObject())
	return b.FinishedBytes()
}

// copyKeyValues copies a [KeyValue] vector (custom_metadata) into b.
func copyKeyValues(b *flatbuffers.Builder, tab flatbuffers.Table, field flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	start, n := tab.Vector(field), tab.VectorLen(field)
	offsets := make([]flatbuffers.UOffsetT, n)
	for i := range n {
		var kv flatbuffers.Table
		kv.Bytes = tab.Bytes
		kv.Pos = tab.Indirect(start + flatbuffers.UOffsetT(4*i))
		var key, value flatbuffers.UOffsetT
		if o := flatbuffers.UOffsetT(kv.Offset(4)); o != 0 {
			key = b.CreateByteString(kv.ByteVector(o + kv.Pos))
		}
		if o := flatbuffers.UOffsetT(kv.Offset(6)); o != 0 {
			value = b.CreateByteString(kv.ByteVector(o + kv.Pos))
		}
		b.StartObject(2)
		if value != 0 {
			b.PrependUOffsetTSlot(1, value, 0)
		}
		if key != 0 {
			b.PrependUOffsetTSlot(0, key, 0)
		}
		offsets[i] = b.EndObject()
	}
	b.StartVector(4, n, 4)
	for i := n - 1; i >= 0; i-- {
		b.PrependUOffsetT(offsets[i])
	}
	return b.EndVector(n)
}
