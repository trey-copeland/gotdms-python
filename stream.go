package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// dataResp is a reply sent as result_begin, part frames, and result_end.
// header becomes result_begin with a "streams" list added. Each stream is
// written by its write func, which must produce exactly size bytes.
type dataResp struct {
	header  map[string]any
	streams []outStream
}

type outStream struct {
	kind  string // "clock", "channel", or "values"
	size  int64
	write func(io.Writer) error
}

type streamOut struct {
	ID    int    `json:"id"`
	Kind  string `json:"kind"`
	Bytes int64  `json:"bytes"`
}

// streamHook, when set by a test, runs before stream id is written. An error
// is reported the way any failure after result_begin is.
var streamHook func(id int) error

// errSink marks a failure writing to the client. The session cannot
// continue after one, so Serve returns it.
type errSink struct{ err error }

func (e *errSink) Error() string { return e.err.Error() }
func (e *errSink) Unwrap() error { return e.err }

// writeData sends a data reply. A failure before result_begin is an error
// frame. A failure after it stops the remaining streams and goes in
// result_end, so the client can raise it and keep the session. Only a
// write to w that fails is returned.
func writeData(w io.Writer, partCap int, d *dataResp) error {
	if partCap <= 0 || partCap > MaxPart {
		partCap = MaxPart
	}
	h := make(map[string]any, len(d.header)+1)
	for k, v := range d.header {
		h[k] = v
	}
	h["op"] = "result_begin"
	list := make([]streamOut, len(d.streams))
	var total int64
	for i, s := range d.streams {
		list[i] = streamOut{ID: i, Kind: s.kind, Bytes: s.size}
		total += s.size
	}
	h["streams"] = list
	b, err := json.Marshal(h)
	if err != nil {
		return writeErr(w, "error", "encode result: "+err.Error(), nil)
	}
	if len(b) > MaxHeader {
		return writeErr(w, "error", fmt.Sprintf("result header is %d bytes, over %d; read fewer channels at a time", len(b), MaxHeader), nil)
	}
	if err := WriteFrame(w, ProtocolVersion, restoreNonFinite(b), nil); err != nil {
		return err
	}
	for i, s := range d.streams {
		err := sendStream(w, partCap, i, s)
		if err == nil {
			continue
		}
		var sink *errSink
		if errors.As(err, &sink) {
			return sink.err
		}
		code, msg, _ := classify(err)
		end := map[string]any{"op": "result_end", "error": map[string]any{"code": code, "message": msg, "stream": i}}
		eb, _ := json.Marshal(end)
		return WriteFrame(w, ProtocolVersion, eb, nil)
	}
	eb, _ := json.Marshal(map[string]any{"op": "result_end", "streams": len(d.streams), "bytes": total})
	return WriteFrame(w, ProtocolVersion, eb, nil)
}

func sendStream(w io.Writer, partCap, id int, s outStream) error {
	if streamHook != nil {
		if err := streamHook(id); err != nil {
			return err
		}
	}
	pw := newPartWriter(w, id, partCap)
	pw.limit = s.size
	err := s.write(pw)
	if ferr := pw.flush(); ferr != nil {
		return ferr
	}
	if pw.werr != nil {
		return &errSink{pw.werr}
	}
	if pw.over != nil {
		// Report this, not however the stream's encoder wrapped it.
		return errCode("error", fmt.Sprintf("stream %d is longer than its declared %d bytes; nothing past them was sent", id, s.size))
	}
	if err != nil {
		return err
	}
	if pw.n != s.size {
		// Short of the declared size. What was sent fits in the client's
		// buffer, so the client drops the partial reply and stays in sync.
		return errCode("error", fmt.Sprintf("stream %d is %d bytes, declared %d", id, pw.n, s.size))
	}
	return nil
}

// smallWrite is the largest write partWriter copies into its staging
// buffer. Arrow writes message headers and padding in small pieces and each
// column buffer in one large Write; large writes go out as part payloads
// straight from the caller's slice.
const smallWrite = 64 << 10

// partWriter turns one Arrow IPC stream into part frames of at most cap
// payload bytes. It never holds more than smallWrite bytes of its own.
// A write that would take the stream past limit, its declared size, is
// refused before any of it is sent: a client allocates exactly the declared
// size, and bytes past it would desynchronize the pipe.
type partWriter struct {
	w     io.Writer
	cap   int
	hdr   []byte
	buf   []byte
	limit int64 // declared stream size; negative means no limit
	n     int64 // stream bytes accepted
	sent  int64 // stream bytes on the wire
	werr  error
	over  error
}

func newPartWriter(w io.Writer, id, partCap int) *partWriter {
	hdr := []byte(`{"op":"part","stream":` + strconv.Itoa(id) + `}`)
	return &partWriter{w: w, cap: partCap, hdr: hdr, limit: -1}
}

func (p *partWriter) Write(b []byte) (int, error) {
	if p.werr != nil {
		return 0, p.werr
	}
	if p.over != nil {
		return 0, p.over
	}
	n := len(b)
	if p.limit >= 0 && p.n+int64(n) > p.limit {
		p.over = errCode("error", fmt.Sprintf("stream is longer than its declared %d bytes", p.limit))
		return 0, p.over
	}
	if len(b) < smallWrite && len(p.buf)+len(b) <= smallWrite && len(p.buf)+len(b) <= p.cap {
		p.buf = append(p.buf, b...)
		p.n += int64(n)
		return n, nil
	}
	if err := p.flush(); err != nil {
		return 0, err
	}
	for len(b) > 0 {
		k := len(b)
		if k > p.cap {
			k = p.cap
		}
		if err := p.emit(b[:k]); err != nil {
			return 0, err
		}
		b = b[k:]
	}
	p.n += int64(n)
	return n, nil
}

func (p *partWriter) flush() error {
	if p.werr != nil {
		return p.werr
	}
	if len(p.buf) == 0 {
		return nil
	}
	err := p.emit(p.buf)
	p.buf = p.buf[:0]
	return err
}

func (p *partWriter) emit(b []byte) error {
	if err := WriteFrame(p.w, ProtocolVersion, p.hdr, b); err != nil {
		p.werr = err
		return err
	}
	p.sent += int64(len(b))
	return nil
}

// countWriter counts bytes and keeps none.
type countWriter struct{ n int64 }

func (c *countWriter) Write(b []byte) (int, error) {
	c.n += int64(len(b))
	return len(b), nil
}

// writeRecord writes rec as one Arrow IPC stream with one record batch.
func writeRecord(w io.Writer, rec arrow.Record) error {
	iw := ipc.NewWriter(w, ipc.WithSchema(rec.Schema()), ipc.WithAllocator(memory.NewGoAllocator()))
	if err := iw.Write(rec); err != nil {
		iw.Close()
		return err
	}
	return iw.Close()
}

// recordStream is a stream whose record is built on demand: once to count
// its bytes for result_begin and once to send it. Columns are views of the
// series' slices, so building costs no copy unless a column is computed
// (scaled values, relative time, converted timestamps).
func recordStream(kind string, build func() (arrow.Record, error)) (outStream, error) {
	rec, err := build()
	if err != nil {
		return outStream{}, err
	}
	var c countWriter
	err = writeRecord(&c, rec)
	rec.Release()
	if err != nil {
		return outStream{}, err
	}
	return outStream{kind: kind, size: c.n, write: func(w io.Writer) error {
		rec, err := build()
		if err != nil {
			return err
		}
		defer rec.Release()
		return writeRecord(w, rec)
	}}, nil
}

// blobStream sends bytes that are already encoded.
func blobStream(kind string, blob []byte) outStream {
	return outStream{kind: kind, size: int64(len(blob)), write: func(w io.Writer) error {
		_, err := w.Write(blob)
		return err
	}}
}

// bytesOf views a slice of fixed-width values as bytes without a copy.
// Arrow IPC is little-endian on every platform this bridge ships for.
func bytesOf[T any](s []T) []byte {
	if len(s) == 0 {
		return nil
	}
	var zero T
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(s))), len(s)*int(unsafe.Sizeof(zero)))
}

// viewArray is an Arrow array over vals without copying. vals must stay
// unchanged until the array is released; the buffer keeps it reachable.
func viewArray[T any](dt arrow.DataType, vals []T) arrow.Array {
	buf := memory.NewBufferBytes(bytesOf(vals))
	data := array.NewData(dt, len(vals), []*memory.Buffer{nil, buf}, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

// oneColumn is a record with a single non-nullable column.
func oneColumn(name string, col arrow.Array, md *arrow.Metadata) arrow.Record {
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: name, Type: col.DataType(), Nullable: false}}, md)
	return array.NewRecord(schema, []arrow.Array{col}, int64(col.Len()))
}
