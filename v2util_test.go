package bridge

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

// v2Reply is a data reply read off the wire and checked against its own
// result_begin: every stream is filled to its declared size, parts never
// exceed MaxPart, and result_end agrees.
type v2Reply struct {
	begin   json.RawMessage
	end     map[string]any
	streams [][]byte
	parts   int
	maxPart int
}

type beginHeader struct {
	Channels []struct {
		Clock  *int `json:"clock"`
		Stream *int `json:"stream"`
	} `json:"channels"`
	Clocks  []clockOut  `json:"clocks"`
	Streams []streamOut `json:"streams"`
}

// readData reads the frames after a result_begin header. It returns an
// error for any framing fault, and the result_end header (which may carry
// an error) otherwise.
func readData(r io.Reader, begin json.RawMessage) (*v2Reply, error) {
	var h beginHeader
	if err := json.Unmarshal(begin, &h); err != nil {
		return nil, err
	}
	out := &v2Reply{begin: begin, streams: make([][]byte, len(h.Streams))}
	for i, s := range h.Streams {
		if s.ID != i {
			return nil, fmt.Errorf("stream %d has id %d", i, s.ID)
		}
		out.streams[i] = make([]byte, 0, s.Bytes)
	}
	for {
		f, err := ReadFrame(r)
		if err != nil {
			return nil, err
		}
		var ph struct {
			Op     string `json:"op"`
			Stream int    `json:"stream"`
		}
		if err := json.Unmarshal(f.Header, &ph); err != nil {
			return nil, err
		}
		switch ph.Op {
		case "part":
			if f.Version != ProtocolVersion {
				return nil, fmt.Errorf("part version %d", f.Version)
			}
			if ph.Stream < 0 || ph.Stream >= len(out.streams) {
				return nil, fmt.Errorf("part for stream %d", ph.Stream)
			}
			if len(f.Payload) == 0 || len(f.Payload) > MaxPart {
				return nil, fmt.Errorf("part of %d bytes", len(f.Payload))
			}
			st := out.streams[ph.Stream]
			if int64(len(st)+len(f.Payload)) > h.Streams[ph.Stream].Bytes {
				return nil, fmt.Errorf("stream %d overflows %d bytes", ph.Stream, h.Streams[ph.Stream].Bytes)
			}
			out.streams[ph.Stream] = append(st, f.Payload...)
			out.parts++
			if len(f.Payload) > out.maxPart {
				out.maxPart = len(f.Payload)
			}
		case "result_end":
			if len(f.Payload) != 0 {
				return nil, errors.New("result_end with payload")
			}
			if err := json.Unmarshal(f.Header, &out.end); err != nil {
				return nil, err
			}
			if out.end["error"] != nil {
				return out, nil
			}
			for i, s := range out.streams {
				if int64(len(s)) != h.Streams[i].Bytes {
					return nil, fmt.Errorf("stream %d has %d of %d bytes", i, len(s), h.Streams[i].Bytes)
				}
			}
			return out, nil
		default:
			return nil, fmt.Errorf("unexpected %s inside a data reply", ph.Op)
		}
	}
}

// legacy returns, per channel in header order, its own stream and its clock
// nanoseconds (nil when it has no clock).
func (v *v2Reply) legacy() ([][]byte, [][]int64, error) {
	var h beginHeader
	if err := json.Unmarshal(v.begin, &h); err != nil {
		return nil, nil, err
	}
	if len(h.Channels) == 0 {
		// read_values lists streams in group order, not channels.
		return v.streams, nil, nil
	}
	clocks := map[int][]int64{}
	for _, c := range h.Clocks {
		cols, ns, err := decodeIPC(v.streams[c.Stream])
		if err != nil {
			return nil, nil, err
		}
		if len(cols) != 1 || ns == nil || len(ns) != c.Samples {
			return nil, nil, fmt.Errorf("clock %d: %d columns, %d samples", c.ID, len(cols), len(ns))
		}
		clocks[c.ID] = ns
	}
	parts := make([][]byte, len(h.Channels))
	ns := make([][]int64, len(h.Channels))
	for i, ch := range h.Channels {
		if ch.Stream == nil {
			return nil, nil, fmt.Errorf("channel %d has no stream", i)
		}
		parts[i] = v.streams[*ch.Stream]
		if ch.Clock != nil {
			c, ok := clocks[*ch.Clock]
			if !ok {
				return nil, nil, fmt.Errorf("channel %d names unknown clock %d", i, *ch.Clock)
			}
			ns[i] = c
		}
	}
	return parts, ns, nil
}

// joinPayload and SplitPayload are the old counted envelope, kept in tests
// so per-channel assertions read the same as before.
func joinPayload(parts [][]byte) []byte {
	var buf bytes.Buffer
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(parts)))
	buf.Write(n[:])
	for _, p := range parts {
		binary.LittleEndian.PutUint32(n[:], uint32(len(p)))
		buf.Write(n[:])
		buf.Write(p)
	}
	return buf.Bytes()
}

func SplitPayload(payload []byte) ([][]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	if len(payload) < 4 {
		return nil, errors.New("short payload")
	}
	n := binary.LittleEndian.Uint32(payload[:4])
	off := 4
	out := make([][]byte, 0, n)
	for i := uint32(0); i < n; i++ {
		if off+4 > len(payload) {
			return nil, errors.New("short payload length")
		}
		ln := int(binary.LittleEndian.Uint32(payload[off : off+4]))
		off += 4
		if off+ln > len(payload) {
			return nil, errors.New("short payload body")
		}
		out = append(out, payload[off:off+ln])
		off += ln
	}
	return out, nil
}

// respPayload sends a dataResp through writeData and returns its streams
// joined as the old envelope.
func respPayload(d any) (map[string]any, []byte, error) {
	resp, ok := d.(*dataResp)
	if !ok {
		return nil, nil, fmt.Errorf("not a data reply: %T", d)
	}
	var buf bytes.Buffer
	if err := writeData(&buf, MaxPart, resp); err != nil {
		return nil, nil, err
	}
	f, err := ReadFrame(&buf)
	if err != nil {
		return nil, nil, err
	}
	v, err := readData(&buf, f.Header)
	if err != nil {
		return nil, nil, err
	}
	if v.end["error"] != nil {
		return nil, nil, fmt.Errorf("result_end error %v", v.end["error"])
	}
	return resp.header, joinPayload(v.streams), nil
}

func decodeIPC(blob []byte) (map[string][]float64, []int64, error) {
	r, err := ipc.NewReader(bytes.NewReader(blob))
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()
	rec, err := r.Read()
	if err != nil {
		return nil, nil, err
	}
	defer rec.Release()
	out := map[string][]float64{}
	var ns []int64
	for i := 0; i < int(rec.NumCols()); i++ {
		name := rec.ColumnName(i)
		switch col := rec.Column(i).(type) {
		case *array.Float64:
			out[name] = append([]float64(nil), col.Float64Values()...)
		case *array.Int64:
			ns = append([]int64{}, col.Int64Values()...)
			out[name] = nil
		default:
			return nil, nil, fmt.Errorf("bridge: unexpected column %s %T", name, rec.Column(i))
		}
	}
	return out, ns, nil
}
