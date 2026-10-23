package bridge

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

// ProtocolVersion is the frame version this package speaks. A frame with
// another version is answered with code bad_version. Version 1 is gone: the
// client and the bridge ship together.
const ProtocolVersion uint16 = 2

// Magic is the first four bytes of every frame.
var Magic = [4]byte{'G', 'M', 'D', 'S'}

// MaxHeader is the largest accepted JSON header. Sample bytes are not in
// the header; they live in the payload.
const MaxHeader = 32 << 20

// MaxPart is the largest payload the bridge puts in one part frame. A data
// response of any size is a sequence of frames no bigger than this.
const MaxPart = 64 << 20

// Frame is one length-prefixed message.
//
//	0  magic[4] "GMDS"
//	4  version uint16 LE
//	6  flags   uint16 LE  (unused, write 0)
//	8  header_len uint32 LE
//	12 payload_len uint32 LE
//	16 header JSON
//	16+header_len payload
//
// Metadata replies and errors are one frame with no payload. A data reply
// (read, read_many, read_values) is a sequence:
//
//	result_begin  header: channels, clocks, and streams with exact byte
//	              sizes; no payload
//	part          header {"op":"part","stream":k}; payload is the next
//	              bytes of Arrow IPC stream k, at most MaxPart
//	...           streams are sent in order, each in one or more parts
//	result_end    header {"op":"result_end","streams":n,"bytes":total},
//	              or {"op":"result_end","error":{"code","message","stream"}}
//	              when the reply failed after result_begin; stream is the
//	              stream that was being sent
//
// No stream gets more bytes than result_begin declared for it, even when it
// fails. After a result_end with an error the session is still usable; the
// client drops the partial streams. A clock stream has one int64 column time_ns. A
// channel stream has y, plus relative_s when asked for, and names its clock
// by id in the result_begin channel list.
//
// write_chunk carries one Arrow IPC stream for one channel as its payload.
// Requests are always single frames.
type Frame struct {
	Version uint16
	Flags   uint16
	Header  json.RawMessage
	Payload []byte
}

// ReadFrame reads one frame. io.EOF means the peer closed before a frame.
// A short read after the magic is an error.
func ReadFrame(r io.Reader) (Frame, error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return Frame{}, err
	}
	if magic != Magic {
		return Frame{}, fmt.Errorf("bridge: bad magic %q", magic)
	}
	var hdr [12]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Frame{}, err
	}
	f := Frame{
		Version: binary.LittleEndian.Uint16(hdr[0:2]),
		Flags:   binary.LittleEndian.Uint16(hdr[2:4]),
	}
	n := binary.LittleEndian.Uint32(hdr[4:8])
	p := binary.LittleEndian.Uint32(hdr[8:12])
	if n > MaxHeader {
		return Frame{}, fmt.Errorf("bridge: header length %d exceeds %d", n, MaxHeader)
	}
	if n > 0 {
		f.Header = make([]byte, n)
		if _, err := io.ReadFull(r, f.Header); err != nil {
			return Frame{}, err
		}
	}
	if p > 0 {
		f.Payload = make([]byte, p)
		if _, err := io.ReadFull(r, f.Payload); err != nil {
			return Frame{}, err
		}
	}
	return f, nil
}

// WriteFrame writes one frame. header is JSON text. payload may be nil.
func WriteFrame(w io.Writer, version uint16, header []byte, payload []byte) error {
	if len(header) > MaxHeader {
		return fmt.Errorf("bridge: header length %d exceeds %d", len(header), MaxHeader)
	}
	var prefix [16]byte
	copy(prefix[0:4], Magic[:])
	binary.LittleEndian.PutUint16(prefix[4:6], version)
	binary.LittleEndian.PutUint16(prefix[6:8], 0)
	if err := checkPayloadLen(uint64(len(payload))); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(prefix[8:12], uint32(len(header)))
	binary.LittleEndian.PutUint32(prefix[12:16], uint32(len(payload)))
	// Prefix and header go out in one write; the payload is written from
	// the caller's slice without a copy.
	head := make([]byte, 0, len(prefix)+len(header))
	head = append(head, prefix[:]...)
	head = append(head, header...)
	if _, err := w.Write(head); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// checkPayloadLen reports whether n fits the uint32 payload length field.
func checkPayloadLen(n uint64) error {
	if n > math.MaxUint32 {
		return fmt.Errorf("bridge: payload length %d exceeds %d", n, uint64(math.MaxUint32))
	}
	return nil
}
