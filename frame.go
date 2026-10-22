package bridge

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
)

// ProtocolVersion is the frame version this package speaks.
// A different version is rejected with code bad_version.
const ProtocolVersion uint16 = 1

// Magic is the first four bytes of every frame.
var Magic = [4]byte{'G', 'M', 'D', 'S'}

// MaxHeader is the largest accepted JSON header. Sample bytes are not in
// the header; they live in the payload.
const MaxHeader = 32 << 20

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
// A data payload is not a single Arrow stream when several channels are
// returned. It is:
//
//	uint32 LE count
//	repeated: uint32 LE byte_len, then an Arrow IPC stream of that length
//
// Each stream has one record batch and its own schema metadata. The header
// JSON lists channels in the same order. Empty responses omit the payload.
//
// write_chunk is the exception: its payload is one Arrow IPC stream for a
// single channel, not the counted envelope. write_begin and write_end carry
// no payload. read_values uses the counted envelope.
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
	if _, err := w.Write(prefix[:]); err != nil {
		return err
	}
	if len(header) > 0 {
		if _, err := w.Write(header); err != nil {
			return err
		}
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// SplitPayload returns each Arrow IPC stream in a data payload.
func SplitPayload(payload []byte) ([][]byte, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	if len(payload) < 4 {
		return nil, errors.New("bridge: short payload")
	}
	n := binary.LittleEndian.Uint32(payload[:4])
	off := 4
	out := make([][]byte, 0, n)
	for i := uint32(0); i < n; i++ {
		if off+4 > len(payload) {
			return nil, errors.New("bridge: short payload length")
		}
		ln := binary.LittleEndian.Uint32(payload[off : off+4])
		off += 4
		if off+int(ln) > len(payload) {
			return nil, errors.New("bridge: short payload body")
		}
		out = append(out, payload[off:off+int(ln)])
		off += int(ln)
	}
	if off != len(payload) {
		return nil, errors.New("bridge: payload trailing bytes")
	}
	return out, nil
}

func joinPayload(parts [][]byte) []byte {
	n := 4
	for _, p := range parts {
		n += 4 + len(p)
	}
	buf := make([]byte, 4, n)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(parts)))
	for _, p := range parts {
		var ln [4]byte
		binary.LittleEndian.PutUint32(ln[:], uint32(len(p)))
		buf = append(buf, ln[:]...)
		buf = append(buf, p...)
	}
	return buf
}

// checkPayloadLen reports whether n fits the uint32 payload length field.
func checkPayloadLen(n uint64) error {
	if n > math.MaxUint32 {
		return fmt.Errorf("bridge: payload length %d exceeds %d; read fewer channels at a time", n, uint64(math.MaxUint32))
	}
	return nil
}
