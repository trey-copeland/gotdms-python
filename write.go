package bridge

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"github.com/trey-copeland/gotdms"
)

// fileWrite is one TDMS file being written by write_begin / write_chunk /
// write_end. Fixed-size channels (including bool and timestamp) share one
// Writer.Stream segment so a large column is copied to disk as each Arrow
// chunk arrives. String channels cannot be streamed; they are buffered with
// ChannelWriter.Write and flushed as a following segment.
//
// create writes a temporary file in the destination directory and renames it
// into place only after the file reopens cleanly. append requires an existing
// file whose File.Status is complete, then writes a new segment at EOF.
type fileWrite struct {
	w          *tdms.Writer
	file       *os.File // append target; create's file is owned by w
	origSize   int64    // append: file size before the write, restored on failure
	path       string
	tmp        string
	mode       string
	channels   []*writeCh
	index      map[string]int
	stream     *tdms.Stream
	streamPos  []int
	active     int
	streamDone bool
	touched    bool
	committed  bool
	received   []int
	chunks     int
	buffered   int
	existing   map[string]tdms.DataType
}

type writeCh struct {
	group  string
	name   string
	wire   string
	dt     tdms.DataType
	rows   int
	stream bool
	cw     *tdms.ChannelWriter
}

func (s *session) abortWrite() {
	if s == nil || s.wr == nil {
		return
	}
	s.wr.abort()
	s.wr = nil
}

func (s *session) dispatchWrite(op string, req map[string]json.RawMessage, payload []byte) (any, []byte, error) {
	switch op {
	case "write_begin":
		if s.wr != nil {
			return nil, nil, errCode("argument", "a TDMS write is already in progress")
		}
		if len(payload) != 0 {
			return nil, nil, errCode("argument", "write_begin does not take a payload")
		}
		fw, err := beginWrite(req)
		if fw != nil {
			s.wr = fw
		}
		if err != nil {
			return nil, nil, err
		}
		return map[string]any{"op": "result", "accepted": true}, nil, nil
	case "write_chunk":
		if s.wr == nil {
			return nil, nil, errCode("argument", "write_begin first")
		}
		if err := s.wr.writeChunk(jsonString(req, "group"), jsonString(req, "channel"), payload); err != nil {
			return nil, nil, err
		}
		return map[string]any{"op": "result", "accepted": true}, nil, nil
	case "write_end":
		if s.wr == nil {
			return nil, nil, errCode("argument", "write_begin first")
		}
		if len(payload) != 0 {
			return nil, nil, errCode("argument", "write_end does not take a payload")
		}
		report, err := s.wr.finish()
		if err != nil {
			return nil, nil, err
		}
		s.wr = nil
		return report, nil, nil
	default:
		return nil, nil, errCode("argument", "unknown op "+op)
	}
}

type propIn struct {
	Name  string          `json:"name"`
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

type chIn struct {
	Name       string          `json:"name"`
	Type       string          `json:"type"`
	Rows       int             `json:"rows"`
	Properties []propIn        `json:"properties"`
	Waveform   json.RawMessage `json:"waveform"`
}

type groupIn struct {
	Name       string   `json:"name"`
	Properties []propIn `json:"properties"`
	Channels   []chIn   `json:"channels"`
}

func beginWrite(req map[string]json.RawMessage) (*fileWrite, error) {
	path := jsonString(req, "path")
	if path == "" {
		return nil, errCode("argument", "write needs a path")
	}
	mode := jsonString(req, "mode")
	if mode == "" {
		mode = "create"
	}
	if mode != "create" && mode != "append" {
		return nil, errCode("argument", "mode must be create or append")
	}
	var props []propIn
	if err := unmarshalOpt(req, "properties", &props); err != nil {
		return nil, err
	}
	var groups []groupIn
	if err := unmarshalOpt(req, "groups", &groups); err != nil {
		return nil, err
	}
	fw := &fileWrite{
		path:  path,
		mode:  mode,
		index: map[string]int{},
	}
	if err := fw.open(); err != nil {
		fw.abort()
		return nil, err
	}
	if err := fw.define(props, groups); err != nil {
		fw.abort()
		return nil, err
	}
	return fw, nil
}

func unmarshalOpt(req map[string]json.RawMessage, key string, dst any) error {
	raw, ok := req[key]
	if !ok || string(raw) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return errCode("argument", key+": "+err.Error())
	}
	return nil
}

func (f *fileWrite) open() error {
	if f.mode == "append" {
		types, err := inspectAppend(f.path)
		if err != nil {
			return err
		}
		f.existing = types
		file, err := os.OpenFile(f.path, os.O_RDWR, 0)
		if err != nil {
			return asArg(err)
		}
		size, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			file.Close()
			return asArg(err)
		}
		w, err := tdms.NewWriter(file)
		if err != nil {
			file.Close()
			return asArg(err)
		}
		f.file = file
		f.origSize = size
		f.w = w
		return nil
	}
	tmp, err := createTemp(f.path)
	if err != nil {
		return asArg(err)
	}
	tmpName := tmp.Name()
	w, err := tdms.NewWriter(tmp)
	if err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return asArg(err)
	}
	f.file = tmp
	f.tmp = tmpName
	f.w = w
	return nil
}

// createTemp makes an empty file next to path for create mode. Like
// os.Create it asks for mode 0666 so the umask applies; os.CreateTemp would
// force 0600. When path already exists the temporary file takes its mode, so
// replacing a file does not change who can read it.
func createTemp(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	perm := os.FileMode(0o666)
	keep := false
	// Windows has only a read-only bit; copying it would leave a temporary
	// file that cannot replace the destination or be removed.
	if st, err := os.Stat(path); err == nil && st.Mode().IsRegular() && runtime.GOOS != "windows" {
		perm = st.Mode().Perm()
		keep = true
	}
	for i := 0; ; i++ {
		var b [6]byte
		if _, err := rand.Read(b[:]); err != nil {
			return nil, err
		}
		name := filepath.Join(dir, base+".tmp-"+hex.EncodeToString(b[:]))
		file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, perm)
		if err != nil {
			if os.IsExist(err) && i < 100 {
				continue
			}
			return nil, err
		}
		if keep {
			// The umask may have cleared bits the old file had.
			_ = file.Chmod(perm)
		}
		return file, nil
	}
}

func (f *fileWrite) define(props []propIn, groups []groupIn) error {
	if err := f.setProps(f.w.SetProperty, props, false); err != nil {
		return err
	}
	seenG := map[string]bool{}
	for _, g := range groups {
		if g.Name == "" {
			return errCode("argument", "group name is empty")
		}
		if seenG[g.Name] {
			return errCode("argument", fmt.Sprintf("duplicate group %q", g.Name))
		}
		seenG[g.Name] = true
		gw := f.w.Group(g.Name)
		if err := f.setProps(gw.SetProperty, g.Properties, false); err != nil {
			return err
		}
		seenC := map[string]bool{}
		for _, c := range g.Channels {
			if c.Name == "" {
				return errCode("argument", fmt.Sprintf("group %q has an empty channel name", g.Name))
			}
			if seenC[c.Name] {
				return errCode("argument", fmt.Sprintf("duplicate channel %s/%s", g.Name, c.Name))
			}
			seenC[c.Name] = true
			if c.Rows < 0 {
				return errCode("argument", fmt.Sprintf("channel %s/%s has a negative length", g.Name, c.Name))
			}
			dt, ok := dataTypeOf(c.Type)
			if !ok {
				return errCode("argument", fmt.Sprintf("channel %s/%s has unsupported type %q", g.Name, c.Name, c.Type))
			}
			key := pairKey(g.Name, c.Name)
			if prev, ok := f.existing[key]; ok && prev != dt {
				return errCode("argument", fmt.Sprintf("channel %s/%s is %s in the file and %s in this write", g.Name, c.Name, prev, dt))
			}
			cw, err := gw.Channel(c.Name, dt)
			if err != nil {
				return asArg(err)
			}
			wave, err := decodeWaveform(c.Waveform)
			if err != nil {
				return err
			}
			if wave != nil && !waveformType(dt) {
				return errCode("argument", fmt.Sprintf("channel %s/%s is %s; waveform properties are only valid on numeric channels", g.Name, c.Name, c.Type))
			}
			if err := applyWaveform(cw.SetProperty, wave); err != nil {
				return err
			}
			if err := f.setProps(cw.SetProperty, c.Properties, wave != nil); err != nil {
				return err
			}
			f.index[key] = len(f.channels)
			f.channels = append(f.channels, &writeCh{
				group:  g.Name,
				name:   c.Name,
				wire:   c.Type,
				dt:     dt,
				rows:   c.Rows,
				stream: c.Type != "string",
				cw:     cw,
			})
		}
	}
	f.received = make([]int, len(f.channels))
	for i, ch := range f.channels {
		if ch.stream && ch.rows > 0 {
			f.streamPos = append(f.streamPos, i)
		}
	}
	if len(f.streamPos) == 0 {
		f.streamDone = true
	}
	return nil
}

func (f *fileWrite) setProps(set func(string, any) error, props []propIn, waveform bool) error {
	seen := map[string]bool{}
	for _, p := range props {
		if p.Name == "" {
			return errCode("argument", "property name is empty")
		}
		if seen[p.Name] {
			return errCode("argument", fmt.Sprintf("duplicate property %q", p.Name))
		}
		seen[p.Name] = true
		if waveform && waveformName(p.Name) {
			return errCode("argument", fmt.Sprintf("property %q duplicates a waveform field", p.Name))
		}
		val, err := propValue(p)
		if err != nil {
			return err
		}
		if err := set(p.Name, val); err != nil {
			return asArg(err)
		}
	}
	return nil
}

func (f *fileWrite) writeChunk(group, name string, payload []byte) error {
	idx, ok := f.index[pairKey(group, name)]
	if !ok {
		return errCode("argument", fmt.Sprintf("unknown channel %s/%s", group, name))
	}
	ch := f.channels[idx]
	if ch.rows == 0 {
		return errCode("argument", fmt.Sprintf("channel %s/%s was declared empty", group, name))
	}
	col, release, err := readArrowColumn(payload)
	if err != nil {
		return err
	}
	defer release()
	vals, err := columnValues(ch.wire, col)
	if err != nil {
		return err
	}
	n := valueCount(vals)
	if n < 0 {
		return errCode("argument", "internal: unsupported value slice")
	}
	if n == 0 {
		return errCode("argument", fmt.Sprintf("channel %s/%s chunk is empty", group, name))
	}
	if f.received[idx]+n > ch.rows {
		return errCode("argument", fmt.Sprintf("channel %s/%s received %d values, declared %d", group, name, f.received[idx]+n, ch.rows))
	}
	if ch.stream {
		if f.streamDone {
			return errCode("argument", fmt.Sprintf("channel %s/%s is already complete", group, name))
		}
		if err := f.ensureStream(); err != nil {
			return err
		}
		want := f.streamPos[f.active]
		if want != idx {
			exp := f.channels[want]
			return errCode("argument", fmt.Sprintf("write chunks must finish %s/%s before %s/%s", exp.group, exp.name, group, name))
		}
		if err := f.stream.Write(ch.cw, vals); err != nil {
			return asArg(err)
		}
		f.chunks++
		f.received[idx] += n
		if f.received[idx] == ch.rows {
			f.active++
			if f.active == len(f.streamPos) {
				if err := f.stream.Finish(); err != nil {
					return asArg(err)
				}
				f.stream = nil
				f.streamDone = true
			}
		}
		return nil
	}
	if !f.streamDone {
		return errCode("argument", "send fixed-size channels before string channels. String channels have no streaming offset table, so they are a later segment.")
	}
	if err := ch.cw.Write(vals); err != nil {
		return asArg(err)
	}
	f.touched = true
	if f.received[idx] == 0 {
		f.buffered++
	}
	f.received[idx] += n
	return nil
}

func (f *fileWrite) ensureStream() error {
	if f.stream != nil || f.streamDone {
		return nil
	}
	chans := make([]*tdms.ChannelWriter, len(f.streamPos))
	counts := make([]int, len(f.streamPos))
	for i, idx := range f.streamPos {
		chans[i] = f.channels[idx].cw
		counts[i] = f.channels[idx].rows
	}
	st, err := f.w.Stream(chans, counts)
	if err != nil {
		return asArg(err)
	}
	f.stream = st
	f.touched = true
	return nil
}

func (f *fileWrite) finish() (map[string]any, error) {
	if !f.streamDone {
		return nil, f.shortError()
	}
	for i, ch := range f.channels {
		if f.received[i] != ch.rows {
			return nil, errCode("argument", fmt.Sprintf("channel %s/%s received %d of %d values", ch.group, ch.name, f.received[i], ch.rows))
		}
	}
	rows := 0
	for _, ch := range f.channels {
		rows += ch.rows
	}
	if err := f.w.Close(); err != nil {
		return nil, asArg(err)
	}
	f.w = nil
	if f.file != nil {
		if err := f.file.Sync(); err != nil {
			return nil, asArg(err)
		}
		if err := f.file.Close(); err != nil {
			f.file = nil
			return nil, asArg(err)
		}
		f.file = nil
	}
	check := f.path
	if f.mode == "create" {
		check = f.tmp
	}
	tf, err := tdms.Open(check)
	if err != nil {
		return nil, asArg(err)
	}
	segs := tf.SegmentCount()
	complete := tf.Status().Complete
	if err := tf.Close(); err != nil {
		return nil, asArg(err)
	}
	if !complete {
		return nil, errCode("argument", "written file does not end on a finished segment")
	}
	if f.mode == "create" {
		if err := replaceFile(f.tmp, f.path); err != nil {
			return nil, asArg(err)
		}
		f.tmp = ""
	}
	f.committed = true
	return map[string]any{
		"op":                "result",
		"path":              f.path,
		"channel_count":     len(f.channels),
		"row_count":         rows,
		"stream_chunks":     f.chunks,
		"buffered_channels": f.buffered,
		"segments":          segs,
	}, nil
}

func (f *fileWrite) shortError() error {
	if f.active < len(f.streamPos) {
		ch := f.channels[f.streamPos[f.active]]
		got := 0
		if f.active < len(f.streamPos) {
			got = f.received[f.streamPos[f.active]]
		}
		return errCode("argument", fmt.Sprintf("channel %s/%s received %d of %d values", ch.group, ch.name, got, ch.rows))
	}
	return errCode("argument", "write ended before every channel was received")
}

// abort drops an unfinished write. create removes its temporary file. append
// cuts the target back to its size before the write, so a reader never sees
// part of an append and the file still accepts the next one.
func (f *fileWrite) abort() {
	if f == nil || f.committed {
		return
	}
	// The writer is not closed: closing would finish the partial segment.
	f.w = nil
	if f.mode == "append" {
		if f.file != nil {
			_ = f.file.Truncate(f.origSize)
			_ = f.file.Close()
			f.file = nil
		} else if f.origSize > 0 {
			// finish closed the file before it failed.
			_ = os.Truncate(f.path, f.origSize)
		}
		return
	}
	if f.file != nil {
		_ = f.file.Close()
		f.file = nil
	}
	if f.tmp != "" {
		_ = os.Remove(f.tmp)
		f.tmp = ""
	}
}

// replaceFile moves tmp over path. It never deletes path first, so a failed
// move leaves the old file in place. os.Rename replaces an existing file on
// every supported OS. On Windows a scanner or indexer can hold either file
// open for a moment, so the move is retried briefly there.
func replaceFile(tmp, path string) error {
	err := os.Rename(tmp, path)
	if err == nil || runtime.GOOS != "windows" {
		return err
	}
	for i := 1; i <= 10 && err != nil; i++ {
		if _, statErr := os.Stat(tmp); statErr != nil {
			return err
		}
		time.Sleep(time.Duration(i) * 20 * time.Millisecond)
		err = os.Rename(tmp, path)
	}
	return err
}

func inspectAppend(path string) (map[string]tdms.DataType, error) {
	f, err := tdms.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errCode("argument", "append needs an existing TDMS file")
		}
		return nil, asArg(err)
	}
	defer f.Close()
	st := f.Status()
	if !st.Complete {
		return nil, errCode("argument", fmt.Sprintf("append needs a TDMS file that ends on a finished segment (status %s). An incomplete final segment is left when a write is interrupted.", st))
	}
	out := map[string]tdms.DataType{}
	for _, g := range f.Groups() {
		for _, ch := range g.Channels() {
			if ch.DataType() == tdms.TypeVoid {
				continue
			}
			out[pairKey(g.Name(), ch.Name())] = ch.DataType()
		}
	}
	return out, nil
}

func pairKey(group, name string) string { return group + "\x00" + name }

func waveformType(dt tdms.DataType) bool {
	switch dt {
	case tdms.TypeInt8, tdms.TypeInt16, tdms.TypeInt32, tdms.TypeInt64,
		tdms.TypeUint8, tdms.TypeUint16, tdms.TypeUint32, tdms.TypeUint64,
		tdms.TypeFloat32, tdms.TypeFloat64:
		return true
	default:
		return false
	}
}

func waveformName(name string) bool {
	switch name {
	case tdms.PropStartTime, tdms.PropIncrement, tdms.PropStartOffset, tdms.PropSamples:
		return true
	default:
		return false
	}
}

type waveform struct {
	start       *tdms.Timestamp
	increment   *float64
	startOffset *float64
	samples     *int64
}

func decodeWaveform(raw json.RawMessage) (*waveform, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, errCode("argument", "waveform: "+err.Error())
	}
	if len(obj) == 0 {
		return nil, errCode("argument", "waveform object is empty")
	}
	for k := range obj {
		if !waveformName(k) {
			return nil, errCode("argument", fmt.Sprintf("unknown waveform field %q", k))
		}
	}
	w := &waveform{}
	if raw, ok := obj[tdms.PropStartTime]; ok {
		if string(bytes.TrimSpace(raw)) == "null" {
			return nil, errCode("argument", "wf_start_time is null")
		}
		ts, err := parseTimestamp(raw)
		if err != nil {
			return nil, err
		}
		w.start = &ts
	}
	if raw, ok := obj[tdms.PropIncrement]; ok {
		f, err := parseJSONFloat(raw)
		if err != nil {
			return nil, err
		}
		w.increment = &f
	}
	if raw, ok := obj[tdms.PropStartOffset]; ok {
		f, err := parseJSONFloat(raw)
		if err != nil {
			return nil, err
		}
		w.startOffset = &f
	}
	if raw, ok := obj[tdms.PropSamples]; ok {
		n, err := parseJSONInt(raw, math.MinInt64, math.MaxInt64)
		if err != nil {
			return nil, err
		}
		w.samples = &n
	}
	return w, nil
}

func applyWaveform(set func(string, any) error, w *waveform) error {
	if w == nil {
		return nil
	}
	if w.start != nil {
		if err := set(tdms.PropStartTime, *w.start); err != nil {
			return asArg(err)
		}
	}
	if w.increment != nil {
		if err := set(tdms.PropIncrement, *w.increment); err != nil {
			return asArg(err)
		}
	}
	if w.startOffset != nil {
		if err := set(tdms.PropStartOffset, *w.startOffset); err != nil {
			return asArg(err)
		}
	}
	if w.samples != nil {
		var v any
		if *w.samples >= math.MinInt32 && *w.samples <= math.MaxInt32 {
			v = int32(*w.samples)
		} else {
			v = *w.samples
		}
		if err := set(tdms.PropSamples, v); err != nil {
			return asArg(err)
		}
	}
	return nil
}

func propValue(p propIn) (any, error) {
	switch p.Type {
	case "int8":
		n, err := parseJSONInt(p.Value, math.MinInt8, math.MaxInt8)
		return int8(n), err
	case "int16":
		n, err := parseJSONInt(p.Value, math.MinInt16, math.MaxInt16)
		return int16(n), err
	case "int32":
		n, err := parseJSONInt(p.Value, math.MinInt32, math.MaxInt32)
		return int32(n), err
	case "int64":
		n, err := parseJSONInt(p.Value, math.MinInt64, math.MaxInt64)
		return n, err
	case "uint8":
		n, err := parseJSONUint(p.Value, math.MaxUint8)
		return uint8(n), err
	case "uint16":
		n, err := parseJSONUint(p.Value, math.MaxUint16)
		return uint16(n), err
	case "uint32":
		n, err := parseJSONUint(p.Value, math.MaxUint32)
		return uint32(n), err
	case "uint64":
		n, err := parseJSONUint(p.Value, 0)
		return n, err
	case "float32":
		f, err := parseJSONFloat(p.Value)
		if err != nil {
			return nil, err
		}
		if f < -math.MaxFloat32 || f > math.MaxFloat32 {
			return nil, errCode("argument", fmt.Sprintf("property %q does not fit in float32", p.Name))
		}
		return float32(f), nil
	case "float64":
		f, err := parseJSONFloat(p.Value)
		return f, err
	case "bool":
		if string(bytes.TrimSpace(p.Value)) != "true" && string(bytes.TrimSpace(p.Value)) != "false" {
			return nil, errCode("argument", fmt.Sprintf("property %q type bool wants true or false", p.Name))
		}
		var v bool
		if err := json.Unmarshal(p.Value, &v); err != nil {
			return nil, errCode("argument", fmt.Sprintf("property %q: %v", p.Name, err))
		}
		return v, nil
	case "string":
		var v string
		if err := json.Unmarshal(p.Value, &v); err != nil {
			return nil, errCode("argument", fmt.Sprintf("property %q type string wants a JSON string", p.Name))
		}
		return v, nil
	case "timestamp":
		ts, err := parseTimestamp(p.Value)
		if err != nil {
			return nil, err
		}
		return ts, nil
	default:
		return nil, errCode("argument", fmt.Sprintf("property %q has unsupported type %q", p.Name, p.Type))
	}
}

func parseTimestamp(raw json.RawMessage) (tdms.Timestamp, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return tdms.Timestamp{}, errCode("argument", "timestamp is null")
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return tdms.Timestamp{}, errCode("argument", "timestamp: "+err.Error())
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return tdms.Timestamp{}, errCode("argument", fmt.Sprintf("timestamp %q: %v", s, err))
		}
		return tdms.NewTimestamp(t), nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return tdms.Timestamp{}, errCode("argument", "timestamp: "+err.Error())
	}
	if rawNs, ok := obj["unix_ns"]; ok {
		n, err := parseJSONInt(rawNs, math.MinInt64, math.MaxInt64)
		if err != nil {
			return tdms.Timestamp{}, err
		}
		return tdms.NewTimestamp(time.Unix(0, n).UTC()), nil
	}
	secRaw, sok := obj["seconds"]
	fracRaw, fok := obj["fraction"]
	if sok && fok {
		sec, err := parseJSONInt(secRaw, math.MinInt64, math.MaxInt64)
		if err != nil {
			return tdms.Timestamp{}, err
		}
		frac, err := parseJSONUint(fracRaw, 0)
		if err != nil {
			return tdms.Timestamp{}, err
		}
		return tdms.Timestamp{Seconds: sec, Fraction: frac}, nil
	}
	return tdms.Timestamp{}, errCode("argument", "timestamp object needs unix_ns or seconds and fraction")
}

func parseJSONInt(raw json.RawMessage, min, max int64) (int64, error) {
	text := string(bytes.TrimSpace(raw))
	if text == "" || text[0] == '"' || text == "null" {
		return 0, errCode("argument", "expected an integer, got "+text)
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, errCode("argument", "expected an integer, got "+text)
	}
	if n < min || n > max {
		return 0, errCode("argument", fmt.Sprintf("integer %d is outside %d..%d", n, min, max))
	}
	return n, nil
}

// parseJSONUint parses a JSON integer. max == 0 means the full uint64 range.
func parseJSONUint(raw json.RawMessage, max uint64) (uint64, error) {
	text := string(bytes.TrimSpace(raw))
	if text == "" || text[0] == '"' || text[0] == '-' || text == "null" {
		return 0, errCode("argument", "expected an unsigned integer, got "+text)
	}
	n, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, errCode("argument", "expected an unsigned integer, got "+text)
	}
	if max != 0 && n > max {
		return 0, errCode("argument", fmt.Sprintf("integer %d is outside 0..%d", n, max))
	}
	return n, nil
}

func parseJSONFloat(raw json.RawMessage) (float64, error) {
	text := bytes.TrimSpace(raw)
	if len(text) == 0 || text[0] == '"' || string(text) == "null" {
		return 0, errCode("argument", "expected a number, got "+string(text))
	}
	f, err := strconv.ParseFloat(string(text), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, errCode("argument", "expected a finite number, got "+string(text))
	}
	return f, nil
}

func asArg(err error) error {
	if err == nil {
		return nil
	}
	var c *coded
	if errors.As(err, &c) {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		return errCode("argument", err.Error())
	}
	switch {
	case errors.Is(err, tdms.ErrArgument),
		errors.Is(err, tdms.ErrType),
		errors.Is(err, tdms.ErrUnsupported),
		errors.Is(err, tdms.ErrLayout),
		errors.Is(err, tdms.ErrBadTag),
		errors.Is(err, tdms.ErrCorrupt),
		errors.Is(err, tdms.ErrVersion),
		errors.Is(err, tdms.ErrTruncated):
		return errCode("argument", err.Error())
	default:
		return err
	}
}
