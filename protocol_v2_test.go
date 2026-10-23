package bridge

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/trey-copeland/gotdms"
)

var v2Epoch = time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

// writeV2Set writes two files. Group "acq" has a numeric Time channel and
// ai0, ai1 on it. Group "wave" has waveform channels: w0 and w1 on the same
// axis (0.25 s, so not the acq clock), slow at 2 s. Group "raw" has an untimed channel. The
// second file continues acq and wave after a gap.
func writeV2Set(t *testing.T, dir string, n int) []string {
	t.Helper()
	var paths []string
	for f := 0; f < 2; f++ {
		path := filepath.Join(dir, "run"+strconv.Itoa(f)+".tdms")
		w, err := tdms.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		t0 := float64(f * (n + 100))
		sec := make([]float64, n)
		a, b := make([]float64, n), make([]float64, n)
		for i := range sec {
			sec[i] = t0 + float64(i)*0.5
			a[i], b[i] = float64(f*n+i), -float64(f*n+i)
		}
		put := func(g, name string, y []float64, wave bool, start time.Time, inc float64) {
			ch, err := w.Group(g).Channel(name, tdms.TypeFloat64)
			if err != nil {
				t.Fatal(err)
			}
			if wave {
				if err := ch.SetProperty(tdms.PropStartTime, start); err != nil {
					t.Fatal(err)
				}
				if err := ch.SetProperty(tdms.PropIncrement, inc); err != nil {
					t.Fatal(err)
				}
			}
			if err := ch.Write(y); err != nil {
				t.Fatal(err)
			}
		}
		put("acq", "Time", sec, false, time.Time{}, 0)
		put("acq", "ai0", a, false, time.Time{}, 0)
		put("acq", "ai1", b, false, time.Time{}, 0)
		ws := v2Epoch.Add(time.Duration(t0 * float64(time.Second)))
		put("wave", "w0", a, true, ws, 0.25)
		put("wave", "w1", b, true, ws, 0.25)
		put("wave", "slow", a, true, ws, 2)
		if f == 0 {
			put("raw", "u", a, false, time.Time{}, 0)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}

func openV2(t *testing.T, c *client, paths []string) {
	t.Helper()
	got := c.call(t, map[string]any{
		"op": "open", "paths": paths, "timestamp_channel": "Time",
		"timestamp_channels": map[string]string{"wave": "", "raw": ""},
		"numeric_epoch":      v2Epoch.Format(time.RFC3339Nano),
	})
	if got.Op != "result" {
		t.Fatalf("open %+v", got)
	}
}

func startParts(t *testing.T, partCap int) *client {
	t.Helper()
	pr, pw := io.Pipe()
	or, ow := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		errc <- ServeParts(pr, ow, partCap)
		ow.Close()
	}()
	return &client{in: pw, out: or, err: errc}
}

var v2Sels = []map[string]string{
	{"name": "ai0", "group": "acq"}, {"name": "ai1", "group": "acq"},
	{"name": "w0", "group": "wave"}, {"name": "w1", "group": "wave"},
	{"name": "slow", "group": "wave"}, {"name": "u", "group": "raw"},
}

func clockIDs(t *testing.T, r reply) []*int {
	t.Helper()
	var h beginHeader
	if err := json.Unmarshal(r.Header, &h); err != nil {
		t.Fatal(err)
	}
	out := make([]*int, len(h.Channels))
	for i, ch := range h.Channels {
		out[i] = ch.Clock
	}
	return out
}

func TestV2SharedClockMapping(t *testing.T) {
	paths := writeV2Set(t, t.TempDir(), 40)
	c := start(t)
	defer c.close()
	openV2(t, c, paths)
	r := c.call(t, map[string]any{"op": "read_many", "channels": v2Sels, "relative": false})
	if r.Op != "result" {
		t.Fatalf("read_many %+v", r)
	}
	var h beginHeader
	if err := json.Unmarshal(r.Header, &h); err != nil {
		t.Fatal(err)
	}
	ids := clockIDs(t, r)
	// ai0 and ai1 read one Time channel per file and are merged per series,
	// so they get different dataset clocks; the bridge proves them equal.
	// w0 and w1 have the same waveform axis. slow is its own axis. u is
	// untimed.
	if ids[0] == nil || ids[1] == nil || *ids[0] != *ids[1] {
		t.Fatalf("acq clocks %v %v", ids[0], ids[1])
	}
	if ids[2] == nil || ids[3] == nil || *ids[2] != *ids[3] || *ids[2] == *ids[0] {
		t.Fatalf("wave clocks %v %v", ids[2], ids[3])
	}
	if ids[4] == nil || *ids[4] == *ids[2] || ids[5] != nil {
		t.Fatalf("slow/untimed clocks %v %v", ids[4], ids[5])
	}
	if len(h.Clocks) != 3 || len(h.Streams) != 3+len(v2Sels) {
		t.Fatalf("clocks %+v streams %d", h.Clocks, len(h.Streams))
	}
	for i, s := range h.Streams {
		want := "channel"
		if i < 3 {
			want = "clock"
		}
		if s.Kind != want {
			t.Fatalf("stream %d kind %s", i, s.Kind)
		}
	}
	if !r.Read[5].AbsoluteOmitted || r.Read[5].Clock != nil {
		t.Fatalf("untimed meta %+v", r.Read[5])
	}
	// Each channel's clock must equal what that channel reads on its own.
	parts, err := SplitPayload(r.Payload)
	if err != nil {
		t.Fatal(err)
	}
	for i, sel := range v2Sels {
		one := c.call(t, map[string]any{"op": "read", "name": sel["name"], "group": sel["group"], "relative": false})
		if !intEq(one.Clocks[0], r.Clocks[i]) {
			t.Fatalf("%s clock differs from a single read", sel["name"])
		}
		y1, _, _ := decodeOne(t, one)
		cols, _, err := decodeIPC(parts[i])
		if err != nil {
			t.Fatal(err)
		}
		if !floatEq(cols["y"], y1) || len(cols) != 1 {
			t.Fatalf("%s y or columns differ: %v", sel["name"], cols)
		}
	}
	// No absolute time: no clocks, no clock ids, relative_s in the channel.
	rel := c.call(t, map[string]any{"op": "read_many", "channels": v2Sels, "absolute": false, "relative": true})
	if err := json.Unmarshal(rel.Header, &h); err != nil {
		t.Fatal(err)
	}
	if len(h.Clocks) != 0 || len(h.Streams) != len(v2Sels) {
		t.Fatalf("relative read clocks %+v", h.Clocks)
	}
	for i, id := range clockIDs(t, rel) {
		if id != nil {
			t.Fatalf("channel %d has clock %d without absolute time", i, *id)
		}
	}
	rparts, _ := SplitPayload(rel.Payload)
	cols, _, err := decodeIPC(rparts[0])
	if err != nil || len(cols["relative_s"]) != 80 || cols["relative_s"][1] != 0.5 {
		t.Fatalf("relative_s %v %v", cols["relative_s"], err)
	}
}

func TestV2PartsSplitAtCap(t *testing.T) {
	paths := writeV2Set(t, t.TempDir(), 3000)
	whole := start(t)
	defer whole.close()
	openV2(t, whole, paths)
	want := whole.call(t, map[string]any{"op": "read_many", "channels": v2Sels})
	for _, cap := range []int{1, 7, 1000, 4096, 1 << 20} {
		c := startParts(t, cap)
		openV2(t, c, paths)
		got := c.call(t, map[string]any{"op": "read_many", "channels": v2Sels})
		if got.Op != "result" || got.Data.maxPart > cap {
			t.Fatalf("cap %d: op %s max part %d", cap, got.Op, got.Data.maxPart)
		}
		if !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("cap %d: streams differ from the unsplit reply", cap)
		}
		if cap <= 1000 && got.Data.parts < 48000/cap {
			t.Fatalf("cap %d: only %d parts", cap, got.Data.parts)
		}
		// The session is usable after a split reply.
		if again := c.call(t, map[string]any{"op": "catalog"}); again.Op != "result" {
			t.Fatalf("cap %d: catalog after read %+v", cap, again)
		}
		c.close()
	}
}

func TestPartWriterFrames(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, cap := range []int{1, 3, 100, smallWrite, 3 * smallWrite} {
		var wire bytes.Buffer
		pw := newPartWriter(&wire, 7, cap)
		var want []byte
		for i := 0; i < 200; i++ {
			n := rng.Intn(10)
			if i%17 == 0 {
				n = rng.Intn(4 * smallWrite)
			}
			b := make([]byte, n)
			rng.Read(b)
			if _, err := pw.Write(b); err != nil {
				t.Fatal(err)
			}
			if len(pw.buf) > smallWrite || len(pw.buf) > cap {
				t.Fatalf("cap %d: staged %d bytes", cap, len(pw.buf))
			}
			want = append(want, b...)
		}
		if err := pw.flush(); err != nil {
			t.Fatal(err)
		}
		var got []byte
		for wire.Len() > 0 {
			f, err := ReadFrame(&wire)
			if err != nil {
				t.Fatal(err)
			}
			if string(f.Header) != `{"op":"part","stream":7}` || len(f.Payload) == 0 || len(f.Payload) > cap {
				t.Fatalf("cap %d: frame %s with %d bytes", cap, f.Header, len(f.Payload))
			}
			got = append(got, f.Payload...)
		}
		if !bytes.Equal(got, want) || pw.n != int64(len(want)) || pw.sent != pw.n {
			t.Fatalf("cap %d: %d bytes back of %d", cap, len(got), len(want))
		}
	}
}

func TestV2MidStreamError(t *testing.T) {
	paths := writeV2Set(t, t.TempDir(), 50)
	c := startParts(t, 64)
	defer c.close()
	openV2(t, c, paths)
	streamHook = func(id int) error {
		if id == 2 {
			return errCode("corrupt", "injected failure in stream 2")
		}
		return nil
	}
	got := c.call(t, map[string]any{"op": "read_many", "channels": v2Sels})
	streamHook = nil
	if got.Op != "error" || got.Code != "corrupt" || got.Message != "injected failure in stream 2" {
		t.Fatalf("mid-stream error %+v", got)
	}
	// Streams 0 and 1 were sent in full and stream 2 not at all.
	if len(got.Data.streams[0]) == 0 || len(got.Data.streams[2]) != 0 {
		t.Fatalf("partial streams %d %d", len(got.Data.streams[0]), len(got.Data.streams[2]))
	}
	ok := c.call(t, map[string]any{"op": "read_many", "channels": v2Sels})
	if ok.Op != "result" {
		t.Fatalf("read after mid-stream error %+v", ok)
	}
}

func TestV2DeclaredSizeIsEnforced(t *testing.T) {
	var buf bytes.Buffer
	d := &dataResp{header: map[string]any{}, streams: []outStream{
		blobStream("values", []byte("abcd")),
		{kind: "values", size: 5, write: func(w io.Writer) error { _, err := w.Write([]byte("xyz")); return err }},
	}}
	if err := writeData(&buf, 2, d); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	v, err := readData(&buf, f.Header)
	if err != nil {
		t.Fatal(err)
	}
	e, _ := v.end["error"].(map[string]any)
	if e == nil || e["code"] != "error" || e["stream"] != float64(1) {
		t.Fatalf("result_end %v", v.end)
	}
}

// A stream longer than declared is cut at the declared size: nothing past
// it reaches the wire, so the client's buffer never overflows, the reply
// ends with an error, and the next request is answered in sync.
func TestV2OversizeStreamIsNotSent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		writes [][]byte
	}{
		{"small staged write", [][]byte{[]byte("abc"), []byte("de"), []byte("f")}},
		{"large write", [][]byte{[]byte("ab"), bytes.Repeat([]byte{'x'}, 2*smallWrite)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writes := tc.writes
			d := &dataResp{header: map[string]any{}, streams: []outStream{
				{kind: "values", size: 5, write: func(w io.Writer) error {
					for _, b := range writes {
						if _, err := w.Write(b); err != nil {
							return err
						}
					}
					return nil
				}},
				blobStream("values", []byte("never")),
			}}
			if err := writeData(&buf, 3, d); err != nil {
				t.Fatal(err)
			}
			buf.WriteString("TRAILER")
			f, err := ReadFrame(&buf)
			if err != nil {
				t.Fatal(err)
			}
			v, err := readData(&buf, f.Header)
			if err != nil {
				t.Fatalf("framing: %v", err)
			}
			e, _ := v.end["error"].(map[string]any)
			if e == nil || e["stream"] != float64(0) || !strings.Contains(e["message"].(string), "longer than its declared 5 bytes") {
				t.Fatalf("result_end %v", v.end)
			}
			if len(v.streams[0]) > 5 || len(v.streams[1]) != 0 {
				t.Fatalf("sent %d and %d bytes", len(v.streams[0]), len(v.streams[1]))
			}
			if buf.String() != "TRAILER" {
				t.Fatalf("reply left %q", buf.String())
			}
		})
	}
}

type failAfter struct {
	n   int
	err error
}

func (f *failAfter) Write(b []byte) (int, error) {
	if f.n <= 0 {
		return 0, f.err
	}
	f.n--
	return len(b), nil
}

func TestV2SinkErrorStopsSession(t *testing.T) {
	boom := errors.New("pipe closed")
	d := &dataResp{header: map[string]any{}, streams: []outStream{blobStream("values", make([]byte, 100))}}
	if err := writeData(&failAfter{n: 2, err: boom}, 10, d); !errors.Is(err, boom) {
		t.Fatalf("err %v", err)
	}
}

func TestV2EmptyAndValuesReplies(t *testing.T) {
	paths := writeV2Set(t, t.TempDir(), 5)
	c := start(t)
	defer c.close()
	got := c.call(t, map[string]any{"op": "read_values", "path": paths[0]})
	if got.Op != "result" {
		t.Fatalf("read_values %+v", got)
	}
	parts, err := SplitPayload(got.Payload)
	if err != nil || len(parts) != 7 {
		t.Fatalf("read_values parts %d %v", len(parts), err)
	}
	h := headerMap(t, got.Header)
	ch := h["groups"].([]any)[0].(map[string]any)["channels"].([]any)
	if ch[2].(map[string]any)["stream"] != float64(2) {
		t.Fatalf("stream index %v", ch[2])
	}
}

// TestV2OverFourGiB streams one channel whose Arrow stream alone is over
// 4 GiB through Serve, checking the framing without keeping the bytes. The
// file stores int16 (1.1 GB on disk); the dataset read returns float64, so
// y is 4.48 GB. The channel is untimed: a timed read would also hold a
// 4.48 GB clock in the dataset layer even for a values-only request. Peak
// memory is about 6 GB, so it runs only when GOTDMS_BRIDGE_BIG is set.
func TestV2OverFourGiB(t *testing.T) {
	if os.Getenv("GOTDMS_BRIDGE_BIG") == "" {
		t.Skip("set GOTDMS_BRIDGE_BIG=1 to run (about 6 GB RAM, 1.2 GB disk)")
	}
	dir := os.Getenv("GOTDMS_BRIDGE_BIG_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	const n = 560_000_000 // 4.48 GB of float64 y
	path := filepath.Join(dir, "big-int16-untimed.tdms")
	if _, err := os.Stat(path); err != nil {
		w, err := tdms.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		ch, err := w.Group("g").Channel("y", tdms.TypeInt16)
		if err != nil {
			t.Fatal(err)
		}
		chunk := make([]int16, 1<<22)
		for off := 0; off < n; off += len(chunk) {
			k := min(len(chunk), n-off)
			for i := 0; i < k; i++ {
				chunk[i] = int16(off + i)
			}
			if err := ch.Write(chunk[:k]); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
	c := start(t)
	defer c.close()
	got := c.call(t, map[string]any{"op": "open", "paths": []string{path}})
	if got.Op != "result" {
		t.Fatalf("open %+v", got)
	}
	b, _ := json.Marshal(map[string]any{"op": "read", "name": "y", "group": "g", "absolute": false, "relative": false})
	if err := WriteFrame(c.in, ProtocolVersion, b, nil); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(c.out)
	if err != nil {
		t.Fatal(err)
	}
	var h beginHeader
	if err := json.Unmarshal(f.Header, &h); err != nil || len(h.Streams) != 1 {
		t.Fatalf("begin %s %v", f.Header, err)
	}
	size := h.Streams[0].Bytes
	if size <= math.MaxUint32 {
		t.Fatalf("stream is %d bytes, not over 4 GiB", size)
	}
	var seen int64
	parts := 0
	checked := 0
	yStart := int64(-1) // stream offset of y: everything before the first full part
	for {
		f, err := ReadFrame(c.out)
		if err != nil {
			t.Fatal(err)
		}
		var fh struct {
			Op    string         `json:"op"`
			Error map[string]any `json:"error"`
		}
		if err := json.Unmarshal(f.Header, &fh); err != nil {
			t.Fatal(err)
		}
		if fh.Op == "result_end" {
			if fh.Error != nil {
				t.Fatalf("end %s", f.Header)
			}
			break
		}
		if fh.Op != "part" {
			t.Fatalf("frame %s", f.Header)
		}
		if len(f.Payload) > MaxPart {
			t.Fatalf("part of %d bytes", len(f.Payload))
		}
		// Full-size parts are y written straight from the series; the
		// first value of each is int16(sample index) as float64.
		if len(f.Payload) == MaxPart {
			if yStart < 0 {
				yStart = seen
			}
			i := int((seen - yStart) / 8)
			v := math.Float64frombits(binary.LittleEndian.Uint64(f.Payload[:8]))
			if v != float64(int16(i)) {
				t.Fatalf("part %d starts with %v, want %v", parts, v, float64(int16(i)))
			}
			checked++
		}
		seen += int64(len(f.Payload))
		parts++
	}
	if seen != size || checked < 60 {
		t.Fatalf("got %d of %d bytes, %d parts checked", seen, size, checked)
	}
	t.Logf("one stream of %d bytes (%.2f GiB) in %d parts; %s", size, float64(size)/(1<<30), parts, peakRSS())
}

// peakRSS is the process high-water mark where /proc reports it.
func peakRSS() string {
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return "peak RSS unknown"
	}
	for _, line := range bytes.Split(b, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("VmHWM:")) {
			return "peak RSS " + string(bytes.TrimSpace(line[6:]))
		}
	}
	return "peak RSS unknown"
}
