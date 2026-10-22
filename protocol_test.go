package bridge

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trey-copeland/gotdms"
	"github.com/trey-copeland/gotdms/dataset"
)

func TestProtocolRoundTrip(t *testing.T) {
	dir := t.TempDir()
	set, err := WriteDemoSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	epoch := set.Epoch
	c := start(t)
	defer c.close()

	bad := c.callVersion(t, 99, map[string]any{"op": "open"})
	if bad.Op != "error" || bad.Code != "bad_version" {
		t.Fatalf("bad version: %+v", bad)
	}

	cat := c.call(t, map[string]any{
		"op":                 "open",
		"paths":              set.Files,
		"timestamp_channel":  "Time",
		"timestamp_channels": map[string]string{"raw": ""},
		"numeric_epoch":      epoch.Format(time.RFC3339Nano),
	})
	if cat.Op != "result" {
		t.Fatalf("open: %+v", cat)
	}
	if len(cat.Payload) != 0 {
		t.Fatalf("catalog payload %d bytes, metadata must not carry samples", len(cat.Payload))
	}
	var ai1 channelJSON
	var found bool
	for _, ch := range cat.Channels {
		if ch.Group == "acq" && ch.Name == "ai1" && ch.File == set.Files[0] {
			ai1 = ch
			found = true
		}
	}
	if !found || !ai1.HasScale || ai1.Scale == nil || ai1.Scale.Slope != 2 || ai1.Scale.Intercept != 0.5 {
		t.Fatalf("ai1 scale: %+v", ai1)
	}
	if ai1.Unit != "V" || ai1.DataType == "" {
		t.Fatalf("ai1 meta: %+v", ai1)
	}

	again := c.call(t, map[string]any{"op": "catalog"})
	if again.Op != "result" || len(again.Payload) != 0 {
		t.Fatalf("catalog reread decoded or failed: payload %d op %s", len(again.Payload), again.Op)
	}

	foundAI := c.call(t, map[string]any{"op": "find", "name": "ai0"})
	groups := map[string]int{}
	for _, ch := range foundAI.Channels {
		groups[ch.Group]++
	}
	if groups["acq"] != 2 || groups["other"] != 1 {
		t.Fatalf("find ai0: %+v", foundAI.Channels)
	}
	searched := c.call(t, map[string]any{"op": "search", "text": "ai1"})
	if len(searched.Channels) == 0 {
		t.Fatal("search empty")
	}

	amb := c.call(t, map[string]any{"op": "read", "name": "ai0"})
	if amb.Code != "ambiguous" || len(amb.Candidates) < 2 {
		t.Fatalf("ambiguous: %+v", amb)
	}

	start := epoch.Add(2 * time.Second)
	end := epoch.Add(5 * time.Second)
	win := c.call(t, map[string]any{
		"op":    "read",
		"name":  "ai0",
		"group": "acq",
		"start": start.Format(time.RFC3339Nano),
		"end":   end.Format(time.RFC3339Nano),
	})
	if win.Op != "result" {
		t.Fatalf("window: %+v", win)
	}
	y, ns, rel := decodeOne(t, win)
	if !floatEq(y, []float64{12, 13, 14}) {
		t.Fatalf("window y %v", y)
	}
	wantNS := []int64{start.UnixNano(), epoch.Add(3 * time.Second).UnixNano(), epoch.Add(4 * time.Second).UnixNano()}
	if !intEq(ns, wantNS) {
		t.Fatalf("window times %v", ns)
	}
	// Default basis is the earliest timed sample, which is the epoch.
	if !floatEq(rel, []float64{2, 3, 4}) {
		t.Fatalf("window relative %v", rel)
	}

	basis := epoch.Add(time.Second)
	relRead := c.call(t, map[string]any{
		"op":    "read",
		"name":  "ai0",
		"group": "acq",
		"start": start.Format(time.RFC3339Nano),
		"end":   end.Format(time.RFC3339Nano),
		"basis": basis.Format(time.RFC3339Nano),
	})
	_, _, rel = decodeOne(t, relRead)
	if !floatEq(rel, []float64{1, 2, 3}) {
		t.Fatalf("basis relative %v", rel)
	}

	many := c.call(t, map[string]any{
		"op": "read_many",
		"channels": []map[string]string{
			{"name": "ai0", "group": "acq"},
			{"name": "ai1", "group": "acq"},
		},
		"start": start.Format(time.RFC3339Nano),
		"end":   end.Format(time.RFC3339Nano),
	})
	parts, err := SplitPayload(many.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || len(many.Read) != 2 {
		t.Fatalf("read_many parts %d headers %d", len(parts), len(many.Read))
	}
	cols0, ns0, err := decodeIPC(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	cols1, ns1, err := decodeIPC(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	if !intEq(ns0, ns1) {
		t.Fatalf("shared clock diverged %v %v", ns0, ns1)
	}
	if !floatEq(cols0["y"], []float64{12, 13, 14}) || !floatEq(cols1["y"], []float64{3, 4, 5}) {
		t.Fatalf("raw many %v %v", cols0["y"], cols1["y"])
	}
	if many.Read[1].Scaled {
		t.Fatal("ai1 scaled without being asked")
	}

	scaled := c.call(t, map[string]any{
		"op":     "read",
		"name":   "ai1",
		"group":  "acq",
		"start":  start.Format(time.RFC3339Nano),
		"end":    end.Format(time.RFC3339Nano),
		"scaled": true,
	})
	sy, _, _ := decodeOne(t, scaled)
	if !floatEq(sy, []float64{6.5, 8.5, 10.5}) || !scaled.Read[0].Scaled {
		t.Fatalf("scaled %v %+v", sy, scaled.Read[0])
	}

	noScale := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq", "scaled": true})
	if noScale.Code != "no_scale" {
		t.Fatalf("no_scale: %+v", noScale)
	}
	nonlin := c.call(t, map[string]any{"op": "read", "name": "poly", "group": "other", "scaled": true})
	if nonlin.Code != "nonlinear_scale" {
		t.Fatalf("nonlinear: %+v", nonlin)
	}
	untimed := c.call(t, map[string]any{
		"op":    "read",
		"name":  "x",
		"group": "raw",
		"start": start.Format(time.RFC3339Nano),
		"end":   end.Format(time.RFC3339Nano),
	})
	if untimed.Code != "untimed" {
		t.Fatalf("untimed: %+v", untimed)
	}

	rawFull := c.call(t, map[string]any{"op": "read", "name": "x", "group": "raw", "absolute": true, "relative": true})
	if rawFull.Op != "result" || !rawFull.Read[0].AbsoluteOmitted {
		t.Fatalf("untimed full: %+v", rawFull.Read)
	}
	uy, uns, urel := decodeOne(t, rawFull)
	if !floatEq(uy, []float64{9, 8, 7}) || uns != nil || !floatEq(urel, []float64{0, 1, 2}) {
		t.Fatalf("untimed values y %v ns %v rel %v", uy, uns, urel)
	}

	done := c.call(t, map[string]any{"op": "close"})
	if done.Op != "result" {
		t.Fatalf("close: %+v", done)
	}
}

func TestConvertBundle(t *testing.T) {
	dir := t.TempDir()
	set, err := WriteDemoSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "bundle")
	start := set.Epoch.Add(2 * time.Second)
	end := set.Epoch.Add(5 * time.Second)
	abs := true
	rel := true
	err = Convert(ConvertOptions{
		Paths:             set.Files,
		TimestampChannel:  "Time",
		TimestampChannels: map[string]string{"raw": ""},
		NumericEpoch:      &set.Epoch,
		Channels:          []dataset.Selector{{Group: "acq", Name: "ai0"}},
		WindowStart:       &start,
		WindowEnd:         &end,
		Absolute:          &abs,
		Relative:          &rel,
		OutDir:            out,
	})
	if err != nil {
		t.Fatal(err)
	}
	man, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Format   string `json:"format"`
		Basis    string `json:"basis"`
		Channels []struct {
			Path     string `json:"path"`
			Name     string `json:"name"`
			Group    string `json:"group"`
			DataType string `json:"data_type"`
			Basis    string `json:"basis"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(man, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Format != "arrow-ipc-stream" || len(doc.Channels) != 1 || doc.Channels[0].Group != "acq" {
		t.Fatalf("manifest %+v", doc)
	}
	if doc.Basis == "" || doc.Channels[0].Basis == "" || doc.Channels[0].DataType == "" {
		t.Fatalf("manifest missing basis or dtype %+v", doc)
	}
	blob, err := os.ReadFile(filepath.Join(out, filepath.FromSlash(doc.Channels[0].Path)))
	if err != nil {
		t.Fatal(err)
	}
	cols, _, err := decodeIPC(blob)
	if err != nil {
		t.Fatal(err)
	}
	if !floatEq(cols["y"], []float64{12, 13, 14}) {
		t.Fatalf("bundle y %v", cols["y"])
	}
	if err := Convert(ConvertOptions{Paths: set.Files, TimestampChannel: "Time", NumericEpoch: &set.Epoch, OutDir: out}); err == nil {
		t.Fatal("second convert overwrote the bundle")
	}
}

func TestScaleMismatch(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.tdms")
	b := filepath.Join(dir, "b.tdms")
	if err := writeMismatch(a, 0, 2); err != nil {
		t.Fatal(err)
	}
	if err := writeMismatch(b, 2, 3); err != nil {
		t.Fatal(err)
	}
	epoch := DemoEpoch
	c := start(t)
	defer c.close()
	open := c.call(t, map[string]any{
		"op":                "open",
		"paths":             []string{a, b},
		"timestamp_channel": "Time",
		"numeric_epoch":     epoch.Format(time.RFC3339Nano),
	})
	if open.Op != "result" {
		t.Fatal(open.Message)
	}
	got := c.call(t, map[string]any{"op": "read", "name": "ai1", "group": "acq", "scaled": true})
	if got.Code != "scale_mismatch" {
		t.Fatalf("mismatch: %+v", got)
	}
}

type client struct {
	in  *io.PipeWriter
	out *io.PipeReader
	err chan error
}

func start(t *testing.T) *client {
	t.Helper()
	pr, pw := io.Pipe()
	or, ow := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		errc <- Serve(pr, ow)
		ow.Close()
	}()
	return &client{in: pw, out: or, err: errc}
}

func (c *client) close() {
	c.in.Close()
	io.Copy(io.Discard, c.out)
}

type reply struct {
	Op          string
	Code        string
	Message     string
	Channels    []channelJSON
	Read        []channelOut
	Candidates  []channelJSON
	Reads       []spanRead
	Limited     bool
	SampleReads int
	Payload     []byte
	Closed      bool
	Header      json.RawMessage
}

func (c *client) call(t *testing.T, header map[string]any) reply {
	t.Helper()
	return c.callVersion(t, ProtocolVersion, header)
}

func (c *client) callVersion(t *testing.T, version uint16, header map[string]any) reply {
	t.Helper()
	b, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(c.in, version, b, nil); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(c.out)
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Op          string          `json:"op"`
		Code        string          `json:"code"`
		Message     string          `json:"message"`
		Channels    json.RawMessage `json:"channels"`
		Candidates  []channelJSON   `json:"candidates"`
		Reads       []spanRead      `json:"reads"`
		Limited     bool            `json:"limited"`
		SampleReads int             `json:"sample_reads"`
		Closed      bool            `json:"closed"`
	}
	if err := json.Unmarshal(f.Header, &raw); err != nil {
		t.Fatal(err)
	}
	r := reply{
		Op: raw.Op, Code: raw.Code, Message: raw.Message,
		Candidates: raw.Candidates, Payload: f.Payload, Closed: raw.Closed,
		Reads: raw.Reads, Limited: raw.Limited, SampleReads: raw.SampleReads,
		Header: f.Header,
	}
	if len(raw.Channels) > 0 && string(raw.Channels) != "null" {
		if len(f.Payload) > 0 {
			if err := json.Unmarshal(raw.Channels, &r.Read); err != nil {
				t.Fatal(err)
			}
		} else if err := json.Unmarshal(raw.Channels, &r.Channels); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func decodeOne(t *testing.T, r reply) (y []float64, ns []int64, rel []float64) {
	t.Helper()
	if r.Op != "result" {
		t.Fatalf("decode error %+v", r)
	}
	parts, err := SplitPayload(r.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("parts %d", len(parts))
	}
	cols, ns, err := decodeIPC(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	return cols["y"], ns, cols["relative_s"]
}

func floatEq(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func intEq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func writeMismatch(path string, t0, slope float64) error {
	w, err := tdms.Create(path)
	if err != nil {
		return err
	}
	g := w.Group("acq")
	timeCh, err := g.Channel("Time", tdms.TypeFloat64)
	if err != nil {
		w.Close()
		return err
	}
	if err := timeCh.Write([]float64{t0, t0 + 1}); err != nil {
		w.Close()
		return err
	}
	ch, err := g.Channel("ai1", tdms.TypeFloat64)
	if err != nil {
		w.Close()
		return err
	}
	if err := ch.SetProperty("NI_Scale[1]_Scale_Type", "Linear"); err != nil {
		w.Close()
		return err
	}
	if err := ch.SetProperty("NI_Scale[1]_Linear_Slope", slope); err != nil {
		w.Close()
		return err
	}
	if err := ch.SetProperty("NI_Scale[1]_Linear_Y_Intercept", 0.0); err != nil {
		w.Close()
		return err
	}
	if err := ch.Write([]float64{1, 2}); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}
