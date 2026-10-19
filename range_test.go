package bridge

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDescribeDoesNotReadSamples(t *testing.T) {
	dir := t.TempDir()
	set, err := WriteDemoSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := start(t)
	defer c.close()
	got := c.call(t, map[string]any{
		"op":                 "describe",
		"paths":              set.Files,
		"timestamp_channel":  "Time",
		"timestamp_channels": map[string]string{"raw": ""},
		"numeric_epoch":      set.Epoch.Format(time.RFC3339Nano),
	})
	if got.Op != "result" {
		t.Fatalf("describe: %+v", got)
	}
	if got.SampleReads != 0 {
		t.Fatalf("describe sample_reads %d", got.SampleReads)
	}
	if len(got.Payload) != 0 {
		t.Fatalf("describe payload %d", len(got.Payload))
	}
	var ai0, raw, timed bool
	for _, ch := range got.Channels {
		if ch.Samples == 0 {
			t.Fatalf("missing length %+v", ch)
		}
		if ch.Group == "acq" && ch.Name == "ai0" {
			ai0 = true
			if !ch.Timed || ch.IsTimestamp || ch.Source != "timestamp" || ch.Timestamp != "Time" {
				t.Fatalf("ai0 clock: %+v", ch)
			}
		}
		if ch.Group == "acq" && ch.Name == "Time" {
			timed = true
			if !ch.IsTimestamp {
				t.Fatalf("Time not declared: %+v", ch)
			}
		}
		if ch.Group == "raw" && ch.Name == "x" {
			raw = true
			if ch.Timed || ch.IsTimestamp {
				t.Fatalf("raw opted in: %+v", ch)
			}
		}
	}
	if !ai0 || !raw || !timed {
		t.Fatalf("catalog incomplete ai0 %v raw %v time %v", ai0, raw, timed)
	}

	// The same files still have sample bytes. A later read must be able to
	// decode them, which is how this test knows describe skipped real data
	// rather than an empty channel.
	opened := c.call(t, map[string]any{
		"op":                 "open",
		"paths":              set.Files,
		"timestamp_channel":  "Time",
		"timestamp_channels": map[string]string{"raw": ""},
		"numeric_epoch":      set.Epoch.Format(time.RFC3339Nano),
	})
	if opened.Op != "result" {
		t.Fatalf("open after describe: %+v", opened)
	}
	full := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq"})
	y, _, _ := decodeOne(t, full)
	if !floatEq(y, []float64{10, 11, 12, 13, 14, 15}) {
		t.Fatalf("full y %v", y)
	}
}

func TestDescribeRequiresEpoch(t *testing.T) {
	dir := t.TempDir()
	set, err := WriteDemoSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := start(t)
	defer c.close()
	got := c.call(t, map[string]any{
		"op":                "describe",
		"paths":             set.Files,
		"timestamp_channel": "Time",
	})
	if got.Code != "argument" || !strings.Contains(got.Message, "numeric_epoch") {
		t.Fatalf("epoch: %+v", got)
	}
}

func TestRangeReadMatchesFull(t *testing.T) {
	dir := t.TempDir()
	set, err := WriteDemoSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	c := start(t)
	defer c.close()
	openDemo(t, c, set)

	full := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq"})
	y, ns, rel := decodeOne(t, full)
	if full.Limited {
		t.Fatal("full read marked limited")
	}
	if !namesOf(full.Reads, "ai0") || namesOf(full.Reads, "ai1") {
		t.Fatalf("full reads %+v", full.Reads)
	}

	lim := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq", "limit": 2})
	ly, lns, lrel := decodeOne(t, lim)
	if !lim.Limited || !floatEq(ly, y[:2]) || !intEq(lns, ns[:2]) || !floatEq(lrel, rel[:2]) {
		t.Fatalf("limit 2 y %v ns %v rel %v limited %v", ly, lns, lrel, lim.Limited)
	}
	if sumLen(lim.Reads, "ai0") != 2 {
		t.Fatalf("ai0 decoded %d, want 2: %+v", sumLen(lim.Reads, "ai0"), lim.Reads)
	}
	if namesOf(lim.Reads, "ai1") || namesOf(lim.Reads, "volt") || fileHas(lim.Reads, "b.tdms", "ai0") {
		t.Fatalf("limit touched an unselected or later channel: %+v", lim.Reads)
	}
	for _, r := range lim.Reads {
		if r.Name == "ai0" && r.Length >= r.ChannelSamples {
			t.Fatalf("prefix decoded the whole channel %+v", r)
		}
	}

	span := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq", "limit": 5})
	sy, _, _ := decodeOne(t, span)
	if !floatEq(sy, y[:5]) || sumLen(span.Reads, "ai0") != 5 || !fileHas(span.Reads, "b.tdms", "ai0") {
		t.Fatalf("span y %v reads %+v", sy, span.Reads)
	}

	both := c.call(t, map[string]any{
		"op": "read_many",
		"channels": []map[string]string{
			{"name": "ai0", "group": "acq"},
			{"name": "ai1", "group": "acq"},
		},
		"limit": 2,
	})
	parts, err := SplitPayload(both.Payload)
	if err != nil {
		t.Fatal(err)
	}
	cols0, _, err := decodeIPC(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	cols1, _, err := decodeIPC(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	if !floatEq(cols0["y"], []float64{10, 11}) || !floatEq(cols1["y"], []float64{1, 2}) {
		t.Fatalf("many %v %v", cols0["y"], cols1["y"])
	}
	if sumLen(both.Reads, "Time") != 2 || sumLen(both.Reads, "ai0") != 2 || sumLen(both.Reads, "ai1") != 2 {
		t.Fatalf("shared clock reads %+v", both.Reads)
	}

	basis := set.Epoch.Add(time.Second)
	relLim := c.call(t, map[string]any{
		"op": "read", "name": "ai0", "group": "acq", "limit": 3,
		"basis": basis.Format(time.RFC3339Nano),
	})
	_, _, rrel := decodeOne(t, relLim)
	if !floatEq(rrel, []float64{-1, 0, 1}) {
		t.Fatalf("basis was rezeroed: %v", rrel)
	}

	off := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq", "offset": 2, "limit": 2})
	oy, _, _ := decodeOne(t, off)
	if !floatEq(oy, []float64{12, 13}) {
		t.Fatalf("offset y %v", oy)
	}
	for _, r := range off.Reads {
		if r.Role == "data" && r.Name == "ai0" && (r.Offset != 2 || r.Length != 2) {
			t.Fatalf("data slice %+v", r)
		}
	}

	scaled := c.call(t, map[string]any{"op": "read", "name": "ai1", "group": "acq", "limit": 2, "scaled": true})
	scy, _, _ := decodeOne(t, scaled)
	if !floatEq(scy, []float64{2.5, 4.5}) || !scaled.Read[0].Scaled {
		t.Fatalf("scaled prefix %v %+v", scy, scaled.Read[0])
	}
}

func TestRangeGapWaveformUntimed(t *testing.T) {
	dir := t.TempDir()
	gap, err := WriteGapSet(dir)
	if err != nil {
		t.Fatal(err)
	}
	waveDir := filepath.Join(dir, "wave")
	if err := os.MkdirAll(waveDir, 0o755); err != nil {
		t.Fatal(err)
	}
	wave, err := WriteWaveformSet(waveDir)
	if err != nil {
		t.Fatal(err)
	}
	c := start(t)
	defer c.close()

	openSet := func(files []string, ts string) {
		t.Helper()
		header := map[string]any{
			"op":            "open",
			"paths":         files,
			"numeric_epoch": gap.Epoch.Format(time.RFC3339Nano),
		}
		if ts != "" {
			header["timestamp_channel"] = ts
		}
		got := c.call(t, header)
		if got.Op != "result" {
			t.Fatalf("open: %+v", got)
		}
	}
	openSet(gap.Files, "Time")
	full := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq"})
	y, ns, _ := decodeOne(t, full)
	if !floatEq(y, []float64{10, 11, 14, 15}) {
		t.Fatalf("gap y %v", y)
	}
	if len(full.Read) != 1 || len(full.Read[0].Gaps) != 1 {
		t.Fatalf("gap meta %+v", full.Read)
	}
	lim := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq", "limit": 3})
	ly, lns, _ := decodeOne(t, lim)
	if !floatEq(ly, y[:3]) || !intEq(lns, ns[:3]) || len(lim.Read[0].Gaps) != 1 {
		t.Fatalf("gap prefix y %v ns %v gaps %+v", ly, lns, lim.Read[0].Gaps)
	}
	if sumLen(lim.Reads, "ai0") != 3 {
		t.Fatalf("gap reads %+v", lim.Reads)
	}
	c.call(t, map[string]any{"op": "close"})

	c2 := start(t)
	defer c2.close()
	got := c2.call(t, map[string]any{"op": "open", "paths": wave.Files})
	if got.Op != "result" {
		t.Fatalf("wave open: %+v", got)
	}
	var timeIsData bool
	for _, ch := range got.Channels {
		if ch.Group == "w" && ch.Name == "Time" {
			timeIsData = !ch.IsTimestamp && !ch.Timed
		}
	}
	if !timeIsData {
		t.Fatal("waveform file inferred Time")
	}
	wy := c2.call(t, map[string]any{"op": "read", "name": "ai0", "group": "w", "limit": 2})
	y, ns, rel := decodeOne(t, wy)
	if !floatEq(y, []float64{10, 11}) {
		t.Fatalf("wave y %v", y)
	}
	if ns[0] != wave.Epoch.UnixNano() || ns[1] != wave.Epoch.Add(time.Second).UnixNano() {
		t.Fatalf("wave times %v", ns)
	}
	if !floatEq(rel, []float64{0, 1}) || sumLen(wy.Reads, "ai0") != 2 || namesOf(wy.Reads, "Time") || namesOf(wy.Reads, "ai1") {
		t.Fatalf("wave reads %+v rel %v", wy.Reads, rel)
	}

	raw := c2.call(t, map[string]any{"op": "read", "name": "Time", "group": "w", "limit": 2, "absolute": false})
	ry, rns, _ := decodeOne(t, raw)
	if !floatEq(ry, []float64{100, 101}) || rns != nil {
		t.Fatalf("Time was treated as a clock y %v ns %v", ry, rns)
	}
}

func openDemo(t *testing.T, c *client, set DemoSet) {
	t.Helper()
	got := c.call(t, map[string]any{
		"op":                 "open",
		"paths":              set.Files,
		"timestamp_channel":  "Time",
		"timestamp_channels": map[string]string{"raw": ""},
		"numeric_epoch":      set.Epoch.Format(time.RFC3339Nano),
	})
	if got.Op != "result" {
		t.Fatalf("open: %+v", got)
	}
}

func sumLen(reads []spanRead, name string) int {
	n := 0
	for _, r := range reads {
		if r.Name == name {
			n += r.Length
		}
	}
	return n
}

func namesOf(reads []spanRead, name string) bool {
	for _, r := range reads {
		if r.Name == name {
			return true
		}
	}
	return false
}

func fileHas(reads []spanRead, suffix, name string) bool {
	for _, r := range reads {
		if r.Name == name && strings.HasSuffix(r.File, suffix) {
			return true
		}
	}
	return false
}
