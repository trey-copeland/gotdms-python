package bridge

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trey-copeland/gotdms"
)

func TestWriteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "types.tdms")
	c := start(t)
	defer c.close()

	when := time.Date(2024, 3, 1, 0, 0, 0, 123456789, time.UTC)
	frac := uint64(1<<63) + 1
	begin := c.callPayload(t, map[string]any{
		"op":   "write_begin",
		"path": path,
		"mode": "create",
		"properties": []map[string]any{
			{"name": "title", "type": "string", "value": "cleaned"},
			{"name": "revision", "type": "int32", "value": 3},
			{"name": "wide", "type": "int64", "value": int64(1) << 40},
			{"name": "flag", "type": "bool", "value": true},
			{"name": "gain", "type": "float64", "value": 1.5},
			{"name": "code", "type": "uint16", "value": 7},
			{"name": "when", "type": "timestamp", "value": when.Format(time.RFC3339Nano)},
			{"name": "exact", "type": "timestamp", "value": map[string]any{"seconds": 5, "fraction": frac}},
		},
		"groups": []map[string]any{
			{
				"name": "types",
				"properties": []map[string]any{
					{"name": "kind", "type": "string", "value": "samples"},
				},
				"channels": []map[string]any{
					chSpec("i8", "int8", 3, nil, nil),
					chSpec("note", "string", 2, nil, nil),
					chSpec("i16", "int16", 2, nil, nil),
					chSpec("i32", "int32", 2, nil, nil),
					chSpec("i64", "int64", 4, nil, nil),
					chSpec("u8", "uint8", 2, nil, nil),
					chSpec("u16", "uint16", 2, nil, nil),
					chSpec("u32", "uint32", 2, nil, nil),
					chSpec("u64", "uint64", 2, nil, nil),
					chSpec("f32", "float32", 2, nil, nil),
					chSpec("f64", "float64", 3, []map[string]any{
						{"name": "unit_string", "type": "string", "value": "V"},
					}, map[string]any{
						"wf_start_time":   when.Format(time.RFC3339Nano),
						"wf_increment":    0.1,
						"wf_start_offset": 0.25,
						"wf_samples":      3,
					}),
					chSpec("flag", "bool", 3, nil, nil),
					chSpec("when", "timestamp", 2, nil, nil),
				},
			},
			{
				"name": "log",
				"properties": []map[string]any{
					{"name": "kind", "type": "string", "value": "event"},
				},
				"channels": []map[string]any{
					chSpec("message", "string", 2, nil, nil),
				},
			},
		},
	}, nil)
	if begin.Op != "result" {
		t.Fatalf("begin: %+v", begin)
	}

	send := func(group, name string, dt tdms.DataType, values any) {
		t.Helper()
		blob, err := encodeNative(dt, values)
		if err != nil {
			t.Fatal(err)
		}
		got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": group, "channel": name}, blob)
		if got.Op != "result" {
			t.Fatalf("chunk %s/%s: %+v", group, name, got)
		}
	}
	// Fixed-size channels in creation order, then strings.
	send("types", "i8", tdms.TypeInt8, []int8{-8, 0, 7})
	send("types", "i16", tdms.TypeInt16, []int16{-300, 300})
	send("types", "i32", tdms.TypeInt32, []int32{-40_000, 40_000})
	send("types", "i64", tdms.TypeInt64, []int64{-(1 << 62), 1 << 53, (1 << 53) + 1, math.MaxInt64})
	send("types", "u8", tdms.TypeUint8, []uint8{0, 255})
	send("types", "u16", tdms.TypeUint16, []uint16{0, 65535})
	send("types", "u32", tdms.TypeUint32, []uint32{0, 4_000_000_000})
	send("types", "u64", tdms.TypeUint64, []uint64{0, math.MaxUint64})
	send("types", "f32", tdms.TypeFloat32, []float32{1.25, -2})
	// Two chunks so the stream path is exercised more than once.
	send("types", "f64", tdms.TypeFloat64, []float64{1.5, 2.5})
	send("types", "f64", tdms.TypeFloat64, []float64{3.5})
	send("types", "flag", tdms.TypeBool, []bool{true, false, true})
	send("types", "when", tdms.TypeTimestamp, []tdms.Timestamp{
		tdms.NewTimestamp(when),
		tdms.NewTimestamp(when.Add(time.Nanosecond)),
	})
	send("types", "note", tdms.TypeString, []string{"", "keep"})
	send("log", "message", tdms.TypeString, []string{"start", "done"})

	end := c.callPayload(t, map[string]any{"op": "write_end"}, nil)
	if end.Op != "result" {
		t.Fatalf("end: %+v", end)
	}
	doc := headerMap(t, end.Header)
	if doc["stream_chunks"].(float64) < 2 {
		t.Fatalf("stream_chunks: %v", doc["stream_chunks"])
	}
	if doc["buffered_channels"].(float64) != 2 {
		t.Fatalf("buffered_channels: %v", doc)
	}

	f, err := tdms.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if !f.Status().Complete {
		t.Fatalf("status %+v", f.Status())
	}
	if got := prop(t, f.Properties(), "title"); got != "cleaned" {
		t.Fatalf("title %v", got)
	}
	if got := prop(t, f.Properties(), "revision"); got != int32(3) {
		t.Fatalf("revision %T %v", got, got)
	}
	if got := prop(t, f.Properties(), "wide"); got != int64(1)<<40 {
		t.Fatalf("wide %T %v", got, got)
	}
	if got := prop(t, f.Properties(), "flag"); got != true {
		t.Fatalf("flag %v", got)
	}
	if got := prop(t, f.Properties(), "code"); got != uint16(7) {
		t.Fatalf("code %T %v", got, got)
	}
	exact := prop(t, f.Properties(), "exact").(tdms.Timestamp)
	if exact.Seconds != 5 || exact.Fraction != frac {
		t.Fatalf("exact %+v", exact)
	}
	g, ok := f.Group("types")
	if !ok {
		t.Fatal("types group missing")
	}
	check := func(name string, want any) {
		t.Helper()
		ch, ok := g.Channel(name)
		if !ok {
			t.Fatalf("missing %s", name)
		}
		got, err := ch.Values()
		if err != nil {
			t.Fatal(err)
		}
		if !deepEqual(got, want) {
			t.Fatalf("%s got %#v want %#v", name, got, want)
		}
	}
	check("i8", []int8{-8, 0, 7})
	check("i16", []int16{-300, 300})
	check("i32", []int32{-40_000, 40_000})
	check("i64", []int64{-(1 << 62), 1 << 53, (1 << 53) + 1, math.MaxInt64})
	check("u8", []uint8{0, 255})
	check("u16", []uint16{0, 65535})
	check("u32", []uint32{0, 4_000_000_000})
	check("u64", []uint64{0, math.MaxUint64})
	check("f32", []float32{1.25, -2})
	check("f64", []float64{1.5, 2.5, 3.5})
	check("flag", []bool{true, false, true})
	check("note", []string{"", "keep"})
	f64, _ := g.Channel("f64")
	if unit, ok := f64.Unit(); !ok || unit != "V" {
		t.Fatalf("unit %q %v", unit, ok)
	}
	inc, ok := f64.Increment()
	if !ok || inc != 0.1 {
		t.Fatalf("increment %v %v", inc, ok)
	}
	off, ok := f64.Property(tdms.PropStartOffset)
	if !ok || off.Value != 0.25 {
		t.Fatalf("offset %+v", off)
	}
	start, ok := f64.StartTime()
	if !ok || !start.Equal(when) {
		t.Fatalf("start %v %v", start, ok)
	}
	rawStart := prop(t, f64.Properties(), tdms.PropStartTime).(tdms.Timestamp)
	back, ok := rawStart.Time()
	if !ok || !back.Equal(when) {
		t.Fatalf("wf_start_time %+v %v", rawStart, back)
	}
	samples, ok := f64.Property(tdms.PropSamples)
	if !ok || samples.Value != int32(3) {
		t.Fatalf("samples %+v", samples)
	}
	log, ok := f.Group("log")
	if !ok {
		t.Fatal("log missing")
	}
	msg, _ := log.Channel("message")
	gotMsg, err := msg.Values()
	if err != nil {
		t.Fatal(err)
	}
	if !deepEqual(gotMsg, []string{"start", "done"}) {
		t.Fatalf("log %v", gotMsg)
	}
	whenCh, _ := g.Channel("when")
	gotTS, err := whenCh.Timestamps()
	if err != nil {
		t.Fatal(err)
	}
	t0, _ := gotTS[0].Time()
	t1, _ := gotTS[1].Time()
	if !t0.Equal(when) || !t1.Equal(when.Add(time.Nanosecond)) {
		t.Fatalf("timestamps %v %v", t0, t1)
	}

	// The same process can still open the file it just wrote.
	opened := c.call(t, map[string]any{"op": "open", "paths": []string{path}})
	if opened.Op != "result" {
		t.Fatalf("open written file: %+v", opened)
	}
	read := c.call(t, map[string]any{"op": "read", "group": "types", "name": "f64"})
	y, _, _ := decodeOne(t, read)
	if !floatEq(y, []float64{1.5, 2.5, 3.5}) {
		t.Fatalf("dataset read %v", y)
	}
}

func TestWriteAppendAndReject(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "run.tdms")
	c := start(t)
	defer c.close()

	writeFloat := func(mode string, values []float64) {
		t.Helper()
		begin := c.callPayload(t, map[string]any{
			"op":   "write_begin",
			"path": path,
			"mode": mode,
			"properties": []map[string]any{
				{"name": "title", "type": "string", "value": "first"},
			},
			"groups": []map[string]any{{
				"name": "data",
				"channels": []map[string]any{
					chSpec("ai0", "float64", len(values), []map[string]any{
						{"name": "unit_string", "type": "string", "value": "V"},
					}, nil),
				},
			}},
		}, nil)
		if begin.Op != "result" {
			t.Fatalf("begin %s: %+v", mode, begin)
		}
		blob, err := encodeNative(tdms.TypeFloat64, values)
		if err != nil {
			t.Fatal(err)
		}
		chunk := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai0"}, blob)
		if chunk.Op != "result" {
			t.Fatalf("chunk %s: %+v", mode, chunk)
		}
		end := c.callPayload(t, map[string]any{"op": "write_end"}, nil)
		if end.Op != "result" {
			t.Fatalf("end %s: %+v", mode, end)
		}
	}
	writeFloat("create", []float64{1, 2, 3})
	// Append more samples without restating the unit. The earlier property stays.
	begin := c.callPayload(t, map[string]any{
		"op":   "write_begin",
		"path": path,
		"mode": "append",
		"groups": []map[string]any{{
			"name": "data",
			"channels": []map[string]any{
				chSpec("ai0", "float64", 2, nil, nil),
			},
		}},
	}, nil)
	if begin.Op != "result" {
		t.Fatalf("append begin: %+v", begin)
	}
	blob, err := encodeNative(tdms.TypeFloat64, []float64{4, 5})
	if err != nil {
		t.Fatal(err)
	}
	if got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai0"}, blob); got.Op != "result" {
		t.Fatalf("append chunk: %+v", got)
	}
	if got := c.callPayload(t, map[string]any{"op": "write_end"}, nil); got.Op != "result" {
		t.Fatalf("append end: %+v", got)
	}
	f, err := tdms.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ch, _ := f.Group("data")
	ai, _ := ch.Channel("ai0")
	got, err := ai.Values()
	if err != nil {
		t.Fatal(err)
	}
	if !deepEqual(got, []float64{1, 2, 3, 4, 5}) {
		t.Fatalf("appended %v", got)
	}
	if unit, ok := ai.Unit(); !ok || unit != "V" {
		t.Fatalf("unit after append %q %v", unit, ok)
	}
	if prop(t, f.Properties(), "title") != "first" {
		t.Fatalf("title dropped")
	}
	f.Close()

	mismatch := c.callPayload(t, map[string]any{
		"op":   "write_begin",
		"path": path,
		"mode": "append",
		"groups": []map[string]any{{
			"name": "data",
			"channels": []map[string]any{
				chSpec("ai0", "int32", 1, nil, nil),
			},
		}},
	}, nil)
	if mismatch.Code != "argument" {
		t.Fatalf("type mismatch: %+v", mismatch)
	}

	missing := c.callPayload(t, map[string]any{
		"op":   "write_begin",
		"path": filepath.Join(dir, "nope.tdms"),
		"mode": "append",
		"groups": []map[string]any{{
			"name":     "data",
			"channels": []map[string]any{chSpec("ai0", "float64", 1, nil, nil)},
		}},
	}, nil)
	if missing.Code != "argument" {
		t.Fatalf("missing append: %+v", missing)
	}

	bad := filepath.Join(dir, "bad.tdms")
	if err := os.WriteFile(bad, []byte("not tdms"), 0o644); err != nil {
		t.Fatal(err)
	}
	junk := c.callPayload(t, map[string]any{
		"op": "write_begin", "path": bad, "mode": "append",
		"groups": []map[string]any{{"name": "data", "channels": []map[string]any{chSpec("ai0", "float64", 1, nil, nil)}}},
	}, nil)
	if junk.Code != "corrupt" {
		t.Fatalf("junk append: %+v", junk)
	}
	// The same bad file is "corrupt" from open and read_values too.
	for _, op := range []string{"open", "describe", "read_values"} {
		got := c.callPayload(t, map[string]any{"op": op, "paths": []string{bad}, "path": bad}, nil)
		if got.Code != "corrupt" {
			t.Fatalf("%s of a junk file: %+v", op, got)
		}
	}
	// A missing path is the caller's mistake from every op.
	gone := filepath.Join(dir, "gone.tdms")
	for _, op := range []string{"open", "describe", "read_values"} {
		got := c.callPayload(t, map[string]any{"op": op, "paths": []string{gone}, "path": gone}, nil)
		if got.Code != "argument" {
			t.Fatalf("%s of a missing file: %+v", op, got)
		}
	}

	inc := filepath.Join(dir, "partial.tdms")
	w, err := tdms.Create(inc)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := w.Group("g").Channel("a", tdms.TypeFloat64)
	if err != nil {
		t.Fatal(err)
	}
	st, err := partial.Stream(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Write(partial, []float64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err == nil {
		t.Fatal("short stream close should fail")
	}
	rejected := c.callPayload(t, map[string]any{
		"op": "write_begin", "path": inc, "mode": "append",
		"groups": []map[string]any{{"name": "g", "channels": []map[string]any{chSpec("a", "float64", 1, nil, nil)}}},
	}, nil)
	if rejected.Code != "argument" {
		t.Fatalf("incomplete append: %+v", rejected)
	}

	// A failed create does not leave the destination behind.
	dest := filepath.Join(dir, "no-write.tdms")
	failed := c.callPayload(t, map[string]any{
		"op": "write_begin", "path": dest, "mode": "create",
		"groups": []map[string]any{{
			"name":     "data",
			"channels": []map[string]any{chSpec("ai0", "decimal", 1, nil, nil)},
		}},
	}, nil)
	if failed.Code != "argument" {
		t.Fatalf("bad type: %+v", failed)
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("failed create left %s (%v)", dest, err)
	}
}

func TestReadValuesNative(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "native.tdms")
	w, err := tdms.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := w.Group("g").Channel("n", tdms.TypeInt64)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Write([]int64{math.MaxInt64, math.MinInt64}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	c := start(t)
	defer c.close()
	got := c.callPayload(t, map[string]any{"op": "read_values", "path": path}, nil)
	if got.Op != "result" {
		t.Fatalf("read_values: %+v", got)
	}
	parts, err := SplitPayload(got.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("parts %d", len(parts))
	}
	col, release, err := readArrowColumn(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	vals, err := columnValues("int64", col)
	if err != nil {
		t.Fatal(err)
	}
	if !deepEqual(vals, []int64{math.MaxInt64, math.MinInt64}) {
		t.Fatalf("values %v", vals)
	}
}

func chSpec(name, typ string, rows int, props []map[string]any, wave map[string]any) map[string]any {
	spec := map[string]any{"name": name, "type": typ, "rows": rows}
	if props != nil {
		spec["properties"] = props
	}
	if wave != nil {
		spec["waveform"] = wave
	}
	return spec
}

func (c *client) callPayload(t *testing.T, header map[string]any, payload []byte) reply {
	t.Helper()
	b, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(c.in, ProtocolVersion, b, payload); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(c.out)
	if err != nil {
		t.Fatal(err)
	}
	return c.finish(t, f)
}

func headerMap(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func prop(t *testing.T, props []tdms.Property, name string) any {
	t.Helper()
	for _, p := range props {
		if p.Name == name {
			return p.Value
		}
	}
	t.Fatalf("missing property %s", name)
	return nil
}

func deepEqual(a, b any) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && string(ab) == string(bb)
}
