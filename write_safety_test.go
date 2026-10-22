package bridge

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/trey-copeland/gotdms"
)

// writeFloatFile creates path with one float64 channel data/ai0 through the
// bridge.
func writeFloatFile(t *testing.T, path string, values []float64) {
	t.Helper()
	c := start(t)
	defer c.close()
	begin := c.callPayload(t, map[string]any{
		"op":   "write_begin",
		"path": path,
		"mode": "create",
		"groups": []map[string]any{{
			"name":     "data",
			"channels": []map[string]any{chSpec("ai0", "float64", len(values), nil, nil)},
		}},
	}, nil)
	if begin.Op != "result" {
		t.Fatalf("begin: %+v", begin)
	}
	blob, err := encodeNative(tdms.TypeFloat64, values)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai0"}, blob); got.Op != "result" {
		t.Fatalf("chunk: %+v", got)
	}
	if got := c.callPayload(t, map[string]any{"op": "write_end"}, nil); got.Op != "result" {
		t.Fatalf("end: %+v", got)
	}
}

func beginAppendTwo(t *testing.T, c *client, path string) {
	t.Helper()
	begin := c.callPayload(t, map[string]any{
		"op":   "write_begin",
		"path": path,
		"mode": "append",
		"groups": []map[string]any{{
			"name": "data",
			"channels": []map[string]any{
				chSpec("ai0", "float64", 2, nil, nil),
				chSpec("ai1", "float64", 2, nil, nil),
			},
		}},
	}, nil)
	if begin.Op != "result" {
		t.Fatalf("append begin: %+v", begin)
	}
}

func readAI0(t *testing.T, path string) []float64 {
	t.Helper()
	f, err := tdms.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if st := f.Status(); !st.Complete {
		t.Fatalf("status %s", st)
	}
	g, ok := f.Group("data")
	if !ok {
		t.Fatal("no group")
	}
	ch, ok := g.Channel("ai0")
	if !ok {
		t.Fatal("no channel")
	}
	vals, err := ch.Float64s()
	if err != nil {
		t.Fatal(err)
	}
	return vals
}

// A failed append must leave the file exactly as it was, so the reader does
// not see half an append and a later append is still accepted.
func TestAppendFailureRestoresFile(t *testing.T) {
	cases := []struct {
		name string
		fail func(t *testing.T, c *client)
	}{
		{"bad chunk", func(t *testing.T, c *client) {
			blob, _ := encodeNative(tdms.TypeFloat64, []float64{5, 6})
			if got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai0"}, blob); got.Op != "result" {
				t.Fatalf("chunk: %+v", got)
			}
			bad, _ := encodeNative(tdms.TypeInt32, []int32{7, 8})
			if got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai1"}, bad); got.Op != "error" {
				t.Fatalf("bad chunk accepted: %+v", got)
			}
		}},
		{"short end", func(t *testing.T, c *client) {
			blob, _ := encodeNative(tdms.TypeFloat64, []float64{5, 6})
			if got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai0"}, blob); got.Op != "result" {
				t.Fatalf("chunk: %+v", got)
			}
			if got := c.callPayload(t, map[string]any{"op": "write_end"}, nil); got.Op != "error" {
				t.Fatalf("short end accepted: %+v", got)
			}
		}},
		{"client gone", func(t *testing.T, c *client) {
			blob, _ := encodeNative(tdms.TypeFloat64, []float64{5, 6})
			if got := c.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": "ai0"}, blob); got.Op != "result" {
				t.Fatalf("chunk: %+v", got)
			}
			c.close()
			if err := <-c.err; err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "run.tdms")
			writeFloatFile(t, path, []float64{1, 2, 3})
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			c := start(t)
			beginAppendTwo(t, c, path)
			tc.fail(t, c)
			c.close()
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("file changed after failed append: %d bytes, was %d", len(after), len(before))
			}
			if got := readAI0(t, path); !deepEqual(got, []float64{1, 2, 3}) {
				t.Fatalf("values %v", got)
			}
			// The file still accepts an append.
			c2 := start(t)
			defer c2.close()
			beginAppendTwo(t, c2, path)
			for _, name := range []string{"ai0", "ai1"} {
				blob, _ := encodeNative(tdms.TypeFloat64, []float64{4, 5})
				if got := c2.callPayload(t, map[string]any{"op": "write_chunk", "group": "data", "channel": name}, blob); got.Op != "result" {
					t.Fatalf("chunk %s: %+v", name, got)
				}
			}
			if got := c2.callPayload(t, map[string]any{"op": "write_end"}, nil); got.Op != "result" {
				t.Fatalf("end: %+v", got)
			}
			if got := readAI0(t, path); !deepEqual(got, []float64{1, 2, 3, 4, 5}) {
				t.Fatalf("values after append %v", got)
			}
		})
	}
}

// replaceFile must never delete the destination when the move fails.
func TestReplaceFileKeepsDestination(t *testing.T) {
	dir := t.TempDir()
	dest := filepath.Join(dir, "keep.tdms")
	if err := os.WriteFile(dest, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceFile(filepath.Join(dir, "missing.tmp"), dest); err == nil {
		t.Fatal("replaceFile with a missing source succeeded")
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "old" {
		t.Fatalf("destination lost: %q %v", got, err)
	}
}

// Non-finite float properties are valid TDMS. They must not break the
// session. JSON has no NaN, so the header carries the NaN, Infinity, and
// -Infinity tokens that Python's json module reads.
func TestNonFinitePropertiesInHeaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nan.tdms")
	w, err := tdms.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w.SetProperty("nan", math.NaN())
	g := w.Group("data")
	g.SetProperty("low", math.Inf(-1))
	ch, err := g.Channel("ai0", tdms.TypeFloat64)
	if err != nil {
		t.Fatal(err)
	}
	ch.SetProperty("high", math.Inf(1))
	ch.SetProperty("half", float32(math.NaN()))
	ch.SetProperty("plain", 1.5)
	if err := ch.Write([]float64{1, 2}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	check := func(name string, header any) {
		t.Helper()
		var buf bytes.Buffer
		if err := writeResult(&buf, header, nil); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		f, err := ReadFrame(&buf)
		if err != nil {
			t.Fatal(err)
		}
		text := string(f.Header)
		for _, tok := range []string{":NaN", ":Infinity", ":-Infinity", ":1.5"} {
			if !bytes.Contains(f.Header, []byte(tok)) {
				t.Fatalf("%s: header lacks %s: %s", name, tok, text)
			}
		}
		if bytes.Contains(f.Header, []byte("gotdms-nonfinite")) {
			t.Fatalf("%s: placeholder left in header: %s", name, text)
		}
		// Swap the tokens for null and the rest must be valid JSON.
		clean := f.Header
		for _, tok := range []string{"-Infinity", "Infinity", "NaN"} {
			clean = bytes.ReplaceAll(clean, []byte(":"+tok), []byte(":null"))
		}
		var v any
		if err := json.Unmarshal(clean, &v); err != nil {
			t.Fatalf("%s: %v: %s", name, err, text)
		}
	}
	h, _, err := readValues(path)
	if err != nil {
		t.Fatal(err)
	}
	check("read_values", h)

	s := &session{}
	raw, _ := json.Marshal(path)
	h, _, err = s.open(map[string]json.RawMessage{"op": json.RawMessage(`"open"`), "paths": json.RawMessage("[" + string(raw) + "]")})
	if err != nil {
		t.Fatal(err)
	}
	check("open", h)
}

// A header that cannot be encoded is an error reply, not a dead session.
func TestUnencodableResultIsError(t *testing.T) {
	var buf bytes.Buffer
	if err := writeResult(&buf, map[string]any{"op": "result", "bad": make(chan int)}, nil); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	m := headerMap(t, f.Header)
	if m["op"] != "error" || m["code"] != "error" {
		t.Fatalf("header %v", m)
	}
}

func TestWriteFrameRejectsHugePayload(t *testing.T) {
	if err := checkPayloadLen(math.MaxUint32); err != nil {
		t.Fatal(err)
	}
	if err := checkPayloadLen(uint64(math.MaxUint32) + 1); err == nil {
		t.Fatal("payload over 4 GiB accepted")
	}
}
