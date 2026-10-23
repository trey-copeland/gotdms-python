package bridge

import (
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/trey-copeland/gotdms"
)

func TestAppendValueJSONMatchesMarshal(t *testing.T) {
	vals := []any{
		"", "plain", "Dev1/ai0", "quote\"", `back\slash`, "<html>&", "tab\t", "é", "\u2028", "del\x7f",
		true, false,
		int8(-8), int16(-1600), int32(1 << 30), int64(-1 << 62), uint8(255), uint16(65535), uint32(1 << 31), uint64(1 << 63),
		0.0, math.Copysign(0, -1), 1.0, 3.0, -2.5, 0.1, 1e-6, 9.99e-7, 1e-7, 1e20, 1e21, 1.5e300, -1e-300,
		math.MaxFloat64, math.SmallestNonzeroFloat64, float64(float32(0.1)), 123456789.123456789,
		jsonFloat(math.NaN()), jsonFloat(math.Inf(1)),
		map[string]any{"fraction": uint64(1 << 63), "seconds": int64(3850070400), "time": "2026-01-01T00:00:00Z"},
		map[string]any{"b<": 1.0, "a": "x"},
		map[string]any{},
		[]int{1, 2},
	}
	r := rand.New(rand.NewSource(1))
	for range 2000 {
		vals = append(vals, math.Float64frombits(r.Uint64()), r.NormFloat64()*math.Pow(10, float64(r.Intn(60)-30)), r.Int63()-r.Int63())
	}
	for _, v := range vals {
		if f, ok := v.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
			continue
		}
		want, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		got, err := appendValueJSON([]byte("prefix"), v)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "prefix"+string(want) {
			t.Fatalf("%#v: got %s want %s", v, got[6:], want)
		}
	}
}

// writePropSet writes two files of group g, untimed. x has the
// same properties in both, except that gain is an int32 in one and a
// float64 in the other: the same JSON, so they match, as before interning.
// y's note differs between the files, so y's properties do not match.
func writePropSet(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	var paths []string
	for i, name := range []string{"a.tdms", "b.tdms"} {
		path := filepath.Join(dir, name)
		w, err := tdms.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		g := w.Group("g")
		for _, ch := range []string{"x", "y"} {
			c, err := g.Channel(ch, tdms.TypeFloat64)
			if err != nil {
				t.Fatal(err)
			}
			c.SetProperty("unit_string", "V")
			if ch == "x" {
				if i == 0 {
					c.SetProperty("gain", int32(3))
				} else {
					c.SetProperty("gain", 3.0)
				}
			} else {
				c.SetProperty("note", name)
			}
			if err := c.Write([]float64{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	return paths
}

func TestCompactCatalogAndReadHeader(t *testing.T) {
	paths := writePropSet(t)
	c := start(t)
	defer c.close()
	cat := c.call(t, map[string]any{"op": "open", "paths": paths})
	if cat.Op != "result" {
		t.Fatalf("open: %+v", cat)
	}
	var wire struct {
		Files []fileMeta `json:"files"`
		Table []struct {
			Name  string          `json:"name"`
			Value json.RawMessage `json:"value"`
		} `json:"property_table"`
		Groups   []catalogGroup   `json:"groups"`
		Channels []catalogChannel `json:"channels"`
	}
	if err := json.Unmarshal(cat.Header, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Files) != 2 || len(wire.Channels) != 4 || len(wire.Groups) != 2 {
		t.Fatalf("catalog shape: %s", cat.Header)
	}
	// Each distinct (name, value) pair is in the table once.
	seen := map[string]bool{}
	for _, e := range wire.Table {
		k := e.Name + "=" + string(e.Value)
		if seen[k] {
			t.Fatalf("duplicate table entry %s", k)
		}
		seen[k] = true
	}
	if !seen[`unit_string="V"`] || !seen["gain=3"] || !seen[`note="a.tdms"`] || !seen[`note="b.tdms"`] {
		t.Fatalf("table: %s", cat.Header)
	}
	for _, g := range wire.Groups {
		if g.File < 0 || g.File > 1 {
			t.Fatalf("group file %d", g.File)
		}
	}
	// The expanded rows are the old full rows.
	for _, row := range cat.Channels {
		if row.Properties["unit_string"] != "V" || row.File == "" {
			t.Fatalf("row: %+v", row)
		}
	}

	got := c.call(t, map[string]any{"op": "read_many", "channels": []map[string]any{{"name": "x", "group": "g"}, {"name": "y", "group": "g"}}})
	if got.Op != "result" || len(got.Read) != 2 {
		t.Fatalf("read: %+v", got)
	}
	var begin struct {
		Channels []struct {
			Name       string          `json:"name"`
			Files      []int           `json:"files"`
			Properties json.RawMessage `json:"properties"`
			Match      bool            `json:"properties_match"`
		} `json:"channels"`
		Reads []struct {
			File json.RawMessage `json:"file"`
		} `json:"reads"`
	}
	if err := json.Unmarshal(got.Data.begin, &begin); err != nil {
		t.Fatal(err)
	}
	for _, ch := range begin.Channels {
		if len(ch.Files) != 2 || ch.Files[0] != 0 || ch.Files[1] != 1 {
			t.Fatalf("%s files %v", ch.Name, ch.Files)
		}
		var at int
		if err := json.Unmarshal(ch.Properties, &at); err != nil {
			t.Fatalf("%s properties is not a catalog index: %s", ch.Name, ch.Properties)
		}
		row := wire.Channels[at]
		if row.Name != ch.Name || row.File != 0 {
			t.Fatalf("%s properties point at %+v", ch.Name, row)
		}
		if want := ch.Name == "x"; ch.Match != want {
			t.Fatalf("%s properties_match %v, want %v", ch.Name, ch.Match, want)
		}
	}
	for _, r := range begin.Reads {
		var k int
		if err := json.Unmarshal(r.File, &k); err != nil || k < 0 || k > 1 {
			t.Fatalf("read file %s", r.File)
		}
	}
	// Resolved, the read meta is what it was: paths and the first file's
	// properties.
	for _, m := range got.Read {
		if len(m.Files) != 2 || m.Files[0] != wire.Files[0].Path || m.Properties["unit_string"] != "V" {
			t.Fatalf("resolved meta: %+v", m)
		}
		if m.Name == "y" && m.Properties["note"] != "a.tdms" {
			t.Fatalf("y properties come from the first file: %+v", m.Properties)
		}
	}
	for _, r := range got.Reads {
		if r.File != wire.Files[0].Path && r.File != wire.Files[1].Path {
			t.Fatalf("read file %q", r.File)
		}
	}
}

// open by glob and by paths gives the same catalog, and a path the
// background scan could not see is scanned again.
func TestPrescanReconciles(t *testing.T) {
	paths := writePropSet(t)
	a := start(t)
	defer a.close()
	byPath := a.call(t, map[string]any{"op": "open", "paths": []string{paths[1], paths[0], paths[1]}})
	b := start(t)
	defer b.close()
	byGlob := b.call(t, map[string]any{"op": "open", "directory": filepath.Dir(paths[0]), "pattern": "*.tdms"})
	if byPath.Op != "result" || byGlob.Op != "result" || len(byPath.Channels) != 4 || len(byGlob.Channels) != 4 {
		t.Fatalf("open: %+v / %+v", byPath, byGlob)
	}

	abs, _ := filepath.Abs(paths[0])
	pre := prescan([]string{paths[0], "", filepath.Join(t.TempDir(), "missing.tdms")})()
	if len(pre) != 1 || pre[abs] == nil {
		t.Fatalf("prescan: %v", pre)
	}
	meta, files, _, err := scanMetadata([]string{abs, paths[1]}, pre)
	if err != nil || len(files) != 2 || len(meta) != 4 {
		t.Fatalf("scanMetadata: %v %v %v", err, files, meta)
	}
	if _, _, _, err := scanMetadata([]string{filepath.Join(t.TempDir(), "gone.tdms")}, pre); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
}

// A file that changes after its prescan (a live acquisition appending, or a
// rewrite) is parsed again, so the catalog matches what dataset.Open saw.
func TestPrescanRescansChangedFile(t *testing.T) {
	paths := writePropSet(t)
	abs, _ := filepath.Abs(paths[0])
	pre := prescan([]string{abs})()
	if pre[abs] == nil {
		t.Fatalf("prescan: %v", pre)
	}
	if _, _, _, err := scanMetadata([]string{abs}, pre); err != nil {
		t.Fatal(err)
	}
	w, err := tdms.Create(abs)
	if err != nil {
		t.Fatal(err)
	}
	c, err := w.Group("g").Channel("z", tdms.TypeFloat64)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write([]float64{1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Coarse file systems keep the same mtime within a tick; move it on so
	// the test does not depend on the size changing.
	later := pre[abs].mod.Add(2 * time.Second)
	if err := os.Chtimes(abs, later, later); err != nil {
		t.Fatal(err)
	}
	meta, _, _, err := scanMetadata([]string{abs}, pre)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := meta[chKey{file: abs, group: "g", name: "z"}]; !ok || len(meta) != 1 {
		t.Fatalf("changed file was not rescanned: %v", meta)
	}
}
