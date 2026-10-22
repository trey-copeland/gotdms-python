package bridge

import (
	"path/filepath"
	"testing"

	"github.com/trey-copeland/gotdms"
)

func TestInterleavedMultiSegmentReadValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ileave.tdms")
	w, err := tdms.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w.SetInterleaved(true)
	a, err := w.Group("acq").Channel("a", tdms.TypeFloat64)
	if err != nil {
		t.Fatal(err)
	}
	b, err := w.Group("acq").Channel("b", tdms.TypeFloat64)
	if err != nil {
		t.Fatal(err)
	}
	// Two segments, and the second is shorter. A scatter that ignores the
	// channel's sample index writes 4 over the start and leaves a hole.
	if err := a.Write([]float64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := b.Write([]float64{10, 20, 30}); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := a.Write([]float64{4}); err != nil {
		t.Fatal(err)
	}
	if err := b.Write([]float64{40}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := tdms.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.SegmentCount() != 2 {
		t.Fatalf("segments %d", f.SegmentCount())
	}
	if err := f.Close(); err != nil {
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
	if len(parts) != 2 {
		t.Fatalf("parts %d", len(parts))
	}
	want := [][]float64{{1, 2, 3, 4}, {10, 20, 30, 40}}
	for i, part := range parts {
		col, release, err := readArrowColumn(part)
		if err != nil {
			t.Fatal(err)
		}
		vals, err := columnValues("float64", col)
		release()
		if err != nil {
			t.Fatal(err)
		}
		if !deepEqual(vals, want[i]) {
			t.Fatalf("channel %d %v", i, vals)
		}
	}
}
