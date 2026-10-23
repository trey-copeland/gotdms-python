package bridge

import (
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// floatItem is a one-column item whose values are base, base+1, ...
// bytes may be overridden to exercise batchCap without large buffers.
func floatItem(rows int, base float64, bytes int64) *packItem {
	vals := make([]float64, rows)
	for i := range vals {
		vals[i] = base + float64(i)
	}
	if bytes < 0 {
		bytes = int64(rows) * 8
	}
	return &packItem{rows: rows, bytes: bytes, cols: []packCol{{base: "y", typ: arrow.PrimitiveTypes.Float64, build: func() (arrow.Array, error) {
		return viewArray(arrow.PrimitiveTypes.Float64, vals), nil
	}}}}
}

// checkPacked encodes the streams, decodes every batch, and checks that each
// item's column holds its values and that every column is named once.
func checkPacked(t *testing.T, items []*packItem, streams []outStream, base func(i int) float64) {
	t.Helper()
	recs := make([][]arrow.Record, len(streams))
	total := 0
	for i, st := range streams {
		var buf bytes.Buffer
		if err := st.write(&buf); err != nil {
			t.Fatal(err)
		}
		if int64(buf.Len()) != st.size {
			t.Fatalf("stream %d wrote %d bytes, declared %d", i, buf.Len(), st.size)
		}
		got, err := batchesOf(buf.Bytes(), st.batches)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		for _, r := range got {
			total += int(r.NumCols())
		}
		recs[i] = got
	}
	seen := map[colRef]bool{}
	for i, it := range items {
		if seen[it.ref] {
			t.Fatalf("item %d ref %+v reused", i, it.ref)
		}
		seen[it.ref] = true
		col := recs[it.ref.Stream][it.ref.Batch].Column(it.ref.Column).(*array.Float64)
		if col.Len() != it.rows {
			t.Fatalf("item %d has %d rows, want %d", i, col.Len(), it.rows)
		}
		for j, v := range col.Float64Values() {
			if v != base(i)+float64(j) {
				t.Fatalf("item %d value %d is %v", i, j, v)
			}
		}
	}
	if total != len(items) {
		t.Fatalf("%d columns for %d items", total, len(items))
	}
}

func TestPackManyEqualChannelsIsOneBatch(t *testing.T) {
	items := make([]*packItem, 785)
	for i := range items {
		items[i] = floatItem(100, float64(i*1000), -1)
	}
	streams, err := packStreams("channel", items, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || streams[0].batches != 1 {
		t.Fatalf("%d streams", len(streams))
	}
	for i, it := range items {
		if it.ref != (colRef{Stream: 3, Batch: 0, Column: i}) {
			t.Fatalf("item %d at %+v: column order must follow the request", i, it.ref)
		}
	}
	for i := range items {
		items[i].ref.Stream -= 3
	}
	checkPacked(t, items, streams, func(i int) float64 { return float64(i * 1000) })
}

func TestPackRaggedChannelsShareAStream(t *testing.T) {
	// 785 channels of 785 lengths: one ragged stream, one batch each.
	items := make([]*packItem, 785)
	for i := range items {
		items[i] = floatItem(i, float64(i), -1)
	}
	streams, err := packStreams("channel", items, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 1 || streams[0].batches != 785 {
		t.Fatalf("%d streams, %d batches", len(streams), streams[0].batches)
	}
	checkPacked(t, items, streams, func(i int) float64 { return float64(i) })
}

func TestPackWideGroupLimit(t *testing.T) {
	// 40 lengths with 3 channels each: the 16 widest-first groups are wide,
	// the rest go to one ragged stream. Larger groups win the wide slots.
	var items []*packItem
	for l := 0; l < 40; l++ {
		n := 3
		if l == 39 {
			n = 5
		}
		for k := 0; k < n; k++ {
			items = append(items, floatItem(10+l, float64(len(items)), -1))
		}
	}
	streams, err := packStreams("channel", items, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != maxWideGroups+1 {
		t.Fatalf("%d streams", len(streams))
	}
	last := items[len(items)-1]
	if streams[last.ref.Stream].batches != 1 || last.ref.Column != 4 {
		t.Fatalf("the 5-channel group is not wide: %+v", last.ref)
	}
	base := make([]float64, len(items))
	for i := range items {
		base[i] = float64(i)
	}
	checkPacked(t, items, streams, func(i int) float64 { return base[i] })
}

func TestPackRespectsBatchCap(t *testing.T) {
	// Wide: 10 equal channels of a nominal 3 MiB split into streams of at
	// most batchCap. Big: a channel over batchCap gets its own stream.
	// Ragged: distinct lengths of a nominal 5 MiB split the same way.
	var items []*packItem
	for k := 0; k < 10; k++ {
		items = append(items, floatItem(8, float64(len(items)), 3<<20))
	}
	big := floatItem(9, float64(len(items)), batchCap+1)
	items = append(items, big)
	for k := 0; k < 7; k++ {
		items = append(items, floatItem(20+k, float64(len(items)), 5<<20))
	}
	streams, err := packStreams("channel", items, 0)
	if err != nil {
		t.Fatal(err)
	}
	per := map[int]int64{}
	for _, it := range items {
		per[it.ref.Stream] += it.bytes
	}
	for sid, n := range per {
		if n > batchCap && sid != big.ref.Stream {
			t.Fatalf("stream %d holds %d bytes", sid, n)
		}
	}
	for _, it := range items {
		if it != big && it.ref.Stream == big.ref.Stream {
			t.Fatal("a channel shares the oversize channel's stream")
		}
	}
	// 10x3 MiB -> 2 wide streams (5+5), 1 big, 7x5 MiB ragged -> 3 streams.
	if len(streams) != 6 {
		t.Fatalf("%d streams", len(streams))
	}
	checkPacked(t, items, streams, func(i int) float64 { return float64(i) })
}

func TestPackMixedLayoutsAndTypes(t *testing.T) {
	// Different column layouts never share a batch.
	f := floatItem(4, 0, -1)
	g := floatItem(4, 100, -1)
	i16 := []int16{1, 2, 3, 4}
	h := &packItem{rows: 4, bytes: 8, cols: []packCol{{base: "y", typ: arrow.PrimitiveTypes.Int16, build: func() (arrow.Array, error) {
		return viewArray(arrow.PrimitiveTypes.Int16, i16), nil
	}}}}
	rel := floatItem(4, 7, -1)
	rel.cols = append(rel.cols, packCol{base: "relative_s", typ: arrow.PrimitiveTypes.Float64, build: rel.cols[0].build})
	items := []*packItem{f, h, g, rel}
	streams, err := packStreams("channel", items, 0)
	if err != nil {
		t.Fatal(err)
	}
	if f.ref.Stream != g.ref.Stream || h.ref.Stream == f.ref.Stream || rel.ref.Stream == f.ref.Stream || rel.ref.Stream == h.ref.Stream {
		t.Fatalf("refs f %+v g %+v h %+v rel %+v", f.ref, g.ref, h.ref, rel.ref)
	}
	if len(streams) != 3 {
		t.Fatalf("%d streams", len(streams))
	}
	for i, st := range streams {
		var buf bytes.Buffer
		if err := st.write(&buf); err != nil {
			t.Fatal(err)
		}
		if _, err := batchesOf(buf.Bytes(), st.batches); err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
	}
}
