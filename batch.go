package bridge

import (
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A data reply packs many channels into few Arrow IPC streams. Every
// stream has one schema and one or more record batches, and every channel
// (or clock) is a column of one batch, named by (stream, batch, column) in
// result_begin. Sending one stream per channel cost a schema message, an
// end-of-stream marker, an IPC writer, and a Python open_stream per channel,
// which dominated reads of many short channels.
//
// packStreams builds the streams:
//
//   - An item over batchCap bytes gets a stream of its own with one batch,
//     as every channel did before batching, so a large channel's buffer is
//     never shared.
//   - Items of equal row count and column layout are "wide": one batch with
//     each item's columns side by side. At most maxWideGroups such row
//     counts are batched this way (the ones with the most items), each split
//     into streams of at most batchCap bytes.
//   - Everything else is "ragged": one stream per column layout, with one
//     record batch per item, again split at batchCap bytes.
//
// So the stream count grows with distinct layouts and with bytes over
// batchCap, never with the channel count alone.
//
// The client views columns in place, so a column it keeps holds its whole
// stream: batchCap bounds what one kept short channel holds of others.
const (
	batchCap      = 16 << 20
	maxWideGroups = 16
)

// packCol is one column of an item.
type packCol struct {
	base  string // "y", "relative_s", or "time_ns"
	typ   arrow.DataType
	build func() (arrow.Array, error)
}

// packItem is one channel or one clock: one or two columns of rows values.
// After packStreams, ref says where its first column is; further columns
// follow it in the same batch.
type packItem struct {
	rows  int
	bytes int64
	cols  []packCol
	ref   colRef
}

// colRef locates a column in a reply.
type colRef struct {
	Stream int `json:"stream"`
	Batch  int `json:"batch"`
	Column int `json:"column"`
}

// layoutKey identifies a column layout: names and types in order.
func layoutKey(it *packItem) string {
	k := ""
	for _, c := range it.cols {
		k += c.base + ":" + c.typ.Fingerprint() + ";"
	}
	return k
}

// packStreams assigns every item a colRef and returns its streams. first is
// the id of the first returned stream.
func packStreams(kind string, items []*packItem, first int) ([]outStream, error) {
	var plans []streamPlan
	var small []*packItem
	for _, it := range items {
		if it.bytes > batchCap {
			plans = append(plans, streamPlan{batches: [][]*packItem{{it}}})
			continue
		}
		small = append(small, it)
	}
	// Wide groups: same rows and layout, at least two items.
	type wideKey struct {
		rows   int
		layout string
	}
	groups := map[wideKey][]*packItem{}
	var order []wideKey
	for _, it := range small {
		k := wideKey{it.rows, layoutKey(it)}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], it)
	}
	ranked := make([]wideKey, 0, len(order))
	for _, k := range order {
		if len(groups[k]) >= 2 {
			ranked = append(ranked, k)
		}
	}
	// Most items first; ties keep first-appearance order.
	sort.SliceStable(ranked, func(a, b int) bool { return len(groups[ranked[a]]) > len(groups[ranked[b]]) })
	if len(ranked) > maxWideGroups {
		ranked = ranked[:maxWideGroups]
	}
	wide := map[wideKey]bool{}
	for _, k := range ranked {
		wide[k] = true
	}
	// Oversize items come first, then wide and ragged-overflow streams in
	// first-appearance order, then the open ragged streams. Stream order is
	// not request order; every channel names its stream, batch, and column.
	ragged := map[string]*streamPlan{}
	var raggedOrder []string
	var raggedBytes = map[string]int64{}
	for _, k := range order {
		members := groups[k]
		if wide[k] {
			var cur []*packItem
			var n int64
			for _, it := range members {
				if len(cur) > 0 && n+it.bytes > batchCap {
					plans = append(plans, streamPlan{batches: [][]*packItem{cur}})
					cur, n = nil, 0
				}
				cur = append(cur, it)
				n += it.bytes
			}
			plans = append(plans, streamPlan{batches: [][]*packItem{cur}})
			continue
		}
		for _, it := range members {
			lk := k.layout
			p := ragged[lk]
			if p != nil && raggedBytes[lk]+it.bytes > batchCap {
				plans = append(plans, *p)
				p = nil
			}
			if p == nil {
				p = &streamPlan{}
				ragged[lk] = p
				raggedBytes[lk] = 0
				raggedOrder = append(raggedOrder, lk)
			}
			p.batches = append(p.batches, []*packItem{it})
			raggedBytes[lk] += it.bytes
		}
	}
	for _, lk := range raggedOrder {
		if p := ragged[lk]; p != nil {
			plans = append(plans, *p)
			ragged[lk] = nil
		}
	}
	out := make([]outStream, 0, len(plans))
	for _, p := range plans {
		sid := first + len(out)
		st, err := p.stream(kind, sid)
		if err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, nil
}

// streamPlan is one stream: batches of items. Every batch has the same
// schema: wide streams have one batch, ragged streams one item per batch.
type streamPlan struct {
	batches [][]*packItem
}

func (p streamPlan) schema() *arrow.Schema {
	var fields []arrow.Field
	b := p.batches[0]
	for i, it := range b {
		for _, c := range it.cols {
			name := c.base
			if len(b) > 1 {
				name += "_" + strconv.Itoa(i)
			}
			fields = append(fields, arrow.Field{Name: name, Type: c.typ, Nullable: c.typ.ID() == arrow.NULL})
		}
	}
	return arrow.NewSchema(fields, nil)
}

// stream assigns refs and returns the outStream, sized by encoding it once
// into a counter. Columns are views of the series' slices, so encoding twice
// copies nothing unless a column is computed.
func (p streamPlan) stream(kind string, sid int) (outStream, error) {
	for bi, b := range p.batches {
		col := 0
		for _, it := range b {
			it.ref = colRef{Stream: sid, Batch: bi, Column: col}
			col += len(it.cols)
		}
	}
	schema := p.schema()
	var c countWriter
	if err := p.write(&c, schema); err != nil {
		return outStream{}, err
	}
	return outStream{kind: kind, size: c.n, batches: len(p.batches), write: func(w io.Writer) error {
		return p.write(w, schema)
	}}, nil
}

// write encodes the stream one batch at a time; a batch's computed
// columns are released before the next batch is built.
func (p streamPlan) write(w io.Writer, schema *arrow.Schema) error {
	iw := ipc.NewWriter(w, ipc.WithSchema(schema), ipc.WithAllocator(memory.NewGoAllocator()))
	for _, b := range p.batches {
		rec, err := buildBatch(schema, b)
		if err != nil {
			iw.Close()
			return err
		}
		err = iw.Write(rec)
		rec.Release()
		if err != nil {
			iw.Close()
			return err
		}
	}
	return iw.Close()
}

func buildBatch(schema *arrow.Schema, items []*packItem) (arrow.Record, error) {
	var cols []arrow.Array
	release := func() {
		for _, c := range cols {
			c.Release()
		}
	}
	rows := 0
	for i, it := range items {
		if i == 0 {
			rows = it.rows
		}
		for _, c := range it.cols {
			a, err := c.build()
			if err != nil {
				release()
				return nil, err
			}
			k := len(cols)
			cols = append(cols, a)
			if a.Len() != rows {
				release()
				return nil, fmt.Errorf("bridge: column %s has %d rows, batch %d", schema.Field(k).Name, a.Len(), rows)
			}
			if !arrow.TypeEqual(a.DataType(), schema.Field(k).Type) {
				release()
				return nil, fmt.Errorf("bridge: column %s is %s, schema %s", schema.Field(k).Name, a.DataType(), schema.Field(k).Type)
			}
		}
	}
	rec := array.NewRecord(schema, cols, int64(rows))
	release()
	return rec, nil
}
