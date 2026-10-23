package bridge

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/trey-copeland/gotdms"
)

// readValues opens one TDMS file and returns native channel values as Arrow
// IPC. This is the inverse of the write ops. It does not go through the
// dataset layer, which converts numeric channels to float64 and rejects
// strings, bools, and timestamp channels.
//
// Each channel is read with Channel.Values. Numeric values are sent as
// views of that slice; strings, bools, and timestamps are converted to Arrow
// arrays and the decoded slice is dropped. Channels are packed into few
// streams the way read_many packs them (see packStreams), and each channel
// names its stream, batch, and column. The streams are sent as part frames,
// so the reply has no size ceiling and is not joined into one buffer.
// Group.Values would split a shared interleaved segment in one pass, but it
// holds every channel of the group at once: about a third more peak memory
// for a few percent of speed. Channel.Values places an interleaved channel
// that continues in a later segment at its own sample index in gotdms v0.1.0
// and v0.1.1.
func readValues(path string) (any, []byte, error) {
	if path == "" {
		return nil, nil, errCode("argument", "read_values needs a path")
	}
	f, err := tdms.Open(path)
	if err != nil {
		return nil, nil, asArg(err)
	}
	defer f.Close()
	groups := make([]map[string]any, 0, len(f.Groups()))
	var items []*packItem
	var refs []map[string]any
	for _, g := range f.Groups() {
		channels := g.Channels()
		chans := make([]map[string]any, 0, len(channels))
		for _, ch := range channels {
			wire, ok := wireOf(ch.DataType())
			if !ok {
				return nil, nil, errCode("argument", fmt.Sprintf("channel %s/%s has unsupported type %s", g.Name(), ch.Name(), ch.DataType()))
			}
			vals, err := ch.Values()
			if err != nil {
				return nil, nil, err
			}
			arr, err := valuesArray(ch.DataType(), vals)
			if err != nil {
				return nil, nil, err
			}
			items = append(items, arrayItem(arr))
			item := map[string]any{
				"name":       ch.Name(),
				"type":       wire,
				"ni_type":    ch.DataType().String(),
				"rows":       ch.Len(),
				"properties": typedProps(ch.Properties()),
			}
			if raw, ok := ch.DAQmx(); ok {
				item["daqmx"] = true
				if raw.Primary >= 0 && raw.Primary < len(raw.Scalers) && raw.Scalers[raw.Primary].Digital {
					item["digital"] = true
				}
			}
			chans = append(chans, item)
			refs = append(refs, item)
		}
		groups = append(groups, map[string]any{
			"name":       g.Name(),
			"properties": typedProps(g.Properties()),
			"channels":   chans,
		})
	}
	streams, err := packStreams("values", items, 0)
	if err != nil {
		return nil, nil, err
	}
	for i, it := range items {
		refs[i]["stream"] = it.ref.Stream
		refs[i]["batch"] = it.ref.Batch
		refs[i]["column"] = it.ref.Column
	}
	header := map[string]any{
		"path":       path,
		"properties": typedProps(f.Properties()),
		"groups":     groups,
	}
	return &dataResp{header: header, streams: streams}, nil, nil
}

// valuesArray is a channel's values as an Arrow array. Fixed-width numeric
// values are viewed without a copy. A void channel (no raw-data index) is an
// empty null array.
func valuesArray(dt tdms.DataType, vals any) (arrow.Array, error) {
	if dt == tdms.TypeVoid || dt == 0 {
		return array.NewNull(0), nil
	}
	switch s := vals.(type) {
	case []int8:
		return viewArray(arrow.PrimitiveTypes.Int8, s), nil
	case []int16:
		return viewArray(arrow.PrimitiveTypes.Int16, s), nil
	case []int32:
		return viewArray(arrow.PrimitiveTypes.Int32, s), nil
	case []int64:
		return viewArray(arrow.PrimitiveTypes.Int64, s), nil
	case []uint8:
		return viewArray(arrow.PrimitiveTypes.Uint8, s), nil
	case []uint16:
		return viewArray(arrow.PrimitiveTypes.Uint16, s), nil
	case []uint32:
		return viewArray(arrow.PrimitiveTypes.Uint32, s), nil
	case []uint64:
		return viewArray(arrow.PrimitiveTypes.Uint64, s), nil
	case []float32:
		return viewArray(arrow.PrimitiveTypes.Float32, s), nil
	case []float64:
		return viewArray(arrow.PrimitiveTypes.Float64, s), nil
	}
	return buildArrow(dt, vals)
}

// arrayItem packs one built array. Its bytes are the array's buffers.
func arrayItem(arr arrow.Array) *packItem {
	var n int64
	for _, b := range arr.Data().Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	return &packItem{rows: arr.Len(), bytes: n, cols: []packCol{{base: "y", typ: arr.DataType(), build: func() (arrow.Array, error) {
		arr.Retain()
		return arr, nil
	}}}}
}

func typedProps(props []tdms.Property) []map[string]any {
	out := make([]map[string]any, 0, len(props))
	for _, p := range props {
		out = append(out, typedProp(p))
	}
	return out
}

func typedProp(p tdms.Property) map[string]any {
	wire, ok := wireOf(p.Type)
	item := map[string]any{"name": p.Name}
	if !ok {
		item["type"] = p.Type.String()
		item["value"] = fmt.Sprint(p.Value)
		return item
	}
	item["type"] = wire
	switch v := p.Value.(type) {
	case tdms.Timestamp:
		obj := map[string]any{
			"seconds":  v.Seconds,
			"fraction": v.Fraction,
		}
		if t, ok := v.Time(); ok {
			obj["time"] = t.UTC().Format(time.RFC3339Nano)
		}
		item["value"] = obj
	case float32:
		item["value"] = jsonFloat(float64(v))
	case float64:
		item["value"] = jsonFloat(v)
	default:
		item["value"] = v
	}
	return item
}
