package bridge

import (
	"fmt"
	"time"

	"github.com/trey-copeland/gotdms"
)

// readValues opens one TDMS file and returns native channel values as Arrow
// IPC. This is the inverse of the write ops. It does not go through the
// dataset layer, which converts numeric channels to float64 and rejects
// strings, bools, and timestamp channels.
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
	var parts [][]byte
	for _, g := range f.Groups() {
		chans := make([]map[string]any, 0, len(g.Channels()))
		for _, ch := range g.Channels() {
			wire, ok := wireOf(ch.DataType())
			if !ok {
				return nil, nil, errCode("argument", fmt.Sprintf("channel %s/%s has unsupported type %s", g.Name(), ch.Name(), ch.DataType()))
			}
			vals, err := ch.Values()
			if err != nil {
				return nil, nil, err
			}
			blob, err := encodeNative(ch.DataType(), vals)
			if err != nil {
				return nil, nil, err
			}
			parts = append(parts, blob)
			chans = append(chans, map[string]any{
				"name":       ch.Name(),
				"type":       wire,
				"ni_type":    ch.DataType().String(),
				"rows":       ch.Len(),
				"properties": typedProps(ch.Properties()),
			})
		}
		groups = append(groups, map[string]any{
			"name":       g.Name(),
			"properties": typedProps(g.Properties()),
			"channels":   chans,
		})
	}
	header := map[string]any{
		"op":         "result",
		"path":       path,
		"properties": typedProps(f.Properties()),
		"groups":     groups,
	}
	return header, joinPayload(parts), nil
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
		item["value"] = float64(v)
	default:
		item["value"] = v
	}
	return item
}
