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
//
// Each group is read with Group.Values. That is the one-pass split of a
// shared interleaved segment. A channel that continues in a later segment
// is written at its own sample index. Channel.Values would also place
// those samples, but it reads each channel on its own.
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
		channels := g.Channels()
		vals, err := g.Values()
		if err != nil {
			return nil, nil, err
		}
		if len(vals) != len(channels) {
			return nil, nil, errCode("argument", fmt.Sprintf("group %s returned %d value sets for %d channels", g.Name(), len(vals), len(channels)))
		}
		chans := make([]map[string]any, 0, len(channels))
		for i, ch := range channels {
			wire, ok := wireOf(ch.DataType())
			if !ok {
				return nil, nil, errCode("argument", fmt.Sprintf("channel %s/%s has unsupported type %s", g.Name(), ch.Name(), ch.DataType()))
			}
			blob, err := encodeNative(ch.DataType(), vals[i])
			if err != nil {
				return nil, nil, err
			}
			parts = append(parts, blob)
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
		item["value"] = jsonFloat(float64(v))
	case float64:
		item["value"] = jsonFloat(v)
	default:
		item["value"] = v
	}
	return item
}
