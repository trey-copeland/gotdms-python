package bridge

import (
	"fmt"
	"strings"
	"time"

	"github.com/trey-copeland/gotdms"
)

type scaleInfo struct {
	hasLinear  bool
	nonlinear  bool
	slope      float64
	intercept  float64
	index      int
	unit       string
	dataType   string
	properties map[string]any
	samples    int
}

type chKey struct {
	file  string
	group string
	name  string
}

type fileMeta struct {
	Path       string         `json:"path"`
	Properties map[string]any `json:"properties"`
}

type groupMeta struct {
	Name       string         `json:"name"`
	File       string         `json:"file"`
	Properties map[string]any `json:"properties"`
}

func scanMetadata(paths []string) (map[chKey]scaleInfo, []fileMeta, []groupMeta, error) {
	channels := make(map[chKey]scaleInfo)
	var files []fileMeta
	var groups []groupMeta
	for _, path := range paths {
		f, err := tdms.Open(path)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, fileMeta{Path: path, Properties: propsToMap(f.Properties())})
		for _, g := range f.Groups() {
			groups = append(groups, groupMeta{
				Name:       g.Name(),
				File:       path,
				Properties: propsToMap(g.Properties()),
			})
			for _, ch := range g.Channels() {
				info := scaleInfo{
					dataType:   ch.DataType().String(),
					properties: propsToMap(ch.Properties()),
					samples:    ch.Len(),
				}
				if u, ok := ch.Unit(); ok {
					info.unit = u
				}
				if sc, ok := ch.LinearScale(); ok {
					info.hasLinear = true
					info.slope = sc.Slope
					info.intercept = sc.Intercept
					info.index = sc.Index
				}
				info.nonlinear = nonlinearScale(ch)
				channels[chKey{file: path, group: ch.GroupName(), name: ch.Name()}] = info
				// Properties, DataType, Unit, LinearScale, and Len are metadata.
				// Values, Float64s, Times, and Cursor are not called.
			}
		}
		f.Close()
	}
	return channels, files, groups, nil
}

func nonlinearScale(ch *tdms.Channel) bool {
	for i := 0; i <= 8; i++ {
		p, ok := ch.Property(fmt.Sprintf("NI_Scale[%d]_Scale_Type", i))
		if !ok {
			continue
		}
		s, ok := p.Value.(string)
		if !ok || s == "" {
			continue
		}
		if !strings.EqualFold(s, "Linear") {
			return true
		}
	}
	return false
}

func propsToMap(props []tdms.Property) map[string]any {
	if len(props) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(props))
	for _, p := range props {
		out[p.Name] = propJSON(p)
	}
	return out
}

func propJSON(p tdms.Property) any {
	switch v := p.Value.(type) {
	case string, bool, float32, float64,
		int8, int16, int32, int64,
		uint8, uint16, uint32, uint64:
		return v
	case tdms.Timestamp:
		m := map[string]any{
			"seconds":  v.Seconds,
			"fraction": v.Fraction,
		}
		if t, ok := v.Time(); ok {
			m["time"] = t.UTC().Format(time.RFC3339Nano)
		}
		return m
	default:
		return fmt.Sprint(v)
	}
}
