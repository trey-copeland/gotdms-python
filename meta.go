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
	// tdmsType is the channel data type. For DAQmx it is the primary scaler.
	tdmsType tdms.DataType
	daqmx    bool
	digital  bool
	// willScale is true when scaled=True should call DAQmxScaledFloat64s.
	// That is a linear or polynomial chain, a scaler selected as the scale
	// output, or NI_Scaling_Status "scaled" (samples are already engineering
	// units). It is false when there is nothing to apply.
	willScale      bool
	daqmxPrescaled bool
	daqmxKey       string
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
				channels[chKey{file: path, group: ch.GroupName(), name: ch.Name()}] = inspectChannel(ch)
				// Properties, DataType, Unit, LinearScale, Len, DAQmx, and
				// DAQmxScaling are metadata. Values, Float64s, Times, Slice,
				// and Cursor are not called.
			}
		}
		f.Close()
	}
	return channels, files, groups, nil
}

// inspectChannel reads scale and DAQmx metadata. It does not read samples.
//
// A DAQmx channel's data type is the primary scaler. Digital lines are uint8.
// willScale is the scaled=True decision: call DAQmxScaledFloat64s when a
// linear or polynomial chain exists, when the scale output is a raw scaler,
// or when NI_Scaling_Status is "scaled". No scale leaves willScale false so
// scaled=True raises NoScaleError. An unsupported scale type is nonlinear
// and is not applied. Ordinary channels are unchanged.
func inspectChannel(ch *tdms.Channel) scaleInfo {
	info := scaleInfo{
		dataType:   ch.DataType().String(),
		properties: propsToMap(ch.Properties()),
		samples:    ch.Len(),
		tdmsType:   ch.DataType(),
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
	raw, ok := ch.DAQmx()
	if !ok {
		return info
	}
	info.daqmx = true
	if raw.Primary >= 0 && raw.Primary < len(raw.Scalers) {
		info.digital = raw.Scalers[raw.Primary].Digital
	}
	sc := ch.DAQmxScaling()
	info.daqmxKey = daqmxKey(sc)
	prescaled, apply, unsupported := daqmxPlan(sc)
	info.daqmxPrescaled = prescaled
	info.willScale = apply && !unsupported
	if unsupported {
		info.hasLinear = false
		info.nonlinear = true
		info.slope = 0
		info.intercept = 0
		info.index = 0
		return info
	}
	if prescaled || !apply {
		info.hasLinear = false
		info.nonlinear = false
		info.slope = 0
		info.intercept = 0
		info.index = 0
		return info
	}
	info.nonlinear = false
	if !daqmxOutputLinear(sc) {
		info.hasLinear = false
		info.slope = 0
		info.intercept = 0
		info.index = 0
	}
	return info
}

// daqmxPlan reports how scaled=True treats this channel.
// prescaled means NI_Scaling_Status is "scaled": the stored samples are
// already engineering units. apply means DAQmxScaledFloat64s should run.
// unsupported means the chain uses a scale type other than linear or polynomial.
func daqmxPlan(sc tdms.DAQmxScaling) (prescaled, apply, unsupported bool) {
	if strings.EqualFold(sc.Status, "scaled") {
		return true, true, false
	}
	if sc.HasCount && sc.Count == 0 {
		return false, false, false
	}
	if !sc.HasCount && len(sc.Scales) == 0 {
		return false, false, false
	}
	for _, s := range sc.Scales {
		switch strings.ToLower(s.Type) {
		case "linear", "polynomial":
		default:
			if s.Type != "" {
				unsupported = true
			}
		}
	}
	if unsupported {
		return false, true, true
	}
	return false, true, false
}

func daqmxOutputLinear(sc tdms.DAQmxScaling) bool {
	final := -1
	if sc.HasCount {
		if sc.Count <= 0 {
			return false
		}
		final = sc.Count - 1
	} else {
		for _, s := range sc.Scales {
			if s.Index > final {
				final = s.Index
			}
		}
	}
	for _, s := range sc.Scales {
		if s.Index == final && strings.EqualFold(s.Type, "linear") {
			return true
		}
	}
	return false
}

func daqmxKey(sc tdms.DAQmxScaling) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s|%d|%t|", sc.Status, sc.Count, sc.HasCount)
	for _, s := range sc.Scales {
		fmt.Fprintf(&b, "%d:%s:%g:%g:%d:%t:%v;", s.Index, s.Type, s.Slope, s.Intercept, s.InputSource, s.InputRaw, s.Coefficients)
	}
	return b.String()
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
	case float32:
		return jsonFloat(float64(v))
	case float64:
		return jsonFloat(v)
	case string, bool,
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
