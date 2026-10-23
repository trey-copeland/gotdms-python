package bridge

import (
	"fmt"
	"os"
	"path/filepath"
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
	// propIDs are the properties interned in the session's property
	// table, in name order. Set by buildCatalog.
	propIDs []int
	samples int
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

// fileScan is one file's metadata: its properties, its groups, and a
// scaleInfo per channel in file order.
type fileScan struct {
	file     fileMeta
	groups   []groupMeta
	channels []chKey
	infos    []scaleInfo
	// size and mod are the file's stat from before it was parsed. A prescan
	// is used only if the file still has them after dataset.Open, so a file
	// written to in between (a live acquisition) is parsed again after the
	// open, as it was before prescan.
	size int64
	mod  time.Time
}

// unchanged reports whether path still has the size and modification time
// fs saw before parsing it.
func (fs *fileScan) unchanged(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode().IsRegular() && st.Size() == fs.size && st.ModTime().Equal(fs.mod)
}

// scanFile parses one file's metadata. It does not read samples.
func scanFile(path string) (*fileScan, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	f, err := tdms.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fs := &fileScan{file: fileMeta{Path: path, Properties: propsToMap(f.Properties())}, size: st.Size(), mod: st.ModTime()}
	for _, g := range f.Groups() {
		fs.groups = append(fs.groups, groupMeta{
			Name:       g.Name(),
			File:       path,
			Properties: propsToMap(g.Properties()),
		})
		for _, ch := range g.Channels() {
			// Properties, DataType, Unit, LinearScale, Len, DAQmx, and
			// DAQmxScaling are metadata. Values, Float64s, Times, Slice,
			// and Cursor are not called.
			fs.channels = append(fs.channels, chKey{file: path, group: ch.GroupName(), name: ch.Name()})
			fs.infos = append(fs.infos, inspectChannel(ch))
		}
	}
	return fs, nil
}

// scanFiles runs scanFile on every path, one file after another. Results
// are in path order; errs[i] is path i's error. It is sequential on
// purpose: the prescan already overlaps dataset.Open, which indexes files
// in parallel, and parsing files in parallel here as well raised the
// bridge's peak with the number of files (about +8 MB median on a 16-file
// glob) for a few ms.
func scanFiles(paths []string) ([]*fileScan, []error) {
	out := make([]*fileScan, len(paths))
	errs := make([]error, len(paths))
	for i, p := range paths {
		out[i], errs[i] = scanFile(p)
	}
	return out, errs
}

// scanMetadata parses each file's metadata and merges it in
// path order. pre holds scans made before the paths were known (see
// session.open); a path found there is not parsed again unless its size or
// modification time changed since that scan.
func scanMetadata(paths []string, pre map[string]*fileScan) (map[chKey]scaleInfo, []fileMeta, []groupMeta, error) {
	scans := make([]*fileScan, len(paths))
	var todo []string
	var at []int
	for i, p := range paths {
		if fs, ok := pre[p]; ok && fs.unchanged(p) {
			scans[i] = fs
			continue
		}
		todo = append(todo, p)
		at = append(at, i)
	}
	got, errs := scanFiles(todo)
	for j, i := range at {
		if errs[j] != nil {
			return nil, nil, nil, errs[j]
		}
		scans[i] = got[j]
	}
	n := 0
	for _, fs := range scans {
		n += len(fs.infos)
	}
	channels := make(map[chKey]scaleInfo, n)
	files := make([]fileMeta, 0, len(scans))
	var groups []groupMeta
	for _, fs := range scans {
		files = append(files, fs.file)
		groups = append(groups, fs.groups...)
		for k, key := range fs.channels {
			channels[key] = fs.infos[k]
		}
	}
	return channels, files, groups, nil
}

// prescan parses the metadata of paths in the background while
// dataset.Open indexes the same files. Core v0.2.0 has no hook that hands
// the bridge the *tdms.File it already parsed, so each file is still parsed
// twice; this only overlaps the two parses. Keys are filepath.Abs, the
// form Dataset.Files uses. A path that fails is left out and is parsed
// again, with its error, by scanMetadata. Call the returned func to wait.
func prescan(paths []string) func() map[string]*fileScan {
	if len(paths) == 0 {
		return func() map[string]*fileScan { return nil }
	}
	done := make(chan map[string]*fileScan, 1)
	go func() {
		abs := make([]string, 0, len(paths))
		seen := make(map[string]struct{}, len(paths))
		for _, p := range paths {
			a, err := filepath.Abs(p)
			if err != nil || p == "" {
				continue
			}
			if _, ok := seen[a]; ok {
				continue
			}
			seen[a] = struct{}{}
			abs = append(abs, a)
		}
		got, errs := scanFiles(abs)
		out := make(map[string]*fileScan, len(abs))
		for i, a := range abs {
			if errs[i] == nil {
				out[a] = got[i]
			}
		}
		done <- out
	}()
	return func() map[string]*fileScan { return <-done }
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

// scaleTypeProps are NI_Scale[0..8]_Scale_Type, built once rather than
// formatted nine times per channel at open.
var scaleTypeProps = func() []string {
	out := make([]string, 9)
	for i := range out {
		out[i] = fmt.Sprintf("NI_Scale[%d]_Scale_Type", i)
	}
	return out
}()

func nonlinearScale(ch *tdms.Channel) bool {
	for _, name := range scaleTypeProps {
		p, ok := ch.Property(name)
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
