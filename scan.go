package bridge

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/trey-copeland/gotdms"
	"github.com/trey-copeland/gotdms/dataset"
)

// spanRead is one Slice the bridge issued while serving a read.
// Length is the number of samples decoded, not the channel length.
// Role is "data" for a requested channel and "timestamp" for a clock read
// only so a data channel could be placed on its time axis.
type spanRead struct {
	File           string `json:"file"`
	Group          string `json:"group"`
	Name           string `json:"name"`
	Role           string `json:"role"`
	Offset         int    `json:"offset"`
	Length         int    `json:"length"`
	ChannelSamples int    `json:"channel_samples"`
}

type parsedSource struct {
	paths    []string
	dir      string
	pattern  string
	epoch    time.Time
	epochSet bool
	overlap  string
	tsName   string
	tsSet    bool
	ts       map[string]string
}

// parseSource reads the same source fields as open. It does not open files.
func parseSource(req map[string]json.RawMessage) (parsedSource, error) {
	var spec parsedSource
	spec.overlap = "keep_first"
	if raw, ok := req["paths"]; ok {
		if err := json.Unmarshal(raw, &spec.paths); err != nil {
			return parsedSource{}, errCode("argument", err.Error())
		}
	}
	spec.dir = jsonString(req, "directory")
	spec.pattern = jsonString(req, "pattern")
	if name, ok := jsonStringOK(req, "timestamp_channel"); ok {
		spec.tsName = name
		spec.tsSet = true
	}
	if raw, ok := req["timestamp_channels"]; ok {
		if err := json.Unmarshal(raw, &spec.ts); err != nil {
			return parsedSource{}, errCode("argument", err.Error())
		}
	}
	if raw, ok := req["numeric_epoch"]; ok && string(raw) != "null" {
		t, err := parseTime(raw)
		if err != nil {
			return parsedSource{}, err
		}
		spec.epoch = t
		spec.epochSet = true
	}
	if p, ok := jsonStringOK(req, "overlap"); ok && p != "" {
		switch p {
		case "keep_first", "keep_last", "error":
			spec.overlap = p
		default:
			return parsedSource{}, errCode("argument", "overlap "+p)
		}
	}
	return spec, nil
}

func resolvePaths(spec parsedSource) ([]string, error) {
	switch {
	case spec.dir != "":
		pattern := spec.pattern
		if pattern == "" {
			pattern = "*.tdms"
		}
		matches, err := filepath.Glob(filepath.Join(spec.dir, pattern))
		if err != nil {
			return nil, errCode("argument", err.Error())
		}
		if len(matches) == 0 {
			return nil, fmt.Errorf("%w: %s", dataset.ErrNoFiles, filepath.Join(spec.dir, pattern))
		}
		return cleanPaths(matches)
	case len(spec.paths) > 0:
		return cleanPaths(spec.paths)
	default:
		return nil, errCode("argument", "needs paths or directory")
	}
}

func cleanPaths(paths []string) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	for _, p := range paths {
		if p == "" {
			return nil, errCode("argument", "empty path")
		}
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[abs]; ok {
			continue
		}
		if _, err := os.Stat(abs); err != nil {
			return nil, err
		}
		seen[abs] = struct{}{}
		out = append(out, abs)
	}
	if len(out) == 0 {
		return nil, dataset.ErrNoFiles
	}
	return out, nil
}

// describe returns catalog metadata without decoding channel samples.
//
// dataset.Open reads the first two samples of a declared clock (or the whole
// clock when the first step is not positive). scan_tdms uses describe instead,
// so building a LazyFrame does not touch raw sample bytes. Groups, channels,
// stored types, sample counts, scales, and waveform properties are metadata.
// A declared timestamp channel is recognized from the caller's name, not from
// the channel's name. Numeric clocks still require numeric_epoch; that check
// uses the stored type and does not read samples.
func (s *session) describe(req map[string]json.RawMessage) (any, []byte, error) {
	spec, err := parseSource(req)
	if err != nil {
		return nil, nil, err
	}
	paths, err := resolvePaths(spec)
	if err != nil {
		return nil, nil, err
	}
	var files []fileMeta
	var groups []groupMeta
	var channels []channelJSON
	sampleReads := 0
	for _, path := range paths {
		fm, gs, chs, n, err := describeFile(path, spec)
		if err != nil {
			return nil, nil, err
		}
		sampleReads += n
		files = append(files, fm)
		groups = append(groups, gs...)
		channels = append(channels, chs...)
	}
	if sampleReads != 0 {
		return nil, nil, errCode("protocol", "describe decoded channel sample data")
	}
	sort.Slice(channels, func(i, j int) bool {
		if channels[i].File != channels[j].File {
			return channels[i].File < channels[j].File
		}
		if channels[i].Group != channels[j].Group {
			return channels[i].Group < channels[j].Group
		}
		return channels[i].Name < channels[j].Name
	})
	return map[string]any{
		"op":           "result",
		"sample_reads": sampleReads,
		"files":        files,
		"groups":       groups,
		"channels":     channels,
	}, nil, nil
}

func describeFile(path string, spec parsedSource) (fileMeta, []groupMeta, []channelJSON, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return fileMeta{}, nil, nil, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fileMeta{}, nil, nil, 0, err
	}
	regions, err := rawRegions(f, st.Size())
	if err != nil {
		return fileMeta{}, nil, nil, 0, err
	}
	cr := &countReader{r: f, size: st.Size(), regions: regions}
	tf, err := tdms.NewReader(cr)
	if err != nil {
		return fileMeta{}, nil, nil, cr.sampleReads, err
	}
	if cr.sampleReads != 0 {
		return fileMeta{}, nil, nil, cr.sampleReads, errCode("protocol", "describe decoded channel sample data in "+path)
	}
	fm := fileMeta{Path: path, Properties: propsToMap(tf.Properties())}
	var groups []groupMeta
	var channels []channelJSON
	for _, g := range tf.Groups() {
		groups = append(groups, groupMeta{
			Name:       g.Name(),
			File:       path,
			Properties: propsToMap(g.Properties()),
		})
		name, mode := declaredClock(spec, g.Name())
		byName := map[string]*tdms.Channel{}
		var chans []*tdms.Channel
		for _, ch := range g.Channels() {
			chans = append(chans, ch)
			byName[ch.Name()] = ch
		}
		var tsCh *tdms.Channel
		tsUsable := false
		if mode == clockNamed {
			tsCh = byName[name]
			if tsCh != nil {
				ok, err := clockUsable(tsCh, spec.epochSet)
				if err != nil {
					return fileMeta{}, nil, nil, cr.sampleReads, err
				}
				tsUsable = ok
			}
		}
		for _, ch := range chans {
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
			c := channelJSON{
				Name:         ch.Name(),
				Group:        ch.GroupName(),
				File:         path,
				Samples:      ch.Len(),
				DataType:     info.dataType,
				Unit:         info.unit,
				HasScale:     info.hasLinear,
				HasNonlinear: info.nonlinear,
				Properties:   info.properties,
				Source:       dataset.TimeNone.String(),
			}
			if info.hasLinear {
				c.Scale = &scaleOut{Slope: info.slope, Intercept: info.intercept, Index: info.index}
			}
			switch mode {
			case clockNamed:
				if ch.Name() == name {
					c.IsTimestamp = true
					if tsUsable {
						c.Timed = true
						c.Source = dataset.TimeTimestamp.String()
					}
				} else if tsCh != nil && tsUsable && tsCh.Len() == ch.Len() {
					c.Timed = true
					c.Source = dataset.TimeTimestamp.String()
					c.Timestamp = name
				}
			case clockOptOut, clockNone:
				timed, start, last, dt, err := waveformMeta(ch)
				if err != nil {
					return fileMeta{}, nil, nil, cr.sampleReads, err
				}
				if timed {
					c.Timed = true
					c.Source = dataset.TimeWaveform.String()
					c.Dt = dt
					if ch.Len() > 0 {
						c.Start = start.UTC().Format(time.RFC3339Nano)
						c.Last = last.UTC().Format(time.RFC3339Nano)
					}
				}
			}
			channels = append(channels, c)
			// Len, properties, DataType, Unit, LinearScale, StartTime, and
			// Increment are metadata. Values, Float64s, Slice, and Cursor are not called.
		}
	}
	if cr.sampleReads != 0 {
		return fileMeta{}, nil, nil, cr.sampleReads, errCode("protocol", "describe decoded channel sample data in "+path)
	}
	return fm, groups, channels, cr.sampleReads, nil
}

const (
	clockNone = iota
	clockNamed
	clockOptOut
)

func declaredClock(spec parsedSource, group string) (string, int) {
	if spec.ts != nil {
		if n, ok := spec.ts[group]; ok {
			if n == "" {
				return "", clockOptOut
			}
			return n, clockNamed
		}
	}
	if spec.tsSet {
		return spec.tsName, clockNamed
	}
	return "", clockNone
}

func clockUsable(ch *tdms.Channel, epochSet bool) (bool, error) {
	if ch == nil {
		return false, nil
	}
	if ch.DataType() == tdms.TypeTimestamp {
		return true, nil
	}
	if !numericTimeDT(ch.DataType()) {
		return false, nil
	}
	if ch.Len() == 0 {
		return true, nil
	}
	if !epochSet {
		return false, errCode("argument", fmt.Sprintf("numeric timestamp channel %q in group %q requires numeric_epoch", ch.Name(), ch.GroupName()))
	}
	return true, nil
}

func numericTimeDT(dt tdms.DataType) bool {
	switch dt {
	case tdms.TypeInt8, tdms.TypeInt16, tdms.TypeInt32, tdms.TypeInt64,
		tdms.TypeUint8, tdms.TypeUint16, tdms.TypeUint32, tdms.TypeUint64,
		tdms.TypeFloat32, tdms.TypeFloat64,
		tdms.TypeFloat32WithUnit, tdms.TypeFloat64WithUnit:
		return true
	default:
		return false
	}
}

func waveformMeta(ch *tdms.Channel) (timed bool, start, last time.Time, dt float64, err error) {
	t0, ok := ch.StartTime()
	if !ok {
		return false, time.Time{}, time.Time{}, 0, nil
	}
	inc := 0.0
	if v, has := ch.Increment(); has {
		inc = v
	}
	if math.IsNaN(inc) || math.IsInf(inc, 0) {
		return false, time.Time{}, time.Time{}, 0, errCode("argument", "wf_increment of "+ch.Path())
	}
	if ch.Len() == 0 {
		return true, t0, t0, inc, nil
	}
	tLast, err := addSeconds(t0, float64(ch.Len()-1)*inc)
	if err != nil {
		return false, time.Time{}, time.Time{}, 0, err
	}
	if inc == 0 {
		return true, t0, t0, 0, nil
	}
	if tLast.Before(t0) {
		return true, tLast, t0, inc, nil
	}
	return true, t0, tLast, inc, nil
}

// rawRegions lists byte ranges that hold channel samples. Lead-in and metadata
// bytes are not included. describe counts ReadAt calls that touch these ranges.
func rawRegions(r io.ReaderAt, size int64) ([][2]int64, error) {
	if size < 28 {
		return nil, fmt.Errorf("%w: short file", tdms.ErrTruncated)
	}
	var regions [][2]int64
	pos := int64(0)
	for pos < size {
		if size-pos < 28 {
			return nil, fmt.Errorf("%w: trailing bytes", tdms.ErrTruncated)
		}
		var hdr [28]byte
		if _, err := r.ReadAt(hdr[:], pos); err != nil {
			return nil, err
		}
		if string(hdr[0:4]) != "TDSm" {
			return nil, fmt.Errorf("%w: bad tag", tdms.ErrBadTag)
		}
		toc := binary.LittleEndian.Uint32(hdr[4:8])
		bo := binary.ByteOrder(binary.LittleEndian)
		if toc&(1<<6) != 0 {
			bo = binary.BigEndian
		}
		next := bo.Uint64(hdr[12:20])
		rawOff := bo.Uint64(hdr[20:28])
		if rawOff > uint64(math.MaxInt64) || (next != ^uint64(0) && next > uint64(math.MaxInt64)) {
			return nil, tdms.ErrCorrupt
		}
		dataPos := pos + 28 + int64(rawOff)
		var segEnd int64
		if next == ^uint64(0) {
			segEnd = size
		} else {
			segEnd = pos + 28 + int64(next)
		}
		if dataPos < segEnd {
			regions = append(regions, [2]int64{dataPos, segEnd})
		}
		if next == ^uint64(0) {
			break
		}
		if segEnd <= pos {
			return nil, tdms.ErrCorrupt
		}
		pos = segEnd
	}
	return regions, nil
}

type countReader struct {
	r           io.ReaderAt
	size        int64
	regions     [][2]int64
	sampleReads int
}

func (c *countReader) Size() int64 { return c.size }

func (c *countReader) ReadAt(p []byte, off int64) (int, error) {
	if len(p) > 0 {
		end := off + int64(len(p))
		for _, reg := range c.regions {
			if end > reg[0] && off < reg[1] {
				c.sampleReads++
				break
			}
		}
	}
	return c.r.ReadAt(p, off)
}

func sampleBound(req map[string]json.RawMessage) (offset, limit int, limited bool, err error) {
	_, hasOff := req["offset"]
	_, hasLim := req["limit"]
	if !hasOff && !hasLim {
		return 0, 0, false, nil
	}
	if hasOff && string(req["offset"]) != "null" {
		offset = jsonInt(req, "offset")
		if offset < 0 {
			return 0, 0, false, errCode("argument", "offset must be >= 0")
		}
		limited = true
	}
	if !hasLim || string(req["limit"]) == "null" {
		if !limited {
			return 0, 0, false, nil
		}
		return offset, -1, true, nil
	}
	limit = jsonInt(req, "limit")
	if limit < 0 {
		return 0, 0, false, errCode("argument", "limit must be >= 0")
	}
	return offset, limit, true, nil
}

// fullReads is the channel list a dataset read decodes in full.
// It is not a measured Slice log. Unselected channels are absent.
// Declared clocks for the selected channels are included once per file.
func (s *session) fullReads(sels []dataset.Selector) []spanRead {
	var out []spanRead
	seenTS := map[string]struct{}{}
	for _, sel := range sels {
		hits, err := s.occurrences(sel)
		if err != nil {
			continue
		}
		for _, o := range hits {
			out = append(out, spanRead{
				File: o.File, Group: o.Group, Name: o.Name, Role: "data",
				Offset: 0, Length: o.Samples, ChannelSamples: o.Samples,
			})
			if o.Source != dataset.TimeTimestamp || o.IsTimestamp || o.Timestamp == "" {
				continue
			}
			key := o.File + "\x00" + o.Group + "\x00" + o.Timestamp
			if _, ok := seenTS[key]; ok {
				continue
			}
			seenTS[key] = struct{}{}
			out = append(out, spanRead{
				File: o.File, Group: o.Group, Name: o.Timestamp, Role: "timestamp",
				Offset: 0, Length: o.Samples, ChannelSamples: o.Samples,
			})
		}
	}
	if out == nil {
		return []spanRead{}
	}
	return out
}
