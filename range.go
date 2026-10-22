package bridge

import (
	"fmt"
	"math"
	"time"

	"github.com/trey-copeland/gotdms"
	"github.com/trey-copeland/gotdms/dataset"
)

// readLimited reads the merged sample range [offset, offset+limit) using
// Channel.Slice. limit < 0 means through the end of the merged series.
//
// A full read goes through dataset and decodes every selected sample. This
// path does not. Contiguous channels are read at the byte offset of the first
// requested sample, so earlier samples of that channel are not decoded.
//
// File order matches dataset: sample-0 time, then file path. Overlap and gap
// rules match dataset (keep_first drops the incoming sample, gaps are recorded
// and Y is not filled). Relative time uses basis, which is the dataset basis
// or the request basis, never the first sample of this range.
//
// Monotonicity and overlap are checked on the samples this call decodes. A
// break that sits strictly after the requested range is not observed, because
// reading it would decode the tail. A full read still checks the whole axis.
func (s *session) readLimited(sels []dataset.Selector, offset, limit int, basis time.Time) ([]dataset.Series, []spanRead, error) {
	cache := &fileCache{files: map[string]*tdms.File{}, axis: map[string]*axisBuf{}}
	defer cache.close()
	out := make([]dataset.Series, len(sels))
	for i, sel := range sels {
		hits, err := s.occurrences(sel)
		if err != nil {
			return nil, nil, err
		}
		ser, err := s.seriesRange(sel, hits, offset, limit, basis, cache, indexOpts{}, nil)
		if err != nil {
			return nil, nil, err
		}
		out[i] = ser
	}
	return out, cache.reads, nil
}

type fileCache struct {
	files  map[string]*tdms.File
	axis   map[string]*axisBuf
	reads  []spanRead
	scaled map[string][]float64
}

type axisBuf struct {
	times []time.Time
	y     []float64
}

func (c *fileCache) close() {
	for _, f := range c.files {
		f.Close()
	}
}

func (c *fileCache) open(path string) (*tdms.File, error) {
	if f, ok := c.files[path]; ok {
		return f, nil
	}
	f, err := tdms.Open(path)
	if err != nil {
		return nil, err
	}
	c.files[path] = f
	return f, nil
}

func (c *fileCache) note(file, group, name, role string, offset, n, total int) {
	if n <= 0 {
		return
	}
	c.reads = append(c.reads, spanRead{
		File: file, Group: group, Name: name, Role: role,
		Offset: offset, Length: n, ChannelSamples: total,
	})
}

func (s *session) seriesRange(sel dataset.Selector, hits []dataset.Occurrence, offset, limit int, basis time.Time, cache *fileCache, opt indexOpts, nat *nativeCol) (dataset.Series, error) {
	if len(hits) == 0 {
		return dataset.Series{}, fmt.Errorf("%w: %q", dataset.ErrNotFound, sel.Name)
	}
	for i := 1; i < len(hits); i++ {
		if hits[i].Timed != hits[0].Timed || hits[i].Source != hits[0].Source || hits[i].IsTimestamp != hits[0].IsTimestamp {
			return dataset.Series{}, fmt.Errorf("%w: %q group %q has mixed time axes", dataset.ErrArgument, hits[0].Name, hits[0].Group)
		}
	}
	ser := dataset.Series{
		Name:        sel.Name,
		Group:       hits[0].Group,
		Timed:       hits[0].Timed,
		IsTimestamp: hits[0].IsTimestamp,
		Source:      hits[0].Source,
		Basis:       basis,
	}
	if sel.Group != "" {
		ser.Group = sel.Group
	}
	if opt.window && !hits[0].Timed {
		return dataset.Series{}, fmt.Errorf("%w: %q", dataset.ErrUntimed, hits[0].Name)
	}
	if !hits[0].Timed {
		return s.untimedRange(ser, hits, offset, limit, cache, opt, nat)
	}
	if limit >= 0 && offset > math.MaxInt-limit {
		return dataset.Series{}, fmt.Errorf("%w: sample range overflows", dataset.ErrArgument)
	}
	ordered, err := s.orderTimed(hits, cache)
	if err != nil {
		return dataset.Series{}, err
	}
	need := offset + limit
	if limit < 0 {
		need = math.MaxInt / 4
	}
	var state []sampleRef
	for _, o := range ordered {
		if len(state) >= need && s.overlap != dataset.OverlapKeepLast {
			break
		}
		if len(state) >= need && s.overlap == dataset.OverlapKeepLast {
			z, err := s.sampleZero(o, cache)
			if err != nil {
				return dataset.Series{}, err
			}
			last := state[len(state)-1]
			if !overlapsTime(z, last.t, last.dt) {
				break
			}
		}
		times, dt, err := s.loadFileTimes(o, state, need, cache)
		if err != nil {
			return dataset.Series{}, err
		}
		state, err = appendFile(state, o.File, times, dt, s.overlap)
		if err != nil {
			return dataset.Series{}, err
		}
	}
	if opt.window {
		kept := make([]sampleRef, 0, len(state))
		for _, sm := range state {
			if !sm.t.Before(opt.start) && sm.t.Before(opt.end) {
				kept = append(kept, sm)
			}
		}
		state = kept
	}
	if offset > len(state) {
		offset = len(state)
	}
	end := len(state)
	if limit >= 0 && offset+limit < end {
		end = offset + limit
	}
	window := state[offset:end]
	if err := s.fillY(sel, hits, window, cache, opt, nat); err != nil {
		return dataset.Series{}, err
	}
	ser.Y = make([]float64, len(window))
	ser.Times = make([]time.Time, len(window))
	var dts []float64
	var files []string
	seen := map[string]struct{}{}
	for i, sm := range window {
		ser.Y[i] = sm.y
		ser.Times[i] = sm.t
		dts = append(dts, sm.dt)
		if _, ok := seen[sm.file]; !ok {
			seen[sm.file] = struct{}{}
			files = append(files, sm.file)
		}
	}
	ser.Files = files
	ser.Gaps = gapsOf(window)
	if d, ok := uniformSampleDts(dts); ok {
		ser.Dt = d
	}
	return ser, nil
}

func (s *session) untimedRange(ser dataset.Series, hits []dataset.Occurrence, offset, limit int, cache *fileCache, opt indexOpts, nat *nativeCol) (dataset.Series, error) {
	ordered := append([]dataset.Occurrence(nil), hits...)
	sortOccurrences(ordered, func(a, b dataset.Occurrence) bool { return a.File < b.File })
	total := 0
	for _, o := range ordered {
		total += o.Samples
	}
	if limit < 0 {
		limit = total - offset
		if limit < 0 {
			limit = 0
		}
	}
	if offset < 0 {
		offset = 0
	}
	endWant := offset + limit
	pos := 0
	var y []float64
	var files []string
	seen := map[string]struct{}{}
	for _, o := range ordered {
		next := pos + o.Samples
		if next <= offset || o.Samples == 0 {
			pos = next
			continue
		}
		if pos >= endWant {
			break
		}
		local := 0
		if offset > pos {
			local = offset - pos
		}
		n := o.Samples - local
		if pos+local+n > endWant {
			n = endWant - (pos + local)
		}
		f, err := cache.open(o.File)
		if err != nil {
			return dataset.Series{}, err
		}
		g, ok := f.Group(o.Group)
		if !ok {
			return dataset.Series{}, fmt.Errorf("%w: %s", dataset.ErrNotFound, o.Group)
		}
		ch, ok := g.Channel(o.Name)
		if !ok {
			return dataset.Series{}, fmt.Errorf("%w: %s", dataset.ErrNotFound, o.Name)
		}
		part, err := s.takeSamples(ch, o.File, o.Group, o.Name, local, n, cache, opt, nat)
		if err != nil {
			return dataset.Series{}, err
		}
		y = append(y, part...)
		if _, ok := seen[o.File]; !ok {
			seen[o.File] = struct{}{}
			files = append(files, o.File)
		}
		pos = next
	}
	ser.Y = y
	ser.Files = files
	ser.Timed = false
	ser.Times = nil
	return ser, nil
}

type sampleRef struct {
	file  string
	index int
	t     time.Time
	dt    float64
	y     float64
}

func (s *session) orderTimed(hits []dataset.Occurrence, cache *fileCache) ([]dataset.Occurrence, error) {
	type keyed struct {
		o dataset.Occurrence
		z time.Time
	}
	keys := make([]keyed, len(hits))
	for i, o := range hits {
		z, err := s.sampleZero(o, cache)
		if err != nil {
			return nil, err
		}
		keys[i] = keyed{o: o, z: z}
	}
	sortSlice(keys, func(a, b keyed) bool {
		if !a.z.Equal(b.z) {
			return a.z.Before(b.z)
		}
		return a.o.File < b.o.File
	})
	out := make([]dataset.Occurrence, len(keys))
	for i, k := range keys {
		out[i] = k.o
	}
	return out, nil
}

func (s *session) sampleZero(o dataset.Occurrence, cache *fileCache) (time.Time, error) {
	if o.Samples == 0 {
		return time.Time{}, nil
	}
	if o.Source == dataset.TimeWaveform {
		f, err := cache.open(o.File)
		if err != nil {
			return time.Time{}, err
		}
		ch, err := channelOf(f, o.Group, o.Name)
		if err != nil {
			return time.Time{}, err
		}
		t0, ok := ch.StartTime()
		if !ok {
			return time.Time{}, fmt.Errorf("%w: waveform %s/%s", dataset.ErrArgument, o.Group, o.Name)
		}
		return t0, nil
	}
	// A positive catalog step means sample 0 is Occurrence.Start. That value
	// was learned from the first two samples at open. Reading it again here
	// would decode a sample only to sort files.
	if o.Dt > 0 || o.Samples <= 1 {
		return o.Start, nil
	}
	times, err := s.axisPrefix(o, 1, "timestamp", cache)
	if err != nil {
		return time.Time{}, err
	}
	if len(times) == 0 {
		return time.Time{}, nil
	}
	return times[0], nil
}

// loadFileTimes reads the prefix of o required to produce `need` merged samples
// given the samples already kept in state. Waveform times are computed from
// wf_start_time and wf_increment. Timestamp times are Slice calls.
func (s *session) loadFileTimes(o dataset.Occurrence, state []sampleRef, need int, cache *fileCache) ([]time.Time, float64, error) {
	if o.Samples == 0 {
		return nil, 0, nil
	}
	end := 0
	var times []time.Time
	for end < o.Samples {
		step := o.Samples - end
		if len(state) < need {
			want := need - len(state)
			if want < 1 {
				want = 1
			}
			if want < step {
				step = want
			}
		}
		if end == 0 && step < 1 {
			step = 1
		}
		if step < 1 {
			break
		}
		role := "timestamp"
		if o.IsTimestamp {
			role = "data"
		}
		next, err := s.axisPrefix(o, end+step, role, cache)
		if err != nil {
			return nil, 0, err
		}
		times = next
		end = len(times)
		dt := s.pieceDt(o, times, cache)
		trial, err := appendFile(cloneRefs(state), o.File, times, dt, s.overlap)
		if err != nil {
			return nil, 0, err
		}
		if len(trial) >= need || end >= o.Samples {
			return times, dt, nil
		}
	}
	return times, s.pieceDt(o, times, cache), nil
}

func (s *session) pieceDt(o dataset.Occurrence, times []time.Time, cache *fileCache) float64 {
	if o.Source == dataset.TimeWaveform {
		f, err := cache.open(o.File)
		if err != nil {
			return 0
		}
		ch, err := channelOf(f, o.Group, o.Name)
		if err != nil {
			return 0
		}
		if v, ok := ch.Increment(); ok {
			return v
		}
		return 0
	}
	return uniformStep(times)
}

func (s *session) axisPrefix(o dataset.Occurrence, n int, role string, cache *fileCache) ([]time.Time, error) {
	if n > o.Samples {
		n = o.Samples
	}
	if n < 0 {
		n = 0
	}
	if o.Source == dataset.TimeWaveform {
		return s.waveformTimes(o, n, cache)
	}
	key := o.File + "\x00" + o.Group + "\x00" + clockName(o)
	buf := cache.axis[key]
	if buf == nil {
		buf = &axisBuf{}
		cache.axis[key] = buf
	}
	if n <= len(buf.times) {
		return buf.times[:n], nil
	}
	f, err := cache.open(o.File)
	if err != nil {
		return nil, err
	}
	ch, err := channelOf(f, o.Group, clockName(o))
	if err != nil {
		return nil, err
	}
	from := len(buf.times)
	take := n - from
	cache.note(o.File, o.Group, clockName(o), role, from, take, ch.Len())
	extraT, extraY, err := readAxis(ch, from, take, s.epoch, s.epochSet)
	if err != nil {
		return nil, err
	}
	if from > 0 && len(extraT) > 0 && extraT[0].Before(buf.times[from-1]) {
		return nil, fmt.Errorf("%w: sample steps backwards", dataset.ErrMonotonic)
	}
	for i := 1; i < len(extraT); i++ {
		if extraT[i].Before(extraT[i-1]) {
			return nil, fmt.Errorf("%w: sample steps backwards", dataset.ErrMonotonic)
		}
	}
	buf.times = append(buf.times, extraT...)
	buf.y = append(buf.y, extraY...)
	return buf.times[:n], nil
}

func clockName(o dataset.Occurrence) string {
	if o.IsTimestamp || o.Timestamp == "" {
		return o.Name
	}
	return o.Timestamp
}

func (s *session) waveformTimes(o dataset.Occurrence, n int, cache *fileCache) ([]time.Time, error) {
	f, err := cache.open(o.File)
	if err != nil {
		return nil, err
	}
	ch, err := channelOf(f, o.Group, o.Name)
	if err != nil {
		return nil, err
	}
	t0, ok := ch.StartTime()
	if !ok {
		return nil, fmt.Errorf("%w: waveform %s/%s", dataset.ErrArgument, o.Group, o.Name)
	}
	inc := 0.0
	if v, has := ch.Increment(); has {
		inc = v
	}
	times := make([]time.Time, n)
	for i := 0; i < n; i++ {
		tm, err := waveformInstant(t0, inc, i)
		if err != nil {
			return nil, err
		}
		times[i] = tm
	}
	// A negative wf_increment steps backwards on purpose. Dataset keeps those
	// samples; monotonicity is a timestamp-channel rule, not a waveform rule.
	return times, nil
}

func (s *session) fillY(sel dataset.Selector, hits []dataset.Occurrence, window []sampleRef, cache *fileCache, opt indexOpts, nat *nativeCol) error {
	if len(window) == 0 {
		return nil
	}
	byFile := map[string]dataset.Occurrence{}
	for _, h := range hits {
		byFile[h.File] = h
	}
	i := 0
	for i < len(window) {
		file := window[i].file
		j := i + 1
		for j < len(window) && window[j].file == file && window[j].index == window[j-1].index+1 {
			j++
		}
		o := byFile[file]
		start := window[i].index
		n := window[j-1].index - start + 1
		info := s.meta[chKey{file: file, group: o.Group, name: sel.Name}]
		if !info.daqmx && o.IsTimestamp {
			key := o.File + "\x00" + o.Group + "\x00" + clockName(o)
			buf := cache.axis[key]
			if buf == nil || start+n > len(buf.y) {
				if _, err := s.axisPrefix(o, start+n, "data", cache); err != nil {
					return err
				}
				buf = cache.axis[key]
			}
			for k := i; k < j; k++ {
				window[k].y = buf.y[window[k].index]
			}
			i = j
			continue
		}
		f, err := cache.open(file)
		if err != nil {
			return err
		}
		ch, err := channelOf(f, o.Group, sel.Name)
		if err != nil {
			return err
		}
		ys, err := s.takeSamples(ch, file, o.Group, sel.Name, start, n, cache, opt, nat)
		if err != nil {
			return err
		}
		if len(ys) != n {
			return fmt.Errorf("%w: read %d of %d", dataset.ErrArgument, len(ys), n)
		}
		for k := i; k < j; k++ {
			window[k].y = ys[window[k].index-start]
		}
		i = j
	}
	return nil
}

func appendFile(state []sampleRef, file string, times []time.Time, dt float64, policy dataset.OverlapPolicy) ([]sampleRef, error) {
	for i, t := range times {
		same := len(state) > 0 && state[len(state)-1].file == file
		if len(state) > 0 && !same {
			last := state[len(state)-1]
			if overlapsTime(t, last.t, last.dt) {
				switch policy {
				case dataset.OverlapError:
					return nil, fmt.Errorf("%w: sample at %s from %s", dataset.ErrOverlap, t.UTC().Format(time.RFC3339Nano), file)
				case dataset.OverlapKeepFirst:
					continue
				case dataset.OverlapKeepLast:
					for len(state) > 0 && !state[len(state)-1].t.Before(t) {
						state = state[:len(state)-1]
					}
				default:
					return nil, dataset.ErrArgument
				}
			}
		}
		state = append(state, sampleRef{file: file, index: i, t: t, dt: dt})
	}
	return state, nil
}

func gapsOf(samples []sampleRef) []dataset.Gap {
	var gaps []dataset.Gap
	for i := 1; i < len(samples); i++ {
		if samples[i].file == samples[i-1].file {
			continue
		}
		prev := samples[i-1]
		if gapBetweenTime(samples[i].t, prev.t, prev.dt) {
			gaps = append(gaps, dataset.Gap{From: addDt(prev.t, prev.dt), To: samples[i].t})
		}
	}
	return gaps
}

func cloneRefs(in []sampleRef) []sampleRef {
	if len(in) == 0 {
		return nil
	}
	out := make([]sampleRef, len(in))
	copy(out, in)
	return out
}

func channelOf(f *tdms.File, group, name string) (*tdms.Channel, error) {
	g, ok := f.Group(group)
	if !ok {
		return nil, fmt.Errorf("%w: %s", dataset.ErrNotFound, group)
	}
	ch, ok := g.Channel(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s/%s", dataset.ErrNotFound, group, name)
	}
	return ch, nil
}

func readFloatRange(ch *tdms.Channel, i, n int) ([]float64, error) {
	if n == 0 {
		return []float64{}, nil
	}
	v, err := ch.Slice(i, n)
	if err != nil {
		return nil, err
	}
	return anyToFloat(v)
}

func readAxis(ch *tdms.Channel, i, n int, epoch time.Time, epochSet bool) ([]time.Time, []float64, error) {
	if n == 0 {
		return nil, nil, nil
	}
	if ch.DataType() == tdms.TypeTimestamp {
		v, err := ch.Slice(i, n)
		if err != nil {
			return nil, nil, err
		}
		raw, ok := v.([]tdms.Timestamp)
		if !ok {
			return nil, nil, fmt.Errorf("%w: timestamp slice %T", dataset.ErrArgument, v)
		}
		times := make([]time.Time, len(raw))
		y := make([]float64, len(raw))
		for k, ts := range raw {
			tm, ok := ts.Time()
			if !ok {
				return nil, nil, fmt.Errorf("%w: channel %q timestamp %d does not fit in time.Time", dataset.ErrArgument, ch.Name(), i+k)
			}
			times[k] = tm
			y[k] = float64(tm.UnixNano()) / 1e9
		}
		return times, y, nil
	}
	if !numericTimeDT(ch.DataType()) {
		return nil, nil, fmt.Errorf("%w: channel %q is not a timestamp axis", dataset.ErrUntimed, ch.Name())
	}
	if !epochSet {
		return nil, nil, fmt.Errorf("%w: numeric timestamp channel %q requires numeric_epoch", dataset.ErrArgument, ch.Name())
	}
	y, err := readFloatRange(ch, i, n)
	if err != nil {
		return nil, nil, err
	}
	times := make([]time.Time, len(y))
	for k, v := range y {
		tm, err := addSeconds(epoch, v)
		if err != nil {
			return nil, nil, fmt.Errorf("channel %q: %w", ch.Name(), err)
		}
		times[k] = tm
	}
	return times, y, nil
}

func anyToFloat(v any) ([]float64, error) {
	switch s := v.(type) {
	case []float64:
		return append([]float64(nil), s...), nil
	case []float32:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []int8:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []int16:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []int32:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []int64:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []uint8:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []uint16:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []uint32:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []uint64:
		return mapFloat(len(s), func(i int) float64 { return float64(s[i]) }), nil
	case []bool:
		return mapFloat(len(s), func(i int) float64 {
			if s[i] {
				return 1
			}
			return 0
		}), nil
	case nil:
		return []float64{}, nil
	default:
		return nil, fmt.Errorf("%w: cannot convert %T to float64", dataset.ErrArgument, v)
	}
}

func mapFloat(n int, f func(int) float64) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = f(i)
	}
	return out
}

func waveformInstant(start time.Time, inc float64, i int) (time.Time, error) {
	if i == 0 || inc == 0 {
		return start, nil
	}
	return addSeconds(start, float64(i)*inc)
}

func addSeconds(epoch time.Time, sec float64) (time.Time, error) {
	if math.IsNaN(sec) || math.IsInf(sec, 0) {
		return time.Time{}, fmt.Errorf("%w: timestamp seconds are not finite", dataset.ErrArgument)
	}
	ns := sec * 1e9
	if ns >= float64(math.MaxInt64) || ns <= float64(math.MinInt64) {
		return time.Time{}, fmt.Errorf("%w: timestamp seconds do not fit in time.Time", dataset.ErrArgument)
	}
	return epoch.Add(time.Duration(math.Round(ns))), nil
}

func uniformStep(times []time.Time) float64 {
	if len(times) < 2 {
		return 0
	}
	dt := times[1].Sub(times[0]).Seconds()
	if dt <= 0 {
		return 0
	}
	for i := 2; i < len(times); i++ {
		step := times[i].Sub(times[i-1]).Seconds()
		if math.Abs(step-dt) > 1e-9 {
			return 0
		}
	}
	return dt
}

func uniformSampleDts(dts []float64) (float64, bool) {
	if len(dts) == 0 {
		return 0, false
	}
	dt := dts[0]
	for _, d := range dts[1:] {
		if d <= 0 || math.Abs(d-dt) > 1e-9*math.Max(1, math.Abs(dt)) {
			return 0, false
		}
	}
	return dt, true
}

func overlapsTime(t, last time.Time, dt float64) bool {
	if dt > 0 {
		return t.Before(addDt(last, dt).Add(-time.Nanosecond))
	}
	if dt < 0 {
		return t.After(addDt(last, dt).Add(time.Nanosecond))
	}
	return !t.After(last)
}

func gapBetweenTime(t, last time.Time, dt float64) bool {
	if dt <= 0 {
		return false
	}
	return t.After(addDt(last, dt).Add(time.Nanosecond))
}

func addDt(t time.Time, dt float64) time.Time {
	step, ok := roundedDt(dt)
	if !ok {
		return t
	}
	return t.Add(step)
}

func roundedDt(dt float64) (time.Duration, bool) {
	if math.IsNaN(dt) || math.IsInf(dt, 0) {
		return 0, false
	}
	ns := math.Round(dt * 1e9)
	if math.IsNaN(ns) || math.IsInf(ns, 0) || ns == 0 {
		return 0, false
	}
	if ns >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64), true
	}
	if ns <= float64(math.MinInt64) {
		return time.Duration(math.MinInt64), true
	}
	return time.Duration(int64(ns)), true
}

func sortOccurrences(s []dataset.Occurrence, less func(a, b dataset.Occurrence) bool) {
	sortSlice(s, less)
}

func sortSlice[T any](s []T, less func(a, b T) bool) {
	if len(s) < 2 {
		return
	}
	// Local insertion sort keeps the file stable and avoids importing sort
	// twice for two element types. Channel counts per selector are small.
	for i := 1; i < len(s); i++ {
		j := i
		for j > 0 && less(s[j], s[j-1]) {
			s[j], s[j-1] = s[j-1], s[j]
			j--
		}
	}
}
