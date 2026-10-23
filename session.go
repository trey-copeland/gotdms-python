package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/trey-copeland/gotdms"
	"github.com/trey-copeland/gotdms/dataset"
)

// Serve reads protocol frames from r and writes responses to w until the
// peer closes the input or sends close. One process holds one dataset.
func Serve(r io.Reader, w io.Writer) error {
	return ServeParts(r, w, MaxPart)
}

// ServeParts is Serve with a smaller part-frame cap, for tests that want
// many parts from small reads. A cap outside 1..MaxPart means MaxPart.
func ServeParts(r io.Reader, w io.Writer, partCap int) error {
	s := &session{partCap: partCap}
	defer s.abortWrite()
	for {
		f, err := ReadFrame(r)
		if err != nil {
			s.abortWrite()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if f.Version != ProtocolVersion {
			if err := writeErr(w, "bad_version", fmt.Sprintf("protocol version %d, want %d", f.Version, ProtocolVersion), nil); err != nil {
				return err
			}
			continue
		}
		var req map[string]json.RawMessage
		if len(f.Header) > 0 {
			if err := json.Unmarshal(f.Header, &req); err != nil {
				if err := writeErr(w, "argument", err.Error(), nil); err != nil {
					return err
				}
				continue
			}
		}
		op := jsonString(req, "op")
		if op == "close" {
			s.abortWrite()
			s.ds = nil
			return writeResult(w, map[string]any{"op": "result", "closed": true}, nil)
		}
		header, payload, err := s.dispatch(op, req, f.Payload)
		if err != nil {
			code, msg, cands := classify(err)
			if err := writeErr(w, code, msg, cands); err != nil {
				return err
			}
			continue
		}
		if d, ok := header.(*dataResp); ok {
			if err := writeData(w, s.partCap, d); err != nil {
				return err
			}
			continue
		}
		if err := writeResult(w, header, payload); err != nil {
			return err
		}
	}
}

type session struct {
	ds       *dataset.Dataset
	meta     map[chKey]scaleInfo
	files    []fileMeta
	groups   []groupMeta
	epoch    time.Time
	epochSet bool
	overlap  dataset.OverlapPolicy
	wr       *fileWrite
	partCap  int
	// occByName indexes occDS's occurrences by channel name. It is rebuilt
	// when ds changes. A session opens one dataset ("dataset already open"
	// otherwise) and ends at close, and Dataset has no mutating methods, so
	// the index is built once and never goes stale.
	occByName map[string][]dataset.Occurrence
	occDS     *dataset.Dataset
	// cat is the compact catalog, built once at open.
	cat *catalogIndex
}

func (s *session) dispatch(op string, req map[string]json.RawMessage, payload []byte) (any, []byte, error) {
	switch op {
	case "write_begin", "write_chunk", "write_end":
		header, out, err := s.dispatchWrite(op, req, payload)
		if err != nil {
			s.abortWrite()
			return nil, nil, err
		}
		return header, out, nil
	case "read_values":
		if s.wr != nil {
			return nil, nil, errCode("argument", "finish the TDMS write first")
		}
		return readValues(jsonString(req, "path"))
	case "open":
		if s.wr != nil {
			return nil, nil, errCode("argument", "finish the TDMS write first")
		}
		return s.open(req)
	case "catalog":
		return s.catalog()
	case "find":
		if err := s.need(); err != nil {
			return nil, nil, err
		}
		return map[string]any{"op": "result", "channels": s.filterName(s.ds.Find(jsonString(req, "name")))}, nil, nil
	case "search":
		if err := s.need(); err != nil {
			return nil, nil, err
		}
		return map[string]any{"op": "result", "channels": s.filterName(s.ds.Search(jsonString(req, "text")))}, nil, nil
	case "match":
		if err := s.need(); err != nil {
			return nil, nil, err
		}
		hits, err := s.ds.Match(jsonString(req, "pattern"))
		if err != nil {
			return nil, nil, err
		}
		return map[string]any{"op": "result", "channels": s.filterName(hits)}, nil, nil
	case "describe":
		return s.describe(req)
	case "read":
		sel := dataset.Selector{Name: jsonString(req, "name"), Group: jsonString(req, "group")}
		return s.finishRead([]dataset.Selector{sel}, req)
	case "read_many":
		var specs []struct {
			Name  string `json:"name"`
			Group string `json:"group"`
		}
		if raw, ok := req["channels"]; ok {
			if err := json.Unmarshal(raw, &specs); err != nil {
				return nil, nil, errCode("argument", err.Error())
			}
		}
		sels := make([]dataset.Selector, len(specs))
		for i, sp := range specs {
			sels[i] = dataset.Selector{Name: sp.Name, Group: sp.Group}
		}
		return s.finishRead(sels, req)
	default:
		return nil, nil, errCode("argument", "unknown op "+op)
	}
}

func (s *session) need() error {
	if s.ds == nil {
		return errCode("argument", "open a dataset first")
	}
	return nil
}

func (s *session) open(req map[string]json.RawMessage) (any, []byte, error) {
	if s.ds != nil {
		return nil, nil, errCode("argument", "dataset already open")
	}
	s.epoch = time.Time{}
	s.epochSet = false
	s.overlap = dataset.OverlapKeepFirst
	var paths []string
	if raw, ok := req["paths"]; ok {
		if err := json.Unmarshal(raw, &paths); err != nil {
			return nil, nil, errCode("argument", err.Error())
		}
	}
	dir := jsonString(req, "directory")
	pattern := jsonString(req, "pattern")
	var opts []dataset.Option
	if n := jsonInt(req, "workers"); n > 0 {
		opts = append(opts, dataset.WithWorkers(n))
	}
	if n := jsonInt(req, "read_workers"); n > 0 {
		opts = append(opts, dataset.WithReadWorkers(n))
	}
	if name, ok := jsonStringOK(req, "timestamp_channel"); ok {
		opts = append(opts, dataset.WithTimestampChannelName(name))
	}
	if raw, ok := req["timestamp_channels"]; ok {
		var per map[string]string
		if err := json.Unmarshal(raw, &per); err != nil {
			return nil, nil, errCode("argument", err.Error())
		}
		for g, name := range per {
			opts = append(opts, dataset.WithTimestampChannel(g, name))
		}
	}
	if raw, ok := req["numeric_epoch"]; ok && string(raw) != "null" {
		t, err := parseTime(raw)
		if err != nil {
			return nil, nil, err
		}
		s.epoch = t
		s.epochSet = true
		opts = append(opts, dataset.WithNumericEpoch(t))
	}
	if raw, ok := req["basis"]; ok && string(raw) != "null" {
		t, err := parseTime(raw)
		if err != nil {
			return nil, nil, err
		}
		opts = append(opts, dataset.WithBasisTime(t))
	}
	if p, ok := jsonStringOK(req, "overlap"); ok {
		switch p {
		case "keep_first", "":
			s.overlap = dataset.OverlapKeepFirst
			opts = append(opts, dataset.WithOverlap(dataset.OverlapKeepFirst))
		case "keep_last":
			s.overlap = dataset.OverlapKeepLast
			opts = append(opts, dataset.WithOverlap(dataset.OverlapKeepLast))
		case "error":
			s.overlap = dataset.OverlapError
			opts = append(opts, dataset.WithOverlap(dataset.OverlapError))
		default:
			return nil, nil, errCode("argument", "overlap "+p)
		}
	}
	var ds *dataset.Dataset
	var err error
	var wait func() map[string]*fileScan
	switch {
	case dir != "":
		if pattern == "" {
			pattern = "*.tdms"
		}
		// The same glob OpenGlob runs. A file that appears or vanishes in
		// between is reconciled by path in scanMetadata.
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		wait = prescan(matches)
		ds, err = dataset.OpenGlob(dir, pattern, opts...)
	case len(paths) > 0:
		wait = prescan(paths)
		ds, err = dataset.OpenFiles(paths, opts...)
	default:
		return nil, nil, errCode("argument", "open needs paths or directory")
	}
	// Always wait, so no file handle outlives open.
	pre := wait()
	if err != nil {
		return nil, nil, err
	}
	meta, files, groups, err := scanMetadata(ds.Files(), pre)
	if err != nil {
		return nil, nil, err
	}
	s.ds = ds
	s.meta = meta
	s.files = files
	s.groups = groups
	s.cat = nil
	if err := s.buildCatalog(); err != nil {
		s.ds = nil
		return nil, nil, err
	}
	return s.catalog()
}

func (s *session) filterName(in []dataset.Occurrence) []channelJSON {
	return s.enrich(in)
}

// channelHead and channelCore are a channel row without its file and
// properties, split so the file key sits after group as it always has.
type channelHead struct {
	Name  string `json:"name"`
	Group string `json:"group"`
}

type channelCore struct {
	IsTimestamp  bool      `json:"is_timestamp"`
	Samples      int       `json:"samples"`
	Timed        bool      `json:"timed"`
	Source       string    `json:"source"`
	Start        string    `json:"start,omitempty"`
	Last         string    `json:"last,omitempty"`
	Dt           float64   `json:"dt"`
	Timestamp    string    `json:"timestamp,omitempty"`
	DataType     string    `json:"data_type,omitempty"`
	Unit         string    `json:"unit,omitempty"`
	HasScale     bool      `json:"has_scale"`
	Scale        *scaleOut `json:"scale,omitempty"`
	HasNonlinear bool      `json:"has_nonlinear_scale"`
	DAQmx        bool      `json:"daqmx,omitempty"`
	Digital      bool      `json:"digital,omitempty"`
	DAQmxScale   bool      `json:"daqmx_scale,omitempty"`
}

// channelJSON is a full channel row: find, search, match, and describe.
type channelJSON struct {
	channelHead
	File string `json:"file"`
	channelCore
	Properties map[string]any `json:"properties,omitempty"`
}

func (s *session) enrich(in []dataset.Occurrence) []channelJSON {
	out := make([]channelJSON, len(in))
	for i, o := range in {
		core, info, ok := s.core(o)
		c := channelJSON{channelHead: channelHead{Name: o.Name, Group: o.Group}, File: o.File, channelCore: core}
		if ok {
			c.Properties = info.properties
		}
		out[i] = c
	}
	return out
}

func (s *session) readMany(sels []dataset.Selector, req map[string]json.RawMessage) ([]*seriesEnc, error) {
	encs, _, _, err := s.readTables(sels, req)
	return encs, err
}

// finishRead plans a data reply: one clock column per distinct time axis
// and one y column (plus relative_s) per channel, packed into streams by
// packStreams, clock streams first. reads lists channel slices this call
// decoded. limited is false for a dataset read of the whole span.
func (s *session) finishRead(sels []dataset.Selector, req map[string]json.RawMessage) (any, []byte, error) {
	encs, reads, limited, err := s.readTables(sels, req)
	if err != nil {
		return nil, nil, err
	}
	if reads == nil {
		reads = []spanRead{}
	}
	d, err := planRead(encs)
	if err != nil {
		return nil, nil, err
	}
	if err := s.compactRead(d, encs, reads); err != nil {
		return nil, nil, err
	}
	d.header["limited"] = limited
	return d, nil, nil
}

type clockOut struct {
	ID      int `json:"id"`
	Samples int `json:"samples"`
	Stream  int `json:"stream"`
	Batch   int `json:"batch"`
	Column  int `json:"column"`
}

// clockSet gives each distinct time axis one id. Series that point to the
// same dataset.Clock share without a comparison. Any other two axes share
// only when their nanoseconds are equal element for element, so a shared id
// is always proven, never assumed.
type clockSet struct {
	byPtr map[*dataset.Clock]int
	byKey map[[3]int64][]int
	ns    [][]int64
}

func (c *clockSet) id(e *seriesEnc) int {
	if e.ser.Clock != nil {
		if id, ok := c.byPtr[e.ser.Clock]; ok {
			return id
		}
	}
	ns := e.timeNs()
	key := [3]int64{int64(len(ns))}
	if len(ns) > 0 {
		key[1], key[2] = ns[0], ns[len(ns)-1]
	}
	id := -1
	for _, cand := range c.byKey[key] {
		if slices.Equal(c.ns[cand], ns) {
			id = cand
			break
		}
	}
	if id < 0 {
		id = len(c.ns)
		c.ns = append(c.ns, ns)
		c.byKey[key] = append(c.byKey[key], id)
	}
	if e.ser.Clock != nil {
		c.byPtr[e.ser.Clock] = id
	}
	return id
}

func planRead(encs []*seriesEnc) (*dataResp, error) {
	clocks := &clockSet{byPtr: map[*dataset.Clock]int{}, byKey: map[[3]int64][]int{}}
	clockOf := make([]int, len(encs))
	for i, e := range encs {
		clockOf[i] = -1
		if e.cols.absolute && e.hasTimes() {
			clockOf[i] = clocks.id(e)
			if !e.cols.relative {
				// The clock holds the nanoseconds now. Times are only needed
				// again for relative_s.
				e.ser.Times = nil
			}
		}
	}
	d := &dataResp{}
	clockItems := make([]*packItem, len(clocks.ns))
	for id, ns := range clocks.ns {
		ns := ns
		clockItems[id] = &packItem{
			rows:  len(ns),
			bytes: int64(len(ns)) * 8,
			cols: []packCol{{base: "time_ns", typ: arrow.PrimitiveTypes.Int64, build: func() (arrow.Array, error) {
				return viewArray(arrow.PrimitiveTypes.Int64, ns), nil
			}}},
		}
	}
	st, err := packStreams("clock", clockItems, 0)
	if err != nil {
		return nil, err
	}
	d.streams = append(d.streams, st...)
	clockList := make([]clockOut, len(clocks.ns))
	for id, it := range clockItems {
		clockList[id] = clockOut{ID: id, Samples: it.rows, Stream: it.ref.Stream, Batch: it.ref.Batch, Column: it.ref.Column}
	}
	items := make([]*packItem, len(encs))
	for i, e := range encs {
		items[i] = e.packItem()
	}
	st, err = packStreams("channel", items, len(d.streams))
	if err != nil {
		return nil, err
	}
	d.streams = append(d.streams, st...)
	metas := make([]channelOut, len(encs))
	for i, e := range encs {
		e.meta.Columns = columnNames(false, e.cols.relative)
		if clockOf[i] >= 0 {
			id := clockOf[i]
			e.meta.Clock = &id
		}
		ref := items[i].ref
		e.meta.Stream, e.meta.Batch, e.meta.Column = &ref.Stream, &ref.Batch, &ref.Column
		if e.cols.relative {
			rc := ref.Column + 1
			e.meta.RelColumn = &rc
		}
		metas[i] = e.meta
	}
	d.header = map[string]any{"channels": metas, "clocks": clockList}
	return d, nil
}

func (s *session) readTables(sels []dataset.Selector, req map[string]json.RawMessage) ([]*seriesEnc, []spanRead, bool, error) {
	if err := s.need(); err != nil {
		return nil, nil, false, err
	}
	if len(sels) == 0 {
		return nil, nil, false, errCode("argument", "no channels")
	}
	cols := columnSet{absolute: jsonBool(req, "absolute", true), relative: jsonBool(req, "relative", true)}
	scaled := jsonBool(req, "scaled", false)
	basis := s.ds.Basis()
	// Series of one read that use the same clock share one []int64 of Unix
	// nanoseconds instead of a []time.Time each.
	ro := []dataset.ReadOption{dataset.WithSharedClock()}
	// A reply with neither absolute nor relative time sends only y, so the
	// read builds no time axis at all.
	valuesOnly := !cols.absolute && !cols.relative
	if valuesOnly {
		ro = append(ro, dataset.WithoutTimes())
	}
	if raw, ok := req["basis"]; ok && string(raw) != "null" {
		t, err := parseTime(raw)
		if err != nil {
			return nil, nil, false, err
		}
		basis = t
		ro = append(ro, dataset.WithReadBasis(t))
	}
	start, end, window, err := windowOf(req)
	if err != nil {
		return nil, nil, false, err
	}
	offset, limit, limited, err := sampleBound(req)
	if err != nil {
		return nil, nil, false, err
	}
	if limited && window {
		return nil, nil, false, errCode("argument", "sample limit and time window are mutually exclusive")
	}
	daqmxAt := map[int]struct{}{}
	for i, sel := range sels {
		ok, err := s.selectorDAQmx(sel)
		if err != nil {
			return nil, nil, limited, err
		}
		if ok {
			daqmxAt[i] = struct{}{}
		}
	}
	var series []dataset.Series
	var natives []nativeCol
	var reads []spanRead
	switch {
	case limited && len(daqmxAt) == 0:
		series, reads, err = s.readLimited(sels, offset, limit, basis)
	case limited:
		series, natives, reads, err = s.readIndexed(sels, offset, limit, basis, indexOpts{scale: scaled, native: true})
	case len(daqmxAt) == 0:
		series, err = s.loadSeries(sels, start, end, window, ro)
		if err == nil {
			reads = s.fullReads(sels)
		}
	default:
		series, natives, reads, err = s.readMixed(sels, daqmxAt, start, end, window, basis, ro, scaled)
	}
	if err != nil {
		return nil, nil, limited, err
	}
	if seriesHook != nil {
		seriesHook(series)
	}
	encs := make([]*seriesEnc, len(series))
	for i := range series {
		if valuesOnly {
			// Offset/limit and DAQmx reads still build times to order merged
			// samples. The reply sends none, so drop them before streaming.
			series[i].Times, series[i].Clock = nil, nil
		}
		ser := series[i]
		info, match, propsAt, err := s.scaleFor(ser, scaled)
		if err != nil {
			return nil, nil, limited, err
		}
		var nat *nativeCol
		if natives != nil {
			nat = &natives[i]
		}
		encs[i], err = prepareSeries(ser, cols, info, scaled, match, nat)
		if err != nil {
			return nil, nil, limited, err
		}
		encs[i].propsAt = propsAt
	}
	return encs, reads, limited, nil
}

// seriesHook sees each read's series before they are encoded. Tests only.
var seriesHook func([]dataset.Series)

func (s *session) loadSeries(sels []dataset.Selector, start, end time.Time, window bool, ro []dataset.ReadOption) ([]dataset.Series, error) {
	if window {
		// One ReadMany: dataset opens each file once and decodes a shared
		// clock once. The bridge does not decode the clock again.
		return s.ds.ReadMany(sels, start, end, ro...)
	}
	timed := make([]dataset.Selector, 0, len(sels))
	untimed := map[int]dataset.Selector{}
	for i, sel := range sels {
		hits, err := s.occurrences(sel)
		if err != nil {
			return nil, err
		}
		if hits[0].Timed {
			timed = append(timed, sel)
		} else {
			untimed[i] = sel
		}
	}
	out := make([]dataset.Series, len(sels))
	if len(timed) > 0 {
		w0, w1, ok := s.spanOf(timed)
		var got []dataset.Series
		var err error
		if ok {
			got, err = s.ds.ReadMany(timed, w0, w1, ro...)
		} else {
			got = make([]dataset.Series, len(timed))
			for i, sel := range timed {
				got[i], err = s.ds.Read(sel, ro...)
				if err != nil {
					return nil, err
				}
			}
		}
		if err != nil {
			return nil, err
		}
		ti := 0
		for i := range sels {
			if _, skip := untimed[i]; skip {
				continue
			}
			out[i] = got[ti]
			ti++
		}
	}
	for i, sel := range untimed {
		ser, err := s.ds.Read(sel, ro...)
		if err != nil {
			return nil, err
		}
		out[i] = ser
	}
	return out, nil
}

func (s *session) spanOf(sels []dataset.Selector) (time.Time, time.Time, bool) {
	var start, last time.Time
	set := false
	for _, sel := range sels {
		hits, err := s.occurrences(sel)
		if err != nil {
			return time.Time{}, time.Time{}, false
		}
		for _, o := range hits {
			if !o.Timed || o.Samples == 0 {
				continue
			}
			if !set || o.Start.Before(start) {
				start = o.Start
			}
			if !set || o.Last.After(last) {
				last = o.Last
			}
			set = true
		}
	}
	if !set {
		return time.Time{}, time.Time{}, false
	}
	return start, last.Add(time.Nanosecond), true
}

func (s *session) occurrences(sel dataset.Selector) ([]dataset.Occurrence, error) {
	if sel.Name == "" {
		return nil, errCode("argument", "empty channel name")
	}
	var hits []dataset.Occurrence
	groups := map[string]struct{}{}
	for _, o := range s.occurrencesNamed(sel.Name) {
		if sel.Group != "" && o.Group != sel.Group {
			continue
		}
		hits = append(hits, o)
		groups[o.Group] = struct{}{}
	}
	if len(hits) == 0 {
		return nil, fmt.Errorf("%w: %q", dataset.ErrNotFound, sel.Name)
	}
	if sel.Group == "" && len(groups) > 1 {
		return nil, &dataset.AmbiguousError{Name: sel.Name, Candidates: hits}
	}
	return hits, nil
}

// occurrencesNamed returns the open dataset's occurrences of one channel
// name, in catalog order. The index is built once per open dataset.
// Dataset.Occurrences copies the whole catalog on every call, and a read
// looks each selector up several times, so scanning it per lookup made a
// read of n channels cost O(n^2): about 85% of the bridge's time on a
// 785-channel file.
func (s *session) occurrencesNamed(name string) []dataset.Occurrence {
	if s.occDS != s.ds || s.occByName == nil {
		all := s.ds.Occurrences()
		idx := make(map[string][]dataset.Occurrence, len(all))
		for _, o := range all {
			idx[o.Name] = append(idx[o.Name], o)
		}
		s.occByName, s.occDS = idx, s.ds
	}
	return s.occByName[name]
}

// scaleFor picks the scale and properties of ser from its first file that
// has the channel and checks the other files against it. propsAt is that
// occurrence's catalog index, or -1. Properties match when every file has
// the same interned property ids, which is the same as equal JSON.
func (s *session) scaleFor(ser dataset.Series, scaled bool) (info scaleInfo, propsMatch bool, propsAt int, err error) {
	propsAt = -1
	files := ser.Files
	if len(files) == 0 {
		hits, err := s.occurrences(dataset.Selector{Name: ser.Name, Group: ser.Group})
		if err != nil {
			return scaleInfo{}, false, -1, err
		}
		for _, h := range hits {
			files = append(files, h.File)
		}
	}
	set := false
	propsMatch = true
	for _, f := range files {
		key := chKey{file: f, group: ser.Group, name: ser.Name}
		m, ok := s.meta[key]
		if !ok {
			continue
		}
		if !set {
			info = m
			set = true
			if at, ok := s.cat.occAt[key]; ok {
				propsAt = at
			}
			continue
		}
		if !slices.Equal(m.propIDs, info.propIDs) {
			propsMatch = false
		}
		if m.hasLinear != info.hasLinear || m.slope != info.slope || m.intercept != info.intercept || m.nonlinear != info.nonlinear {
			if scaled {
				return scaleInfo{}, false, -1, errCode("scale_mismatch", "linear scale differs across files of "+ser.Group+"/"+ser.Name)
			}
		}
		if (m.daqmx || info.daqmx) && (m.daqmx != info.daqmx || m.daqmxKey != info.daqmxKey || m.willScale != info.willScale || m.digital != info.digital || m.tdmsType != info.tdmsType) {
			if scaled {
				return scaleInfo{}, false, -1, errCode("scale_mismatch", "DAQmx scale differs across files of "+ser.Group+"/"+ser.Name)
			}
		}
	}
	if !set {
		return scaleInfo{}, true, -1, nil
	}
	if info.daqmx {
		if scaled {
			if info.nonlinear {
				return scaleInfo{}, false, -1, errCode("nonlinear_scale", "channel "+ser.Group+"/"+ser.Name+" has a DAQmx scale this bridge does not apply")
			}
			if !info.willScale {
				return scaleInfo{}, false, -1, errCode("no_scale", "channel "+ser.Group+"/"+ser.Name+" has no DAQmx scale")
			}
		}
		return info, propsMatch, propsAt, nil
	}
	if scaled {
		if info.nonlinear && !info.hasLinear {
			return scaleInfo{}, false, -1, errCode("nonlinear_scale", "channel "+ser.Group+"/"+ser.Name+" has a non-linear scale; gotdms does not apply it")
		}
		if !info.hasLinear {
			return scaleInfo{}, false, -1, errCode("no_scale", "channel "+ser.Group+"/"+ser.Name+" has no NI linear scale")
		}
	}
	return info, propsMatch, propsAt, nil
}

func windowOf(req map[string]json.RawMessage) (time.Time, time.Time, bool, error) {
	_, hasS := req["start"]
	_, hasE := req["end"]
	if !hasS && !hasE {
		return time.Time{}, time.Time{}, false, nil
	}
	if string(req["start"]) == "null" && string(req["end"]) == "null" {
		return time.Time{}, time.Time{}, false, nil
	}
	if !hasS || !hasE || string(req["start"]) == "null" || string(req["end"]) == "null" {
		return time.Time{}, time.Time{}, false, errCode("argument", "window needs both start and end")
	}
	start, err := parseTime(req["start"])
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	end, err := parseTime(req["end"])
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	return start, end, true, nil
}

func parseTime(raw json.RawMessage) (time.Time, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return time.Time{}, errCode("argument", "time must be an RFC3339 string")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errCode("argument", err.Error())
	}
	return t, nil
}

type coded struct {
	code string
	msg  string
}

func (e *coded) Error() string { return e.msg }

func errCode(code, msg string) error { return &coded{code: code, msg: msg} }

// isCorrupt reports whether err means the TDMS bytes themselves are bad:
// a wrong segment tag, an unknown version, a structure that contradicts the
// format, or a file that ends early. Those are sent with code "corrupt" so a
// caller can tell a bad file from a bad argument.
func isCorrupt(err error) bool {
	return errors.Is(err, tdms.ErrBadTag) ||
		errors.Is(err, tdms.ErrVersion) ||
		errors.Is(err, tdms.ErrCorrupt) ||
		errors.Is(err, tdms.ErrTruncated)
}

func classify(err error) (string, string, []channelJSON) {
	var c *coded
	if errors.As(err, &c) {
		return c.code, c.msg, nil
	}
	var amb *dataset.AmbiguousError
	if errors.As(err, &amb) {
		out := make([]channelJSON, len(amb.Candidates))
		for i, o := range amb.Candidates {
			out[i] = channelJSON{
				channelHead: channelHead{Name: o.Name, Group: o.Group},
				channelCore: channelCore{
					IsTimestamp: o.IsTimestamp, Samples: o.Samples, Timed: o.Timed,
					Source: o.Source.String(), Dt: o.Dt, Timestamp: o.Timestamp,
				},
				File: o.File,
			}
		}
		return "ambiguous", amb.Error(), out
	}
	// A missing path is the caller's mistake on every entry point, the same
	// as asArg reports it for read_values and append.
	if errors.Is(err, os.ErrNotExist) {
		return "argument", err.Error(), nil
	}
	if isCorrupt(err) {
		return "corrupt", err.Error(), nil
	}
	switch {
	case errors.Is(err, dataset.ErrUntimed):
		return "untimed", err.Error(), nil
	case errors.Is(err, dataset.ErrNotFound):
		return "not_found", err.Error(), nil
	case errors.Is(err, dataset.ErrNoFiles):
		return "no_files", err.Error(), nil
	case errors.Is(err, dataset.ErrOverlap):
		return "overlap", err.Error(), nil
	case errors.Is(err, dataset.ErrMonotonic):
		return "monotonic", err.Error(), nil
	case errors.Is(err, dataset.ErrArgument):
		return "argument", err.Error(), nil
	default:
		return "error", err.Error(), nil
	}
}

func writeErr(w io.Writer, code, msg string, cands []channelJSON) error {
	h := map[string]any{"op": "error", "code": code, "message": msg}
	if len(cands) > 0 {
		h["candidates"] = cands
	}
	b, err := json.Marshal(h)
	if err != nil {
		return err
	}
	return WriteFrame(w, ProtocolVersion, b, nil)
}

// writeResult sends a result frame. A header that cannot be encoded, or a
// payload too large for one frame, is sent as an error reply so the session
// stays usable.
func writeResult(w io.Writer, header any, payload []byte) error {
	b, err := json.Marshal(header)
	if err != nil {
		return writeErr(w, "error", "encode result: "+err.Error(), nil)
	}
	if err := checkPayloadLen(uint64(len(payload))); err != nil {
		return writeErr(w, "error", err.Error(), nil)
	}
	return WriteFrame(w, ProtocolVersion, restoreNonFinite(b), payload)
}

func jsonString(req map[string]json.RawMessage, key string) string {
	s, _ := jsonStringOK(req, key)
	return s
}

func jsonStringOK(req map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := req[key]
	if !ok || string(raw) == "null" {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func jsonInt(req map[string]json.RawMessage, key string) int {
	raw, ok := req[key]
	if !ok {
		return 0
	}
	var n int
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0
	}
	return n
}

func jsonBool(req map[string]json.RawMessage, key string, def bool) bool {
	raw, ok := req[key]
	if !ok {
		return def
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return def
	}
	return b
}
