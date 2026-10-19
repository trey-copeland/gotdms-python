package bridge

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/trey-copeland/gotdms/dataset"
)

// Serve reads protocol frames from r and writes responses to w until the
// peer closes the input or sends close. One process holds one dataset.
func Serve(r io.Reader, w io.Writer) error {
	s := &session{}
	for {
		f, err := ReadFrame(r)
		if err != nil {
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
			s.ds = nil
			return writeResult(w, map[string]any{"op": "result", "closed": true}, nil)
		}
		header, payload, err := s.dispatch(op, req)
		if err != nil {
			code, msg, cands := classify(err)
			if err := writeErr(w, code, msg, cands); err != nil {
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
}

func (s *session) dispatch(op string, req map[string]json.RawMessage) (any, []byte, error) {
	switch op {
	case "open":
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
	switch {
	case dir != "":
		if pattern == "" {
			pattern = "*.tdms"
		}
		ds, err = dataset.OpenGlob(dir, pattern, opts...)
	case len(paths) > 0:
		ds, err = dataset.OpenFiles(paths, opts...)
	default:
		return nil, nil, errCode("argument", "open needs paths or directory")
	}
	if err != nil {
		return nil, nil, err
	}
	meta, files, groups, err := scanMetadata(ds.Files())
	if err != nil {
		return nil, nil, err
	}
	s.ds = ds
	s.meta = meta
	s.files = files
	s.groups = groups
	return s.catalog()
}

func (s *session) catalog() (any, []byte, error) {
	if err := s.need(); err != nil {
		return nil, nil, err
	}
	return map[string]any{
		"op":       "result",
		"basis":    s.ds.Basis().UTC().Format(time.RFC3339Nano),
		"files":    s.files,
		"groups":   s.groups,
		"channels": s.enrich(s.ds.Occurrences()),
	}, nil, nil
}

func (s *session) filterName(in []dataset.Occurrence) []channelJSON {
	return s.enrich(in)
}

type channelJSON struct {
	Name         string         `json:"name"`
	Group        string         `json:"group"`
	File         string         `json:"file"`
	IsTimestamp  bool           `json:"is_timestamp"`
	Samples      int            `json:"samples"`
	Timed        bool           `json:"timed"`
	Source       string         `json:"source"`
	Start        string         `json:"start,omitempty"`
	Last         string         `json:"last,omitempty"`
	Dt           float64        `json:"dt"`
	Timestamp    string         `json:"timestamp,omitempty"`
	DataType     string         `json:"data_type,omitempty"`
	Unit         string         `json:"unit,omitempty"`
	HasScale     bool           `json:"has_scale"`
	Scale        *scaleOut      `json:"scale,omitempty"`
	HasNonlinear bool           `json:"has_nonlinear_scale"`
	Properties   map[string]any `json:"properties,omitempty"`
}

func (s *session) enrich(in []dataset.Occurrence) []channelJSON {
	out := make([]channelJSON, len(in))
	for i, o := range in {
		c := channelJSON{
			Name:        o.Name,
			Group:       o.Group,
			File:        o.File,
			IsTimestamp: o.IsTimestamp,
			Samples:     o.Samples,
			Timed:       o.Timed,
			Source:      o.Source.String(),
			Dt:          o.Dt,
			Timestamp:   o.Timestamp,
		}
		if o.Timed && o.Samples > 0 {
			c.Start = o.Start.UTC().Format(time.RFC3339Nano)
			c.Last = o.Last.UTC().Format(time.RFC3339Nano)
		}
		if info, ok := s.meta[chKey{file: o.File, group: o.Group, name: o.Name}]; ok {
			c.DataType = info.dataType
			c.Unit = info.unit
			c.HasScale = info.hasLinear
			c.HasNonlinear = info.nonlinear
			c.Properties = info.properties
			if info.hasLinear {
				c.Scale = &scaleOut{Slope: info.slope, Intercept: info.intercept, Index: info.index}
			}
		}
		out[i] = c
	}
	return out
}

func (s *session) readMany(sels []dataset.Selector, req map[string]json.RawMessage) ([]table, error) {
	tables, _, _, err := s.readTables(sels, req)
	return tables, err
}

// finishRead packs channel tables and attaches the sample-range log.
// reads lists channel slices this call decoded. limited is false for a
// dataset read of the whole span. Old clients ignore both fields.
func (s *session) finishRead(sels []dataset.Selector, req map[string]json.RawMessage) (any, []byte, error) {
	tables, reads, limited, err := s.readTables(sels, req)
	if err != nil {
		return nil, nil, err
	}
	header, payload, err := packTables(tables)
	if err != nil {
		return nil, nil, err
	}
	h, ok := header.(map[string]any)
	if !ok {
		return nil, nil, errCode("error", "internal header type")
	}
	if reads == nil {
		reads = []spanRead{}
	}
	h["reads"] = reads
	h["limited"] = limited
	return h, payload, nil
}

func (s *session) readTables(sels []dataset.Selector, req map[string]json.RawMessage) ([]table, []spanRead, bool, error) {
	if err := s.need(); err != nil {
		return nil, nil, false, err
	}
	if len(sels) == 0 {
		return nil, nil, false, errCode("argument", "no channels")
	}
	cols := columnSet{absolute: jsonBool(req, "absolute", true), relative: jsonBool(req, "relative", true)}
	scaled := jsonBool(req, "scaled", false)
	basis := s.ds.Basis()
	var ro []dataset.ReadOption
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
	var series []dataset.Series
	var reads []spanRead
	if limited {
		series, reads, err = s.readLimited(sels, offset, limit, basis)
	} else {
		series, err = s.loadSeries(sels, start, end, window, ro)
		if err == nil {
			reads = s.fullReads(sels)
		}
	}
	if err != nil {
		return nil, nil, limited, err
	}
	tables := make([]table, len(series))
	for i, ser := range series {
		info, match, err := s.scaleFor(ser, scaled)
		if err != nil {
			return nil, nil, limited, err
		}
		tables[i], err = encodeSeries(ser, cols, info, scaled && info.hasLinear, match)
		if err != nil {
			return nil, nil, limited, err
		}
	}
	return tables, reads, limited, nil
}

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
	for _, o := range s.ds.Occurrences() {
		if o.Name != sel.Name {
			continue
		}
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

func (s *session) scaleFor(ser dataset.Series, scaled bool) (scaleInfo, bool, error) {
	files := ser.Files
	if len(files) == 0 {
		hits, err := s.occurrences(dataset.Selector{Name: ser.Name, Group: ser.Group})
		if err != nil {
			return scaleInfo{}, false, err
		}
		for _, h := range hits {
			files = append(files, h.File)
		}
	}
	var info scaleInfo
	set := false
	propsMatch := true
	var first []byte
	for _, f := range files {
		m, ok := s.meta[chKey{file: f, group: ser.Group, name: ser.Name}]
		if !ok {
			continue
		}
		blob, err := json.Marshal(m.properties)
		if err != nil {
			return scaleInfo{}, false, err
		}
		if first == nil {
			first = blob
			info = m
			set = true
			continue
		}
		if !bytes.Equal(first, blob) {
			propsMatch = false
		}
		if m.hasLinear != info.hasLinear || m.slope != info.slope || m.intercept != info.intercept || m.nonlinear != info.nonlinear {
			if scaled {
				return scaleInfo{}, false, errCode("scale_mismatch", "linear scale differs across files of "+ser.Group+"/"+ser.Name)
			}
		}
	}
	if !set {
		return scaleInfo{}, true, nil
	}
	if scaled {
		if info.nonlinear && !info.hasLinear {
			return scaleInfo{}, false, errCode("nonlinear_scale", "channel "+ser.Group+"/"+ser.Name+" has a non-linear scale; gotdms does not apply it")
		}
		if !info.hasLinear {
			return scaleInfo{}, false, errCode("no_scale", "channel "+ser.Group+"/"+ser.Name+" has no NI linear scale")
		}
	}
	return info, propsMatch, nil
}

func packTables(tables []table) (any, []byte, error) {
	metas := make([]channelOut, len(tables))
	parts := make([][]byte, len(tables))
	for i, t := range tables {
		metas[i] = t.meta
		parts[i] = t.ipc
	}
	header := map[string]any{"op": "result", "channels": metas}
	if len(parts) == 0 {
		return header, nil, nil
	}
	return header, joinPayload(parts), nil
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
				Name: o.Name, Group: o.Group, File: o.File,
				IsTimestamp: o.IsTimestamp, Samples: o.Samples, Timed: o.Timed,
				Source: o.Source.String(), Dt: o.Dt, Timestamp: o.Timestamp,
			}
		}
		return "ambiguous", amb.Error(), out
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

func writeResult(w io.Writer, header any, payload []byte) error {
	b, err := json.Marshal(header)
	if err != nil {
		return err
	}
	return WriteFrame(w, ProtocolVersion, b, payload)
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
