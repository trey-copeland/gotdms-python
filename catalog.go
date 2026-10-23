package bridge

import (
	"encoding/json"
	"math"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/trey-copeland/gotdms/dataset"
)

// The open and catalog replies are compact. A wide file repeats the same
// path on every channel and mostly the same properties (wf_increment,
// unit_string, NI_Scale_*), so the catalog sends each once:
//
//   - files[i] is file i. groups[].file and channels[].file are indexes
//     into files.
//   - property_table[k] is one {"name", "value"} pair, unique by name and
//     encoded value. channels[].properties lists table indexes in name
//     order, and is absent when the channel has no properties.
//
// Read headers point back into this catalog instead of repeating paths and
// properties; see readChannel. The Python client expands the catalog to the
// shape it had before (one path string and one properties dict per
// channel), so Session.open, Session.catalog, Channel.properties, and
// Series.meta are unchanged. find, search, match, and describe still send
// full rows (channelJSON); they are small.

// propTable interns channel property pairs.
type propTable struct {
	ids     map[string]int
	entries []json.RawMessage
}

// intern returns the ids of props, in name order.
func (t *propTable) intern(props map[string]any) ([]int, error) {
	if len(props) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(props))
	for k := range props {
		names = append(names, k)
	}
	sort.Strings(names)
	ids := make([]int, len(names))
	var buf []byte
	for i, name := range names {
		v, err := appendValueJSON(buf[:0], props[name])
		if err != nil {
			return nil, err
		}
		buf = v
		key := name + "\x00" + string(v)
		id, ok := t.ids[key]
		if !ok {
			n, err := json.Marshal(name)
			if err != nil {
				return nil, err
			}
			entry := make([]byte, 0, len(n)+len(v)+18)
			entry = append(entry, `{"name":`...)
			entry = append(entry, n...)
			entry = append(entry, `,"value":`...)
			entry = append(entry, v...)
			entry = append(entry, '}')
			id = len(t.entries)
			t.ids[key] = id
			t.entries = append(t.entries, entry)
		}
		ids[i] = id
	}
	return ids, nil
}

// appendValueJSON appends the JSON of a propJSON value: the bytes
// json.Marshal produces, without its reflection for the common scalar
// types. Anything else goes through json.Marshal.
func appendValueJSON(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case string:
		if plainJSONString(x) {
			b = append(b, '"')
			b = append(b, x...)
			return append(b, '"'), nil
		}
	case bool:
		return strconv.AppendBool(b, x), nil
	case int8:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int16:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(b, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(b, x, 10), nil
	case uint8:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint16:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(b, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(b, x, 10), nil
	case float64:
		if !math.IsNaN(x) && !math.IsInf(x, 0) {
			return appendFloatJSON(b, x), nil
		}
	case map[string]any:
		// A timestamp: {"fraction", "seconds", "time"}. Keys in order,
		// like json.Marshal.
		keys := make([]string, 0, len(x))
		for k := range x {
			if !plainJSONString(k) {
				goto slow
			}
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b = append(b, '{')
		for i, k := range keys {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, '"')
			b = append(b, k...)
			b = append(b, '"', ':')
			var err error
			if b, err = appendValueJSON(b, x[k]); err != nil {
				return nil, err
			}
		}
		return append(b, '}'), nil
	}
slow:
	j, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, j...), nil
}

// plainJSONString reports whether s needs no escaping in encoding/json:
// printable ASCII other than the quote, backslash, and the HTML characters
// json.Marshal escapes.
func plainJSONString(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x7f || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			return false
		}
	}
	return true
}

// appendFloatJSON formats a finite float64 the way encoding/json does
// (the ES6 number format).
func appendFloatJSON(b []byte, f float64) []byte {
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		n := len(b)
		if n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return b
}

// catalogGroup is groupMeta with the file interned.
type catalogGroup struct {
	Name       string         `json:"name"`
	File       int            `json:"file"`
	Properties map[string]any `json:"properties"`
}

// catalogChannel is channelJSON with the file and properties interned.
type catalogChannel struct {
	channelHead
	File int `json:"file"`
	channelCore
	Properties []int `json:"properties,omitempty"`
}

// catalogIndex is built once per open. occAt maps an occurrence to its
// index in catalog channels; fileAt maps a path to its index in files.
type catalogIndex struct {
	props    propTable
	groups   []catalogGroup
	channels []catalogChannel
	occAt    map[chKey]int
	fileAt   map[string]int
}

// buildCatalog interns properties into s.meta (scaleInfo.propIDs) and
// builds the catalog rows. Property ids are assigned in catalog order, then
// for any scanned channel the dataset does not list, in key order, so the
// table is deterministic. scaleFor compares the ids of every scanned
// channel, so all of them are interned.
func (s *session) buildCatalog() error {
	occ := s.ds.Occurrences()
	c := &catalogIndex{
		props:  propTable{ids: map[string]int{}, entries: []json.RawMessage{}},
		occAt:  make(map[chKey]int, len(occ)),
		fileAt: make(map[string]int, len(s.files)),
	}
	for i, f := range s.files {
		c.fileAt[f.Path] = i
	}
	fileIdx := func(path string) int {
		if i, ok := c.fileAt[path]; ok {
			return i
		}
		// Not expected: Dataset.Files and scanMetadata list the same paths.
		i := len(s.files)
		s.files = append(s.files, fileMeta{Path: path, Properties: map[string]any{}})
		c.fileAt[path] = i
		return i
	}
	c.groups = make([]catalogGroup, len(s.groups))
	for i, g := range s.groups {
		c.groups[i] = catalogGroup{Name: g.Name, File: fileIdx(g.File), Properties: g.Properties}
	}
	// Occurrences are unique by (file, group, name), so each key is
	// interned once here.
	c.channels = make([]catalogChannel, len(occ))
	matched := 0
	for i, o := range occ {
		key := chKey{file: o.File, group: o.Group, name: o.Name}
		info, ok := s.meta[key]
		if ok {
			ids, err := c.props.intern(info.properties)
			if err != nil {
				return err
			}
			info.propIDs = ids
			s.meta[key] = info
			matched++
		}
		c.occAt[key] = i
		row := catalogChannel{channelHead: channelHead{Name: o.Name, Group: o.Group}, File: fileIdx(o.File), channelCore: s.coreOf(o, info, ok)}
		row.Properties = info.propIDs
		c.channels[i] = row
	}
	if matched < len(s.meta) {
		var rest []chKey
		for key := range s.meta {
			if _, ok := c.occAt[key]; !ok {
				rest = append(rest, key)
			}
		}
		slices.SortFunc(rest, func(a, b chKey) int {
			switch {
			case a.file != b.file:
				return compareStr(a.file, b.file)
			case a.group != b.group:
				return compareStr(a.group, b.group)
			}
			return compareStr(a.name, b.name)
		})
		for _, key := range rest {
			info := s.meta[key]
			ids, err := c.props.intern(info.properties)
			if err != nil {
				return err
			}
			info.propIDs = ids
			s.meta[key] = info
		}
	}
	s.cat = c
	return nil
}

func compareStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (s *session) catalog() (any, []byte, error) {
	if err := s.need(); err != nil {
		return nil, nil, err
	}
	if s.cat == nil {
		if err := s.buildCatalog(); err != nil {
			return nil, nil, err
		}
	}
	files := s.files
	if files == nil {
		files = []fileMeta{}
	}
	return map[string]any{
		"op":             "result",
		"basis":          s.ds.Basis().UTC().Format(time.RFC3339Nano),
		"files":          files,
		"groups":         s.cat.groups,
		"property_table": s.cat.props.entries,
		"channels":       s.cat.channels,
	}, nil, nil
}

// core fills the fields a catalog row and a find row share.
func (s *session) core(o dataset.Occurrence) (channelCore, scaleInfo, bool) {
	info, ok := s.meta[chKey{file: o.File, group: o.Group, name: o.Name}]
	return s.coreOf(o, info, ok), info, ok
}

func (s *session) coreOf(o dataset.Occurrence, info scaleInfo, ok bool) channelCore {
	c := channelCore{
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
	if ok {
		c.DataType = info.dataType
		c.Unit = info.unit
		c.HasScale = info.hasLinear
		c.HasNonlinear = info.nonlinear
		c.DAQmx = info.daqmx
		c.Digital = info.digital
		c.DAQmxScale = info.willScale
		if info.hasLinear {
			c.Scale = &scaleOut{Slope: info.slope, Intercept: info.intercept, Index: info.index}
		}
	}
	return c
}

// readChannel is a read header's channel: channelOut with files as
// indexes into the catalog's files, and properties as the index of the
// catalog channel whose properties apply (the first file of the read that
// has the channel). Keys keep channelOut's order. Bundles (convert) still
// write channelOut with paths and the properties object.
type readChannel struct {
	outHead
	Files []int `json:"files"`
	outMid
	Properties *int `json:"properties,omitempty"`
	outTail
}

// readSpan is spanRead with the file interned. File comes first and
// shadows spanRead's, so the keys keep spanRead's order.
type readSpan struct {
	File int `json:"file"`
	spanRead
}

// compactRead rewrites a planned read header to point into the catalog.
func (s *session) compactRead(d *dataResp, encs []*seriesEnc, reads []spanRead) error {
	metas, _ := d.header["channels"].([]channelOut)
	if len(metas) != len(encs) {
		return errCode("error", "read header and series disagree")
	}
	out := make([]readChannel, len(metas))
	for i, m := range metas {
		rc := readChannel{outHead: m.outHead, outMid: m.outMid, outTail: m.outTail}
		if m.Files != nil {
			rc.Files = make([]int, len(m.Files))
		}
		for j, f := range m.Files {
			k, ok := s.cat.fileAt[f]
			if !ok {
				return errCode("error", "read touched a file the catalog does not list: "+f)
			}
			rc.Files[j] = k
		}
		if p := encs[i].propsAt; p >= 0 {
			rc.Properties = &p
		}
		out[i] = rc
	}
	d.header["channels"] = out
	spans := make([]readSpan, len(reads))
	for i, r := range reads {
		k, ok := s.cat.fileAt[r.File]
		if !ok {
			return errCode("error", "read touched a file the catalog does not list: "+r.File)
		}
		spans[i] = readSpan{spanRead: r, File: k}
	}
	d.header["reads"] = spans
	return nil
}
