package bridge

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/trey-copeland/gotdms"
	"github.com/trey-copeland/gotdms/dataset"
)

type columnSet struct {
	absolute bool
	relative bool
}

type channelOut struct {
	Name            string         `json:"name"`
	Group           string         `json:"group"`
	Samples         int            `json:"samples"`
	Timed           bool           `json:"timed"`
	IsTimestamp     bool           `json:"is_timestamp"`
	Source          string         `json:"source"`
	Dt              float64        `json:"dt"`
	Basis           string         `json:"basis,omitempty"`
	Files           []string       `json:"files"`
	Columns         []string       `json:"columns"`
	Scaled          bool           `json:"scaled"`
	HasScale        bool           `json:"has_scale"`
	Scale           *scaleOut      `json:"scale,omitempty"`
	HasNonlinear    bool           `json:"has_nonlinear_scale"`
	DAQmx           bool           `json:"daqmx,omitempty"`
	Digital         bool           `json:"digital,omitempty"`
	DAQmxScale      bool           `json:"daqmx_scale,omitempty"`
	Unit            string         `json:"unit,omitempty"`
	DataType        string         `json:"data_type,omitempty"`
	Properties      map[string]any `json:"properties,omitempty"`
	PropertiesMatch bool           `json:"properties_match"`
	Gaps            []gapOut       `json:"gaps,omitempty"`
	AbsoluteOmitted bool           `json:"absolute_omitted,omitempty"`
	// Clock is the result_begin clock id with this channel's times. It is
	// absent for an untimed channel and when absolute time was not asked
	// for. Stream is the channel's stream id. Bundles have neither: each
	// bundle file carries its own time_ns column.
	Clock  *int `json:"clock,omitempty"`
	Stream *int `json:"stream,omitempty"`
}

type scaleOut struct {
	Slope     float64 `json:"slope"`
	Intercept float64 `json:"intercept"`
	Index     int     `json:"index"`
}

type gapOut struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// seriesEnc is one channel of a read, checked and ready to encode.
type seriesEnc struct {
	ser         dataset.Series
	cols        columnSet
	info        scaleInfo
	applyLinear bool
	useNative   bool
	nat         *nativeCol
	meta        channelOut
}

func prepareSeries(s dataset.Series, cols columnSet, info scaleInfo, requestScaled, propsMatch bool, nat *nativeCol) (*seriesEnc, error) {
	applyLinear := requestScaled && info.hasLinear && !info.daqmx
	metaScaled := applyLinear || (requestScaled && info.daqmx && info.willScale)
	useNative := info.daqmx && !metaScaled
	if useNative {
		if nat == nil || !nat.set {
			return nil, errCode("error", "missing DAQmx values for "+s.Group+"/"+s.Name)
		}
		if nat.dt != info.tdmsType && info.tdmsType != 0 {
			return nil, errCode("error", "DAQmx type changed for "+s.Group+"/"+s.Name)
		}
		if nativeLen(nat.vals) != len(s.Y) {
			return nil, errCode("error", fmt.Sprintf("DAQmx %s/%s has %d native values and %d samples", s.Group, s.Name, nativeLen(nat.vals), len(s.Y)))
		}
		if arrowOf(nat.dt) == nil {
			return nil, errCode("argument", "cannot read "+nat.dt.String()+" as a Polars column")
		}
	}
	e := &seriesEnc{ser: s, cols: cols, info: info, applyLinear: applyLinear, useNative: useNative, nat: nat}
	e.meta = channelOut{
		Name:            s.Name,
		Group:           s.Group,
		Samples:         len(s.Y),
		Timed:           s.Timed,
		IsTimestamp:     s.IsTimestamp,
		Source:          s.Source.String(),
		Dt:              s.Dt,
		Files:           append([]string(nil), s.Files...),
		Scaled:          metaScaled,
		HasScale:        info.hasLinear,
		HasNonlinear:    info.nonlinear,
		DAQmx:           info.daqmx,
		Digital:         info.digital,
		DAQmxScale:      info.willScale,
		Unit:            info.unit,
		DataType:        info.dataType,
		Properties:      info.properties,
		PropertiesMatch: propsMatch,
		AbsoluteOmitted: cols.absolute && !e.hasTimes(),
	}
	if !s.Basis.IsZero() || s.Timed {
		e.meta.Basis = s.Basis.UTC().Format(time.RFC3339Nano)
	}
	if info.hasLinear {
		e.meta.Scale = &scaleOut{Slope: info.slope, Intercept: info.intercept, Index: info.index}
	}
	for _, g := range s.Gaps {
		e.meta.Gaps = append(e.meta.Gaps, gapOut{
			From: g.From.UTC().Format(time.RFC3339Nano),
			To:   g.To.UTC().Format(time.RFC3339Nano),
		})
	}
	return e, nil
}

// hasTimes reports whether the series has one absolute time per sample.
func (e *seriesEnc) hasTimes() bool {
	s := e.ser
	if !s.Timed {
		return false
	}
	if s.Clock != nil {
		return s.Clock.Len() == len(s.Y)
	}
	return len(s.Times) == len(s.Y)
}

// timeNs returns the sample times as Unix nanoseconds. A shared Clock is
// returned as is; Times are converted into a new slice.
func (e *seriesEnc) timeNs() []int64 {
	if e.ser.Clock != nil {
		return e.ser.Clock.Ns
	}
	ns := make([]int64, len(e.ser.Times))
	for i, t := range e.ser.Times {
		ns[i] = t.UnixNano()
	}
	return ns
}

func (e *seriesEnc) yArray() (arrow.Array, error) {
	if e.useNative {
		return nativeArray(e.nat.dt, e.nat.vals)
	}
	if e.applyLinear {
		vals := make([]float64, len(e.ser.Y))
		for i, v := range e.ser.Y {
			vals[i] = v*e.info.slope + e.info.intercept
		}
		return viewArray(arrow.PrimitiveTypes.Float64, vals), nil
	}
	return viewArray(arrow.PrimitiveTypes.Float64, e.ser.Y), nil
}

// record is the channel's Arrow record: y, then time_ns when timeNs is not
// nil, then relative_s when asked for. A read reply sends time in clock
// streams and passes nil; a bundle file carries its own time_ns.
func (e *seriesEnc) record(timeNs []int64, withTime bool) (arrow.Record, error) {
	y, err := e.yArray()
	if err != nil {
		return nil, err
	}
	fields := []arrow.Field{{Name: "y", Type: y.DataType(), Nullable: false}}
	cols := []arrow.Array{y}
	if withTime {
		fields = append(fields, arrow.Field{Name: "time_ns", Type: arrow.PrimitiveTypes.Int64, Nullable: false})
		cols = append(cols, viewArray(arrow.PrimitiveTypes.Int64, timeNs))
	}
	if e.cols.relative {
		rel := e.ser.RelativeTimes()
		if len(rel) != len(e.ser.Y) {
			for _, c := range cols {
				c.Release()
			}
			return nil, fmt.Errorf("bridge: relative time length %d, samples %d", len(rel), len(e.ser.Y))
		}
		fields = append(fields, arrow.Field{Name: "relative_s", Type: arrow.PrimitiveTypes.Float64, Nullable: false})
		cols = append(cols, viewArray(arrow.PrimitiveTypes.Float64, rel))
	}
	md := schemaMeta(e.meta)
	rec := array.NewRecord(arrow.NewSchema(fields, &md), cols, int64(len(e.ser.Y)))
	for _, c := range cols {
		c.Release()
	}
	return rec, nil
}

func columnNames(withTime, relative bool) []string {
	out := []string{"y"}
	if withTime {
		out = append(out, "time_ns")
	}
	if relative {
		out = append(out, "relative_s")
	}
	return out
}

func arrowOf(dt tdms.DataType) arrow.DataType {
	switch dt {
	case tdms.TypeInt8:
		return arrow.PrimitiveTypes.Int8
	case tdms.TypeInt16:
		return arrow.PrimitiveTypes.Int16
	case tdms.TypeInt32:
		return arrow.PrimitiveTypes.Int32
	case tdms.TypeInt64:
		return arrow.PrimitiveTypes.Int64
	case tdms.TypeUint8:
		return arrow.PrimitiveTypes.Uint8
	case tdms.TypeUint16:
		return arrow.PrimitiveTypes.Uint16
	case tdms.TypeUint32:
		return arrow.PrimitiveTypes.Uint32
	case tdms.TypeUint64:
		return arrow.PrimitiveTypes.Uint64
	case tdms.TypeFloat32:
		return arrow.PrimitiveTypes.Float32
	case tdms.TypeFloat64:
		return arrow.PrimitiveTypes.Float64
	case tdms.TypeTimestamp:
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	default:
		return nil
	}
}

// nativeArray views DAQmx native values as an Arrow array. Numeric types
// are not copied. Timestamps are converted to UTC nanoseconds.
func nativeArray(dt tdms.DataType, vals any) (arrow.Array, error) {
	bad := func() (arrow.Array, error) {
		return nil, errCode("argument", fmt.Sprintf("%s values have Go type %T", dt, vals))
	}
	switch dt {
	case tdms.TypeInt8:
		s, ok := vals.([]int8)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeInt16:
		s, ok := vals.([]int16)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeInt32:
		s, ok := vals.([]int32)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeInt64:
		s, ok := vals.([]int64)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeUint8:
		s, ok := vals.([]uint8)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeUint16:
		s, ok := vals.([]uint16)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeUint32:
		s, ok := vals.([]uint32)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeUint64:
		s, ok := vals.([]uint64)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeFloat32:
		s, ok := vals.([]float32)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeFloat64:
		s, ok := vals.([]float64)
		if !ok {
			return bad()
		}
		return viewArray(arrowOf(dt), s), nil
	case tdms.TypeTimestamp:
		s, ok := vals.([]tdms.Timestamp)
		if !ok {
			return bad()
		}
		ns := make([]int64, len(s))
		for i, ts := range s {
			t, ok := ts.Time()
			if !ok {
				return nil, errCode("argument", "timestamp does not fit in UTC nanoseconds")
			}
			ns[i] = t.UnixNano()
		}
		return viewArray(arrowOf(dt), ns), nil
	default:
		return nil, errCode("argument", fmt.Sprintf("cannot read %s as a Polars column", dt))
	}
}

func schemaMeta(m channelOut) arrow.Metadata {
	props, _ := json.Marshal(m.Properties)
	props = restoreNonFinite(props)
	files, _ := json.Marshal(m.Files)
	files = restoreNonFinite(files)
	gaps, _ := json.Marshal(m.Gaps)
	keys := []string{
		"gotdms.name", "gotdms.group", "gotdms.data_type", "gotdms.timed",
		"gotdms.is_timestamp", "gotdms.source", "gotdms.dt", "gotdms.basis",
		"gotdms.scaled", "gotdms.has_scale", "gotdms.has_nonlinear_scale",
		"gotdms.unit", "gotdms.files", "gotdms.properties", "gotdms.gaps",
		"gotdms.samples",
	}
	vals := []string{
		m.Name, m.Group, m.DataType, strconv.FormatBool(m.Timed),
		strconv.FormatBool(m.IsTimestamp), m.Source, strconv.FormatFloat(m.Dt, 'g', -1, 64), m.Basis,
		strconv.FormatBool(m.Scaled), strconv.FormatBool(m.HasScale), strconv.FormatBool(m.HasNonlinear),
		m.Unit, string(files), string(props), string(gaps),
		strconv.Itoa(m.Samples),
	}
	if m.DAQmx {
		keys = append(keys, "gotdms.daqmx", "gotdms.digital")
		vals = append(vals, "true", strconv.FormatBool(m.Digital))
	}
	if m.Scale != nil {
		keys = append(keys, "gotdms.scale_slope", "gotdms.scale_intercept", "gotdms.scale_index")
		vals = append(vals,
			strconv.FormatFloat(m.Scale.Slope, 'g', -1, 64),
			strconv.FormatFloat(m.Scale.Intercept, 'g', -1, 64),
			strconv.Itoa(m.Scale.Index),
		)
	}
	return arrow.NewMetadata(keys, vals)
}
