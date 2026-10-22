package bridge

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

type table struct {
	meta channelOut
	ipc  []byte
}

func encodeSeries(s dataset.Series, cols columnSet, info scaleInfo, requestScaled, propsMatch bool, nat *nativeCol) (table, error) {
	applyLinear := requestScaled && info.hasLinear && !info.daqmx
	metaScaled := applyLinear || (requestScaled && info.daqmx && info.willScale)
	useNative := info.daqmx && !metaScaled
	yType := arrow.DataType(arrow.PrimitiveTypes.Float64)
	if useNative {
		if nat == nil || !nat.set {
			return table{}, errCode("error", "missing DAQmx values for "+s.Group+"/"+s.Name)
		}
		if nat.dt != info.tdmsType && info.tdmsType != 0 {
			return table{}, errCode("error", "DAQmx type changed for "+s.Group+"/"+s.Name)
		}
		if nativeLen(nat.vals) != len(s.Y) {
			return table{}, errCode("error", fmt.Sprintf("DAQmx %s/%s has %d native values and %d samples", s.Group, s.Name, nativeLen(nat.vals), len(s.Y)))
		}
		yType = arrowOf(nat.dt)
		if yType == nil {
			return table{}, errCode("argument", "cannot read "+nat.dt.String()+" as a Polars column")
		}
	}
	fields := []arrow.Field{{
		Name: "y", Type: yType, Nullable: false,
	}}
	columns := []string{"y"}
	abs := cols.absolute && s.Timed && len(s.Times) == len(s.Y)
	if abs {
		fields = append(fields, arrow.Field{Name: "time_ns", Type: arrow.PrimitiveTypes.Int64, Nullable: false})
		columns = append(columns, "time_ns")
	}
	if cols.relative {
		fields = append(fields, arrow.Field{Name: "relative_s", Type: arrow.PrimitiveTypes.Float64, Nullable: false})
		columns = append(columns, "relative_s")
	}
	meta := channelOut{
		Name:            s.Name,
		Group:           s.Group,
		Samples:         len(s.Y),
		Timed:           s.Timed,
		IsTimestamp:     s.IsTimestamp,
		Source:          s.Source.String(),
		Dt:              s.Dt,
		Files:           append([]string(nil), s.Files...),
		Columns:         columns,
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
		AbsoluteOmitted: cols.absolute && !abs,
	}
	if !s.Basis.IsZero() || s.Timed {
		meta.Basis = s.Basis.UTC().Format(time.RFC3339Nano)
	}
	if info.hasLinear {
		meta.Scale = &scaleOut{Slope: info.slope, Intercept: info.intercept, Index: info.index}
	}
	for _, g := range s.Gaps {
		meta.Gaps = append(meta.Gaps, gapOut{
			From: g.From.UTC().Format(time.RFC3339Nano),
			To:   g.To.UTC().Format(time.RFC3339Nano),
		})
	}
	md := schemaMeta(meta)
	schema := arrow.NewSchema(fields, &md)
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	if useNative {
		if err := writeNative(b.Field(0), nat.dt, nat.vals); err != nil {
			return table{}, err
		}
	} else {
		yb := b.Field(0).(*array.Float64Builder)
		if applyLinear {
			vals := make([]float64, len(s.Y))
			for i, v := range s.Y {
				vals[i] = v*info.slope + info.intercept
			}
			yb.AppendValues(vals, nil)
		} else if len(s.Y) > 0 {
			yb.AppendValues(s.Y, nil)
		}
	}
	next := 1
	if abs {
		ns := make([]int64, len(s.Times))
		for i, t := range s.Times {
			ns[i] = t.UnixNano()
		}
		b.Field(next).(*array.Int64Builder).AppendValues(ns, nil)
		next++
	}
	if cols.relative {
		rel := s.RelativeTimes()
		if len(rel) != len(s.Y) {
			return table{}, fmt.Errorf("bridge: relative time length %d, samples %d", len(rel), len(s.Y))
		}
		if len(rel) > 0 {
			b.Field(next).(*array.Float64Builder).AppendValues(rel, nil)
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(pool))
	if err := w.Write(rec); err != nil {
		w.Close()
		return table{}, err
	}
	if err := w.Close(); err != nil {
		return table{}, err
	}
	return table{meta: meta, ipc: buf.Bytes()}, nil
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

func writeNative(b array.Builder, dt tdms.DataType, vals any) error {
	switch dt {
	case tdms.TypeInt8:
		s, ok := vals.([]int8)
		if !ok {
			return errCode("argument", fmt.Sprintf("int8 values have Go type %T", vals))
		}
		b.(*array.Int8Builder).AppendValues(s, nil)
	case tdms.TypeInt16:
		s, ok := vals.([]int16)
		if !ok {
			return errCode("argument", fmt.Sprintf("int16 values have Go type %T", vals))
		}
		b.(*array.Int16Builder).AppendValues(s, nil)
	case tdms.TypeInt32:
		s, ok := vals.([]int32)
		if !ok {
			return errCode("argument", fmt.Sprintf("int32 values have Go type %T", vals))
		}
		b.(*array.Int32Builder).AppendValues(s, nil)
	case tdms.TypeInt64:
		s, ok := vals.([]int64)
		if !ok {
			return errCode("argument", fmt.Sprintf("int64 values have Go type %T", vals))
		}
		b.(*array.Int64Builder).AppendValues(s, nil)
	case tdms.TypeUint8:
		s, ok := vals.([]uint8)
		if !ok {
			return errCode("argument", fmt.Sprintf("uint8 values have Go type %T", vals))
		}
		b.(*array.Uint8Builder).AppendValues(s, nil)
	case tdms.TypeUint16:
		s, ok := vals.([]uint16)
		if !ok {
			return errCode("argument", fmt.Sprintf("uint16 values have Go type %T", vals))
		}
		b.(*array.Uint16Builder).AppendValues(s, nil)
	case tdms.TypeUint32:
		s, ok := vals.([]uint32)
		if !ok {
			return errCode("argument", fmt.Sprintf("uint32 values have Go type %T", vals))
		}
		b.(*array.Uint32Builder).AppendValues(s, nil)
	case tdms.TypeUint64:
		s, ok := vals.([]uint64)
		if !ok {
			return errCode("argument", fmt.Sprintf("uint64 values have Go type %T", vals))
		}
		b.(*array.Uint64Builder).AppendValues(s, nil)
	case tdms.TypeFloat32:
		s, ok := vals.([]float32)
		if !ok {
			return errCode("argument", fmt.Sprintf("float32 values have Go type %T", vals))
		}
		b.(*array.Float32Builder).AppendValues(s, nil)
	case tdms.TypeFloat64:
		s, ok := vals.([]float64)
		if !ok {
			return errCode("argument", fmt.Sprintf("float64 values have Go type %T", vals))
		}
		b.(*array.Float64Builder).AppendValues(s, nil)
	case tdms.TypeTimestamp:
		s, ok := vals.([]tdms.Timestamp)
		if !ok {
			return errCode("argument", fmt.Sprintf("timestamp values have Go type %T", vals))
		}
		tb := b.(*array.TimestampBuilder)
		for _, ts := range s {
			t, ok := ts.Time()
			if !ok {
				return errCode("argument", "timestamp does not fit in UTC nanoseconds")
			}
			tb.Append(arrow.Timestamp(t.UnixNano()))
		}
	default:
		return errCode("argument", fmt.Sprintf("cannot read %s as a Polars column", dt))
	}
	return nil
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

func decodeIPC(blob []byte) (map[string][]float64, []int64, error) {
	r, err := ipc.NewReader(bytes.NewReader(blob))
	if err != nil {
		return nil, nil, err
	}
	defer r.Release()
	rec, err := r.Read()
	if err != nil {
		return nil, nil, err
	}
	defer rec.Release()
	out := map[string][]float64{}
	var ns []int64
	for i := 0; i < int(rec.NumCols()); i++ {
		name := rec.ColumnName(i)
		switch col := rec.Column(i).(type) {
		case *array.Float64:
			out[name] = append([]float64(nil), col.Float64Values()...)
		case *array.Int64:
			ns = append([]int64(nil), col.Int64Values()...)
			out[name] = nil
		default:
			return nil, nil, fmt.Errorf("bridge: unexpected column %s %T", name, rec.Column(i))
		}
	}
	return out, ns, nil
}
