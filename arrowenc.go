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

func encodeSeries(s dataset.Series, cols columnSet, info scaleInfo, scaled, propsMatch bool) (table, error) {
	fields := []arrow.Field{{
		Name: "y", Type: arrow.PrimitiveTypes.Float64, Nullable: false,
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
		Scaled:          scaled,
		HasScale:        info.hasLinear,
		HasNonlinear:    info.nonlinear,
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
	yb := b.Field(0).(*array.Float64Builder)
	if scaled {
		vals := make([]float64, len(s.Y))
		for i, v := range s.Y {
			vals[i] = v*info.slope + info.intercept
		}
		yb.AppendValues(vals, nil)
	} else if len(s.Y) > 0 {
		yb.AppendValues(s.Y, nil)
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

func schemaMeta(m channelOut) arrow.Metadata {
	props, _ := json.Marshal(m.Properties)
	files, _ := json.Marshal(m.Files)
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
