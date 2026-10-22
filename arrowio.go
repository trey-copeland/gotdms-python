package bridge

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/trey-copeland/gotdms"
)

// wireOf is the protocol name for a TDMS type this bridge can put in a
// Polars column. Float-with-unit uses the plain float wire name; ni_type
// keeps the NI enum name. ok is false for extended and complex values.
// A decoded DAQmx channel's DataType is the primary scaler, so it uses
// that scaler's wire name. Digital lines are uint8.
func wireOf(dt tdms.DataType) (string, bool) {
	switch dt {
	case tdms.TypeInt8:
		return "int8", true
	case tdms.TypeInt16:
		return "int16", true
	case tdms.TypeInt32:
		return "int32", true
	case tdms.TypeInt64:
		return "int64", true
	case tdms.TypeUint8:
		return "uint8", true
	case tdms.TypeUint16:
		return "uint16", true
	case tdms.TypeUint32:
		return "uint32", true
	case tdms.TypeUint64:
		return "uint64", true
	case tdms.TypeFloat32, tdms.TypeFloat32WithUnit:
		return "float32", true
	case tdms.TypeFloat64, tdms.TypeFloat64WithUnit:
		return "float64", true
	case tdms.TypeBool:
		return "bool", true
	case tdms.TypeString:
		return "string", true
	case tdms.TypeTimestamp:
		return "timestamp", true
	case tdms.TypeVoid:
		return "void", true
	default:
		return "", false
	}
}

func dataTypeOf(wire string) (tdms.DataType, bool) {
	switch wire {
	case "int8":
		return tdms.TypeInt8, true
	case "int16":
		return tdms.TypeInt16, true
	case "int32":
		return tdms.TypeInt32, true
	case "int64":
		return tdms.TypeInt64, true
	case "uint8":
		return tdms.TypeUint8, true
	case "uint16":
		return tdms.TypeUint16, true
	case "uint32":
		return tdms.TypeUint32, true
	case "uint64":
		return tdms.TypeUint64, true
	case "float32":
		return tdms.TypeFloat32, true
	case "float64":
		return tdms.TypeFloat64, true
	case "bool":
		return tdms.TypeBool, true
	case "string":
		return tdms.TypeString, true
	case "timestamp":
		return tdms.TypeTimestamp, true
	default:
		return 0, false
	}
}

// readArrowColumn reads one single-column Arrow IPC stream.
// The release function frees the column. Call it after the values have been
// copied out: numeric slices alias the column buffer.
func readArrowColumn(payload []byte) (arrow.Array, func(), error) {
	if len(payload) == 0 {
		return nil, nil, errCode("argument", "write_chunk is missing an Arrow IPC payload")
	}
	r, err := ipc.NewReader(bytes.NewReader(payload), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return nil, nil, errCode("argument", "arrow chunk: "+err.Error())
	}
	if !r.Next() {
		readErr := r.Err()
		r.Release()
		if readErr == nil || readErr == io.EOF {
			return nil, nil, errCode("argument", "arrow chunk has no record batch")
		}
		return nil, nil, errCode("argument", "arrow chunk: "+readErr.Error())
	}
	rec := r.Record()
	rec.Retain()
	if r.Next() {
		rec.Release()
		r.Release()
		return nil, nil, errCode("argument", "arrow chunk has more than one record batch")
	}
	if err := r.Err(); err != nil && err != io.EOF {
		rec.Release()
		r.Release()
		return nil, nil, errCode("argument", "arrow chunk: "+err.Error())
	}
	if rec.NumCols() != 1 {
		rec.Release()
		r.Release()
		return nil, nil, errCode("argument", fmt.Sprintf("arrow chunk has %d columns, want 1", rec.NumCols()))
	}
	col := rec.Column(0)
	col.Retain()
	rec.Release()
	return col, func() {
		col.Release()
		r.Release()
	}, nil
}

func arrowMatches(wire string, dt arrow.DataType) error {
	switch wire {
	case "int8":
		if dt.ID() != arrow.INT8 {
			return arrowMismatch(wire, dt)
		}
	case "int16":
		if dt.ID() != arrow.INT16 {
			return arrowMismatch(wire, dt)
		}
	case "int32":
		if dt.ID() != arrow.INT32 {
			return arrowMismatch(wire, dt)
		}
	case "int64":
		if dt.ID() != arrow.INT64 {
			return arrowMismatch(wire, dt)
		}
	case "uint8":
		if dt.ID() != arrow.UINT8 {
			return arrowMismatch(wire, dt)
		}
	case "uint16":
		if dt.ID() != arrow.UINT16 {
			return arrowMismatch(wire, dt)
		}
	case "uint32":
		if dt.ID() != arrow.UINT32 {
			return arrowMismatch(wire, dt)
		}
	case "uint64":
		if dt.ID() != arrow.UINT64 {
			return arrowMismatch(wire, dt)
		}
	case "float32":
		if dt.ID() != arrow.FLOAT32 {
			return arrowMismatch(wire, dt)
		}
	case "float64":
		if dt.ID() != arrow.FLOAT64 {
			return arrowMismatch(wire, dt)
		}
	case "bool":
		if dt.ID() != arrow.BOOL {
			return arrowMismatch(wire, dt)
		}
	case "string":
		if dt.ID() != arrow.STRING && dt.ID() != arrow.LARGE_STRING {
			return arrowMismatch(wire, dt)
		}
	case "timestamp":
		ts, ok := dt.(*arrow.TimestampType)
		if !ok || ts.Unit != arrow.Nanosecond {
			return arrowMismatch(wire, dt)
		}
		switch ts.TimeZone {
		case "", "UTC", "utc", "Etc/UTC":
		default:
			return errCode("argument", fmt.Sprintf("timestamp timezone %q, want UTC", ts.TimeZone))
		}
	default:
		return errCode("argument", "unsupported column type "+wire)
	}
	return nil
}

func arrowMismatch(wire string, dt arrow.DataType) error {
	return errCode("argument", fmt.Sprintf("arrow type %s does not match declared %s", dt, wire))
}

// columnValues returns the column as the slice ChannelWriter.Write accepts.
// Numeric and bool slices of fixed width may alias col. String and timestamp
// slices are copies. Nulls are rejected for every type, including strings.
func columnValues(wire string, col arrow.Array) (any, error) {
	if err := arrowMatches(wire, col.DataType()); err != nil {
		return nil, err
	}
	if col.NullN() > 0 {
		return nil, errCode("argument", "Arrow column contains nulls. TDMS channels are not nullable. Nulls are not written as zero or as empty strings.")
	}
	switch wire {
	case "int8":
		return col.(*array.Int8).Int8Values(), nil
	case "int16":
		return col.(*array.Int16).Int16Values(), nil
	case "int32":
		return col.(*array.Int32).Int32Values(), nil
	case "int64":
		return col.(*array.Int64).Int64Values(), nil
	case "uint8":
		return col.(*array.Uint8).Uint8Values(), nil
	case "uint16":
		return col.(*array.Uint16).Uint16Values(), nil
	case "uint32":
		return col.(*array.Uint32).Uint32Values(), nil
	case "uint64":
		return col.(*array.Uint64).Uint64Values(), nil
	case "float32":
		return col.(*array.Float32).Float32Values(), nil
	case "float64":
		return col.(*array.Float64).Float64Values(), nil
	case "bool":
		a := col.(*array.Boolean)
		out := make([]bool, a.Len())
		for i := range out {
			out[i] = a.Value(i)
		}
		return out, nil
	case "string":
		switch a := col.(type) {
		case *array.String:
			out := make([]string, a.Len())
			for i := range out {
				out[i] = a.Value(i)
			}
			return out, nil
		case *array.LargeString:
			out := make([]string, a.Len())
			for i := range out {
				out[i] = a.Value(i)
			}
			return out, nil
		default:
			return nil, arrowMismatch(wire, col.DataType())
		}
	case "timestamp":
		a := col.(*array.Timestamp)
		vals := a.TimestampValues()
		out := make([]tdms.Timestamp, len(vals))
		for i, v := range vals {
			// The chunk was checked to be nanoseconds. NewTimestamp keeps
			// the nanosecond that Timestamp.Time round-trips.
			out[i] = tdms.NewTimestamp(time.Unix(0, int64(v)).UTC())
		}
		return out, nil
	default:
		return nil, errCode("argument", "unsupported column type "+wire)
	}
}

func valueCount(v any) int {
	switch s := v.(type) {
	case []int8:
		return len(s)
	case []int16:
		return len(s)
	case []int32:
		return len(s)
	case []int64:
		return len(s)
	case []uint8:
		return len(s)
	case []uint16:
		return len(s)
	case []uint32:
		return len(s)
	case []uint64:
		return len(s)
	case []float32:
		return len(s)
	case []float64:
		return len(s)
	case []bool:
		return len(s)
	case []string:
		return len(s)
	case []tdms.Timestamp:
		return len(s)
	default:
		return -1
	}
}

// encodeNative writes one single-column Arrow IPC stream of TDMS values.
// A void channel (no raw-data index) becomes an empty null array.
func encodeNative(dt tdms.DataType, values any) ([]byte, error) {
	if dt == tdms.TypeVoid || dt == 0 {
		return writeArrowArray(array.NewNull(0))
	}
	arr, err := buildArrow(dt, values)
	if err != nil {
		return nil, err
	}
	return writeArrowArray(arr)
}

func writeArrowArray(arr arrow.Array) ([]byte, error) {
	defer arr.Release()
	nullable := arr.DataType().ID() == arrow.NULL
	schema := arrow.NewSchema([]arrow.Field{{
		Name: "y", Type: arr.DataType(), Nullable: nullable,
	}}, nil)
	rec := array.NewRecord(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(memory.NewGoAllocator()))
	if err := w.Write(rec); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func buildArrow(dt tdms.DataType, values any) (arrow.Array, error) {
	pool := memory.NewGoAllocator()
	switch dt {
	case tdms.TypeInt8:
		s, err := int8Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewInt8Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeInt16:
		s, err := int16Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewInt16Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeInt32:
		s, err := int32Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewInt32Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeInt64:
		s, err := int64Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewInt64Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeUint8:
		s, err := uint8Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewUint8Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeUint16:
		s, err := uint16Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewUint16Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeUint32:
		s, err := uint32Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewUint32Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeUint64:
		s, err := uint64Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewUint64Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeFloat32, tdms.TypeFloat32WithUnit:
		s, err := float32Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewFloat32Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeFloat64, tdms.TypeFloat64WithUnit:
		s, err := float64Slice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewFloat64Builder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeBool:
		s, err := boolSlice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewBooleanBuilder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeString:
		s, err := stringSlice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewStringBuilder(pool)
		defer b.Release()
		b.AppendValues(s, nil)
		return b.NewArray(), nil
	case tdms.TypeTimestamp:
		s, err := timestampSlice(values)
		if err != nil {
			return nil, err
		}
		b := array.NewTimestampBuilder(pool, &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"})
		defer b.Release()
		for _, ts := range s {
			t, ok := ts.Time()
			if !ok {
				return nil, errCode("argument", "timestamp does not fit in UTC nanoseconds")
			}
			b.Append(arrow.Timestamp(t.UnixNano()))
		}
		return b.NewArray(), nil
	default:
		return nil, errCode("argument", fmt.Sprintf("cannot read %s as a Polars column", dt))
	}
}

func int8Slice(v any) ([]int8, error) {
	if v == nil {
		return []int8{}, nil
	}
	s, ok := v.([]int8)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("int8 values have Go type %T", v))
	}
	return s, nil
}

func int16Slice(v any) ([]int16, error) {
	if v == nil {
		return []int16{}, nil
	}
	s, ok := v.([]int16)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("int16 values have Go type %T", v))
	}
	return s, nil
}

func int32Slice(v any) ([]int32, error) {
	if v == nil {
		return []int32{}, nil
	}
	s, ok := v.([]int32)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("int32 values have Go type %T", v))
	}
	return s, nil
}

func int64Slice(v any) ([]int64, error) {
	if v == nil {
		return []int64{}, nil
	}
	s, ok := v.([]int64)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("int64 values have Go type %T", v))
	}
	return s, nil
}

func uint8Slice(v any) ([]uint8, error) {
	if v == nil {
		return []uint8{}, nil
	}
	s, ok := v.([]uint8)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("uint8 values have Go type %T", v))
	}
	return s, nil
}

func uint16Slice(v any) ([]uint16, error) {
	if v == nil {
		return []uint16{}, nil
	}
	s, ok := v.([]uint16)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("uint16 values have Go type %T", v))
	}
	return s, nil
}

func uint32Slice(v any) ([]uint32, error) {
	if v == nil {
		return []uint32{}, nil
	}
	s, ok := v.([]uint32)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("uint32 values have Go type %T", v))
	}
	return s, nil
}

func uint64Slice(v any) ([]uint64, error) {
	if v == nil {
		return []uint64{}, nil
	}
	s, ok := v.([]uint64)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("uint64 values have Go type %T", v))
	}
	return s, nil
}

func float32Slice(v any) ([]float32, error) {
	if v == nil {
		return []float32{}, nil
	}
	s, ok := v.([]float32)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("float32 values have Go type %T", v))
	}
	return s, nil
}

func float64Slice(v any) ([]float64, error) {
	if v == nil {
		return []float64{}, nil
	}
	s, ok := v.([]float64)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("float64 values have Go type %T", v))
	}
	return s, nil
}

func boolSlice(v any) ([]bool, error) {
	if v == nil {
		return []bool{}, nil
	}
	s, ok := v.([]bool)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("bool values have Go type %T", v))
	}
	return s, nil
}

func stringSlice(v any) ([]string, error) {
	if v == nil {
		return []string{}, nil
	}
	s, ok := v.([]string)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("string values have Go type %T", v))
	}
	return s, nil
}

func timestampSlice(v any) ([]tdms.Timestamp, error) {
	if v == nil {
		return []tdms.Timestamp{}, nil
	}
	s, ok := v.([]tdms.Timestamp)
	if !ok {
		return nil, errCode("argument", fmt.Sprintf("timestamp values have Go type %T", v))
	}
	return s, nil
}
