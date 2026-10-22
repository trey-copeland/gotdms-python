package bridge

import (
	"errors"
	"fmt"
	"time"

	"github.com/trey-copeland/gotdms"
	"github.com/trey-copeland/gotdms/dataset"
)

// indexOpts selects how an index-aware read treats DAQmx channels.
//
// native keeps the primary scaler's stored type (digital lines are uint8
// 0/1). scale, with willScale, replaces those samples with
// DAQmxScaledFloat64s. A time window keeps samples in [start, end). Ordinary
// channels ignore both flags and stay on the float64 dataset path.
type indexOpts struct {
	window bool
	start  time.Time
	end    time.Time
	scale  bool
	native bool
}

// nativeCol is the primary scaler's raw values, in series order.
type nativeCol struct {
	set     bool
	digital bool
	dt      tdms.DataType
	vals    any
}

func (s *session) readIndexed(sels []dataset.Selector, offset, limit int, basis time.Time, opt indexOpts) ([]dataset.Series, []nativeCol, []spanRead, error) {
	cache := &fileCache{files: map[string]*tdms.File{}, axis: map[string]*axisBuf{}}
	defer cache.close()
	out := make([]dataset.Series, len(sels))
	nats := make([]nativeCol, len(sels))
	for i, sel := range sels {
		hits, err := s.occurrences(sel)
		if err != nil {
			return nil, nil, nil, err
		}
		var nat *nativeCol
		if opt.native || opt.scale {
			nat = &nats[i]
		}
		ser, err := s.seriesRange(sel, hits, offset, limit, basis, cache, opt, nat)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := s.finishNative(sel, ser, nat, opt); err != nil {
			return nil, nil, nil, err
		}
		out[i] = ser
	}
	return out, nats, cache.reads, nil
}

func (s *session) readMixed(sels []dataset.Selector, daqmxAt map[int]struct{}, start, end time.Time, window bool, basis time.Time, ro []dataset.ReadOption, scaled bool) ([]dataset.Series, []nativeCol, []spanRead, error) {
	series := make([]dataset.Series, len(sels))
	natives := make([]nativeCol, len(sels))
	var reads []spanRead
	var plain []dataset.Selector
	var plainAt []int
	var dx []dataset.Selector
	var dxAt []int
	for i, sel := range sels {
		if _, ok := daqmxAt[i]; ok {
			dx = append(dx, sel)
			dxAt = append(dxAt, i)
			continue
		}
		plain = append(plain, sel)
		plainAt = append(plainAt, i)
	}
	if len(plain) > 0 {
		got, err := s.loadSeries(plain, start, end, window, ro)
		if err != nil {
			return nil, nil, nil, err
		}
		for i, ser := range got {
			series[plainAt[i]] = ser
		}
		reads = append(reads, s.fullReads(plain)...)
	}
	if len(dx) > 0 {
		opt := indexOpts{scale: scaled, native: true}
		if window {
			opt.window = true
			opt.start = start
			opt.end = end
		}
		got, nats, dxReads, err := s.readIndexed(dx, 0, -1, basis, opt)
		if err != nil {
			return nil, nil, nil, err
		}
		for i, ser := range got {
			series[dxAt[i]] = ser
			natives[dxAt[i]] = nats[i]
		}
		reads = append(reads, dxReads...)
	}
	if reads == nil {
		reads = []spanRead{}
	}
	return series, natives, reads, nil
}

func (s *session) selectorDAQmx(sel dataset.Selector) (bool, error) {
	hits, err := s.occurrences(sel)
	if err != nil {
		return false, err
	}
	saw, sawNot := false, false
	for _, h := range hits {
		if s.meta[chKey{file: h.File, group: h.Group, name: h.Name}].daqmx {
			saw = true
		} else {
			sawNot = true
		}
	}
	if saw && sawNot {
		return false, errCode("argument", hits[0].Group+"/"+hits[0].Name+" mixes DAQmx and standard files")
	}
	return saw, nil
}

func (s *session) finishNative(sel dataset.Selector, ser dataset.Series, nat *nativeCol, opt indexOpts) error {
	if nat == nil || !opt.native {
		return nil
	}
	hits, err := s.occurrences(sel)
	if err != nil {
		return err
	}
	var info scaleInfo
	found := false
	for _, h := range hits {
		m, ok := s.meta[chKey{file: h.File, group: h.Group, name: h.Name}]
		if ok && m.daqmx {
			info = m
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	if opt.scale && info.willScale {
		return nil
	}
	if nat.set {
		if nativeLen(nat.vals) != len(ser.Y) {
			return errCode("error", fmt.Sprintf("DAQmx %s/%s has %d native values and %d samples", ser.Group, ser.Name, nativeLen(nat.vals), len(ser.Y)))
		}
		return nil
	}
	if len(ser.Y) != 0 {
		return errCode("error", "missing DAQmx values for "+ser.Group+"/"+ser.Name)
	}
	empty, err := zeroSlice(info.tdmsType)
	if err != nil {
		return err
	}
	nat.dt = info.tdmsType
	nat.digital = info.digital
	nat.vals = empty
	nat.set = true
	return nil
}

func (s *session) takeSamples(ch *tdms.Channel, file, group, name string, start, n int, cache *fileCache, opt indexOpts, nat *nativeCol) ([]float64, error) {
	info := s.meta[chKey{file: file, group: group, name: name}]
	if info.daqmx && opt.scale && info.willScale {
		full, err := cache.scaledValues(file, group, name, ch)
		if err != nil {
			return nil, err
		}
		if start < 0 || n < 0 || start > len(full) || n > len(full)-start {
			return nil, fmt.Errorf("%w: DAQmx scale range %d:%d of %d", dataset.ErrArgument, start, n, len(full))
		}
		return append([]float64{}, full[start:start+n]...), nil
	}
	if !info.daqmx {
		cache.note(file, group, name, "data", start, n, ch.Len())
		return readFloatRange(ch, start, n)
	}
	raw, err := ch.Slice(start, n)
	if err != nil {
		return nil, err
	}
	cache.note(file, group, name, "data", start, n, ch.Len())
	if opt.native && nat != nil {
		if err := nat.append(ch.DataType(), info.digital, raw); err != nil {
			return nil, err
		}
	}
	return anyToFloat(raw)
}

func (c *fileCache) scaledValues(file, group, name string, ch *tdms.Channel) ([]float64, error) {
	if c.scaled == nil {
		c.scaled = map[string][]float64{}
	}
	key := file + "\x00" + group + "\x00" + name
	if y, ok := c.scaled[key]; ok {
		return y, nil
	}
	// The scale chain is defined on the whole channel. DAQmxScaledFloat64s
	// reads every raw sample the chain needs, then the caller keeps the
	// requested indexes.
	c.note(file, group, name, "data", 0, ch.Len(), ch.Len())
	y, err := ch.DAQmxScaledFloat64s()
	if err != nil {
		if errors.Is(err, tdms.ErrUnsupported) {
			return nil, errCode("nonlinear_scale", err.Error())
		}
		return nil, err
	}
	c.scaled[key] = y
	return y, nil
}

func (n *nativeCol) append(dt tdms.DataType, digital bool, v any) error {
	if !n.set {
		n.dt = dt
		n.digital = digital
		n.vals = v
		n.set = true
		return nil
	}
	if n.dt != dt || n.digital != digital {
		return errCode("argument", "DAQmx scaler type changed across files")
	}
	cat, err := concatNative(n.vals, v)
	if err != nil {
		return err
	}
	n.vals = cat
	return nil
}

func concatNative(a, b any) (any, error) {
	switch x := a.(type) {
	case []int8:
		y, ok := b.([]int8)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]int8{}, x...), y...), nil
	case []int16:
		y, ok := b.([]int16)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]int16{}, x...), y...), nil
	case []int32:
		y, ok := b.([]int32)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]int32{}, x...), y...), nil
	case []int64:
		y, ok := b.([]int64)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]int64{}, x...), y...), nil
	case []uint8:
		y, ok := b.([]uint8)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]uint8{}, x...), y...), nil
	case []uint16:
		y, ok := b.([]uint16)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]uint16{}, x...), y...), nil
	case []uint32:
		y, ok := b.([]uint32)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]uint32{}, x...), y...), nil
	case []uint64:
		y, ok := b.([]uint64)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]uint64{}, x...), y...), nil
	case []float32:
		y, ok := b.([]float32)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]float32{}, x...), y...), nil
	case []float64:
		y, ok := b.([]float64)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]float64{}, x...), y...), nil
	case []tdms.Timestamp:
		y, ok := b.([]tdms.Timestamp)
		if !ok {
			return nil, nativeType(b)
		}
		return append(append([]tdms.Timestamp{}, x...), y...), nil
	default:
		return nil, nativeType(a)
	}
}

func nativeType(v any) error {
	return errCode("argument", fmt.Sprintf("DAQmx values have Go type %T", v))
}

func nativeLen(v any) int {
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
	case []tdms.Timestamp:
		return len(s)
	default:
		return -1
	}
}

func zeroSlice(dt tdms.DataType) (any, error) {
	switch dt {
	case tdms.TypeInt8:
		return []int8{}, nil
	case tdms.TypeInt16:
		return []int16{}, nil
	case tdms.TypeInt32:
		return []int32{}, nil
	case tdms.TypeInt64:
		return []int64{}, nil
	case tdms.TypeUint8:
		return []uint8{}, nil
	case tdms.TypeUint16:
		return []uint16{}, nil
	case tdms.TypeUint32:
		return []uint32{}, nil
	case tdms.TypeUint64:
		return []uint64{}, nil
	case tdms.TypeFloat32:
		return []float32{}, nil
	case tdms.TypeFloat64:
		return []float64{}, nil
	case tdms.TypeTimestamp:
		return []tdms.Timestamp{}, nil
	default:
		return nil, errCode("argument", "cannot read "+dt.String()+" as a Polars column")
	}
}
