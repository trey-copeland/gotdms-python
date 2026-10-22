package bridge

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/trey-copeland/gotdms"
)

func TestDAQmxNativeAndScaled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "daq.tdms")
	if err := os.WriteFile(path, daqmxAnalogDigital(), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := tdms.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ai := mustCh(t, f, "acq", "ai0")
	line := mustCh(t, f, "acq", "line0")
	if ai.DataType() != tdms.TypeInt16 || line.DataType() != tdms.TypeUint8 {
		t.Fatalf("types %s %s", ai.DataType(), line.DataType())
	}
	if _, ok := ai.DAQmx(); !ok {
		t.Fatal("ai0 is not DAQmx")
	}
	raw, _ := line.DAQmx()
	if !raw.Scalers[raw.Primary].Digital {
		t.Fatal("line0 is not digital")
	}
	assertSlice(t, mustVals(t, ai), []int16{1, 2, -1})
	assertSlice(t, mustVals(t, line), []uint8{1, 0, 1})
	scaled, err := ai.DAQmxScaledFloat64s()
	if err != nil {
		t.Fatal(err)
	}
	if !floatEq(scaled, []float64{2.5, 4.5, -1.5}) {
		t.Fatalf("library scale %v", scaled)
	}
	f.Close()

	header, payload, err := readValues(path)
	if err != nil {
		t.Fatal(err)
	}
	hdr := header.(map[string]any)
	groups := hdr["groups"].([]map[string]any)
	chans := groups[0]["channels"].([]map[string]any)
	if chans[0]["daqmx"] != true || chans[0]["type"] != "int16" || chans[0]["ni_type"] != "tdsTypeI16" {
		t.Fatalf("ai0 header %+v", chans[0])
	}
	if chans[1]["digital"] != true || chans[1]["type"] != "uint8" {
		t.Fatalf("line header %+v", chans[1])
	}
	parts, err := SplitPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := arrowInt16(t, parts[0]); !int16Eq(got, []int16{1, 2, -1}) {
		t.Fatalf("read_values ai0 %v", got)
	}
	if got := arrowUint8(t, parts[1]); !uint8Eq(got, []uint8{1, 0, 1}) {
		t.Fatalf("read_values line %v", got)
	}

	c := start(t)
	defer c.close()
	desc := c.call(t, map[string]any{"op": "describe", "paths": []string{path}})
	if desc.Op != "result" || desc.SampleReads != 0 {
		t.Fatalf("describe %+v", desc)
	}
	var sawAI, sawLine bool
	for _, ch := range desc.Channels {
		if ch.Name == "ai0" {
			sawAI = ch.DAQmx && ch.DAQmxScale && ch.HasScale && ch.DataType == "tdsTypeI16" && !ch.Digital
		}
		if ch.Name == "line0" {
			sawLine = ch.DAQmx && ch.Digital && !ch.DAQmxScale && ch.DataType == "tdsTypeU8"
		}
	}
	if !sawAI || !sawLine {
		t.Fatalf("catalog %+v", desc.Channels)
	}
	opened := c.call(t, map[string]any{"op": "open", "paths": []string{path}})
	if opened.Op != "result" {
		t.Fatalf("open %+v", opened)
	}

	rawRead := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq"})
	if rawRead.Op != "result" || len(rawRead.Read) != 1 || rawRead.Read[0].Scaled || !rawRead.Read[0].DAQmx {
		t.Fatalf("raw read %+v", rawRead)
	}
	if got := arrowInt16(t, onlyPart(t, rawRead.Payload)); !int16Eq(got, []int16{1, 2, -1}) {
		t.Fatalf("raw y %v", got)
	}
	lineRead := c.call(t, map[string]any{"op": "read", "name": "line0", "group": "acq"})
	if !lineRead.Read[0].Digital {
		t.Fatalf("digital meta %+v", lineRead.Read[0])
	}
	if got := arrowUint8(t, onlyPart(t, lineRead.Payload)); !uint8Eq(got, []uint8{1, 0, 1}) {
		t.Fatalf("digital y %v", got)
	}
	scaledRead := c.call(t, map[string]any{"op": "read", "name": "ai0", "group": "acq", "scaled": true})
	if !scaledRead.Read[0].Scaled || scaledRead.Read[0].DataType != "tdsTypeI16" {
		t.Fatalf("scaled meta %+v", scaledRead.Read[0])
	}
	y, _, _ := decodeOne(t, scaledRead)
	if !floatEq(y, []float64{2.5, 4.5, -1.5}) {
		t.Fatalf("scaled y %v", y)
	}
	noScale := c.call(t, map[string]any{"op": "read", "name": "line0", "group": "acq", "scaled": true})
	if noScale.Code != "no_scale" {
		t.Fatalf("digital scale %+v", noScale)
	}
	limited := c.call(t, map[string]any{"op": "read", "name": "line0", "group": "acq", "limit": 2})
	if !limited.Limited {
		t.Fatal("limit was ignored")
	}
	if got := arrowUint8(t, onlyPart(t, limited.Payload)); !uint8Eq(got, []uint8{1, 0}) {
		t.Fatalf("limit %v", got)
	}
	t0 := time.Date(1904, 1, 1, 0, 0, 0, 0, time.UTC)
	win := c.call(t, map[string]any{
		"op": "read", "name": "ai0", "group": "acq",
		"start": t0.Add(time.Second).Format(time.RFC3339Nano),
		"end":   t0.Add(3 * time.Second).Format(time.RFC3339Nano),
	})
	if win.Op != "result" {
		t.Fatalf("window %+v", win)
	}
	if got := arrowInt16(t, onlyPart(t, win.Payload)); !int16Eq(got, []int16{2, -1}) {
		t.Fatalf("window y %v", got)
	}
}

func TestDAQmxPolynomialPrescaledAndUnsupported(t *testing.T) {
	dir := t.TempDir()
	poly := filepath.Join(dir, "poly.tdms")
	plain := filepath.Join(dir, "prescaled.tdms")
	table := filepath.Join(dir, "table.tdms")
	for path, body := range map[string][]byte{
		poly:  daqmxPoly(),
		plain: daqmxPrescaled(),
		table: daqmxTable(),
	} {
		if err := os.WriteFile(path, body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	c := start(t)
	defer c.close()
	if err := openOne(c, t, poly); err != nil {
		t.Fatal(err)
	}
	got := c.call(t, map[string]any{"op": "read", "name": "p", "group": "g", "scaled": true})
	if got.Op != "result" || !got.Read[0].Scaled || !got.Read[0].DAQmxScale {
		t.Fatalf("poly %+v", got)
	}
	y, _, _ := decodeOne(t, got)
	if !floatEq(y, []float64{3, 5, -1}) {
		t.Fatalf("poly y %v", y)
	}
	raw := c.call(t, map[string]any{"op": "read", "name": "p", "group": "g"})
	if raw.Read[0].Scaled {
		t.Fatal("poly raw was scaled")
	}
	if vals := arrowInt16(t, onlyPart(t, raw.Payload)); !int16Eq(vals, []int16{1, 2, -1}) {
		t.Fatalf("poly raw %v", vals)
	}

	c.close()
	c = start(t)
	defer c.close()
	cat := c.call(t, map[string]any{"op": "open", "paths": []string{plain}})
	if cat.Op != "result" || !cat.Channels[0].DAQmxScale || cat.Channels[0].HasScale {
		t.Fatalf("prescaled catalog %+v", cat.Channels)
	}
	pre := c.call(t, map[string]any{"op": "read", "name": "p", "group": "g", "scaled": true})
	if !pre.Read[0].Scaled {
		t.Fatalf("prescaled %+v", pre.Read[0])
	}
	y, _, _ = decodeOne(t, pre)
	if !floatEq(y, []float64{1, 2, -1}) {
		t.Fatalf("prescaled y %v", y)
	}

	c.close()
	c = start(t)
	defer c.close()
	if cat = c.call(t, map[string]any{"op": "open", "paths": []string{table}}); cat.Op != "result" || !cat.Channels[0].HasNonlinear {
		t.Fatalf("table catalog %+v", cat)
	}
	kept := c.call(t, map[string]any{"op": "read", "name": "p", "group": "g"})
	if kept.Op != "result" || kept.Read[0].Scaled {
		t.Fatalf("table raw %+v", kept)
	}
	if vals := arrowInt16(t, onlyPart(t, kept.Payload)); !int16Eq(vals, []int16{1, 2, -1}) {
		t.Fatalf("table raw %v", vals)
	}
	bad := c.call(t, map[string]any{"op": "read", "name": "p", "group": "g", "scaled": true})
	if bad.Code != "nonlinear_scale" {
		t.Fatalf("table scale %+v", bad)
	}
}

func openOne(c *client, t *testing.T, path string) error {
	t.Helper()
	got := c.call(t, map[string]any{"op": "open", "paths": []string{path}})
	if got.Op != "result" {
		t.Fatalf("open %+v", got)
	}
	return nil
}

func mustCh(t *testing.T, f *tdms.File, group, name string) *tdms.Channel {
	t.Helper()
	g, ok := f.Group(group)
	if !ok {
		t.Fatalf("group %s", group)
	}
	ch, ok := g.Channel(name)
	if !ok {
		t.Fatalf("channel %s", name)
	}
	return ch
}

func mustVals(t *testing.T, ch *tdms.Channel) any {
	t.Helper()
	v, err := ch.Values()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func assertSlice[T comparable](t *testing.T, got any, want []T) {
	t.Helper()
	s, ok := got.([]T)
	if !ok || len(s) != len(want) {
		t.Fatalf("got %T %v want %v", got, got, want)
	}
	for i := range want {
		if s[i] != want[i] {
			t.Fatalf("got %v want %v", s, want)
		}
	}
}

func onlyPart(t *testing.T, payload []byte) []byte {
	t.Helper()
	parts, err := SplitPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 {
		t.Fatalf("parts %d", len(parts))
	}
	return parts[0]
}

func arrowInt16(t *testing.T, blob []byte) []int16 {
	t.Helper()
	col := arrowCol(t, blob)
	s, ok := col.(*array.Int16)
	if !ok {
		t.Fatalf("arrow %T %s", col, col.DataType())
	}
	return append([]int16(nil), s.Int16Values()...)
}

func arrowUint8(t *testing.T, blob []byte) []uint8 {
	t.Helper()
	col := arrowCol(t, blob)
	s, ok := col.(*array.Uint8)
	if !ok {
		t.Fatalf("arrow %T %s", col, col.DataType())
	}
	return append([]uint8(nil), s.Uint8Values()...)
}

func arrowCol(t *testing.T, blob []byte) arrow.Array {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(blob))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	rec, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	col := rec.Column(0)
	col.Retain()
	t.Cleanup(col.Release)
	return col
}

func int16Eq(a, b []int16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func uint8Eq(a, b []uint8) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// daqmxAnalogDigital is a hand-built little-endian segment. ai0 is an int16
// format-changing scaler with a linear DAQmx scale. line0 is a digital line
// in the second raw buffer. The bytes are not copied from npTDMS.
func daqmxAnalogDigital() []byte {
	widths := []uint32{2, 1}
	ai := tdmsObj{
		path:  "/'acq'/'ai0'",
		index: formatIndex(3, scalerFormat(3, 0, 0, 0), 1, widths),
		props: linearWaveProps(),
	}
	line := tdmsObj{
		path:  "/'acq'/'line0'",
		index: digitalIndex(3, scalerDigital(0, 1, 0, 0), 1, widths),
		props: waveProps(),
	}
	raw := []byte{0x01, 0x00, 0x02, 0x00, 0xFF, 0xFF, 0x01, 0x00, 0x03}
	return daqmxSegment([]tdmsObj{ai, line}, raw)
}

func daqmxPoly() []byte {
	props := [][]byte{
		propStr("NI_Scaling_Status", "unscaled"),
		propU32Name("NI_Number_Of_Scales", 1),
		propStr("NI_Scale[0]_Scale_Type", "Polynomial"),
		propU32Name("NI_Scale[0]_Polynomial_Coefficients_Size", 2),
		propF64Name("NI_Scale[0]_Polynomial_Coefficients[0]", 1),
		propF64Name("NI_Scale[0]_Polynomial_Coefficients[1]", 2),
		propU32Name("NI_Scale[0]_Polynomial_Input_Source", 0xFFFFFFFF),
	}
	return oneInt16(props)
}

func daqmxPrescaled() []byte {
	return oneInt16([][]byte{propStr("NI_Scaling_Status", "scaled")})
}

func daqmxTable() []byte {
	props := [][]byte{
		propStr("NI_Scaling_Status", "unscaled"),
		propU32Name("NI_Number_Of_Scales", 1),
		propStr("NI_Scale[0]_Scale_Type", "Table"),
	}
	return oneInt16(props)
}

func oneInt16(props [][]byte) []byte {
	widths := []uint32{2}
	obj := tdmsObj{
		path:  "/'g'/'p'",
		index: formatIndex(3, scalerFormat(3, 0, 0, 0), 1, widths),
		props: props,
	}
	return daqmxSegment([]tdmsObj{obj}, []byte{0x01, 0x00, 0x02, 0x00, 0xFF, 0xFF})
}

type tdmsObj struct {
	path  string
	index []byte
	props [][]byte
}

func daqmxSegment(objs []tdmsObj, raw []byte) []byte {
	meta := putU32(nil, uint32(len(objs)))
	for _, o := range objs {
		meta = putU32(meta, uint32(len(o.path)))
		meta = append(meta, o.path...)
		meta = append(meta, o.index...)
		meta = putU32(meta, uint32(len(o.props)))
		for _, p := range o.props {
			meta = append(meta, p...)
		}
	}
	toc := tdms.TocMetaData | tdms.TocNewObjList | tdms.TocRawData | tdms.TocDAQmxRawData
	file := make([]byte, 28, 28+len(meta)+len(raw))
	copy(file[:4], "TDSm")
	binary.LittleEndian.PutUint32(file[4:8], toc)
	binary.LittleEndian.PutUint32(file[8:12], 4713)
	binary.LittleEndian.PutUint64(file[12:20], uint64(len(meta)+len(raw)))
	binary.LittleEndian.PutUint64(file[20:28], uint64(len(meta)))
	file = append(file, meta...)
	return append(file, raw...)
}

func formatIndex(n uint64, scaler []byte, nScaler int, widths []uint32) []byte {
	return daqIndex(tdms.RawIndexDAQmxFormatChanging, n, scaler, nScaler, widths)
}

func digitalIndex(n uint64, scaler []byte, nScaler int, widths []uint32) []byte {
	return daqIndex(tdms.RawIndexDAQmxDigitalObserved, n, scaler, nScaler, widths)
}

func daqIndex(header uint32, n uint64, scaler []byte, nScaler int, widths []uint32) []byte {
	b := putU32(nil, header)
	b = putU32(b, 0xFFFFFFFF)
	b = putU32(b, 1)
	b = putU64(b, n)
	b = putU32(b, uint32(nScaler))
	b = append(b, scaler...)
	b = putU32(b, uint32(len(widths)))
	for _, w := range widths {
		b = putU32(b, w)
	}
	return b
}

func scalerFormat(typ, buf, off, scale uint32) []byte {
	b := putU32(nil, typ)
	b = putU32(b, buf)
	b = putU32(b, off)
	b = putU32(b, 0)
	return putU32(b, scale)
}

func scalerDigital(typ, buf, bit, scale uint32) []byte {
	b := putU32(nil, typ)
	b = putU32(b, buf)
	b = putU32(b, bit)
	b = append(b, 0)
	return putU32(b, scale)
}

func waveProps() [][]byte {
	return [][]byte{
		propRaw("wf_start_time", tdms.TypeTimestamp, make([]byte, 16)),
		propF64Name("wf_increment", 1),
	}
}

func linearWaveProps() [][]byte {
	// Scale 1 is the output. Input source 0 has no NI_Scale type, so it is
	// the raw scaler whose ScaleID is 0. That is the NI linear example.
	return append(waveProps(),
		propStr("NI_Scaling_Status", "unscaled"),
		propU32Name("NI_Number_Of_Scales", 2),
		propStr("NI_Scale[1]_Scale_Type", "Linear"),
		propF64Name("NI_Scale[1]_Linear_Slope", 2),
		propF64Name("NI_Scale[1]_Linear_Y_Intercept", 0.5),
		propU32Name("NI_Scale[1]_Linear_Input_Source", 0),
	)
}

func propRaw(name string, dt tdms.DataType, val []byte) []byte {
	b := putU32(nil, uint32(len(name)))
	b = append(b, name...)
	b = putU32(b, uint32(dt))
	return append(b, val...)
}

func propStr(name, val string) []byte {
	body := putU32(nil, uint32(len(val)))
	body = append(body, val...)
	return propRaw(name, tdms.TypeString, body)
}

func propF64Name(name string, v float64) []byte {
	return propRaw(name, tdms.TypeFloat64, putF64(nil, v))
}

func propU32Name(name string, v uint32) []byte {
	return propRaw(name, tdms.TypeUint32, putU32(nil, v))
}

func putU32(b []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(b, buf[:]...)
}

func putU64(b []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(b, buf[:]...)
}

func putF64(b []byte, v float64) []byte {
	return putU64(b, math.Float64bits(v))
}
