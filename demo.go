package bridge

import (
	"path/filepath"
	"time"

	"github.com/trey-copeland/gotdms"
)

// DemoEpoch is the numeric epoch of the files WriteDemoSet writes.
// Timestamp channels store seconds since this instant.
var DemoEpoch = time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)

// DemoSet is a small multi-file TDMS set for tests and the demo subcommand.
//
//	a.tdms  group acq  Time 0,1,2,3  ai0 10..13  ai1 1..4 (linear scale 2*x+0.5)
//	        group fast Time 0,0.5,1,1.5,2  volt 0..4
//	b.tdms  group acq  Time 4,5  ai0 14,15  ai1 5,6 (same linear scale)
//	c.tdms  group other Time 0,1  ai0 7,8  poly 0,1 (polynomial scale, not applied)
//	        group raw   x 9,8,7 (no timestamp channel)
//	        group dups  Time 0,0,1  d 1,2,3
type DemoSet struct {
	Dir   string
	Files []string
	Epoch time.Time
}

// WriteDemoSet writes the demo files into dir using the core writer.
func WriteDemoSet(dir string) (DemoSet, error) {
	a := filepath.Join(dir, "a.tdms")
	b := filepath.Join(dir, "b.tdms")
	c := filepath.Join(dir, "c.tdms")
	if err := writeA(a); err != nil {
		return DemoSet{}, err
	}
	if err := writeB(b); err != nil {
		return DemoSet{}, err
	}
	if err := writeC(c); err != nil {
		return DemoSet{}, err
	}
	return DemoSet{Dir: dir, Files: []string{a, b, c}, Epoch: DemoEpoch}, nil
}

func writeA(path string) error {
	w, err := tdms.Create(path)
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.SetProperty("title", "demo-a"); err != nil {
		return err
	}
	g := w.Group("acq")
	if err := writeF64(g, "Time", []float64{0, 1, 2, 3}); err != nil {
		return err
	}
	if err := writeF64(g, "ai0", []float64{10, 11, 12, 13}); err != nil {
		return err
	}
	if err := writeScaledAI1(g, []float64{1, 2, 3, 4}); err != nil {
		return err
	}
	fast := w.Group("fast")
	if err := writeF64(fast, "Time", []float64{0, 0.5, 1, 1.5, 2}); err != nil {
		return err
	}
	return writeF64(fast, "volt", []float64{0, 1, 2, 3, 4})
}

func writeB(path string) error {
	w, err := tdms.Create(path)
	if err != nil {
		return err
	}
	defer w.Close()
	g := w.Group("acq")
	if err := writeF64(g, "Time", []float64{4, 5}); err != nil {
		return err
	}
	if err := writeF64(g, "ai0", []float64{14, 15}); err != nil {
		return err
	}
	return writeScaledAI1(g, []float64{5, 6})
}

func writeC(path string) error {
	w, err := tdms.Create(path)
	if err != nil {
		return err
	}
	defer w.Close()
	other := w.Group("other")
	if err := writeF64(other, "Time", []float64{0, 1}); err != nil {
		return err
	}
	if err := writeF64(other, "ai0", []float64{7, 8}); err != nil {
		return err
	}
	poly, err := other.Channel("poly", tdms.TypeFloat64)
	if err != nil {
		return err
	}
	if err := poly.SetProperty("NI_Scale[0]_Scale_Type", "Polynomial"); err != nil {
		return err
	}
	if err := poly.Write([]float64{0, 1}); err != nil {
		return err
	}
	raw := w.Group("raw")
	if err := writeF64(raw, "x", []float64{9, 8, 7}); err != nil {
		return err
	}
	dups := w.Group("dups")
	if err := writeF64(dups, "Time", []float64{0, 0, 1}); err != nil {
		return err
	}
	return writeF64(dups, "d", []float64{1, 2, 3})
}

// WriteWaveformSet writes one file whose time axis is waveform properties.
// A channel named Time is ordinary data. Nothing is inferred from that name.
//
//	w.tdms  group w  ai0 10..13 and ai1 1..4 share wf_start_time DemoEpoch
//	        and wf_increment 1s. Time stores 100..103 and is not a clock.
func WriteWaveformSet(dir string) (DemoSet, error) {
	path := filepath.Join(dir, "w.tdms")
	w, err := tdms.Create(path)
	if err != nil {
		return DemoSet{}, err
	}
	defer w.Close()
	g := w.Group("w")
	if err := writeWave(g, "ai0", DemoEpoch, 1, []float64{10, 11, 12, 13}); err != nil {
		return DemoSet{}, err
	}
	if err := writeWave(g, "ai1", DemoEpoch, 1, []float64{1, 2, 3, 4}); err != nil {
		return DemoSet{}, err
	}
	if err := writeF64(g, "Time", []float64{100, 101, 102, 103}); err != nil {
		return DemoSet{}, err
	}
	return DemoSet{Dir: dir, Files: []string{path}, Epoch: DemoEpoch}, nil
}

// WriteGapSet writes two files with a hole between them.
//
//	ga.tdms  acq Time 0,1  ai0 10,11
//	gb.tdms  acq Time 4,5  ai0 14,15
//
// The merged series is four samples. The instants 2s and 3s are a gap, not
// null-filled rows.
func WriteGapSet(dir string) (DemoSet, error) {
	a := filepath.Join(dir, "ga.tdms")
	b := filepath.Join(dir, "gb.tdms")
	if err := writeStampFile(a, []float64{0, 1}, []float64{10, 11}); err != nil {
		return DemoSet{}, err
	}
	if err := writeStampFile(b, []float64{4, 5}, []float64{14, 15}); err != nil {
		return DemoSet{}, err
	}
	return DemoSet{Dir: dir, Files: []string{a, b}, Epoch: DemoEpoch}, nil
}

func writeStampFile(path string, times, values []float64) error {
	w, err := tdms.Create(path)
	if err != nil {
		return err
	}
	defer w.Close()
	g := w.Group("acq")
	if err := writeF64(g, "Time", times); err != nil {
		return err
	}
	return writeF64(g, "ai0", values)
}

func writeWave(g *tdms.GroupWriter, name string, start time.Time, inc float64, vals []float64) error {
	ch, err := g.Channel(name, tdms.TypeFloat64)
	if err != nil {
		return err
	}
	if err := ch.SetProperty(tdms.PropStartTime, start); err != nil {
		return err
	}
	if err := ch.SetProperty(tdms.PropIncrement, inc); err != nil {
		return err
	}
	return ch.Write(vals)
}

func writeF64(g *tdms.GroupWriter, name string, vals []float64) error {
	ch, err := g.Channel(name, tdms.TypeFloat64)
	if err != nil {
		return err
	}
	return ch.Write(vals)
}

func writeScaledAI1(g *tdms.GroupWriter, vals []float64) error {
	ch, err := g.Channel("ai1", tdms.TypeFloat64)
	if err != nil {
		return err
	}
	if err := ch.SetProperty(tdms.PropUnitString, "V"); err != nil {
		return err
	}
	if err := ch.SetProperty("NI_Scale[1]_Scale_Type", "Linear"); err != nil {
		return err
	}
	if err := ch.SetProperty("NI_Scale[1]_Linear_Slope", 2.0); err != nil {
		return err
	}
	if err := ch.SetProperty("NI_Scale[1]_Linear_Y_Intercept", 0.5); err != nil {
		return err
	}
	return ch.Write(vals)
}
