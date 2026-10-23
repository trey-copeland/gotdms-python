package bridge

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/trey-copeland/gotdms/dataset"
)

// ConvertOptions is the batch conversion of one file set.
// Channels empty means every channel in the catalog.
// Window is half-open. Both ends empty reads each channel's full span.
// Scaled applies an NI linear scale; it does not invent one.
// Absolute and Relative nil mean true: the bundle includes time_ns and
// relative_s unless the caller sets the pointer to false.
type ConvertOptions struct {
	Paths             []string
	Directory         string
	Pattern           string
	TimestampChannel  string
	TimestampChannels map[string]string
	NumericEpoch      *time.Time
	Basis             *time.Time
	Overlap           string
	Workers           int
	ReadWorkers       int
	Channels          []dataset.Selector
	WindowStart       *time.Time
	WindowEnd         *time.Time
	Absolute          *bool
	Relative          *bool
	Scaled            bool
	OutDir            string
}

// Convert writes a directory bundle Polars can open with one call.
//
//	manifest.json
//	data/0.arrow
//	data/1.arrow
//
// Each .arrow file is one Arrow IPC stream for one channel. manifest.json
// repeats the schema metadata (name, group, dtype, basis, scale, properties)
// and the relative data path. Samples are the Arrow columns, not CSV and
// not base64. Convert refuses to overwrite an existing manifest.json.
func Convert(opt ConvertOptions) error {
	if opt.OutDir == "" {
		return fmt.Errorf("bridge: convert needs an output directory")
	}
	if _, err := os.Stat(filepath.Join(opt.OutDir, "manifest.json")); err == nil {
		return fmt.Errorf("bridge: %s already contains manifest.json", opt.OutDir)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	req := map[string]json.RawMessage{}
	put := func(k string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		req[k] = b
	}
	if len(opt.Paths) > 0 {
		put("paths", opt.Paths)
	}
	if opt.Directory != "" {
		put("directory", opt.Directory)
		pat := opt.Pattern
		if pat == "" {
			pat = "*.tdms"
		}
		put("pattern", pat)
	}
	if opt.TimestampChannel != "" {
		put("timestamp_channel", opt.TimestampChannel)
	}
	if len(opt.TimestampChannels) > 0 {
		put("timestamp_channels", opt.TimestampChannels)
	}
	if opt.NumericEpoch != nil {
		put("numeric_epoch", opt.NumericEpoch.UTC().Format(time.RFC3339Nano))
	}
	if opt.Basis != nil {
		put("basis", opt.Basis.UTC().Format(time.RFC3339Nano))
	}
	if opt.Overlap != "" {
		put("overlap", opt.Overlap)
	}
	if opt.Workers > 0 {
		put("workers", opt.Workers)
	}
	if opt.ReadWorkers > 0 {
		put("read_workers", opt.ReadWorkers)
	}
	s := &session{}
	if _, _, err := s.open(req); err != nil {
		return err
	}
	sels := opt.Channels
	if len(sels) == 0 {
		seen := map[string]struct{}{}
		for _, o := range s.ds.Occurrences() {
			k := o.Group + "\x00" + o.Name
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			sels = append(sels, dataset.Selector{Name: o.Name, Group: o.Group})
		}
	}
	readReq := map[string]json.RawMessage{}
	putR := func(k string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			panic(err)
		}
		readReq[k] = b
	}
	abs := true
	if opt.Absolute != nil {
		abs = *opt.Absolute
	}
	rel := true
	if opt.Relative != nil {
		rel = *opt.Relative
	}
	putR("absolute", abs)
	putR("relative", rel)
	putR("scaled", opt.Scaled)
	if opt.Basis != nil {
		putR("basis", opt.Basis.UTC().Format(time.RFC3339Nano))
	}
	if opt.WindowStart != nil || opt.WindowEnd != nil {
		if opt.WindowStart == nil || opt.WindowEnd == nil {
			return fmt.Errorf("bridge: window needs both start and end")
		}
		putR("start", opt.WindowStart.UTC().Format(time.RFC3339Nano))
		putR("end", opt.WindowEnd.UTC().Format(time.RFC3339Nano))
	}
	encs, err := s.readMany(sels, readReq)
	if err != nil {
		return err
	}
	return writeBundle(opt.OutDir, s.ds.Basis(), encs)
}

type bundleChannel struct {
	channelOut
	Path string `json:"path"`
}

func writeBundle(dir string, basis time.Time, encs []*seriesEnc) error {
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		return err
	}
	entries := make([]bundleChannel, len(encs))
	for i, e := range encs {
		rel := fmt.Sprintf("data/%d.arrow", i)
		// A bundle file stands alone, so it keeps its own time_ns column.
		withTime := e.cols.absolute && e.hasTimes()
		var ns []int64
		if withTime {
			ns = e.timeNs()
		}
		e.meta.Columns = columnNames(withTime, e.cols.relative)
		if err := writeBundleFile(filepath.Join(dir, filepath.FromSlash(rel)), e, ns, withTime); err != nil {
			return err
		}
		entries[i] = bundleChannel{channelOut: e.meta, Path: rel}
	}
	doc := struct {
		GotdmsBridge int             `json:"gotdms_bridge"`
		Format       string          `json:"format"`
		Basis        string          `json:"basis"`
		Channels     []bundleChannel `json:"channels"`
	}{
		GotdmsBridge: int(ProtocolVersion),
		Format:       "arrow-ipc-stream",
		Basis:        basis.UTC().Format(time.RFC3339Nano),
		Channels:     entries,
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	// Properties may contain the non-finite placeholder. The manifest is
	// JSON that Python reads, so those become NaN and Infinity tokens.
	b = restoreNonFinite(b)
	b = append(b, '\n')
	return os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0o644)
}

func writeBundleFile(path string, e *seriesEnc, ns []int64, withTime bool) error {
	rec, err := e.record(ns, withTime)
	if err != nil {
		return err
	}
	defer rec.Release()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := writeRecord(f, rec); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
