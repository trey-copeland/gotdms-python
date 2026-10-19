// Command gotdms-bridge is the Python bridge for gotdms.
//
//	gotdms-bridge serve
//	gotdms-bridge convert --dir run --timestamp-channel Time --numeric-epoch 2024-03-01T00:00:00Z --out out
//	gotdms-bridge demo --out fixtures
//
// serve speaks protocol version 1 on stdin and stdout. convert writes an
// Arrow IPC bundle. demo writes the small multi-file set used by tests.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/trey-copeland/gotdms-python"
	"github.com/trey-copeland/gotdms/dataset"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = bridge.Serve(os.Stdin, os.Stdout)
	case "convert":
		err = runConvert(os.Args[2:])
	case "demo":
		err = runDemo(os.Args[2:])
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "gotdms-bridge: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "gotdms-bridge: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `gotdms-bridge is the MIT Python bridge for gotdms.

  gotdms-bridge serve
  gotdms-bridge convert [options]
  gotdms-bridge demo --out dir

serve reads versioned frames on stdin and writes them on stdout.
Sample columns are Arrow IPC streams in the frame payload, not JSON.
convert writes manifest.json and data/N.arrow. Samples stay raw unless
--scaled is set and every file of the channel has the same NI linear scale.
`)
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	out := fs.String("out", "", "directory to write")
	kind := fs.String("set", "demo", "demo, waveform, or gap")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("demo needs --out")
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	var set bridge.DemoSet
	var err error
	switch *kind {
	case "demo":
		set, err = bridge.WriteDemoSet(*out)
	case "waveform":
		set, err = bridge.WriteWaveformSet(*out)
	case "gap":
		set, err = bridge.WriteGapSet(*out)
	default:
		return fmt.Errorf("unknown demo set %q", *kind)
	}
	if err != nil {
		return err
	}
	doc := struct {
		Epoch string   `json:"epoch"`
		Dir   string   `json:"dir"`
		Files []string `json:"files"`
		Set   string   `json:"set"`
	}{Epoch: set.Epoch.Format(time.RFC3339Nano), Dir: set.Dir, Files: set.Files, Set: *kind}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(doc)
}

func runConvert(args []string) error {
	fs := flag.NewFlagSet("convert", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var paths multi
	fs.Var(&paths, "path", "TDMS file (repeatable)")
	dir := fs.String("dir", "", "directory of TDMS files")
	pattern := fs.String("pattern", "*.tdms", "glob inside --dir")
	ts := fs.String("timestamp-channel", "", "timestamp channel name for every group")
	var per multi
	fs.Var(&per, "group-timestamp", "group=channel override; empty channel opts the group out")
	epoch := fs.String("numeric-epoch", "", "RFC3339 epoch for numeric timestamp channels")
	basis := fs.String("basis", "", "RFC3339 basis for relative time")
	overlap := fs.String("overlap", "", "keep_first, keep_last, or error")
	workers := fs.Int("workers", 0, "catalog workers")
	readWorkers := fs.Int("read-workers", 0, "read workers")
	var channels multi
	fs.Var(&channels, "channel", "channel name or group/name (repeatable)")
	start := fs.String("start", "", "window start RFC3339, inclusive")
	end := fs.String("end", "", "window end RFC3339, exclusive")
	absolute := fs.Bool("absolute", true, "include absolute time_ns")
	relative := fs.Bool("relative", true, "include basis-relative relative_s")
	scaled := fs.Bool("scaled", false, "apply an NI linear scale; error if none exists")
	out := fs.String("out", "", "output directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opt := bridge.ConvertOptions{
		Paths:            paths,
		Directory:        *dir,
		Pattern:          *pattern,
		TimestampChannel: *ts,
		Workers:          *workers,
		ReadWorkers:      *readWorkers,
		Overlap:          *overlap,
		Absolute:         absolute,
		Relative:         relative,
		Scaled:           *scaled,
		OutDir:           *out,
	}
	if *epoch != "" {
		t, err := time.Parse(time.RFC3339Nano, *epoch)
		if err != nil {
			return fmt.Errorf("numeric-epoch: %w", err)
		}
		opt.NumericEpoch = &t
	}
	if *basis != "" {
		t, err := time.Parse(time.RFC3339Nano, *basis)
		if err != nil {
			return fmt.Errorf("basis: %w", err)
		}
		opt.Basis = &t
	}
	if *start != "" || *end != "" {
		if *start == "" || *end == "" {
			return fmt.Errorf("window needs both --start and --end")
		}
		a, err := time.Parse(time.RFC3339Nano, *start)
		if err != nil {
			return err
		}
		b, err := time.Parse(time.RFC3339Nano, *end)
		if err != nil {
			return err
		}
		opt.WindowStart = &a
		opt.WindowEnd = &b
	}
	if len(per) > 0 {
		opt.TimestampChannels = map[string]string{}
		for _, item := range per {
			group, name, ok := strings.Cut(item, "=")
			if !ok {
				return fmt.Errorf("group-timestamp %q wants group=channel", item)
			}
			opt.TimestampChannels[group] = name
		}
	}
	for _, item := range channels {
		group, name, ok := strings.Cut(item, "/")
		if ok {
			opt.Channels = append(opt.Channels, dataset.Selector{Group: group, Name: name})
		} else {
			opt.Channels = append(opt.Channels, dataset.Selector{Name: item})
		}
	}
	if err := bridge.Convert(opt); err != nil {
		return err
	}
	fmt.Println(*out)
	return nil
}

type multi []string

func (m *multi) String() string { return strings.Join(*m, ",") }

func (m *multi) Set(v string) error {
	*m = append(*m, v)
	return nil
}
