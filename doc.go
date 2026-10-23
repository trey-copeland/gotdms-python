// Package bridge is the gotdms Python bridge. It is a separate Go module so
// Apache Arrow can be linked into the gotdms-bridge binary without adding
// dependencies to the core TDMS module.
//
// The interactive protocol is version 2, little-endian frames on stdin and
// stdout. See Frame in frame.go. Sample columns travel as Arrow IPC streams
// inside frame payloads. They are not JSON and not base64. A data reply is
// result_begin, part frames of at most MaxPart bytes, and result_end, so it
// has no size ceiling, and the bridge writes each stream straight from the
// series' slices without building the reply in memory. Channels and clocks
// are columns packed into few streams (see packStreams); each distinct time
// axis is sent once, and channels name their clock by id.
//
// Batch conversion writes a directory bundle: manifest.json plus one Arrow
// IPC stream per channel. The Python client turns those streams into Polars
// frames. gotdms returns raw samples. A linear scale is
// applied only when the request sets scaled, and only when every contributing
// file has the same NI linear scale. A DAQmx channel is the primary scaler's
// stored type (digital lines are uint8). scaled=True on a DAQmx channel
// applies DAQmxScaledFloat64s and returns float64. No scale is an error.
//
// describe returns the same catalog fields as open and does not decode
// channel samples. read and read_many accept an optional sample offset and
// limit. The response adds reads (the slices that were decoded) and limited.
// Omitting both fields keeps the full dataset read.
//
// write_begin, write_chunk, and write_end write TDMS files. read_values
// returns stored channel values in native Arrow types. Each channel is read
// on its own, so an interleaved channel that continues in a later segment
// stays in sample order.
package bridge
