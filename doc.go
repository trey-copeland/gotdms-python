// Package bridge is the gotdms Python bridge. It is a separate Go module so
// Apache Arrow can be linked into the gotdms-bridge binary without adding
// dependencies to the core TDMS module.
//
// The interactive protocol is version 1, little-endian frames on stdin and
// stdout. See Frame in frame.go. Sample columns travel as Arrow IPC streams
// inside the frame payload. They are not JSON and not base64.
//
// Batch conversion writes a directory bundle: manifest.json plus one Arrow
// IPC stream per channel. The Python client turns those streams into Polars
// frames. gotdms returns raw samples. A linear scale is
// applied only when the request sets scaled, and only when every contributing
// file has the same NI linear scale.
package bridge
