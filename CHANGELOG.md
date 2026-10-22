# Changelog

Nothing in this repository has been tagged. Nothing has been uploaded to PyPI.

## 0.1.0 (unreleased)

First packaging of the Python client and the `gotdms-bridge` binary. `0.1.0` is the version in `pyproject.toml`. It is not a git tag.

- Interactive `serve` protocol version 1 and `convert` bundles, Arrow IPC.
- `scan_tdms` returns a Polars `LazyFrame`. `describe` reads catalog metadata and does not decode samples. `read` and `read_many` take optional `offset` and `limit` and report `reads` and `limited`. Protocol version stays 1.
- `write_tdms` writes one or more Polars groups to a TDMS file: file, group, and channel properties (including `unit_string` and explicit integer widths), waveform properties, string channels, and channels whose lengths differ. `read_tdms` reads those samples back in the stored type. `Session.read` stays float64. Fixed-size columns use `Writer.Stream`. Create replaces the destination only after the file reopens. Append adds a segment when the existing file ends on a finished segment. Protocol version stays 1 (`write_begin`, `write_chunk`, `write_end`, `read_values`).
- Polars frames via `as_polars`. numpy arrays for raw samples. Pandas is not a dependency. npTDMS is not a dependency. CI installs it only as a reader of files this package wrote.
- Wheels embed one platform binary. Source distributions ship `go.mod` and `go.sum` and do not vendor gotdms. `go build` resolves `github.com/trey-copeland/gotdms` at the pseudo-version required in `go.mod`.
- That pseudo-version is gotdms `main` at `9ad35ef91606a11bac250d0fc763f32fab09b6b8`. It includes `Writer.Stream` and `File.Status`. Switch the requirement to `v0.1.0` once gotdms is tagged. See `EXTRACTION.md`.
