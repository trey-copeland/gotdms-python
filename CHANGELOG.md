# Changelog

Nothing in this repository has been tagged. Nothing has been uploaded to PyPI.

## 0.1.0 (unreleased)

First packaging of the Python client and the `gotdms-bridge` binary. `0.1.0` is the version in `pyproject.toml`. It is not a git tag.

- Interactive `serve` protocol version 1 and `convert` bundles, Arrow IPC.
- `scan_tdms` returns a Polars `LazyFrame`. `describe` reads catalog metadata and does not decode samples. `read` and `read_many` take optional `offset` and `limit` and report `reads` and `limited`. Protocol version stays 1.
- `write_tdms` writes one or more Polars groups to a TDMS file: file, group, and channel properties (including `unit_string` and explicit integer widths), waveform properties, string channels, and channels whose lengths differ. `read_tdms` reads those samples back in the stored type. `Session.read` stays float64. Fixed-size columns use `Writer.Stream`. Create replaces the destination only after the file reopens. Append adds a segment when the existing file ends on a finished segment. Protocol version stays 1 (`write_begin`, `write_chunk`, `write_end`, `read_values`).
- Write and read fixes from review. A failed or interrupted append truncates the file back to its original length. Before this fix it left a partial segment that readers returned and later appends refused. Create mode follows the umask, so it no longer forces 0600, and replacing a file keeps its mode. Replacing a file never deletes the destination first. On Windows the move is retried briefly instead. NaN and infinite float properties no longer stop the bridge in `open`, `scan_tdms`, or `read_tdms`. A result header that cannot be encoded, or a payload over 4 GiB, is now an error reply.
- Polars frames via `as_polars`. numpy arrays for raw samples. Pandas is not a dependency. npTDMS is not a dependency. CI installs it only as a reader of files this package wrote.
- Wheels embed one platform binary. Source distributions ship `go.mod` and `go.sum` and do not vendor gotdms. `go build` resolves `github.com/trey-copeland/gotdms` at the pseudo-version required in `go.mod`.
- That pseudo-version is gotdms `main` at `622cb34bacb80fd810d46ee77183e2bae80bf52d` (`v0.0.0-20261022022333-622cb34bacb8`). It includes `Writer.Stream`, `File.Status`, and DAQmx raw reads. Switch the requirement to `v0.1.0` once gotdms is tagged. See `EXTRACTION.md`.
- DAQmx raw channels are read. Index words `0x1269`, `0x1369`, and `0x126A` become the primary scaler's stored type. Digital lines are uint8 0/1, which Polars reads as `UInt8`. `open`, `read`, `read_tdms`, and `scan_tdms` keep those values raw unless `scaled=True`. `scaled=True` applies the DAQmx linear or polynomial chain (`DAQmxScaledFloat64s`) and returns float64. A channel with no DAQmx scale raises `NoScaleError`. An unsupported scale type raises `NonlinearScaleError`. `NI_Scaling_Status` of `scaled` means the stored samples are already engineering units, so `scaled=True` returns those values as float64. Ordinary channels still apply only an NI linear scale. The column is the primary scaler. Writing a DAQmx raw index is not supported. Protocol version stays 1.
