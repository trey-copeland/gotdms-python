# Changelog

Nothing in this repository has been tagged. Nothing has been uploaded to PyPI.

## 0.1.0 (unreleased)

First packaging of the Python client and the `gotdms-bridge` binary. `0.1.0` is the version in `pyproject.toml`. It is not a git tag.

- Interactive `serve` protocol version 1 and `convert` bundles, Arrow IPC.
- `scan_tdms` returns a Polars `LazyFrame`. `describe` reads catalog metadata and does not decode samples. `read` and `read_many` take optional `offset` and `limit` and report `reads` and `limited`. Protocol version stays 1.
- Polars frames via `as_polars`. numpy arrays for raw samples. Pandas is not a dependency.
- Wheels embed one platform binary. Source distributions ship `go.mod` and `go.sum` and do not vendor gotdms. `go build` resolves `github.com/trey-copeland/gotdms` at the pseudo-version required in `go.mod`.
- That pseudo-version is gotdms `main` at `1b2458daaea8bf4f424b88fba93228a302ac05c1`. Switch the requirement to `v0.1.0` once gotdms is tagged. See `EXTRACTION.md`.
