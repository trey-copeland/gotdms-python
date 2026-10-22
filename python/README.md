# Python package

The installable project is the repository root: `pyproject.toml`, `README.md`, and `hatch_build.py` live next to the Go module. This folder is the import package (`python/src/gotdms`) and the tests.

`pip install gotdms` uses a wheel that bundles `gotdms-bridge`. Resolution order is an explicit `binary=` argument, then `GOTDMS_BRIDGE`, then the bundled file. There is no `PATH` search. The error names `pip install gotdms` and the `go build` command.

NumPy (BSD-3-Clause), Polars, and pyarrow are dependencies. Pandas is not. npTDMS is not. `scan_tdms` is the lazy Polars entry point. `open` remains the eager session. `write_tdms` and `read_tdms` are the file export and its native read.
