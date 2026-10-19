# gotdms

Python client for the MIT [gotdms](https://github.com/trey-copeland/gotdms) TDMS reader. Analysis stays in Python. Frames are Polars. Samples are NumPy arrays. The package does not import npTDMS.

NumPy is BSD-3-Clause. Polars and `polars-runtime-32` are MIT. pyarrow, used to read the Arrow IPC stream, is Apache-2.0. The license inventory is `THIRD_PARTY.md`.

Wheels bundle the `gotdms-bridge` binary for one platform. `locate_binary` checks an explicit path, then the `GOTDMS_BRIDGE` environment variable, then that bundled file. If none of those exist it raises `GotdmsError` and tells you to `pip install gotdms` or to build the binary with Go. It does not search `PATH`. Installing the source distribution compiles the binary (`CGO_ENABLED=0`, Go on `PATH`, network access to the module proxy) and tags the wheel for the machine it is building on. The sdist does not vendor gotdms.

```python
import gotdms

with gotdms.open(
    ["a.tdms", "b.tdms"],
    timestamp_channel="Time",
    numeric_epoch="2024-03-01T00:00:00Z",
) as session:
    values = session["acq"]["ai0"].data()  # raw float64
    times = session["acq"]["ai0"].time_track(absolute_time=True)
    frame = session.as_polars(time="column", align="strict")

lazy = gotdms.scan_tdms(
    "run/*.tdms",
    timestamp_channel="Time",
    numeric_epoch="2024-03-01T00:00:00Z",
)
frame = lazy.select("time", "acq/ai0").head(1000).collect()
```

`scan_tdms` returns a Polars `LazyFrame`. Calling it reads groups, channels, types, and sample counts. It does not decode channel samples. `select` then `head` or `limit`, with no filter, decodes only those channels and only that prefix (`Channel.Slice` on the bridge). A predicate runs in Python after the read. `slice(offset, length)` is not pushed on Polars 2: the selected columns are read in full and Polars slices the batch. The same timestamp rules as `open` apply, including a caller-supplied `basis` that a row limit does not move. See [`docs/nptdms-migration.md`](docs/nptdms-migration.md).

Polars has no row index. npTDMS `as_dataframe(time_index=True)` is the `time` column (`Datetime(ns, UTC)`). `align="strict"` requires equal time vectors and inserts no nulls. `align="nptdms"` is a full outer join on that column and inserts nulls where npTDMS inserted NaNs. `scaled` defaults to false. npTDMS scales unless you opt out. Pass `scaled=True` only when an NI linear scale should be applied. A missing scale is an error. Non-linear scales and DAQmx are not applied.

`time_track(absolute_time=False)` is seconds since the basis you set, not npTDMS's zero-at-first-sample track.

A pandas object, if you still want one, is `frame.to_pandas()` on the Polars result. This package does not depend on pandas.

Batch conversion, from the bundled binary or from `GOTDMS_BRIDGE`:

```bash
gotdms-bridge convert \
  --dir run --timestamp-channel Time \
  --numeric-epoch 2024-03-01T00:00:00Z \
  --out bundle
```

```python
bundle = gotdms.load_bundle("bundle")
frame = bundle.as_polars()
```

The long npTDMS mapping, including the LGPL-3.0 and MIT license note, is [`docs/nptdms-migration.md`](docs/nptdms-migration.md). This project's own license is MIT (`LICENSE`).

Python 3.10 or newer. The client and the bridge speak protocol version 1. That protocol number, not a core git tag, is the compatibility check between them.

This repository is [gotdms-python](https://github.com/trey-copeland/gotdms-python). The proposed PyPI distribution name is `gotdms`. The import package is `gotdms`. Nothing has been published to PyPI, and this repository has no release tag. `0.1.0` in `pyproject.toml` is the version recorded for that first release. It is not a tag.

The Go module requires [gotdms](https://github.com/trey-copeland/gotdms) at `v0.0.0-20261019014027-1b2458daaea8`, which is gotdms `main` commit `1b2458daaea8bf4f424b88fba93228a302ac05c1`. That commit has the dataset API this binary imports. gotdms has no release tag yet. When `v0.1.0` is tagged there, this requirement should become `github.com/trey-copeland/gotdms v0.1.0`. There is no `replace` directive. See [`EXTRACTION.md`](EXTRACTION.md).
