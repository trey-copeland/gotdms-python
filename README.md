# gotdms

Python client for the MIT [gotdms](https://github.com/trey-copeland/gotdms) TDMS reader and writer. Analysis stays in Python. Frames are Polars. Samples are NumPy arrays. The package does not import npTDMS.

NumPy is BSD-3-Clause. Polars and `polars-runtime-32` are MIT. pyarrow, used to read the Arrow IPC stream, is Apache-2.0. The license inventory is `THIRD_PARTY.md`.

The package is not on PyPI yet; publishing it as `gotdms` is planned, so `pip install gotdms` does not work today. Install it from a checkout of this repository:

```bash
pip install .
```

That builds a wheel for the machine it runs on and compiles the `gotdms-bridge` binary into it (`CGO_ENABLED=0`, Go 1.22 or newer on `PATH`, network access to the Go module proxy). The source tree does not vendor gotdms.

A wheel bundles the `gotdms-bridge` binary for one platform. `locate_binary` checks an explicit path, then the `GOTDMS_BRIDGE` environment variable, then that bundled file. If none of those exist it raises `GotdmsError`, and the message says to build the binary with Go and point `GOTDMS_BRIDGE` at it. (The message also suggests `pip install gotdms`, which only works once the package is published.) It does not search `PATH`.

```python
import gotdms

with gotdms.open(
    ["a.tdms", "b.tdms"],
    timestamp_channel="Time",
    numeric_epoch="2024-03-01T00:00:00Z",
) as session:
    values = session["acq"]["ai0"].data()  # raw float64
    times = session["acq"]["ai0"].time_track(absolute_time=True)
    frame = session["acq"].as_polars(time="column", align="strict")

lazy = gotdms.scan_tdms(
    "run/*.tdms",
    timestamp_channel="Time",
    numeric_epoch="2024-03-01T00:00:00Z",
)
frame = lazy.select("time", "acq/ai0").head(1000).collect()
```

```python
import polars as pl
import gotdms

gotdms.write_tdms(
    "cleaned.tdms",
    {
        "data": pl.DataFrame({"voltage": [1.0, 1.5, 1.25], "current": [0.1, 0.2, 0.15]}),
        "log": pl.DataFrame({"message": ["opened", "cleaned"]}),
    },
    properties={"title": "cleaned"},
    channel_properties={"data": {"voltage": {"unit_string": "V"}, "current": {"unit_string": "A"}}},
)
native = gotdms.read_tdms("cleaned.tdms")
```

`write_tdms` takes one Polars frame or a mapping of group name to a frame (or to a mapping of channel name to a Series, when lengths differ). `read_tdms` returns the stored types. `session["group"]["channel"].data()` stays float64 and still rejects a string channel. The [migration note](docs/nptdms-migration.md) maps this to `TdmsWriter`.

`scan_tdms` returns a Polars `LazyFrame`. Calling it reads groups, channels, types, and sample counts. It does not decode channel samples. `select` then `head` or `limit`, with no filter, decodes only those channels and only that prefix (`Channel.Slice` on the bridge). A predicate runs in Python after the read. `slice(offset, length)` is not pushed on Polars 2: the selected columns are read in full and Polars slices the batch. The same timestamp rules as `open` apply, including a caller-supplied `basis` that a row limit does not move. See [`docs/nptdms-migration.md`](docs/nptdms-migration.md).

Polars has no row index. npTDMS `as_dataframe(time_index=True)` is the `time` column (`Datetime(ns, UTC)`). `align="strict"` requires equal time vectors and inserts no nulls. `align="nptdms"` is a full outer join on that column and inserts nulls where npTDMS inserted NaNs. `scaled` defaults to false. npTDMS scales unless you opt out. Pass `scaled=True` only when a scale should be applied. A missing scale is an error. An ordinary channel applies an NI linear scale. A DAQmx channel stays in the primary scaler's stored type (a digital line is `UInt8` of 0 and 1) until `scaled=True`, which applies a DAQmx linear or polynomial chain and returns float64. Other non-linear scales are not applied.

`time_track(absolute_time=False)` is seconds since the basis you set, not npTDMS's zero-at-first-sample track.

A pandas object, if you still want one, is `frame.to_pandas()` on the Polars result. This package does not depend on pandas.

Batch conversion, from the bundled binary or from `GOTDMS_BRIDGE`. Installing the package does not put `gotdms-bridge` on `PATH`; `python -c 'import gotdms; print(gotdms.locate_binary())'` prints the path the client uses.

```bash
gotdms-bridge convert \
  --dir run --timestamp-channel Time \
  --numeric-epoch 2024-03-01T00:00:00Z \
  --channel acq/ai0 --channel acq/ai1 \
  --out bundle
```

```python
import gotdms

bundle = gotdms.load_bundle("bundle")
frame = bundle.as_polars()
```

`as_polars` defaults to `align="strict"`, so the selected channels must share one time vector. Channels on different clocks raise `AlignmentError`; select channels that share one, or pass `align="nptdms"` for a full join.

The long npTDMS mapping, including the LGPL-3.0 and MIT license note, is [`docs/nptdms-migration.md`](docs/nptdms-migration.md). This project's own license is MIT (`LICENSE`).

Python 3.10 or newer. The client and the bridge speak protocol version 2. That protocol number, not a core git tag, is the compatibility check between them. A version 1 bridge (from an older checkout, through `GOTDMS_BRIDGE`) is refused with a `ProtocolError` that says to rebuild it.

This repository is [gotdms-python](https://github.com/trey-copeland/gotdms-python). The proposed PyPI distribution name is `gotdms`. The import package is `gotdms`. Nothing has been published to PyPI, and this repository has no release tag. `0.1.0` in `pyproject.toml` is the version recorded for that first release. It is not a tag.

The Go module requires [gotdms](https://github.com/trey-copeland/gotdms) `v0.2.0` (`290bb791b00ad56edeee814960062107d0600cc8`). That is the first core tag with `dataset.WithSharedClock` and `dataset.WithoutTimes`, which protocol 2 reads with. There is no `replace` directive. See [`EXTRACTION.md`](EXTRACTION.md).

Contributing: see [`CONTRIBUTING.md`](CONTRIBUTING.md).
