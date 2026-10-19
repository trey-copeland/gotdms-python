# From npTDMS to gotdms

This is the post-analysis path for a Python tool that uses numpy and Polars and today imports [npTDMS](https://github.com/adamreeve/npTDMS). The tool stays in Python. `gotdms-bridge` reads TDMS. This package does not import npTDMS.

This note is not legal advice. It records the licenses as published and the behavioral differences that matter when moving a call site.

## License

npTDMS is distributed under the GNU Lesser General Public License, version 3.0 only.

- The repository ships that text as [`COPYING.LESSER`](https://github.com/adamreeve/npTDMS/blob/master/COPYING.LESSER).
- [PyPI](https://pypi.org/project/npTDMS/) lists the project license as LGPL-3.0.

The gotdms core library and this bridge are MIT. This tree's text is [`LICENSE`](../LICENSE). numpy, used for the raw arrays, is BSD-3-Clause ([numpy's LICENSE.txt](https://github.com/numpy/numpy/blob/main/LICENSE.txt)). Polars, the dataframe layer, is MIT. pyarrow, the Arrow interchange, is Apache-2.0. The Python package is the decoupling path: call sites depend on this MIT client and the MIT bridge instead of importing the LGPL-3.0 library. This package does not depend on pandas.

## What you run

Build the bridge from the separate Go module. That module is the only place Apache Arrow is linked. The core `github.com/trey-copeland/gotdms` module has no third-party requirements.

```bash
# From the root of github.com/trey-copeland/gotdms-python.
go build -o gotdms-bridge ./cmd/gotdms-bridge
export GOTDMS_BRIDGE="$PWD/gotdms-bridge"
```

A platform wheel bundles `gotdms-bridge`. `locate_binary` uses an explicit path, then `GOTDMS_BRIDGE`, then that file. NumPy, Polars, and pyarrow are dependencies of the Python package (`requires-python` is `>=3.10` because Polars 2.0 requires it). If a caller still wants a pandas object, Polars provides `frame.to_pandas()`. That conversion is not a dependency of this package.

## Two modes

Interactive mode is a long-lived `gotdms-bridge serve` process. `gotdms.open` starts it, `Session` sends one request per call, and `close` shuts it down. Open a file set once, then ask for a catalog, a channel, or a time window. A multi-channel read is one request and one response, so a shared timestamp channel is decoded once in the dataset layer and is not decoded again in Python.

Batch mode is `gotdms-bridge convert`. It takes the same file set, timestamp declaration, basis, window, and channel selection, and writes a directory you can load with one `load_bundle` call. Use it when the analysis runs after the files are already converted.

Both modes return raw samples unless you set `scaled=True` or `--scaled`.

## Call-site map

| npTDMS | gotdms |
| --- | --- |
| `TdmsFile.read_metadata(path)` | `gotdms.open(...)` then `session.catalog()`, `find`, `search`. Data-channel samples are not decoded. Opening still reads declared timestamp channels so the dataset can learn each file's span. |
| `TdmsFile.read(path)` | `gotdms.open(...)` then `session.read_all()` or `session.as_polars()`. |
| `tdms[group][channel]` | `session[group][channel]`. A bare `session.channel(name)` works only when the name is in one group. Two groups raise `AmbiguousChannelError`. |
| `channel.properties` | `channel.properties`. Compared per file; `properties_match` is false when files disagree. |
| `channel[:]` / `read_data(scaled=True)` | `channel.data()`. Raw. Pass `scaled=True` to apply an NI linear scale. |
| `channel.raw_data` / `read_data(scaled=False)` | `channel.data()` with the default `scaled=False`. |
| `channel.time_track(absolute_time=True)` | `channel.time_track(absolute_time=True)`, `numpy.datetime64[ns]`. |
| `channel.time_track(absolute_time=False)` | `channel.time_track(absolute_time=False)` is seconds since the basis, not npTDMS's zero-at-first-sample track. See below. |
| `as_dataframe(time_index=True, absolute_time=..., scaled_data=True)` | `as_polars(time="column", absolute_time=..., align="nptdms", scaled=True)`. Polars has no row index, so the DatetimeIndex is a `time` column. Default `align` is `"strict"` and default `scaled` is `False`. |
| `tdmsinfo` | No CLI inspector. Use `catalog` or the convert manifest. |
| HDF5 export | Not provided. |

## Before and after

npTDMS, one file, scaled by default, relative time from the waveform start:

```python
from nptdms import TdmsFile

with TdmsFile.read("run.tdms") as tdms:
    group = tdms["acq"]
    channel = group["ai0"]
    props = channel.properties
    values = channel[:]  # scaled
    times = channel.time_track(absolute_time=True)
    relative = channel.time_track()  # wf_start_offset + i * wf_increment
    frame = tdms.as_dataframe(time_index=True, absolute_time=True)
```

Metadata only:

```python
meta = TdmsFile.read_metadata("run.tdms")
names = [channel.name for channel in meta["acq"].channels()]
```

gotdms, a file set, raw unless you scale, time from a declared channel:

```python
import gotdms

with gotdms.open(
    ["a.tdms", "b.tdms"],
    timestamp_channel="Time",
    numeric_epoch="2024-03-01T00:00:00Z",
    basis="2024-03-01T00:00:01Z",
) as session:
    props = session["acq"]["ai0"].properties
    values = session["acq"]["ai0"].data()  # raw
    scaled = session["acq"]["ai1"].data(scaled=True)  # linear scale only
    times = session["acq"]["ai0"].time_track(absolute_time=True)
    relative = session["acq"]["ai0"].time_track(absolute_time=False)
    frame = session.as_polars(time="column", absolute_time=True, align="strict")
    window = session.read_many(
        [{"name": "ai0", "group": "acq"}, {"name": "ai1", "group": "acq"}],
        start="2024-03-01T00:00:02Z",
        end="2024-03-01T00:00:05Z",
    )
```

Catalog, the `read_metadata` equivalent:

```python
with gotdms.open(["a.tdms"], timestamp_channel="Time", numeric_epoch="2024-03-01T00:00:00Z") as session:
    names = [channel.name for channel in session["acq"]]
    catalog = session.catalog()
```

Batch, then one Polars load:

```bash
gotdms-bridge convert \
  --dir run --pattern '*.tdms' \
  --timestamp-channel Time \
  --numeric-epoch 2024-03-01T00:00:00Z \
  --basis 2024-03-01T00:00:01Z \
  --channel acq/ai0 --channel acq/ai1 \
  --start 2024-03-01T00:00:02Z --end 2024-03-01T00:00:05Z \
  --out bundle
```

```python
import gotdms

bundle = gotdms.load_bundle("bundle")
frame = bundle.as_polars(time="column", align="strict")
```

## Scaling

gotdms returns the stored samples. `Channel.LinearScale` in the core library is metadata; `Read` does not apply it. The bridge does the same.

When an `NI_Scale[i]_Scale_Type` of `Linear` is present, with a slope and intercept, the catalog and the Arrow schema carry `has_scale`, `scale.slope`, `scale.intercept`, and `scale.index`. `scaled=True` (or `--scaled`) applies `raw * slope + intercept`, the same arithmetic as `LinearScale.Apply`. The response sets `scaled` so you can see that it happened.

`scaled=True` on a channel with no linear scale raises `NoScaleError`. The bridge does not return raw data under that flag. A scale whose type is not `Linear` raises `NonlinearScaleError` and is not applied. If the files of one merged channel disagree on the linear scale, `scaled=True` raises `ScaleMismatchError`.

npTDMS does the opposite on purpose: data read from a channel is scaled, and raw samples are `raw_data` or `read_data(scaled=False)`. `as_dataframe(scaled_data=True)` is the npTDMS default. Do not treat a gotdms array as engineering units unless `scaled` was set and the response says so.

## Time

Declare the timestamp channel. `timestamp_channel="Time"` applies to every group. `timestamp_channels={"raw": ""}` opts `raw` out. A numeric clock is seconds since `numeric_epoch`. There is no default epoch, so 0 is not silently the Unix epoch. A timestamp-typed channel is absolute and does not use the numeric epoch.

If a group has no declaration, waveform `wf_start_time` and `wf_increment` are the fallback. Nothing else is inferred.

`basis` is the origin for relative time. Relative time is `sample_time - basis` in seconds, negative before the basis. The default basis, when you omit it, is the earliest timed sample in the catalog, chosen once at open. A window does not move it, so the first returned sample is not re-zeroed.

npTDMS `time_track(absolute_time=False)` needs `wf_increment` and `wf_start_offset` and returns `offset + i * increment`. With the usual zero offset, the first sample is 0. `absolute_time=True` additionally needs `wf_start_time`. gotdms does not require those properties when a timestamp channel was declared, and `absolute_time=False` is not that npTDMS array.

A window is half-open: `start <= t < end`. Pass both ends. A window on an untimed channel raises `UntimedError`.

`read_many` is the call to use for several channels. One response carries every channel's values and the shared `time_ns` column. Python does not loop per channel and does not decode the clock.

## Polars frames

`as_polars` returns a `polars.DataFrame`. Polars has no row index, so there is no `DatetimeIndex` and no `time="index"`. npTDMS `as_dataframe(time_index=True)` maps to a proper column: `time`, dtype `Datetime(ns, UTC)`, when `absolute_time` is true. `absolute_time=False` uses a `relative_s` float column of seconds since the basis. `time="none"` omits that column. Passing `time="index"` raises `ArgumentError` and points at the column.

`align="strict"` is the default. Every channel must share the same time vector. The frame is built column-wise and has no nulls introduced by alignment. If the vectors differ, you get `AlignmentError`.

`align="nptdms"` is the opt-in that matches npTDMS index alignment. npTDMS outer-aligns channels on the time index and writes NaN where a channel has no sample. Polars expresses that as a full outer join on the time column, and the missing cells are null rather than NaN:

```python
left.join(right, on="time", how="full", coalesce=True).sort("time")
```

`as_polars(..., align="nptdms")` does that join across the selected channels and sorts by `time` (or by `relative_s`). Duplicate timestamps in one channel raise `AlignmentError` instead of repeating rows.

A full join matches equal timestamps only. When two channels are close in time but not equal, the caller uses Polars' as-of join, which this package does not run for you:

```python
left.join_asof(right, on="time", strategy="backward")
```

`strategy` is `"backward"`, `"forward"`, or `"nearest"`. That is a different choice from npTDMS NaN alignment. Pick it at the call site when equal timestamps are the wrong match.

`as_polars` on a group or a whole session skips declared timestamp channels; they are the clock. Read them by name when you want the stored numbers. A pandas object is one call on the result, `frame.to_pandas()`, and is not imported here.

## Batch bundle

`gotdms-bridge convert` refuses to replace an existing `manifest.json`.

```text
bundle/
  manifest.json
  data/0.arrow
  data/1.arrow
```

`manifest.json` fields:

- `gotdms_bridge`: protocol version `1`
- `format`: `arrow-ipc-stream`
- `basis`: dataset basis, RFC3339
- `channels[]`: `path` (`data/N.arrow`), `name`, `group`, `data_type`, `basis`, `timed`, `scaled`, `has_scale`, `scale`, `unit`, `properties`, `files`, `columns`, `gaps`

Each `.arrow` file is one Arrow IPC stream with one record batch. The schema metadata repeats those facts under `gotdms.*` keys. Columns are `y` (`float64`), `time_ns` (`int64` Unix nanoseconds) when absolute time was requested and the channel is timed, and `relative_s` (`float64`) when relative time was requested. An untimed channel omits `time_ns` and sets `absolute_omitted` rather than writing zeros.

## Interactive frames

`serve` reads and writes little-endian frames on stdin and stdout. The definition in code is `Frame` in `frame.go`.

```text
0   magic "GMDS"
4   version uint16 = 1
6   flags uint16 = 0
8   header_len uint32
12  payload_len uint32
16  header JSON
    payload
```

A data payload is `uint32` count, then for each channel `uint32` byte length and one Arrow IPC stream. The header's `channels` array is in the same order. Empty payloads are metadata replies. Errors are `{"op":"error","code":"...","message":"..."}` and the Python client raises the matching exception. A version other than 1 returns `bad_version` and the process keeps reading.

Requests: `open`, `catalog`, `find`, `search`, `match`, `read`, `read_many`, `close`. `open` takes `paths` or `directory` plus `pattern`, `timestamp_channel`, `timestamp_channels`, `numeric_epoch`, `basis`, `overlap` (`keep_first`, `keep_last`, `error`), `workers`, and `read_workers`. Reads take `absolute` (default true), `relative` (default true), `scaled` (default false), optional `basis`, and optional `start` and `end`.

## Gaps that remain

- DAQmx is not supported. There is no DAQmx scaler map and no DAQmx scaling. A raw DAQmx channel is not turned into engineering units.
- Non-linear NI scales are reported and not applied.
- No HDF5 export.
- No `tdmsinfo` CLI.
- Memory mapping is intentionally absent.
- `Channel.Slice` and `CursorAt` read a sample range without walking earlier samples. Dataset waveform windows do not call them. The cursor starts at sample 0, batches before the window are discarded, and the walk stops at the window, so a tail window still decodes the prefix. A declared timestamp axis is decoded in full on read because the times are the samples. Catalog of a uniform clock reads the first two samples and derives the span; a first step that is not positive is scanned in full. This bridge calls dataset and does not add its own range read.
- Eager `read_all` is an explicit call. `open` alone does not pull every sample into Python.
- File-status for an incomplete final segment is the core reader's behavior; the bridge does not add an npTDMS `file_status` object.
- Integers wider than the float64 mantissa are returned as `float64`, which is what the dataset layer already does.
