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

A platform wheel bundles `gotdms-bridge`. `locate_binary` uses an explicit path, then `GOTDMS_BRIDGE`, then that file. NumPy, Polars, and pyarrow are dependencies of the Python package, and on Windows so is `tzdata` (Apache-2.0), because Polars needs an IANA time zone database to turn the `Datetime("ns", "UTC")` time columns into Python datetimes and Windows has none (`requires-python` is `>=3.10` because Polars 2.0 requires it). If a caller still wants a pandas object, Polars provides `frame.to_pandas()`. That conversion is not a dependency of this package.

## Two modes

Interactive mode is a long-lived `gotdms-bridge serve` process. `gotdms.open` starts it, `Session` sends one request per call, and `close` shuts it down. Open a file set once, then ask for a catalog, a channel, or a time window. A multi-channel read is one request and one response, so a shared timestamp channel is decoded once in the dataset layer and is not decoded again in Python.

Batch mode is `gotdms-bridge convert`. It takes the same file set, timestamp declaration, basis, window, and channel selection, and writes a directory you can load with one `load_bundle` call. Use it when the analysis runs after the files are already converted.

Both modes return raw samples unless you set `scaled=True` or `--scaled`.

## Call-site map

| npTDMS | gotdms |
| --- | --- |
| `TdmsFile.read_metadata(path)` | `gotdms.open(...)` then `session.catalog()`, `find`, `search`. Data-channel samples are not decoded. Opening still reads declared timestamp channels so the dataset can learn each file's span. |
| `TdmsFile.read(path)` | `gotdms.open(...)` then `session.read_all()` or `session.as_polars()`. For a lazy frame, `gotdms.scan_tdms(...).collect()`. |
| `tdms[group][channel]` | `session[group][channel]`. A bare `session.channel(name)` works only when the name is in one group. Two groups raise `AmbiguousChannelError`. |
| `channel.properties` | `channel.properties`. Compared per file; `properties_match` is false when files disagree. |
| `channel[:]` / `read_data(scaled=True)` | `channel.data()`. Raw. Pass `scaled=True` to apply an NI linear scale. |
| `channel.raw_data` / `read_data(scaled=False)` | `channel.data()` with the default `scaled=False`. |
| `channel.time_track(absolute_time=True)` | `channel.time_track(absolute_time=True)`, `numpy.datetime64[ns]`. |
| `channel.time_track(absolute_time=False)` | `channel.time_track(absolute_time=False)` is seconds since the basis, not npTDMS's zero-at-first-sample track. See below. |
| `as_dataframe(time_index=True, absolute_time=..., scaled_data=True)` | `as_polars(time="column", absolute_time=..., align="nptdms", scaled=True)`, or `scan_tdms(..., absolute_time=..., align="nptdms", scaled=True).collect()`. Polars has no row index, so the DatetimeIndex is a `time` column. Default `align` is `"strict"` and default `scaled` is `False`. |
| `TdmsWriter` and `write_segment` | `gotdms.write_tdms(path, data, ...)`. See [Writing files](#writing-files). |
| `RootObject(properties=...)` | `properties=` on `write_tdms`. |
| `GroupObject(name, properties=...)` | A group key in `data`, and `group_properties=`. |
| `ChannelObject(group, channel, data, properties=...)` | A column of that group's frame, and `channel_properties=`. `unit_string` is a string property. |
| `TdmsWriter(path, mode="a")` | `write_tdms(..., mode="append")` on a file that already ends on a finished segment. |
| `read_data(scaled=False)` when you need the stored type | `gotdms.read_tdms(path)`. `Session.read` and `channel.data()` stay float64. |
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

Lazy scan, in the spirit of `polars.scan_parquet`. The `LazyFrame` is built from catalog metadata. Samples are decoded at `collect`:

```python
import gotdms

frame = gotdms.scan_tdms(
    "run/*.tdms",
    timestamp_channel="Time",
    numeric_epoch="2024-03-01T00:00:00Z",
).select("time", "acq/ai0").head(1000).collect()
```

`source` is one file, a list of files, a directory (with `pattern`, default `*.tdms`), or a non-recursive glob. `**` is an error. Columns are `group/name`, matched across files the same way as a dataset. Ambiguous group/name time axes are an error. A gap is reported on the read and is not a null row under `align="strict"`. An absolute-time collect that includes an untimed channel raises `UntimedError`, the same as `as_polars`. Timed channels that do not share a time vector raise `AlignmentError` under `align="strict"`.

On Polars 2, `select` without a predicate pushes projection: the bridge decodes the selected data channels and the timestamp channel those columns use, not the rest of the file. `head(n)` and `limit(n)` without a predicate push `n` as a sample limit (offset 0) and the bridge reads that prefix with `Channel.Slice`. `filter` is applied in Python after the TDMS read. A filter written before `select` of a different column does not push projection, because the predicate still needs the other columns. `head` after `filter` is not pushed: Polars 2 does not pass `n_rows` with a predicate. `slice(offset, length)` is not pushed either (`n_rows` is `None`); the selected columns are read in full and Polars slices the batch. Direct `read` and `read_many` callers can still pass `offset`.

`basis` is applied to the whole series. `head` does not re-zero relative time to the first returned sample.

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

`scaled=True` on an ordinary channel with no linear scale raises `NoScaleError`. The bridge does not return raw data under that flag. A scale whose type is not `Linear` raises `NonlinearScaleError` and is not applied. If the files of one merged channel disagree on the linear scale, `scaled=True` raises `ScaleMismatchError`.

A DAQmx channel is different. `open`, `read`, `read_tdms`, and `scan_tdms` return the primary scaler's stored type, raw. A digital line (`0x126A`) is `UInt8` with values 0 and 1. `scaled=True` calls `DAQmxScaledFloat64s`: a linear or polynomial chain becomes float64 engineering units. `NI_Scaling_Status` of `scaled` means those stored samples are already engineering units, so the flag returns them as float64 and sets `scaled`. No DAQmx scale raises `NoScaleError`. A scale type other than linear or polynomial raises `NonlinearScaleError`. The Polars column is the primary scaler. Other scalers on that channel are not extra columns. `write_tdms` does not emit a DAQmx raw index.

npTDMS does the opposite on purpose: data read from a channel is scaled, and raw samples are `raw_data` or `read_data(scaled=False)`. `as_dataframe(scaled_data=True)` is the npTDMS default. Do not treat a gotdms array as engineering units unless `scaled` was set and the response says so.

## Time

Declare the timestamp channel. `timestamp_channel="Time"` applies to every group. `timestamp_channels={"raw": ""}` opts `raw` out. A numeric clock is seconds since `numeric_epoch`. There is no default epoch, so 0 is not silently the Unix epoch. A timestamp-typed channel is absolute and does not use the numeric epoch.

If a group has no declaration, waveform `wf_start_time` and `wf_increment` are the fallback. Nothing else is inferred.

`basis` is the origin for relative time. Relative time is `sample_time - basis` in seconds, negative before the basis. The default basis, when you omit it, is the earliest timed sample in the catalog, chosen once at open (and once per `scan_tdms` collect, which opens the dataset then). A window does not move it, and neither does `head` or `limit`, so the first returned sample is not re-zeroed.

npTDMS `time_track(absolute_time=False)` needs `wf_increment` and `wf_start_offset` and returns `offset + i * increment`. With the usual zero offset, the first sample is 0. `absolute_time=True` additionally needs `wf_start_time`. gotdms does not require those properties when a timestamp channel was declared, and `absolute_time=False` is not that npTDMS array.

A window is half-open: `start <= t < end`. Pass both ends. A window on an untimed channel raises `UntimedError`.

`read_many` is the call to use for several channels. One response carries every channel's values and the shared `time_ns` column, packed as columns of a few Arrow streams. Python decodes each stream once and each clock once.

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

- `gotdms_bridge`: protocol version of the bridge that wrote it (`2`). The bundle layout itself did not change with protocol 2.
- `format`: `arrow-ipc-stream`
- `basis`: dataset basis, RFC3339
- `channels[]`: `path` (`data/N.arrow`), `name`, `group`, `data_type`, `basis`, `timed`, `scaled`, `has_scale`, `scale`, `unit`, `properties`, `files`, `columns`, `gaps`

Each `.arrow` file is one Arrow IPC stream with one record batch. The schema metadata repeats those facts under `gotdms.*` keys. Columns are `y` (`float64`), `time_ns` (`int64` Unix nanoseconds) when absolute time was requested and the channel is timed, and `relative_s` (`float64`) when relative time was requested. An untimed channel omits `time_ns` and sets `absolute_omitted` rather than writing zeros.

## Interactive frames

`serve` reads and writes little-endian frames on stdin and stdout. The definition in code is `Frame` in `frame.go`.

```text
0   magic "GMDS"
4   version uint16 = 2
6   flags uint16 = 0
8   header_len uint32
12  payload_len uint32
16  header JSON
    payload
```

Metadata replies are one `{"op":"result",...}` frame with no payload. Errors are `{"op":"error","code":"...","message":"..."}` and the Python client raises the matching exception. A version other than 2 returns `bad_version` and the process keeps reading. Version 1 is gone; the client refuses a version 1 bridge with a `ProtocolError`.

A data reply (`read`, `read_many`, `read_values`) is a sequence of frames, so it has no size ceiling:

```text
result_begin  {"op":"result_begin", "channels":[...], "clocks":[...],
               "streams":[{"id":0,"kind":"clock","bytes":N,"batches":B}, ...],
               "reads":[...], "limited":false}            no payload
part          {"op":"part","stream":k}                     next bytes of stream k, at most 64 MiB
...           streams in id order, each in one or more parts
result_end    {"op":"result_end","streams":n,"bytes":total}
          or  {"op":"result_end","error":{"code":"...","message":"...","stream":k}}
```

Every stream is one Arrow IPC stream with one schema and `batches` record batches, and `bytes` is its exact size, so the client allocates each stream's buffer once and fills it in place. Channels and clocks are columns: each `channels[]` entry names its `stream`, `batch`, and `column` (its `y`), plus `relative_column` (its `relative_s`, in the same batch) when relative time was requested. Each `clocks[]` entry names `stream`, `batch`, and `column` the same way. A clock column is `time_ns` (`int64` Unix nanoseconds). Each distinct time axis is sent once: channels on one timestamp channel, waveform channels with the same start, increment, and sample range, and any other channels whose times are equal sample for sample share a clock. A timed channel names its `clock` id when absolute time was requested. An untimed channel has no `clock` and sets `absolute_omitted` when absolute time was requested. `read_values` streams have kind `values`, and each channel entry names its `stream`, `batch`, and `column`.

The bridge packs columns into few streams. Channels (or clocks) with the same length and column layout are columns of one record batch, named `y_0`, `y_1`, ... (`relative_s_0`, ... and `time_ns_0`, ...) in that batch. Up to 16 such lengths are packed this way, the ones with the most channels first. Every other channel goes into a "ragged" stream for its column layout, with one record batch per channel and columns named `y` (and `relative_s`, or `time_ns`). A stream holds at most 16 MiB of such channels; a channel over 16 MiB gets a stream of its own. The column a reference names must be its own (`y`, `y_<n>`, and so on), and the client checks that no column is named twice and that every column is named. A stream's buffer lives as long as any array from it, so keeping one short channel can keep up to 16 MiB of the others alive; `numpy.copy` it to drop them. Before this, every channel and clock was a stream of its own: on a 785-channel file a full read paid 785 schema messages, Arrow writers, and `open_stream` calls, which made the bridge far slower than npTDMS on files with many short channels.

An error after `result_begin` (for example, a value that cannot be encoded, or a stream that came out longer or shorter than declared) ends the reply with `result_end` and an `error`. The bridge never sends more bytes of a stream than declared. The client drops the partial streams, raises the matching exception, and keeps the session. A reply that stops without `result_end` (the bridge died, or the client was interrupted) leaves the pipe out of sync, so the client kills the bridge and the session raises `ProtocolError` from then on. `GOTDMS_BRIDGE_PART_BYTES` (an integer from 1 to 67108864) lowers the part cap for tests; it cannot raise it, and any other value makes `gotdms-bridge serve` exit with status 2. The client rejects any frame payload over 64 MiB, a part for a stream before the previous stream is complete, and a `result_end` whose `streams` or `bytes` disagree with `result_begin`.

Requests: `open`, `describe`, `catalog`, `find`, `search`, `match`, `read`, `read_many`, `close`, `write_begin`, `write_chunk`, `write_end`, `read_values`. `open` and `describe` take `paths` or `directory` plus `pattern`, `timestamp_channel`, `timestamp_channels`, `numeric_epoch`, `basis`, `overlap` (`keep_first`, `keep_last`, `error`), `workers`, and `read_workers`. `describe` returns `sample_reads` (0), `files`, `groups`, and `channels`. It does not call `dataset.Open` and does not decode samples. Reads take `absolute` (default true), `relative` (default true), `scaled` (default false), optional `basis`, optional `start` and `end`, and optional `offset` and `limit`.

`write_begin`, `write_chunk`, and `write_end` are the export. `write_begin` names the path, mode (`create` or `append`), file properties, and groups. `write_chunk` carries one Arrow IPC stream for one channel, column name `y`. `write_end` closes the file. `read_values` is the inverse: native channel values, not the float64 dataset read. A `write_chunk` payload is the Arrow stream itself; requests are always single frames.

`offset` and `limit` select a half-open sample range of the merged series, `[offset, offset+limit)`. Omit both for the full dataset read, which is unchanged. `limit` without `offset` is a prefix. `offset` without `limit` reads through the end. A time window together with a sample range is an error. The response adds `reads` and `limited`. `reads` lists each decoded slice: `file`, `group`, `name`, `role` (`data` or `timestamp`), `offset`, `length`, and `channel_samples`. `limited` is true when a sample range was applied. A Python read that asked for a range and got `limited: false` raises `ProtocolError`.

## Writing files

`gotdms.write_tdms` is the `TdmsWriter` path. It does not import npTDMS. A cleaned export is one file with several groups: the samples, an uncertainties group, a design group, and a log group. Groups may have different lengths. A group given as a mapping of `polars.Series` may contain channels of different lengths. A group given as a DataFrame is rectangular.

```python
import gotdms
import polars as pl

when = "2024-06-01T12:00:00Z"
gotdms.write_tdms(
    "cleaned.tdms",
    {
        "data": pl.DataFrame({"voltage": [1.0, 1.5, 1.25, 1.75], "current": [0.1, 0.2, 0.15, 0.25]}),
        "uncertainties": pl.DataFrame({"voltage": [0.01, 0.02, 0.01], "current": [0.001, 0.002, 0.001]}),
        "design": {
            "setpoint": pl.Series("setpoint", [10.0, 20.0]),
            "label": pl.Series("label", ["low", "mid", "high"]),
        },
        "log": pl.DataFrame({"message": ["opened", "cleaned", ""], "level": pl.Series("level", [1, 2, 2], dtype=pl.Int32)}),
    },
    properties={"title": "cleaned run", "revision": 4, "approved": False},
    group_properties={"data": {"role": "samples"}, "log": {"role": "event"}},
    channel_properties={
        "data": {"voltage": {"unit_string": "V"}, "current": {"unit_string": "A"}},
        "uncertainties": {"voltage": {"unit_string": "V"}, "current": {"unit_string": "A"}},
        "design": {"setpoint": {"unit_string": "V"}},
    },
    waveforms={"data": {"voltage": {"wf_start_time": when, "wf_increment": 0.001, "wf_start_offset": 0.0}}},
)
contents = gotdms.read_tdms("cleaned.tdms")
```

npTDMS, the same file:

```python
from datetime import datetime, timezone

from nptdms import ChannelObject, GroupObject, RootObject, TdmsWriter

when = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
with TdmsWriter("cleaned.tdms") as writer:
    writer.write_segment([
        RootObject(properties={"title": "cleaned run", "revision": 4, "approved": False}),
        GroupObject("data", properties={"role": "samples"}),
        ChannelObject("data", "voltage", [1.0, 1.5, 1.25, 1.75], properties={
            "unit_string": "V",
            "wf_start_time": when,
            "wf_increment": 0.001,
            "wf_start_offset": 0.0,
            "wf_samples": 4,
        }),
        ChannelObject("data", "current", [0.1, 0.2, 0.15, 0.25], properties={"unit_string": "A"}),
    ])
```

`write_tdms` writes little-endian segments, version 4713, and does not write a `.tdms_index`. `mode="create"` writes a temporary file beside the destination and renames it into place only after the file reopens. A failed create leaves an existing destination alone. `mode="append"` requires a file whose final segment is complete. It adds a segment at the end. It does not rewrite earlier segments, so a property set on the first write is still there after an append that does not restate it. A channel whose stored type differs from the new column is rejected. An append that fails or is interrupted is cut back to the original length. Only a killed bridge process or a lost machine can leave an incomplete final segment, and a later append will refuse that file. A created file follows the umask. A replaced file keeps its permission bits. NaN and infinite float properties read back as Python floats.

Dtypes are Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64, Boolean, String, and Datetime. Other dtypes raise `ArgumentError`. Values are not coerced. A null in any column, including a string column, is an error. Nulls are not stored as zero or as `""`. An empty string is a real value. A column with no rows is stored without a raw-data index, so `read_tdms` reports type `void` and does not keep the Polars dtype.

Datetime columns are TDMS timestamps and come back as `Datetime(ns, UTC)`. A naive datetime is UTC. An aware datetime is converted to UTC. Milliseconds and microseconds widen to nanoseconds. A property datetime is a Python datetime, so it has microsecond resolution. `{"seconds": int, "fraction": int}` keeps the full 2^-64 fraction. `seconds` is the TDMS count from 1904-01-01 00:00:00 UTC, the same field npTDMS uses, not a Unix timestamp.

Properties are bool, int, float, str, datetime, or `gotdms.Property(value, tdms_type)` when the width matters. A plain int is int32 when it fits and int64 otherwise. Waveform fields are `wf_start_time`, `wf_increment`, `wf_start_offset`, and `wf_samples`. If a waveform dict omits `wf_samples`, it is the column length. Waveform fields belong on numeric channels. The same name cannot be both a waveform field and a channel property.

`read_tdms` returns those stored types. `Session.read`, `channel.data()`, and `as_polars` still return float64 for an ordinary numeric channel and still reject a string channel. A DAQmx channel is the primary scaler's stored type on those paths too. Open the file with `gotdms.open` to catalog it, including a log group, and use `read_tdms` when the log strings or the integer width matter. An ordinary integer wider than the float64 mantissa is still float64 on that dataset path.

Fixed-size columns, including bool and timestamp, are streamed with `Writer.Stream` in Arrow chunks. String columns cannot be streamed, because the offset table needs every length, so they are buffered and written as a following segment when the file also has fixed-size columns.

npTDMS reads the file this writes. `read_data(scaled=False)` is the raw samples. npTDMS scales by default, and this package does not. CI installs npTDMS only as that oracle. The package does not depend on it.

## Gaps that remain

- DAQmx writes are not supported. Reads return the primary scaler, raw, and `scaled=True` applies only a linear or polynomial DAQmx chain.
- Non-linear NI scales on an ordinary channel are reported and not applied. A DAQmx polynomial chain is applied when `scaled=True`.
- No HDF5 export.
- No `tdmsinfo` CLI.
- Memory mapping is intentionally absent.
- `Channel.Slice` and `CursorAt` read a sample range without walking earlier samples. Dataset time windows do not use them: a waveform window still starts its cursor at sample 0, and a timestamp axis is decoded in full. `offset` and `limit` on `read` and `read_many` are a bridge range read. They call `Slice` for the merged sample range and do not decode the other samples of the selected channels. A full read of an ordinary channel, and any `start`/`end` window on one, still goes through dataset. A DAQmx read uses the same index path for a full span, a sample range, and a time window, and the Arrow column keeps the scaler type. `describe` does not read samples. `open` still learns each file's span from declared clocks (first two samples when the first step is positive, otherwise the whole clock). That catalog read is not listed in `reads`. A `head` or `limit` on `scan_tdms` does not move `basis`.
- Eager `read_all` is an explicit call. `open` alone does not pull every sample into Python.
- File-status for an incomplete final segment is the core reader's behavior; the bridge does not add an npTDMS `file_status` object.
- Ordinary integers wider than the float64 mantissa are returned as `float64` from `Session.read`, `channel.data()`, and `as_polars`, which is what the dataset layer already does. `read_tdms` keeps the stored width, including int64 and uint64 past 2^53. A DAQmx scaler keeps that width on `read` and `scan_tdms` as well.
