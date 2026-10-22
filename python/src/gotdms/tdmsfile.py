"""Write and read TDMS files as Polars frames.

``write_tdms`` is the export path. ``read_tdms`` is its inverse: stored
samples come back in the channel's TDMS type, not as the float64 series
``Session.read`` returns. Neither function imports npTDMS.
"""

from __future__ import annotations

import io
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from .client import _raise_protocol, locate_binary
from .codec import read_frame, split_payload, write_frame
from .errors import ArgumentError, ProtocolError

_MAX_PAYLOAD = 2**31 - 1
_WIDTH = {
    "int8": 1,
    "uint8": 1,
    "bool": 1,
    "int16": 2,
    "uint16": 2,
    "int32": 4,
    "uint32": 4,
    "float32": 4,
    "int64": 8,
    "uint64": 8,
    "float64": 8,
    "timestamp": 16,
}
_NUMERIC = {
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float32",
    "float64",
}
_INT_RANGE = {
    "int8": (-(2**7), 2**7 - 1),
    "int16": (-(2**15), 2**15 - 1),
    "int32": (-(2**31), 2**31 - 1),
    "int64": (-(2**63), 2**63 - 1),
}
_UINT_MAX = {
    "uint8": 2**8 - 1,
    "uint16": 2**16 - 1,
    "uint32": 2**32 - 1,
    "uint64": 2**64 - 1,
}
_WAVE_FIELDS = ("wf_start_time", "wf_increment", "wf_start_offset", "wf_samples")


class Property:
    """A TDMS property value with an explicit stored type.

    A plain bool, int, float, str, or datetime is inferred: bool stays
    bool, int is int32 when it fits and int64 otherwise, float is float64,
    str is string, and datetime is a UTC timestamp. Use Property when the
    width matters, for example ``Property(1, "uint16")`` or
    ``Property(1, "int64")``.
    """

    def __init__(self, value, tdms_type: str):
        if not isinstance(tdms_type, str) or not tdms_type:
            raise ArgumentError("TDMS property type must be a non-empty string")
        self.value = value
        self.tdms_type = tdms_type

    def __repr__(self):
        return f"Property({self.value!r}, {self.tdms_type!r})"


class WriteResult:
    """What ``write_tdms`` wrote."""

    def __init__(self, path, channel_count, row_count, stream_chunks, buffered_channels, segments):
        self.path = path
        self.channel_count = channel_count
        self.row_count = row_count
        self.stream_chunks = stream_chunks
        self.buffered_channels = buffered_channels
        self.segments = segments

    def __repr__(self):
        return (
            f"WriteResult(path={self.path!r}, channels={self.channel_count}, "
            f"rows={self.row_count}, stream_chunks={self.stream_chunks}, "
            f"buffered_channels={self.buffered_channels}, segments={self.segments})"
        )


class TdmsChannel:
    """One channel from ``read_tdms``, in its stored type."""

    def __init__(self, name, group, tdms_type, ni_type, properties, typed_properties, values):
        self.name = name
        self.group = group
        self.type = tdms_type
        self.ni_type = ni_type
        self.properties = properties
        self.typed_properties = typed_properties
        self.values = values

    def __repr__(self):
        return f"TdmsChannel({self.group!r}/{self.name!r}, {self.ni_type}, rows={self.values.len()})"


class TdmsGroup:
    """One group from ``read_tdms``."""

    def __init__(self, name, properties, typed_properties, channels):
        self.name = name
        self.properties = properties
        self.typed_properties = typed_properties
        self.channels = channels

    def frame(self):
        """Polars frame of this group's channels.

        Channels in a TDMS group do not have to share a length. This raises
        :class:`ArgumentError` when they differ. Read those channels from
        :attr:`channels` instead.
        """
        import polars as pl

        if not self.channels:
            return pl.DataFrame()
        lengths = {name: channel.values.len() for name, channel in self.channels.items()}
        if len(set(lengths.values())) != 1:
            raise ArgumentError(
                f"group {self.name!r} channels have different lengths {lengths}. "
                "Read them from TdmsGroup.channels."
            )
        return pl.DataFrame({name: channel.values for name, channel in self.channels.items()})

    def __repr__(self):
        return f"TdmsGroup({self.name!r}, channels={list(self.channels)})"


class TdmsContents:
    """A TDMS file read as native Polars columns."""

    def __init__(self, path, properties, typed_properties, groups):
        self.path = path
        self.properties = properties
        self.typed_properties = typed_properties
        self.groups = groups

    def frames(self):
        """``{group: frame}`` for every group whose channels share a length."""
        return {name: group.frame() for name, group in self.groups.items()}

    def __repr__(self):
        return f"TdmsContents(path={self.path!r}, groups={list(self.groups)})"


def write_tdms(
    path,
    data,
    *,
    group=None,
    properties=None,
    group_properties=None,
    channel_properties=None,
    waveforms=None,
    mode="create",
    chunk_bytes=1 << 20,
    binary=None,
):
    """Write a TDMS file from Polars data.

    ``data`` is one :class:`polars.DataFrame`, a mapping of group name to
    DataFrame, or a mapping of group name to a mapping of channel name to
    :class:`polars.Series`. A single frame is written as group ``group``
    (default ``"data"``). Groups keep the mapping's order. Columns keep the
    frame's or mapping's order. A DataFrame is rectangular, so its channels
    share a length. A mapping of Series does not: channels in one group may
    differ in length, and groups may differ from each other. That is the
    layout a cleaned export uses for a data group, an uncertainties group, a
    design group, and a log group.

    ``properties`` is the file property dict. ``group_properties`` maps a
    group name to that group's properties. ``channel_properties`` maps a
    group name to a dict of channel name to properties. ``unit_string`` is an
    ordinary string property. Values are bool, int, float, str, datetime, or
    :class:`Property`. See :class:`Property` for how plain ints are stored.

    ``waveforms`` maps a group name to a dict of channel name to waveform
    fields: ``wf_start_time``, ``wf_increment``, ``wf_start_offset``, and
    ``wf_samples``. Any of those fields may be omitted. When a waveform dict
    is present and ``wf_samples`` is omitted, it is set to the column length.
    Waveform fields are only valid on numeric channels. A name set both as a
    waveform field and as a channel property is an error.

    ``mode="create"`` replaces ``path`` only after the file has been written
    and reopened. A failed create leaves an existing file in place.
    ``mode="append"`` adds a segment to an existing TDMS file that ends on a
    finished segment. The core writer does not rewrite earlier segments.
    Channels you name gain the new samples. A channel that already has a
    different type is rejected. An append that fails or is interrupted is
    cut back to the file's original length. Only a bridge process that is
    killed outright, or a lost machine, can leave an incomplete final
    segment, which a later append will refuse.

    A new file gets the usual mode for the umask, as with ``open(path, "w")``.
    Replacing an existing file keeps that file's permission bits.

    Fixed-size columns, including bool and timestamp, are streamed with
    ``Writer.Stream`` in Arrow chunks of about ``chunk_bytes``. String
    columns cannot be streamed (the TDMS offset table needs every length),
    so they are buffered and written as a following segment when the file
    also has fixed-size columns.

    Null handling: TDMS channels are not nullable. A null in any column,
    including strings, raises :class:`ArgumentError`. Nulls are not replaced
    with zero or with an empty string. An empty string is a real value and
    is written. A column with no rows is stored without a raw-data index, so
    readers report it as void and the Polars dtype is not kept. Write at
    least one value to keep a dtype.

    Dtypes: Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64,
    Float32, Float64, Boolean, String, and Datetime. Anything else,
    including Date, Duration, Categorical, List, Array, and Struct, raises
    :class:`ArgumentError`. Values are not coerced.

    Datetime values are written as TDMS timestamps and read back as
    ``Datetime(ns, UTC)``. A naive datetime is UTC, not local time. An aware
    datetime is converted to UTC. Milliseconds and microseconds are widened
    to nanoseconds, which is exact. The read side of this package also
    reports time as UTC nanoseconds, so a round trip keeps that nanosecond.
    The TDMS fraction is 2^-64 second; bits below a nanosecond are not in a
    Polars datetime. A property datetime is a Python datetime, which has
    microsecond resolution. Pass ``{"seconds": int, "fraction": int}`` to
    keep a full TDMS fraction on a property. ``seconds`` counts from the
    TDMS epoch, 1904-01-01 00:00:00 UTC, which is the field npTDMS stores.
    It is not a Unix timestamp. Use a datetime or an RFC3339 string for a
    civil time.

    This function does not import pandas.
    """
    frames = _frames(data, group)
    if mode not in ("create", "append"):
        raise ArgumentError("mode must be create or append")
    if isinstance(chunk_bytes, bool) or not isinstance(chunk_bytes, int) or chunk_bytes < 1:
        raise ArgumentError("chunk_bytes must be a positive integer")
    chunk_bytes = min(chunk_bytes, _MAX_PAYLOAD)
    prepared = {name: _prepare_frame(frame, name) for name, frame in frames.items()}
    group_properties = _nested(group_properties, frames, "group_properties")
    channel_properties = _nested(channel_properties, frames, "channel_properties")
    waveforms = _nested(waveforms, frames, "waveforms")

    groups = []
    columns = []
    for name, cols in prepared.items():
        channel_props = _nested(channel_properties.get(name), {c["name"]: c for c in cols}, f"{name} channel_properties")
        channel_waves = _nested(waveforms.get(name), {c["name"]: c for c in cols}, f"{name} waveforms")
        wire_channels = []
        for col in cols:
            props = dict(channel_props.get(col["name"]) or {})
            wave = channel_waves.get(col["name"])
            if wave is not None and col["wire"] not in _NUMERIC:
                raise ArgumentError(
                    f"column {name}/{col['name']} is {col['wire']}; "
                    "waveform properties are only valid on numeric channels"
                )
            encoded_wave = _waveform(wave, col["series"].len(), f"{name}/{col['name']}")
            if encoded_wave is not None:
                overlap = set(props) & set(encoded_wave)
                if overlap:
                    listed = ", ".join(sorted(overlap))
                    raise ArgumentError(
                        f"{name}/{col['name']} sets {listed} both as a property and as a waveform field"
                    )
            wire_channels.append(
                {
                    "name": col["name"],
                    "type": col["wire"],
                    "rows": col["series"].len(),
                    "properties": _encode_properties(props, f"{name}/{col['name']}"),
                    "waveform": encoded_wave,
                }
            )
            columns.append(col)
        groups.append(
            {
                "name": name,
                "properties": _encode_properties(group_properties.get(name), name),
                "channels": wire_channels,
            }
        )

    header = {
        "op": "write_begin",
        "path": str(path),
        "mode": mode,
        "properties": _encode_properties(properties, "file"),
        "groups": groups,
    }
    with _Bridge(binary) as bridge:
        bridge.call(header)
        for col in _send_order(columns):
            for blob in _chunks(col["series"], col["wire"], chunk_bytes):
                bridge.call(
                    {"op": "write_chunk", "group": col["group"], "channel": col["name"]},
                    blob,
                )
        result, _payload = bridge.call({"op": "write_end"})
    return WriteResult(
        path=result.get("path", str(path)),
        channel_count=int(result.get("channel_count", 0)),
        row_count=int(result.get("row_count", 0)),
        stream_chunks=int(result.get("stream_chunks", 0)),
        buffered_channels=int(result.get("buffered_channels", 0)),
        segments=int(result.get("segments", 0)),
    )


def read_tdms(path, *, binary=None) -> TdmsContents:
    """Read one TDMS file into native Polars columns.

    Numeric channels stay in their stored width. Strings stay strings.
    Timestamps are ``Datetime(ns, UTC)``. This is not :func:`gotdms.open`:
    samples are not scaled, files are not merged, and a string channel is
    not converted to float64. ``Session.read`` returns float64 for an
    ordinary numeric channel. A DAQmx channel is the primary scaler's stored
    type on both paths. Digital lines are ``UInt8`` 0 and 1.

    A channel with no raw-data index has type ``"void"`` and an empty Null
    series. Extended and complex channels raise :class:`GotdmsError`.
    DAQmx raw indexes are read. The column is the primary scaler. Other
    scalers on that channel are not extra columns. This function does not
    apply ``NI_Scale`` formulas; pass ``scaled=True`` to :meth:`Session.read`
    or :func:`gotdms.scan_tdms` for a DAQmx linear or polynomial chain.

    Each channel is read on its own. An interleaved channel that continues
    in a later segment keeps its samples in order.

    File, group, and channel properties are on ``properties`` as Python
    values. Timestamp properties there are timezone-aware datetimes, which
    have microsecond resolution. ``typed_properties`` keeps the TDMS type
    name and, for timestamps, ``seconds`` and ``fraction``.
    """
    with _Bridge(binary) as bridge:
        header, payload = bridge.call({"op": "read_values", "path": str(path)})
    parts = split_payload(payload)
    flat = []
    for group in header.get("groups") or []:
        for channel in group.get("channels") or []:
            flat.append((group["name"], channel))
    if len(parts) != len(flat):
        raise ProtocolError(f"{len(flat)} channels and {len(parts)} Arrow payloads")
    groups = {}
    cursor = 0
    for group in header.get("groups") or []:
        channels = {}
        for channel in group.get("channels") or []:
            series = _series_from_ipc(channel["name"], parts[cursor], channel.get("type"))
            cursor += 1
            typed = channel.get("properties") or []
            channels[channel["name"]] = TdmsChannel(
                name=channel["name"],
                group=group["name"],
                tdms_type=channel.get("type", ""),
                ni_type=channel.get("ni_type", ""),
                properties=_plain_props(typed),
                typed_properties=typed,
                values=series,
            )
        typed = group.get("properties") or []
        groups[group["name"]] = TdmsGroup(
            name=group["name"],
            properties=_plain_props(typed),
            typed_properties=typed,
            channels=channels,
        )
    typed = header.get("properties") or []
    return TdmsContents(
        path=str(path),
        properties=_plain_props(typed),
        typed_properties=typed,
        groups=groups,
    )


def _frames(data, group):
    import polars as pl

    if isinstance(data, pl.DataFrame):
        name = "data" if group is None else group
        if not isinstance(name, str) or not name:
            raise ArgumentError("group must be a non-empty string")
        return {name: data}
    if isinstance(data, dict):
        if group is not None:
            raise ArgumentError("group is only used when data is a single DataFrame")
        if not data:
            raise ArgumentError("data has no groups")
        frames = {}
        for name, frame in data.items():
            if not isinstance(name, str) or not name:
                raise ArgumentError("group names must be non-empty strings")
            if not isinstance(frame, (pl.DataFrame, dict)):
                raise ArgumentError(
                    f"group {name!r} must be a polars.DataFrame or a mapping of channel name to Series"
                )
            frames[name] = frame
        return frames
    raise ArgumentError(
        "data must be a polars.DataFrame or a mapping of group name to DataFrame or Series mapping"
    )


def _nested(mapping, allowed, what):
    if mapping is None:
        return {}
    if not isinstance(mapping, dict):
        raise ArgumentError(f"{what} must be a dict")
    extra = [key for key in mapping if key not in allowed]
    if extra:
        raise ArgumentError(f"unknown name in {what}: {extra!r}")
    return mapping


def _iter_columns(frame, group):
    import polars as pl

    if isinstance(frame, pl.DataFrame):
        for name in frame.columns:
            yield name, frame.get_column(name)
        return
    if isinstance(frame, dict):
        for name, series in frame.items():
            if not isinstance(series, pl.Series):
                raise ArgumentError(
                    f"group {group!r} channel {name!r} must be a polars.Series"
                )
            yield name, series
        return
    raise ArgumentError(
        f"group {group!r} must be a polars.DataFrame or a mapping of channel name to Series"
    )


def _prepare_frame(frame, group):
    columns = []
    seen = set()
    for name, series in _iter_columns(frame, group):
        if not isinstance(name, str) or not name:
            raise ArgumentError(f"group {group!r} column names must be non-empty strings")
        if name in seen:
            raise ArgumentError(f"duplicate column {group}/{name}")
        seen.add(name)
        if series.name != name:
            series = series.rename(name)
        if series.null_count():
            raise ArgumentError(
                f"column {group}/{name} contains nulls. TDMS channels are not nullable. "
                "Nulls are not written as zero or as empty strings. "
                "Drop them or fill them before writing."
            )
        wire = _wire_dtype(series.dtype, f"{group}/{name}")
        if wire == "timestamp":
            series = _as_utc_ns(series)
        columns.append({"group": group, "name": name, "wire": wire, "series": series})
    return columns


def _wire_dtype(dtype, where):
    import polars as pl

    simple = {
        pl.Int8: "int8",
        pl.Int16: "int16",
        pl.Int32: "int32",
        pl.Int64: "int64",
        pl.UInt8: "uint8",
        pl.UInt16: "uint16",
        pl.UInt32: "uint32",
        pl.UInt64: "uint64",
        pl.Float32: "float32",
        pl.Float64: "float64",
        pl.Boolean: "bool",
        pl.String: "string",
        pl.Utf8: "string",
    }
    if dtype in simple:
        return simple[dtype]
    if isinstance(dtype, pl.Datetime):
        return "timestamp"
    raise ArgumentError(
        f"column {where} has unsupported dtype {dtype}. "
        "gotdms writes Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, "
        "Float32, Float64, Boolean, String, and Datetime. It does not coerce this dtype."
    )


def _as_utc_ns(series):
    import polars as pl

    dtype = series.dtype
    if dtype.time_zone is None:
        series = series.dt.replace_time_zone("UTC")
    elif dtype.time_zone != "UTC":
        series = series.dt.convert_time_zone("UTC")
    if series.dtype.time_unit != "ns":
        series = series.cast(pl.Datetime("ns", "UTC"))
    return series


def _send_order(columns):
    fixed = [col for col in columns if col["wire"] != "string" and col["series"].len()]
    strings = [col for col in columns if col["wire"] == "string" and col["series"].len()]
    return fixed + strings


def _chunks(series, wire, chunk_bytes):
    n = series.len()
    if n == 0:
        return
    width = _WIDTH.get(wire)
    if width:
        rows = max(1, min(n, chunk_bytes // width))
        if rows * width > _MAX_PAYLOAD:
            rows = max(1, _MAX_PAYLOAD // width)
    else:
        total = int(series.str.len_bytes().sum()) + n * 4
        avg = max(1, total // n)
        rows = max(1, min(n, chunk_bytes // avg))
    off = 0
    while off < n:
        take = min(rows, n - off)
        blob = _series_ipc(series.slice(off, take))
        if len(blob) > _MAX_PAYLOAD:
            if take == 1:
                raise ArgumentError(
                    f"column {series.name!r} row {off} does not fit in one Arrow frame"
                )
            rows = max(1, take // 2)
            continue
        yield blob
        off += take


def _series_ipc(series) -> bytes:
    import pyarrow as pa

    array = series.to_arrow()
    if isinstance(array, pa.ChunkedArray):
        array = array.combine_chunks()
    batch = pa.record_batch([array], names=["y"])
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue()


def _series_from_ipc(name, blob, wire):
    import pyarrow as pa
    import polars as pl

    reader = pa.ipc.open_stream(io.BytesIO(blob))
    table = reader.read_all()
    if table.num_columns != 1:
        raise ProtocolError(f"channel {name} Arrow stream has {table.num_columns} columns")
    values = pl.from_arrow(table.column(0))
    if isinstance(values, pl.DataFrame):
        values = values.to_series(0)
    if not isinstance(values, pl.Series):
        values = pl.Series(values)
    if wire == "void":
        return pl.Series(name, [], dtype=pl.Null)
    return values.rename(name)


def _waveform(spec, rows, where):
    if spec is None:
        return None
    if not isinstance(spec, dict) or not spec:
        raise ArgumentError(f"waveform for {where} must be a non-empty dict")
    unknown = [key for key in spec if key not in _WAVE_FIELDS]
    if unknown:
        raise ArgumentError(f"unknown waveform field for {where}: {unknown!r}")
    out = {}
    if "wf_start_time" in spec:
        out["wf_start_time"] = _timestamp_wire(spec["wf_start_time"], f"{where} wf_start_time")
    if "wf_increment" in spec:
        out["wf_increment"] = _finite_float(spec["wf_increment"], f"{where} wf_increment")
    if "wf_start_offset" in spec:
        out["wf_start_offset"] = _finite_float(spec["wf_start_offset"], f"{where} wf_start_offset")
    if "wf_samples" in spec:
        out["wf_samples"] = _plain_int(spec["wf_samples"], f"{where} wf_samples")
    else:
        out["wf_samples"] = int(rows)
    return out


def _encode_properties(props, where):
    if props is None:
        return []
    if not isinstance(props, dict):
        raise ArgumentError(f"{where} properties must be a dict")
    out = []
    seen = set()
    for name, value in props.items():
        if not isinstance(name, str) or not name:
            raise ArgumentError(f"{where} property names must be non-empty strings")
        if name in seen:
            raise ArgumentError(f"duplicate property {name!r}")
        seen.add(name)
        out.append(_encode_prop(name, value))
    return out


def _encode_prop(name, value):
    if isinstance(value, Property):
        wire = _property_wire(value.tdms_type, value.value, name)
        return {"name": name, "type": value.tdms_type, "value": wire}
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, bool):
        return {"name": name, "type": "bool", "value": value}
    if isinstance(value, int):
        if -(2**31) <= value <= 2**31 - 1:
            return {"name": name, "type": "int32", "value": value}
        if -(2**63) <= value <= 2**63 - 1:
            return {"name": name, "type": "int64", "value": value}
        raise ArgumentError(
            f"property {name!r} does not fit in int64. Pass Property(value, 'uint64') "
            "when the value is an unsigned 64-bit integer."
        )
    if isinstance(value, float):
        _finite_float(value, f"property {name!r}")
        return {"name": name, "type": "float64", "value": value}
    if isinstance(value, str):
        return {"name": name, "type": "string", "value": value}
    if isinstance(value, datetime) or isinstance(value, np.datetime64):
        return {"name": name, "type": "timestamp", "value": _timestamp_wire(value, f"property {name!r}")}
    if isinstance(value, dict) and set(value) >= {"seconds", "fraction"} and set(value) <= {"seconds", "fraction"}:
        return {
            "name": name,
            "type": "timestamp",
            "value": _timestamp_wire(value, f"property {name!r}"),
        }
    raise ArgumentError(
        f"property {name!r} has unsupported Python type {type(value).__name__}. "
        "Use bool, int, float, str, datetime, or gotdms.Property."
    )


def _property_wire(tdms_type, value, name):
    if isinstance(value, np.generic):
        value = value.item()
    if tdms_type in _INT_RANGE:
        number = _plain_int(value, f"property {name!r}")
        lo, hi = _INT_RANGE[tdms_type]
        if number < lo or number > hi:
            raise ArgumentError(f"property {name!r} value {number} does not fit in {tdms_type}")
        return number
    if tdms_type in _UINT_MAX:
        if isinstance(value, bool) or not isinstance(value, int) or isinstance(value, np.bool_):
            raise ArgumentError(f"property {name!r} type {tdms_type} wants an integer")
        if value < 0 or value > _UINT_MAX[tdms_type]:
            raise ArgumentError(f"property {name!r} value {value} does not fit in {tdms_type}")
        return value
    if tdms_type == "float64":
        return _finite_float(value, f"property {name!r}")
    if tdms_type == "float32":
        number = _finite_float(value, f"property {name!r}")
        if number < -np.finfo(np.float32).max or number > np.finfo(np.float32).max:
            raise ArgumentError(f"property {name!r} does not fit in float32")
        return number
    if tdms_type == "bool":
        if not isinstance(value, bool):
            raise ArgumentError(f"property {name!r} type bool wants True or False")
        return value
    if tdms_type == "string":
        if not isinstance(value, str):
            raise ArgumentError(f"property {name!r} type string wants a str")
        return value
    if tdms_type == "timestamp":
        return _timestamp_wire(value, f"property {name!r}")
    raise ArgumentError(
        f"property {name!r} has unsupported TDMS type {tdms_type!r}. "
        "Use int8, int16, int32, int64, uint8, uint16, uint32, uint64, "
        "float32, float64, bool, string, or timestamp."
    )


def _plain_int(value, where):
    if isinstance(value, np.integer):
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ArgumentError(f"{where} wants an integer")
    return value


def _finite_float(value, where):
    if isinstance(value, bool):
        raise ArgumentError(f"{where} wants a float")
    if isinstance(value, (int, np.integer)):
        value = float(value)
    if isinstance(value, np.floating):
        value = float(value)
    if not isinstance(value, float):
        raise ArgumentError(f"{where} wants a float")
    if not np.isfinite(value):
        raise ArgumentError(f"{where} wants a finite float")
    return value


def _timestamp_wire(value, where):
    if isinstance(value, dict) and "seconds" in value and "fraction" in value:
        seconds = _plain_int(value["seconds"], f"{where} seconds")
        fraction = _plain_int(value["fraction"], f"{where} fraction")
        if seconds < -(2**63) or seconds > 2**63 - 1:
            raise ArgumentError(f"{where} seconds do not fit in int64")
        if fraction < 0 or fraction > 2**64 - 1:
            raise ArgumentError(f"{where} fraction does not fit in uint64")
        return {"seconds": seconds, "fraction": fraction}
    if isinstance(value, np.datetime64):
        ns = int(value.astype("datetime64[ns]").astype(np.int64))
        return {"unix_ns": ns}
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    raise ArgumentError(f"{where} wants a datetime, an RFC3339 string, or seconds and fraction")


def _plain_props(items):
    return {item["name"]: _plain_value(item) for item in items}


def _plain_value(item):
    value = item.get("value")
    if item.get("type") == "timestamp" and isinstance(value, dict):
        text = value.get("time")
        if text:
            return _datetime_from_wire(text)
    return value


def _datetime_from_wire(text):
    """RFC3339Nano to a timezone-aware datetime.

    ``datetime`` stores microseconds. A longer fraction is truncated here.
    ``typed_properties`` still has ``seconds`` and ``fraction``.
    """
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dot = text.find(".")
    if dot != -1:
        i = dot + 1
        while i < len(text) and text[i].isdigit():
            i += 1
        digits = text[dot + 1 : i]
        if len(digits) > 6:
            text = text[: dot + 1] + digits[:6] + text[i:]
    return datetime.fromisoformat(text)


class _Bridge:
    """One short-lived ``gotdms-bridge serve`` process."""

    def __init__(self, binary):
        binary = locate_binary(binary)
        self._proc = subprocess.Popen(
            [binary, "serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._stderr = bytearray()
        self._drain = threading.Thread(target=self._read_stderr, name="gotdms-bridge-stderr", daemon=True)
        self._drain.start()
        self._closed = False

    def _read_stderr(self):
        stream = self._proc.stderr
        if stream is None:
            return
        try:
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    return
                self._stderr += chunk
        except Exception:
            return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def call(self, header, payload=b""):
        if self._closed or self._proc.poll() is not None:
            raise ProtocolError(self._dead())
        try:
            write_frame(self._proc.stdin, header, payload)
            decoded, body = read_frame(self._proc.stdout)
        except ProtocolError:
            raise
        except Exception as exc:
            raise ProtocolError(self._dead()) from exc
        if decoded.get("op") == "error":
            _raise_protocol(decoded)
        return decoded, body

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            if self._proc.poll() is None and self._proc.stdin is not None:
                write_frame(self._proc.stdin, {"op": "close"})
                read_frame(self._proc.stdout)
        except Exception:
            pass
        finally:
            for stream in (self._proc.stdin, self._proc.stdout, self._proc.stderr):
                if stream is None:
                    continue
                try:
                    stream.close()
                except Exception:
                    pass
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
                self._proc.wait(timeout=5)

    def _dead(self):
        err = bytes(self._stderr).decode(errors="replace").strip()
        if err:
            return err
        return f"gotdms-bridge exited {self._proc.returncode}"
