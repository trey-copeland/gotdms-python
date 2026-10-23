"""Lazy Polars scans over TDMS files.

``scan_tdms`` returns a :class:`polars.LazyFrame`. Building that frame reads
catalog metadata only. Channel samples are decoded when the frame is
collected, and only for the columns and row prefix Polars actually pushed
down.

This uses :func:`polars.io.plugins.register_io_source`, the public IO-plugin
API in Polars 2. Polars marks that function unstable, but it is the
documented plugin entry point (https://docs.pola.rs/user-guide/plugins/io_plugins).
Nothing here imports a private ``polars._`` module. Polars is MIT, which is
already the dataframe dependency of this package.
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

from .client import Session
from .codec import as_polars
from .errors import ArgumentError, GotdmsError

# Polars 2.0 does not push a slice offset or a row limit through a predicate.
# The source has to apply n_rows itself when the engine sends it. A predicate
# has to be applied here as well: the engine does not filter the batch again.
_TIME_ABSOLUTE = "time"
_TIME_RELATIVE = "relative_s"


def scan_tdms(
    source,
    *,
    pattern="*.tdms",
    timestamp_channel=None,
    timestamp_channels=None,
    numeric_epoch=None,
    basis=None,
    overlap=None,
    workers=None,
    read_workers=None,
    absolute_time=True,
    time="column",
    scaled=False,
    align="strict",
    binary=None,
    trace=None,
):
    """Lazy frame over one TDMS file, a list of files, a directory, or a glob.

    ``source`` follows the same dataset rules as :func:`gotdms.open`. A list
    is an explicit file list. A directory uses ``pattern`` (default
    ``*.tdms``). A string or path that contains glob characters is split into
    a directory and a pattern (``run/*.tdms``). ``**`` is rejected: the bridge
    uses ``filepath.Glob``, which does not recurse.

    Timestamp rules match the eager dataset API. Only a declared timestamp
    channel is a clock. There is no name-based inference. A group with no
    declaration uses waveform ``wf_start_time`` and ``wf_increment`` when
    both are present. A numeric clock requires ``numeric_epoch``. Relative
    time is seconds since ``basis``. The default basis is the earliest timed
    sample, fixed when a collect opens the dataset, and a ``head`` or
    ``limit`` does not move it.

    Channels are matched across files by group and name. The same name in two
    groups is two columns (``group/name``). One group/name whose files do not
    share a time axis raises :class:`ArgumentError`. Gaps stay on the read
    header. Strict alignment does not insert null rows.

    What Polars 2 pushes into this source:

    - Column projection, when the plan has no predicate, or when the
      predicate only needs the projected columns. The bridge decodes those
      data channels and the timestamp axis they use. It does not decode
      other data channels. Waveform time comes from channel properties.
    - ``head(n)`` and ``limit(n)`` when the plan has no predicate. ``n`` is
      sent as a sample ``limit`` and the bridge reads it with ``Channel.Slice``.
    - A predicate is applied in Python after that read. It is not evaluated
      inside the bridge. ``filter`` before ``select`` of a different column
      passes no projection, so the collect reads every data column.
    - ``slice(offset, length)`` is not pushed: Polars 2 calls this source
      with ``n_rows=None`` and slices the batch afterward. The bridge still
      accepts ``offset`` on ``read`` and ``read_many`` for direct callers.
    - ``batch_size`` is ignored. One collect is one bridge read, so the
      merged series keeps a single basis and a single gap list.

    ``trace``, when it is a list, receives one dict per collect that reaches
    the bridge. Keys are ``reads``, ``limited``, ``with_columns``, ``n_rows``,
    ``limit_pushed``, ``predicate``, and ``channels``. ``reads`` is the
    bridge log of decoded sample ranges. Opening the dataset still follows
    the catalog rule (the first two samples of a uniform clock, or the whole
    clock when the first step is not positive). That catalog read is not an
    entry in ``reads``.

    The returned frame's ``gotdms_sample_reads`` attribute is the describe
    counter. It is 0. ``collect_schema`` and ``select`` do not collect.
    """
    pl = _import_polars()
    if time == "index":
        raise ArgumentError(
            "Polars has no row index. npTDMS time_index=True is the 'time' column "
            "from time='column', or 'relative_s' when absolute_time is false"
        )
    if time not in ("column", "none"):
        raise ArgumentError("time must be column or none")
    if align not in ("strict", "nptdms"):
        raise ArgumentError("align must be strict or nptdms")
    if trace is not None and not isinstance(trace, list):
        raise ArgumentError("trace must be a list")

    source_kwargs = _source_kwargs(source, pattern)
    open_kwargs = {
        "timestamp_channel": timestamp_channel,
        "timestamp_channels": timestamp_channels,
        "numeric_epoch": numeric_epoch,
        "basis": basis,
        "overlap": overlap,
        "workers": workers,
        "read_workers": read_workers,
        **source_kwargs,
    }
    with Session(binary=binary) as session:
        described = session.describe(**open_kwargs)
    channels = described.get("channels") or []
    _reject_mixed_axes(channels)
    data_columns = _data_columns(channels)
    if not data_columns:
        raise ArgumentError("no channels")

    time_name = None
    schema = {}
    if time == "column":
        time_name = _TIME_ABSOLUTE if absolute_time else _TIME_RELATIVE
        if absolute_time:
            schema[time_name] = pl.Datetime("ns", "UTC")
        else:
            schema[time_name] = pl.Float64
    by_name = {}
    for column in data_columns:
        name = f"{column['group']}/{column['name']}"
        by_name[name] = column
        schema[name] = _polars_dtype(pl, column, scaled)
    time_only = _time_only_specs(data_columns)

    def io_source(with_columns, predicate, n_rows, batch_size):
        # batch_size is only a hint. Paging a merged series would split its
        # basis and its gap list, so a collect is one read.
        del batch_size
        projected = None if with_columns is None else list(with_columns)
        if n_rows == 0:
            frame = _empty_frame(pl, schema, projected)
            if predicate is not None:
                frame = frame.filter(predicate)
            _note(
                trace,
                reads=[],
                limited=False,
                with_columns=projected,
                n_rows=n_rows,
                limit_pushed=False,
                predicate=predicate is not None,
                channels=[],
            )
            yield frame
            return

        names = _projected_data(projected, by_name, time_name)
        if not names and time_name is not None and (projected is None or time_name in projected):
            specs = time_only
        else:
            specs = [{"group": by_name[name]["group"], "name": by_name[name]["name"]} for name in names]
        # A predicate has to see every row it might keep. Pushing limit
        # before the filter would drop rows the predicate still needs.
        limit = None
        if predicate is None and n_rows is not None:
            limit = int(n_rows)

        with Session(binary=binary) as session:
            session.open(**open_kwargs)
            series, header = session.read_channels(
                specs,
                absolute=bool(absolute_time),
                relative=not absolute_time,
                scaled=scaled,
                basis=basis,
                limit=limit,
            )
        frame = as_polars(series, time=time, absolute_time=absolute_time, align=align)
        if predicate is not None:
            frame = frame.filter(predicate)
            if n_rows is not None:
                frame = frame.head(int(n_rows))
        if projected is not None:
            frame = frame.select(projected)
        _note(
            trace,
            reads=list(header.get("reads") or []),
            limited=bool(header.get("limited")),
            with_columns=projected,
            n_rows=n_rows,
            limit_pushed=limit is not None,
            predicate=predicate is not None,
            channels=list(header.get("channels") or []),
        )
        yield frame

    from polars.io.plugins import register_io_source

    frame = register_io_source(
        io_source,
        schema=schema,
        validate_schema=True,
        # Each collect re-reads the files. A pure source would let Polars
        # reuse one batch and skip a later collect.
        is_pure=False,
        explain_name="gotdms",
        explain_detail=(
            "TDMS scan. Projects columns and pushes head/limit when there is "
            "no predicate. Predicates run in Python. slice offsets are not pushed."
        ),
    )
    frame.gotdms_sample_reads = described.get("sample_reads", 0)
    return frame


def _polars_dtype(pl, column, scaled):
    """Column dtype for a scan schema.

    Ordinary channels are float64, which is what ``Session.read`` returns.
    A DAQmx channel uses the primary scaler's stored type unless ``scaled``
    is set. ``scaled=True`` is float64 engineering units. Digital lines are
    UInt8 of 0 and 1.
    """
    if scaled or not column.get("daqmx"):
        return pl.Float64
    return {
        "tdsTypeI8": pl.Int8,
        "tdsTypeI16": pl.Int16,
        "tdsTypeI32": pl.Int32,
        "tdsTypeI64": pl.Int64,
        "tdsTypeU8": pl.UInt8,
        "tdsTypeU16": pl.UInt16,
        "tdsTypeU32": pl.UInt32,
        "tdsTypeU64": pl.UInt64,
        "tdsTypeSingleFloat": pl.Float32,
        "tdsTypeDoubleFloat": pl.Float64,
        "tdsTypeTimeStamp": pl.Datetime("ns", "UTC"),
    }.get(column.get("data_type"), pl.Float64)


def _import_polars():
    try:
        import polars as pl
    except ImportError as exc:
        raise GotdmsError("polars is required for scan_tdms") from exc
    return pl


def _source_kwargs(source, pattern) -> dict:
    if isinstance(source, (list, tuple)):
        if not source:
            raise ArgumentError("source has no paths")
        return {"paths": [os.fspath(path) for path in source]}
    if isinstance(source, (str, os.PathLike)):
        text = os.fspath(source)
        if glob.has_magic(text):
            directory, found = os.path.split(text)
            _reject_recursive(found)
            if directory == "":
                directory = "."
            return {"directory": directory, "pattern": found}
        path = Path(text)
        if path.is_dir():
            _reject_recursive(pattern)
            return {"directory": text, "pattern": pattern or "*.tdms"}
        return {"paths": [text]}
    raise ArgumentError("source must be a path, a directory, a glob, or a list of paths")


def _reject_recursive(pattern):
    if pattern and "**" in pattern:
        raise ArgumentError(
            "recursive ** globs are not supported; pass a directory and a non-recursive pattern"
        )


def _reject_mixed_axes(channels):
    seen = {}
    for channel in channels:
        key = (channel.get("group", ""), channel.get("name", ""))
        signature = (
            bool(channel.get("timed")),
            channel.get("source") or "none",
            bool(channel.get("is_timestamp")),
        )
        previous = seen.get(key)
        if previous is None:
            seen[key] = signature
            continue
        if previous != signature:
            raise ArgumentError(f"{key[1]!r} group {key[0]!r} has mixed time axes")


def _data_columns(channels):
    seen = set()
    out = []
    for channel in channels:
        if channel.get("is_timestamp"):
            continue
        key = (channel.get("group", ""), channel.get("name", ""))
        if key in seen:
            continue
        seen.add(key)
        out.append(channel)
    return out


def _time_only_specs(data_columns):
    """Selectors that can build the time column without extra data channels.

    A timestamp-timed group contributes its clock. A waveform group contributes
    its first data channel, because the axis is that channel's properties and
    the bridge returns time on a channel read. An untimed group contributes
    every data channel so index alignment still sees each of them.
    """
    grouped = {}
    for column in data_columns:
        grouped.setdefault(column.get("group", ""), []).append(column)
    specs = []
    for group, columns in grouped.items():
        timestamped = [
            column
            for column in columns
            if column.get("timed") and column.get("source") == "timestamp" and column.get("timestamp")
        ]
        waveforms = [
            column for column in columns if column.get("timed") and column.get("source") == "waveform"
        ]
        if timestamped:
            specs.append({"group": group, "name": timestamped[0]["timestamp"]})
        elif waveforms:
            specs.append({"group": group, "name": waveforms[0]["name"]})
        for column in columns:
            if not column.get("timed"):
                specs.append({"group": group, "name": column["name"]})
    return specs


def _projected_data(with_columns, by_name, time_name):
    if with_columns is None:
        return list(by_name)
    return [name for name in with_columns if name != time_name and name in by_name]


def _empty_frame(pl, schema, with_columns):
    if with_columns is None:
        selected = schema
    else:
        selected = {name: schema[name] for name in with_columns}
    return pl.DataFrame(schema=selected)


def _note(trace, **entry):
    if trace is not None:
        trace.append(entry)
