"""Long-lived client for the gotdms-bridge ``serve`` process.

One process, one open dataset, many reads. Each request is one frame and
each response is one frame. Channels that share a timestamp channel share
one clock decode in the dataset layer, but each channel's Arrow stream
still carries its own copy of the time columns. This module does not
import npTDMS.
"""

from __future__ import annotations

import os
import subprocess
import threading
import weakref
from datetime import datetime, timezone
from pathlib import Path

from ._process import StderrTail, kill, shutdown, start_drain
from .codec import as_polars, read_reply, series_from_reply, write_frame
from .errors import (
    AmbiguousChannelError,
    ArgumentError,
    CorruptFileError,
    GotdmsError,
    MonotonicError,
    NoFilesError,
    NoScaleError,
    NonlinearScaleError,
    NotFoundError,
    OverlapError,
    ProtocolError,
    ScaleMismatchError,
    UntimedError,
)

_CODE_TYPES = {
    "argument": ArgumentError,
    "corrupt": CorruptFileError,
    "untimed": UntimedError,
    "not_found": NotFoundError,
    "no_files": NoFilesError,
    "no_scale": NoScaleError,
    "nonlinear_scale": NonlinearScaleError,
    "scale_mismatch": ScaleMismatchError,
    "overlap": OverlapError,
    "monotonic": MonotonicError,
    "bad_version": ProtocolError,
    "protocol": ProtocolError,
    "session_closed": GotdmsError,
    "error": GotdmsError,
}


def bundled_binary() -> str | None:
    """Path of the binary shipped in this wheel, if this install has one."""
    name = "gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge"
    path = Path(__file__).resolve().parent / "_bin" / name
    if path.is_file():
        return str(path)
    return None


def locate_binary(explicit=None) -> str:
    """Find gotdms-bridge.

    An explicit ``binary=`` argument wins, then the ``GOTDMS_BRIDGE``
    environment variable, then the binary bundled in the wheel. A wheel
    install therefore uses the bundled binary unless the caller overrides
    it. There is no PATH search. If none of the three exist, this raises
    with install instructions.
    """
    if explicit:
        return explicit
    env = os.environ.get("GOTDMS_BRIDGE")
    if env:
        return env
    bundled = bundled_binary()
    if bundled:
        return bundled
    raise GotdmsError(
        "gotdms-bridge was not found. Install a platform wheel so the binary "
        "is bundled (`pip install gotdms`), or set GOTDMS_BRIDGE to a binary "
        "from `go build -o gotdms-bridge ./cmd/gotdms-bridge` in the "
        "gotdms-python source tree (Go required). Protocol version 2."
    )


class Session:
    """A dataset held open in one gotdms-bridge process.

    ``TdmsFile.read_metadata`` maps to :meth:`open` and :meth:`catalog`.
    Those calls do not decode data-channel samples. Opening still asks the
    dataset layer to read declared timestamp channels so it can learn each
    file's span. ``TdmsFile.read`` maps to :meth:`open` plus :meth:`read_all` or
    :meth:`as_polars`.

    The binary is not safe to share across threads. This class locks the
    pipe so one session is still safe to call from several threads.

    Prefer :meth:`close` or a ``with`` block. A session that is dropped
    without either still stops its bridge process when it is garbage
    collected, or at interpreter exit at the latest.
    """

    def __init__(self, binary=None):
        binary = locate_binary(binary)
        self.binary = binary
        self._proc = subprocess.Popen(
            [binary, "serve"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        self._lock = threading.Lock()
        # The drain thread and the finalizer hold the process and the tail,
        # not self, so an unclosed session can still be collected.
        self._stderr = StderrTail()
        self._drain = start_drain(self._proc, self._stderr)
        self._finalizer = weakref.finalize(self, shutdown, self._proc, self._lock)
        self._catalog = None
        self._closed = False
        self._broken = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def close(self):
        """Ask the bridge to close, then stop the process."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            # The lock is already held, so stop the finalizer and shut down
            # here without it. detach() returns None if it already ran.
            if self._finalizer.detach() is not None:
                shutdown(self._proc)

    def open(
        self,
        paths=None,
        *,
        directory=None,
        pattern="*.tdms",
        timestamp_channel=None,
        timestamp_channels=None,
        numeric_epoch=None,
        basis=None,
        overlap=None,
        workers=None,
        read_workers=None,
    ):
        """Open a file set and return the catalog.

        ``paths`` is an explicit list. ``directory`` plus ``pattern`` is a
        glob. Declare timestamp channels the way the dataset package does:
        ``timestamp_channel`` is the global name, and ``timestamp_channels``
        maps a group to a name. An empty string opts that group out. Numeric
        clocks need ``numeric_epoch``; there is no default epoch. ``basis``
        is the relative-time origin. It is not moved to the first sample.

        A DAQmx channel is cataloged as its primary scaler. ``data_type`` is
        that stored type. Digital lines are ``tdsTypeU8``. ``daqmx`` is true
        on those catalog rows. Samples stay raw until a read passes
        ``scaled=True``.
        """
        header = _dataset_header(
            "open",
            paths,
            directory=directory,
            pattern=pattern,
            timestamp_channel=timestamp_channel,
            timestamp_channels=timestamp_channels,
            numeric_epoch=numeric_epoch,
            basis=basis,
            overlap=overlap,
            workers=workers,
            read_workers=read_workers,
        )
        self._catalog = _expand_catalog(self._rpc(header))
        return self._catalog

    def describe(
        self,
        paths=None,
        *,
        directory=None,
        pattern="*.tdms",
        timestamp_channel=None,
        timestamp_channels=None,
        numeric_epoch=None,
        basis=None,
        overlap=None,
        workers=None,
        read_workers=None,
    ):
        """Metadata for a file set. Does not decode channel samples.

        ``open`` asks the dataset layer to read declared timestamp channels
        so it can learn each file's span. ``describe`` does not. It reads
        groups, channels, stored types, sample counts, scales, and waveform
        properties. ``sample_reads`` in the result is the number of raw-data
        reads, and it is 0. A numeric timestamp channel still requires
        ``numeric_epoch``; that check uses the stored type.
        """
        header = _dataset_header(
            "describe",
            paths,
            directory=directory,
            pattern=pattern,
            timestamp_channel=timestamp_channel,
            timestamp_channels=timestamp_channels,
            numeric_epoch=numeric_epoch,
            basis=basis,
            overlap=overlap,
            workers=workers,
            read_workers=read_workers,
        )
        described = self._rpc(header)
        if described.get("sample_reads", 0) != 0:
            raise ProtocolError("describe decoded channel sample data")
        return described

    def catalog(self):
        """Return groups, channels, and properties. Does not decode samples."""
        self._catalog = _expand_catalog(self._rpc({"op": "catalog"}))
        return self._catalog

    def find(self, name: str):
        return self._rpc({"op": "find", "name": name}).get("channels", [])

    def search(self, text: str):
        return self._rpc({"op": "search", "text": text}).get("channels", [])

    def match(self, pattern: str):
        return self._rpc({"op": "match", "pattern": pattern}).get("channels", [])

    def groups(self):
        """Groups in catalog order. This is the ``TdmsFile.groups`` shape."""
        seen = []
        for item in self._occurrences():
            if item["group"] not in seen:
                seen.append(item["group"])
        return [Group(self, name) for name in seen]

    def group(self, name: str) -> Group:
        if not any(item["group"] == name for item in self._occurrences()):
            raise NotFoundError(f"group {name!r} not found")
        return Group(self, name)

    def __getitem__(self, name: str) -> Group:
        return self.group(name)

    def channel(self, name: str, group: str | None = None) -> Channel:
        """Look up one channel. A bare name that occurs in two groups raises.

        npTDMS always indexes ``file[group][channel]``. gotdms allows a bare
        name when it is unique across groups.
        """
        hits = [
            item
            for item in self._occurrences()
            if item["name"] == name and (group is None or item["group"] == group)
        ]
        if not hits:
            raise NotFoundError(f"channel {name!r} not found")
        groups = {item["group"] for item in hits}
        if group is None and len(groups) > 1:
            raise AmbiguousChannelError(f"ambiguous channel {name!r}", hits)
        return Channel(self, name, hits[0]["group"], hits)

    def read(self, name: str, group: str | None = None, **kwargs):
        """Read one channel. See :meth:`read_many` for the time and scale options."""
        if group is None:
            group = self.channel(name).group
        return self.read_many([{"name": name, "group": group}], **kwargs)[0]

    def read_many(
        self,
        channels,
        *,
        start=None,
        end=None,
        absolute=True,
        relative=None,
        scaled=False,
        basis=None,
        offset=None,
        limit=None,
    ):
        """Read several channels in one request.

        The bridge calls dataset ``ReadMany`` once, so channels that share a
        timestamp channel share one clock decode. Each distinct time axis is
        sent once, and channels on it get the same ``time_ns`` array object
        (read-only, like every array from a read). Channels are packed as
        columns of a few Arrow streams, and ``y`` is a view of its stream's
        buffer: keeping one channel keeps that stream alive, which holds at
        most 16 MiB of other channels (a channel over 16 MiB has a stream of
        its own). ``numpy.copy`` a short channel you keep for long. Pass both
        ``start`` and ``end`` for a half-open window. Omit both for the full
        span.

        ``offset`` and ``limit`` select a sample range of the merged series
        instead of a time window. The bridge reads that range with
        ``Channel.Slice`` and does not decode the other samples. Omit both
        for the full series. A time window and a sample range together are
        an error.

        ``absolute`` (default True) returns ``time_ns``. ``relative``
        defaults to ``None``, which means ``not absolute``: the bridge sends
        one time column, never both. ``Series.relative_s`` is still
        available with absolute time: it is computed in Python from
        ``time_ns`` and the basis on first access, bit-for-bit what the
        bridge computes. Pass ``relative=True`` to have it computed during
        the read. ``absolute=False`` alone (or with ``relative=True``) asks
        the bridge for ``relative_s`` only. ``absolute=False,
        relative=False`` sends sample values only, and ``relative_s`` is
        then ``None`` for a timed channel.

        ``scaled`` defaults to False. gotdms returns raw samples. Set it only
        when you want a scale applied. There is no silent scale. npTDMS
        returns scaled data unless you pass ``scaled=False`` or read
        ``raw_data``.

        An ordinary channel applies an NI linear scale
        (``raw * slope + intercept``). A missing linear scale raises
        :class:`NoScaleError`. A non-linear scale raises
        :class:`NonlinearScaleError` and is not applied.

        A DAQmx channel stays in the primary scaler's stored type when
        ``scaled`` is false. Digital lines are ``uint8`` 0 and 1. ``scaled=True``
        applies the DAQmx linear or polynomial chain and returns ``float64``.
        ``NI_Scaling_Status`` of ``scaled`` means the stored samples are
        already engineering units, so the same flag returns those raw values
        as ``float64``. No DAQmx scale raises :class:`NoScaleError`. A scale
        type other than linear or polynomial raises
        :class:`NonlinearScaleError`. The column is the primary scaler.
        Other scalers on the same channel are not extra columns.
        """
        series, _header = self.read_channels(
            channels,
            start=start,
            end=end,
            absolute=absolute,
            relative=relative,
            scaled=scaled,
            basis=basis,
            offset=offset,
            limit=limit,
        )
        return series

    def read_channels(
        self,
        channels,
        *,
        start=None,
        end=None,
        absolute=True,
        relative=None,
        scaled=False,
        basis=None,
        offset=None,
        limit=None,
    ):
        """Like :meth:`read_many`, and also return the bridge header.

        The header's ``reads`` list is the sample ranges the bridge decoded.
        ``limited`` is true when ``offset`` or ``limit`` was applied. A bridge
        that ignores a requested range raises :class:`ProtocolError`.
        """
        specs = []
        for item in channels:
            if isinstance(item, Channel):
                specs.append({"name": item.name, "group": item.group})
            elif isinstance(item, str):
                ch = self.channel(item)
                specs.append({"name": ch.name, "group": ch.group})
            else:
                specs.append({"name": item["name"], "group": item.get("group", "")})
        # relative=None means "not absolute". Relative time is derived from
        # time_ns in Python when absolute time is on the wire, so the bridge
        # sends at most one time column.
        if relative is None:
            relative = not absolute
        header = {
            "op": "read_many",
            "channels": specs,
            "absolute": bool(absolute),
            "relative": bool(relative) and not absolute,
            "scaled": bool(scaled),
        }
        if (start is None) ^ (end is None):
            raise ArgumentError("window needs both start and end")
        if start is not None:
            header["start"] = _rfc3339(start)
            header["end"] = _rfc3339(end)
        if basis is not None:
            header["basis"] = _rfc3339(basis)
        if offset is not None:
            header["offset"] = int(offset)
        if limit is not None:
            header["limit"] = int(limit)
        reply = self._rpc_raw(header)
        if reply.streams is None:
            raise ProtocolError("read reply is not a data reply; rebuild gotdms-bridge")
        header_out = reply.header
        if (offset is not None or limit is not None) and not header_out.get("limited"):
            raise ProtocolError(
                "gotdms-bridge ignored the sample range. Rebuild gotdms-bridge; "
                "protocol 2 range reads are required."
            )
        _resolve_read_header(header_out, self._catalog)
        series = series_from_reply(header_out, reply.streams)
        if relative and absolute:
            for item in series:
                _ = item.relative_s
        return series, header_out

    def read_all(self, **kwargs):
        """Eager read of every data channel, one ``read_many``.

        Declared timestamp channels are omitted because they are the clock.
        Read one by name when you want its stored values. This is the
        ``TdmsFile.read`` data step after :meth:`open`.
        """
        return self.read_many(self._data_channels(), **kwargs)

    def as_polars(
        self,
        channels=None,
        group: str | None = None,
        *,
        time="column",
        absolute_time=True,
        align="strict",
        scaled=False,
        start=None,
        end=None,
        basis=None,
    ):
        """Polars frame for a channel list, one group, or the whole set.

        Polars has no row index. npTDMS ``time_index=True`` (a DatetimeIndex)
        is the ``time`` column produced by ``time="column"``. See
        :func:`gotdms.codec.as_polars` for the strict and full-join alignment.
        ``scaled`` is forwarded to the read and defaults to False, unlike
        npTDMS ``as_dataframe(scaled_data=True)``. A pandas object, if you
        still want one, is ``frame.to_pandas()`` on the result. This package
        does not depend on pandas.
        """
        if channels is None:
            if group is None:
                selected = self._data_channels()
            else:
                selected = self.group(group)._data_channels()
        else:
            selected = channels
        # Only the time column the frame uses is sent.
        series = self.read_many(
            selected,
            start=start,
            end=end,
            absolute=bool(absolute_time),
            relative=not absolute_time,
            scaled=scaled,
            basis=basis,
        )
        return as_polars(series, time=time, absolute_time=absolute_time, align=align)

    def _data_channels(self):
        seen = set()
        out = []
        for item in self._occurrences():
            if item.get("is_timestamp"):
                continue
            key = (item["group"], item["name"])
            if key in seen:
                continue
            seen.add(key)
            out.append({"name": item["name"], "group": item["group"]})
        return out

    def _occurrences(self):
        if self._catalog is None:
            raise ArgumentError("open a dataset first")
        return self._catalog.get("channels", [])

    def _rpc(self, header: dict) -> dict:
        reply = self._rpc_raw(header)
        if reply.payload or reply.streams is not None:
            raise ProtocolError("unexpected payload on a metadata response")
        return reply.header

    def _rpc_raw(self, header: dict):
        """Send one request and read its whole reply.

        Anything that stops the read partway (a framing fault, a dead
        bridge, KeyboardInterrupt) leaves the pipe out of sync, so the
        bridge is killed and the session refuses further calls. An error
        the bridge reports, before or after ``result_begin``, leaves the
        session usable.
        """
        with self._lock:
            if self._closed:
                raise GotdmsError("session is closed", "session_closed")
            if self._broken is not None:
                raise ProtocolError(f"session is unusable after an interrupted reply ({self._broken}); open a new one")
            if self._proc.poll() is not None:
                raise ProtocolError(self._dead_message())
            try:
                write_frame(self._proc.stdin, header)
                reply = read_reply(self._proc.stdout)
            except BaseException as exc:
                dead = self._proc.poll() is not None
                self._break(exc)
                if isinstance(exc, ProtocolError) or not isinstance(exc, Exception):
                    raise
                if dead or self._stderr.text():
                    raise ProtocolError(self._dead_message()) from exc
                raise ProtocolError(f"{type(exc).__name__}: {exc}") from exc
        if reply.header.get("op") == "error":
            _raise_protocol(reply.header)
        if reply.error is not None:
            _raise_protocol(reply.error)
        return reply

    def _break(self, exc: BaseException) -> None:
        """Kill a bridge whose pipe is out of sync. The lock is held."""
        self._broken = type(exc).__name__
        if self._finalizer.detach() is not None:
            kill(self._proc)

    def _dead_message(self) -> str:
        err = self._stderr.text()
        if err:
            return err
        return f"gotdms-bridge exited {self._proc.returncode}"


class Group:
    """One TDMS group. ``group["channel"]`` matches npTDMS group lookup."""

    def __init__(self, session: Session, name: str):
        self.session = session
        self.name = name

    def channels(self):
        seen = []
        for item in self.session._occurrences():
            if item["group"] == self.name and item["name"] not in seen:
                seen.append(item["name"])
        return [self.session.channel(name, self.name) for name in seen]

    def __iter__(self):
        return iter(self.channels())

    def __getitem__(self, name: str) -> Channel:
        return self.session.channel(name, self.name)

    def properties(self):
        """Group properties per file. Catalog metadata only."""
        if self.session._catalog is None:
            raise ArgumentError("open a dataset first")
        return [
            item
            for item in self.session._catalog.get("groups", [])
            if item.get("name") == self.name
        ]

    def as_polars(self, **kwargs):
        """Data channels in this group, as a Polars frame. Timestamp channels are omitted."""
        return self.session.as_polars(group=self.name, **kwargs)

    def _data_channels(self):
        seen = set()
        out = []
        for item in self.session._occurrences():
            if item["group"] != self.name or item.get("is_timestamp"):
                continue
            if item["name"] in seen:
                continue
            seen.add(item["name"])
            out.append({"name": item["name"], "group": self.name})
        return out


class Channel:
    """One channel name in one group, possibly merged across files.

    ``data`` and ``time_track`` each issue a read. Prefer :meth:`Session.read_many`
    when you need several channels, so the clock is decoded once.
    """

    def __init__(self, session: Session, name: str, group: str, occurrences: list):
        self.session = session
        self.name = name
        self.group = group
        self.occurrences = occurrences

    @property
    def path(self) -> str:
        return f"{self.group}/{self.name}"

    @property
    def properties(self) -> dict:
        """Properties from the first file. See :attr:`properties_match`."""
        return dict(self.occurrences[0].get("properties") or {})

    @property
    def properties_match(self) -> bool:
        import json

        blobs = [
            json.dumps(item.get("properties") or {}, sort_keys=True, default=str)
            for item in self.occurrences
        ]
        return len(set(blobs)) <= 1

    @property
    def data_type(self):
        return self.occurrences[0].get("data_type")

    @property
    def daqmx(self) -> bool:
        """True when the stored samples are DAQmx raw data.

        The column is the primary scaler. ``data_type`` is that scaler's
        type. Digital lines also set :attr:`digital`.
        """
        return any(item.get("daqmx") for item in self.occurrences)

    @property
    def digital(self) -> bool:
        """True when the primary DAQmx scaler is a digital line of 0 and 1."""
        return any(item.get("digital") for item in self.occurrences)

    @property
    def unit(self):
        return self.occurrences[0].get("unit") or ""

    @property
    def scale(self):
        """The NI linear scale, or None. Presence is not application.

        npTDMS applies this kind of scale unless you opt out. gotdms records
        it here and applies it only when a read passes ``scaled=True``.
        """
        return self.occurrences[0].get("scale")

    @property
    def has_scale(self) -> bool:
        return any(item.get("has_scale") for item in self.occurrences)

    @property
    def has_nonlinear_scale(self) -> bool:
        return any(item.get("has_nonlinear_scale") for item in self.occurrences)

    @property
    def is_timestamp(self) -> bool:
        return any(item.get("is_timestamp") for item in self.occurrences)

    def data(self, **kwargs):
        """Samples from :meth:`Session.read`.

        Ordinary channels are ``float64``, or linear-scaled ``float64`` when
        ``scaled=True``. A DAQmx channel is the stored scaler type. Digital
        lines are ``uint8`` 0 and 1. ``scaled=True`` on a DAQmx channel is
        ``float64`` engineering units.
        """
        return self.session.read(self.name, self.group, **kwargs).y

    def time_track(self, absolute_time: bool = True, **kwargs):
        """See :meth:`gotdms.codec.Series.time_track`.

        ``absolute_time=False`` is basis-relative. It is not the npTDMS
        relative track, and this method does not require ``wf_start_time``.
        The time axis comes from the declared timestamp channel, or from
        waveform properties when the group has no declaration.
        """
        kwargs.pop("absolute", None)
        kwargs.pop("relative", None)
        series = self.session.read(
            self.name,
            self.group,
            absolute=bool(absolute_time),
            relative=not absolute_time,
            **kwargs,
        )
        return series.time_track(absolute_time)

    def as_polars(self, **kwargs):
        return self.session.as_polars(channels=[self], **kwargs)


def open(paths=None, *, binary=None, **kwargs) -> Session:
    """Open a dataset. Close it with :meth:`Session.close` or a ``with`` block.

    This is the eager-session entry. It corresponds to ``TdmsFile.read`` only
    after you call :meth:`Session.read_all` or :meth:`Session.as_polars`.
    :meth:`Session.open` itself is the metadata step, like
    ``TdmsFile.read_metadata``, except declared timestamp channels are read
    to compute the span.
    """
    session = Session(binary=binary)
    try:
        session.open(paths, **kwargs)
    except Exception:
        session.close()
        raise
    return session


def _index(i, n: int, what: str) -> int:
    if type(i) is not int or not 0 <= i < n:
        raise ProtocolError(f"{what} index {i!r} is out of range")
    return i


def _copy_props(props: dict) -> dict:
    # A timestamp property is a dict; copy it so no two channels, and no
    # catalog row and Series, share a mutable value.
    return {k: (dict(v) if type(v) is dict else v) for k, v in props.items()}


def _expand_catalog(cat: dict) -> dict:
    """Expand the bridge's compact catalog to one path string and one
    properties dict per row, the shape ``open`` and ``catalog`` return.

    On the wire, ``groups[].file`` and ``channels[].file`` index ``files``,
    and ``channels[].properties`` lists indexes into ``property_table``,
    which holds each distinct ``{"name", "value"}`` pair once.
    """
    try:
        files = cat["files"]
        table = cat.pop("property_table")
        paths = [f["path"] for f in files]
        pairs = [(e["name"], e["value"]) for e in table]
        nf, np_ = len(paths), len(pairs)
        for g in cat["groups"]:
            g["file"] = paths[_index(g["file"], nf, "group file")]
        for c in cat["channels"]:
            c["file"] = paths[_index(c["file"], nf, "channel file")]
            ids = c.get("properties")
            if ids is None:
                continue
            props = {}
            for i in ids:
                if type(i) is not int or not 0 <= i < np_:
                    _index(i, np_, "property")
                k, v = pairs[i]
                props[k] = dict(v) if type(v) is dict else v
            c["properties"] = props
    except (KeyError, TypeError, AttributeError) as exc:
        raise ProtocolError(f"malformed catalog: {exc!r}") from None
    return cat


def _resolve_read_header(header: dict, catalog) -> None:
    """Point a read header's catalog references back at paths and
    properties, so ``Series.meta`` and ``reads`` look as they always did.

    A read header's channel ``files`` index the catalog's ``files``, and
    ``properties`` is the index of the catalog channel whose properties
    apply. ``reads[].file`` indexes ``files`` too.
    """
    if catalog is None:
        raise ProtocolError("read reply before a catalog; open a dataset first")
    try:
        paths = [f["path"] for f in catalog["files"]]
        rows = catalog["channels"]
        nf, nr = len(paths), len(rows)
        for m in header["channels"]:
            fl = m.get("files")
            if fl is not None:
                m["files"] = [paths[_index(i, nf, "read file")] for i in fl]
            at = m.get("properties")
            if at is None:
                m.pop("properties", None)
                continue
            row = rows[_index(at, nr, "properties")]
            if row["name"] != m["name"] or row["group"] != m["group"] or (m.get("files") and row["file"] not in m["files"]):
                raise ProtocolError(
                    f"read header properties for {m['group']}/{m['name']} point at "
                    f"{row['group']}/{row['name']} in {row['file']}, not a file of this read"
                )
            props = row.get("properties")
            # Replace in place so the key keeps its position; an empty
            # dict was omitted before, so drop the key then.
            if props:
                m["properties"] = _copy_props(props)
            else:
                del m["properties"]
        for r in header.get("reads") or ():
            r["file"] = paths[_index(r["file"], nf, "read file")]
    except (KeyError, TypeError, AttributeError) as exc:
        raise ProtocolError(f"malformed read header: {exc!r}") from None


def _dataset_header(
    op,
    paths=None,
    *,
    directory=None,
    pattern="*.tdms",
    timestamp_channel=None,
    timestamp_channels=None,
    numeric_epoch=None,
    basis=None,
    overlap=None,
    workers=None,
    read_workers=None,
):
    header = {"op": op}
    if paths:
        header["paths"] = [str(p) for p in paths]
    if directory:
        header["directory"] = str(directory)
        header["pattern"] = pattern or "*.tdms"
    if timestamp_channel is not None:
        header["timestamp_channel"] = timestamp_channel
    if timestamp_channels:
        header["timestamp_channels"] = dict(timestamp_channels)
    if numeric_epoch is not None:
        header["numeric_epoch"] = _rfc3339(numeric_epoch)
    if basis is not None:
        header["basis"] = _rfc3339(basis)
    if overlap is not None:
        header["overlap"] = overlap
    if workers:
        header["workers"] = int(workers)
    if read_workers:
        header["read_workers"] = int(read_workers)
    return header


def _rfc3339(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    raise ArgumentError("time must be a datetime or an RFC3339 string")


def _raise_protocol(decoded: dict):
    code = decoded.get("code") or "error"
    message = decoded.get("message") or code
    if code == "ambiguous":
        raise AmbiguousChannelError(message, decoded.get("candidates") or [])
    cls = _CODE_TYPES.get(code, GotdmsError)
    raise cls(message)
