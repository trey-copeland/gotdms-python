"""Frame codec and Arrow IPC decode for gotdms-bridge.

The interactive frame is little-endian:

    magic "GMDS" | version uint16 | flags uint16 | header_len uint32 | payload_len uint32
    header JSON | payload

A multi-channel payload is ``uint32 count`` then repeated ``uint32 len`` plus
one Arrow IPC stream. Sample columns are not JSON and not base64.
"""

from __future__ import annotations

import io
import json
import struct

import numpy as np

from .errors import AlignmentError, ArgumentError, GotdmsError, ProtocolError, UntimedError

MAGIC = b"GMDS"
VERSION = 1
_PREFIX = struct.Struct("<4sHHII")


def write_frame(stream, header: dict, payload: bytes | None = None) -> None:
    """Write one protocol frame. ``payload`` is raw bytes, not JSON.

    Read responses and ``read_values`` use the counted multi-channel
    envelope inside the payload. ``write_chunk`` puts one Arrow IPC stream
    in the payload directly.
    """
    body = json.dumps(header, separators=(",", ":")).encode()
    payload = payload or b""
    if len(payload) > 0xFFFFFFFF:
        raise ArgumentError("Arrow chunk exceeds the frame payload limit (4 GiB)")
    stream.write(_PREFIX.pack(MAGIC, VERSION, 0, len(body), len(payload)))
    if body:
        stream.write(body)
    if payload:
        stream.write(payload)
    stream.flush()


def read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise ProtocolError("bridge closed the stream")
        buf += chunk
    return bytes(buf)


def read_frame(stream):
    magic, version, _flags, hlen, plen = _PREFIX.unpack(read_exact(stream, _PREFIX.size))
    if magic != MAGIC:
        raise ProtocolError(f"bad magic {magic!r}")
    header = read_exact(stream, hlen) if hlen else b""
    payload = read_exact(stream, plen) if plen else b""
    if version != VERSION:
        raise ProtocolError(f"protocol version {version}, want {VERSION}")
    if not header:
        return {}, payload
    return json.loads(header), payload


def split_payload(payload: bytes) -> list[bytes]:
    if not payload:
        return []
    if len(payload) < 4:
        raise ProtocolError("short payload")
    (count,) = struct.unpack_from("<I", payload, 0)
    off = 4
    parts = []
    for _ in range(count):
        if off + 4 > len(payload):
            raise ProtocolError("short payload length")
        (ln,) = struct.unpack_from("<I", payload, off)
        off += 4
        end = off + ln
        if end > len(payload):
            raise ProtocolError("short payload body")
        parts.append(payload[off:end])
        off = end
    if off != len(payload):
        raise ProtocolError("payload trailing bytes")
    return parts


class Series:
    """One channel read. ``y`` is raw unless this read passed ``scaled=True``."""

    def __init__(self, meta: dict, y: np.ndarray, time_ns, relative_s):
        self.meta = meta
        self.name = meta.get("name", "")
        self.group = meta.get("group", "")
        self.y = y
        self.time_ns = time_ns
        self.relative_s = relative_s
        self.timed = bool(meta.get("timed"))
        self.scaled = bool(meta.get("scaled"))
        self.basis = meta.get("basis") or None
        self.samples = int(meta.get("samples", len(y)))

    def time_track(self, absolute_time: bool = True) -> np.ndarray:
        """Return the time axis for this series.

        ``absolute_time=True`` is ``numpy.datetime64[ns]`` UTC timestamps.
        ``absolute_time=False`` is seconds since the basis used for the read
        (negative before the basis). That is not npTDMS
        ``time_track(absolute_time=False)``, which is ``wf_start_offset`` plus
        the sample index times ``wf_increment`` and is zero at the first
        sample when the offset is zero. An untimed series returns sample
        indexes. gotdms does not build a DAQmx time track.
        """
        if absolute_time:
            if self.time_ns is None:
                raise UntimedError(f"{self.group}/{self.name} has no absolute time")
            return self.time_ns.astype("datetime64[ns]")
        if self.relative_s is None:
            raise GotdmsError("relative time was not in this response")
        return self.relative_s


def decode_ipc(blob: bytes):
    try:
        import pyarrow.ipc as ipc
    except ImportError as exc:
        raise GotdmsError(
            "pyarrow is required to decode Arrow IPC; install gotdms[arrow]"
        ) from exc
    reader = ipc.open_stream(io.BytesIO(blob))
    table = reader.read_all()
    names = set(table.column_names)
    y = np.asarray(table.column("y").to_numpy(zero_copy_only=False), dtype=np.float64)
    time_ns = None
    if "time_ns" in names:
        time_ns = np.asarray(table.column("time_ns").to_numpy(zero_copy_only=False), dtype=np.int64)
    relative = None
    if "relative_s" in names:
        relative = np.asarray(table.column("relative_s").to_numpy(zero_copy_only=False), dtype=np.float64)
    return y, time_ns, relative


def series_from_parts(metas: list, parts: list[bytes]) -> list[Series]:
    if len(metas) != len(parts):
        raise ProtocolError(f"{len(metas)} channel headers and {len(parts)} payloads")
    out = []
    for meta, blob in zip(metas, parts):
        y, time_ns, relative = decode_ipc(blob)
        out.append(Series(meta, y, time_ns, relative))
    return out


def as_polars(series: list[Series], *, time: str = "column", absolute_time: bool = True, align: str = "strict"):
    """Build one Polars DataFrame from series that were returned together.

    Polars has no row index. npTDMS ``as_dataframe(time_index=True)`` puts
    time in a ``DatetimeIndex``. The same idea here is a real column:
    ``time`` (``Datetime(ns, UTC)``) when ``absolute_time`` is true, or
    ``relative_s`` (seconds since the basis) when it is false. ``time="column"``
    is that column. ``time="none"`` omits it. ``time="index"`` is rejected.

    ``absolute_time=False`` is basis-relative seconds, not npTDMS's
    zero-at-first-sample axis.

    ``align="strict"`` requires identical time vectors and does not insert
    nulls. ``align="nptdms"`` is an explicit full outer join on that time
    column (``how="full", coalesce=True``), which is how npTDMS lined channels
    up on an index and filled gaps with NaN. Polars inserts null, not NaN.
    The joined frame is sorted by the time column. Duplicate timestamps in
    one channel raise AlignmentError instead of repeating rows.

    Timestamps that are close but not equal do not match. That is a
    ``DataFrame.join_asof`` on the ``time`` column, which this helper does
    not do. Columns are ``group/name``. Scaling happens on the read, not here.
    """
    try:
        import polars as pl
    except ImportError as exc:
        raise GotdmsError("polars is required for as_polars; install gotdms[polars]") from exc
    if time == "index":
        raise ArgumentError(
            "Polars has no row index. npTDMS time_index=True is the 'time' column "
            "from time='column', or 'relative_s' when absolute_time is false"
        )
    if time not in ("column", "none"):
        raise ArgumentError("time must be column or none")
    if align not in ("strict", "nptdms"):
        raise ArgumentError("align must be strict or nptdms")
    if not series:
        raise ArgumentError("no channels")
    keys = []
    for item in series:
        if absolute_time:
            if item.time_ns is None:
                raise UntimedError(f"{item.group}/{item.name} has no absolute time")
            keys.append(np.asarray(item.time_ns))
        else:
            if item.relative_s is None:
                raise GotdmsError(f"{item.group}/{item.name} has no relative time")
            keys.append(np.asarray(item.relative_s, dtype=np.float64))
    names = [f"{item.group}/{item.name}" for item in series]
    key_name = "time" if absolute_time else "relative_s"
    if align == "strict":
        for key in keys[1:]:
            if not np.array_equal(keys[0], key):
                raise AlignmentError(
                    "channels do not share a time vector; pass align='nptdms' for a full join"
                )
        frame = pl.DataFrame({names[i]: np.asarray(series[i].y, dtype=np.float64) for i in range(len(series))})
        if time == "none":
            return frame
        return frame.insert_column(0, _time_series(keys[0], key_name, absolute_time))
    frames = []
    for item, key, name in zip(series, keys, names):
        if len(np.unique(key)) != len(key):
            raise AlignmentError(f"{name} has duplicate timestamps")
        frames.append(
            pl.DataFrame(
                [
                    _time_series(key, key_name, absolute_time),
                    pl.Series(name, np.asarray(item.y, dtype=np.float64)),
                ]
            )
        )
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.join(frame, on=key_name, how="full", coalesce=True)
    merged = merged.sort(key_name)
    if time == "none":
        return merged.drop(key_name)
    return merged


def _time_series(values, name: str, absolute: bool):
    import polars as pl

    if absolute:
        return pl.Series(name, np.asarray(values)).cast(pl.Datetime("ns", "UTC"))
    return pl.Series(name, np.asarray(values, dtype=np.float64))
