"""Frame codec and Arrow IPC decode for gotdms-bridge.

The interactive frame is little-endian:

    magic "GMDS" | version uint16 | flags uint16 | header_len uint32 | payload_len uint32
    header JSON | payload

A multi-channel payload is ``uint32 count`` then repeated ``uint32 len`` plus
one Arrow IPC stream. Sample columns are not JSON and not base64.

A payload is read once into one preallocated ``bytearray``. Each channel's
stream is a ``memoryview`` into it, and Arrow decodes through
``pyarrow.py_buffer`` without copying, so fixed-width columns are numpy views
of that buffer. A stream that would start off an 8-byte boundary is moved
in place first (see :func:`split_payload`), so those views are aligned. The
buffer lives as long as any array from the response.
"""

from __future__ import annotations

import json
import re
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


def read_exact(stream, n: int) -> bytearray:
    """Read exactly ``n`` bytes into one new ``bytearray``.

    ``readinto`` fills the preallocated buffer in place. A stream without
    ``readinto`` falls back to ``read`` and one copy per chunk.
    """
    buf = bytearray(n)
    view = memoryview(buf)
    got = 0
    readinto = getattr(stream, "readinto", None)
    while got < n:
        if readinto is not None:
            k = readinto(view[got:])
        else:
            chunk = stream.read(n - got)
            k = len(chunk) if chunk else 0
            view[got : got + k] = chunk or b""
        if not k:
            raise ProtocolError("bridge closed the stream")
        got += k
    view.release()
    return buf


def read_frame(stream):
    """Read one frame. The payload is a ``bytearray`` (empty when absent)."""
    magic, version, _flags, hlen, plen = _PREFIX.unpack(read_exact(stream, _PREFIX.size))
    if magic != MAGIC:
        raise ProtocolError(f"bad magic {magic!r}")
    header = read_exact(stream, hlen) if hlen else b""
    payload = read_exact(stream, plen) if plen else bytearray()
    if version != VERSION:
        raise ProtocolError(f"protocol version {version}, want {VERSION}")
    if not header:
        return {}, payload
    return json.loads(header), payload


#: Arrow IPC aligns every body buffer to 8 bytes from the start of its
#: stream, so a stream that starts on an 8-byte boundary decodes to aligned
#: columns.
_ALIGN = 8


def split_payload(payload) -> list[memoryview]:
    """Split the counted envelope into one read-only view per Arrow stream.

    The views share ``payload``. In the envelope each stream follows a 4-byte
    length, so with 8-byte-padded streams every other one starts 4 bytes off
    an 8-byte boundary. Arrow decodes such a stream to misaligned numpy and
    Polars buffers. When ``payload`` is a ``bytearray`` (what
    :func:`read_frame` returns), a misaligned stream is moved left in place
    over its own, already parsed, length prefix, so no memory is allocated.
    A stream that cannot move that way, or one in an immutable ``bytes``,
    is copied into its own aligned buffer.
    """
    if not payload:
        return []
    if len(payload) < 4:
        raise ProtocolError("short payload")
    (count,) = struct.unpack_from("<I", payload, 0)
    spans = []
    off = 4
    for _ in range(count):
        if off + 4 > len(payload):
            raise ProtocolError("short payload length")
        (ln,) = struct.unpack_from("<I", payload, off)
        off += 4
        end = off + ln
        if end > len(payload):
            raise ProtocolError("short payload body")
        spans.append((off, ln))
        off = end
    if off != len(payload):
        raise ProtocolError("payload trailing bytes")
    return _aligned_views(payload, spans)


def _aligned_views(payload, spans) -> list[memoryview]:
    import ctypes

    mutable = isinstance(payload, bytearray)
    backing = np.frombuffer(payload, dtype=np.uint8)
    base = backing.ctypes.data
    whole = memoryview(payload).toreadonly()
    parts = []
    free = 0  # first byte not used by an earlier stream
    for start, ln in spans:
        shift = (base + start) % _ALIGN
        if shift == 0 or ln == 0:
            parts.append(whole[start : start + ln])
            free = start + ln
            continue
        if mutable and start - shift >= free:
            ctypes.memmove(base + start - shift, base + start, ln)
            parts.append(whole[start - shift : start - shift + ln])
            free = start - shift + ln
            continue
        own = np.empty(ln + _ALIGN, dtype=np.uint8)
        pad = (-own.ctypes.data) % _ALIGN
        own = own[pad : pad + ln]
        own[:] = backing[start : start + ln]
        parts.append(memoryview(own).toreadonly())
        free = start + ln
    return parts


class Series:
    """One channel read. ``y`` is raw unless this read passed ``scaled=True``.

    ``relative_s`` is seconds since ``basis``. When the bridge did not send
    it, it is computed on first access from ``time_ns`` and ``basis`` with
    the same integer and float steps as Go's ``Series.RelativeTimes``, so
    the values are bit-for-bit what the bridge would have sent. An untimed
    series gives sample indexes, as in Go. It is ``None`` only for a timed
    series read with ``absolute=False, relative=False``.
    """

    def __init__(self, meta: dict, y: np.ndarray, time_ns, relative_s):
        self.meta = meta
        self.name = meta.get("name", "")
        self.group = meta.get("group", "")
        self.y = y
        self.time_ns = time_ns
        self._relative_s = relative_s
        self.timed = bool(meta.get("timed"))
        self.scaled = bool(meta.get("scaled"))
        self.basis = meta.get("basis") or None
        self.samples = int(meta.get("samples", len(y)))

    @property
    def relative_s(self):
        if self._relative_s is None:
            self._relative_s = _relative_for(self)
        return self._relative_s

    @relative_s.setter
    def relative_s(self, value):
        self._relative_s = value

    def time_track(self, absolute_time: bool = True) -> np.ndarray:
        """Return the time axis for this series.

        ``absolute_time=True`` is ``numpy.datetime64[ns]`` UTC timestamps.
        ``absolute_time=False`` is seconds since the basis used for the read
        (negative before the basis). That is not npTDMS
        ``time_track(absolute_time=False)``, which is ``wf_start_offset`` plus
        the sample index times ``wf_increment`` and is zero at the first
        sample when the offset is zero. An untimed series returns sample
        indexes. A DAQmx channel uses the same waveform or timestamp axis as
        any other channel. There is no separate DAQmx time track.
        """
        if absolute_time:
            if self.time_ns is None:
                raise UntimedError(f"{self.group}/{self.name} has no absolute time")
            return self.time_ns.astype("datetime64[ns]")
        if self.relative_s is None:
            raise GotdmsError("relative time was not in this response")
        return self.relative_s


_NS_PER_S = 1_000_000_000
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1
_RFC3339_UTC = re.compile(
    r"^(-?\d{4,})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$"
)


def _days_from_civil(y: int, m: int, d: int) -> int:
    """Days since 1970-01-01 in the proleptic Gregorian calendar, any year."""
    y -= m <= 2
    # Python's // already floors, so no C-style adjustment for negative y.
    era = y // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def rfc3339_to_unix_ns(text: str) -> int:
    """Exact Unix nanoseconds of a UTC RFC3339Nano string from the bridge.

    The bridge formats ``basis`` with Go ``time.RFC3339Nano`` in UTC. The
    result is a Python int, so it is exact even outside the int64 range.
    """
    match = _RFC3339_UTC.match(text)
    if match is None:
        raise ProtocolError(f"basis {text!r} is not a UTC RFC3339 time")
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    frac = match.group(7) or ""
    days = _days_from_civil(year, month, day)
    seconds = days * 86400 + hour * 3600 + minute * 60 + second
    return seconds * _NS_PER_S + int(frac.ljust(9, "0") or 0)


def relative_seconds(time_ns, basis_ns: int) -> np.ndarray:
    """Seconds from ``basis_ns`` to each of ``time_ns``, as Go computes them.

    Go's ``RelativeTimes`` is ``t.Sub(basis).Seconds()``. ``Sub`` is an int64
    nanosecond difference that saturates at the int64 limits, and
    ``Duration.Seconds`` is ``float64(d / 1e9) + float64(d % 1e9) / 1e9`` with
    Go's truncating division. This repeats those steps, so each float64 is
    identical to the bridge's ``relative_s``.
    """
    t = np.asarray(time_ns, dtype=np.int64)
    out = np.empty(t.shape, dtype=np.float64)
    # Blocks keep the int64 temporaries small next to a long channel.
    for lo in range(0, t.size, _RELATIVE_BLOCK):
        hi = min(lo + _RELATIVE_BLOCK, t.size)
        out[lo:hi] = _relative_block(t[lo:hi], basis_ns)
    return out


_RELATIVE_BLOCK = 1 << 18


def _relative_block(t: np.ndarray, basis_ns: int) -> np.ndarray:
    if _INT64_MIN <= basis_ns <= _INT64_MAX:
        b = np.int64(basis_ns)
        d = t - b  # wraps on overflow; fixed below
        overflow = ((t ^ b) & (t ^ d)) < 0
        if overflow.any():
            d = np.where(overflow, np.where(t > b, _INT64_MAX, _INT64_MIN), d).astype(np.int64)
    else:
        # A basis outside int64 nanoseconds. Rare, so exact Python ints.
        exact = t.astype(object) - basis_ns
        d = np.array([min(max(v, _INT64_MIN), _INT64_MAX) for v in exact], dtype=np.int64)
    # Go truncates toward zero. numpy floors, so step back toward zero when
    # a negative d has a remainder.
    sec, nsec = np.divmod(d, _NS_PER_S)
    fix = (nsec != 0) & (d < 0)
    if fix.any():
        sec = sec + fix
        nsec = nsec - fix * _NS_PER_S
    return sec.astype(np.float64) + nsec.astype(np.float64) / 1e9


def _relative_for(series: "Series"):
    if not series.timed:
        return np.arange(len(series.y), dtype=np.float64)
    if series.time_ns is None or series.basis is None:
        return None
    return relative_seconds(series.time_ns, rfc3339_to_unix_ns(series.basis))


def decode_ipc(blob):
    """Decode one Arrow IPC stream from bytes, a bytearray, or a memoryview.

    The stream is wrapped with ``pyarrow.py_buffer``, not copied. Fixed-width
    columns without nulls come back as numpy views of ``blob``.
    """
    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc
    except ImportError as exc:
        raise GotdmsError(
            "pyarrow is required to decode Arrow IPC. It is a gotdms "
            "dependency; reinstall gotdms or run `pip install pyarrow`"
        ) from exc
    reader = ipc.open_stream(pa.py_buffer(blob))
    table = reader.read_all()
    names = set(table.column_names)
    y_col = table.column("y")
    # DAQmx columns keep the scaler's stored type. Digital lines are uint8.
    # Ordinary dataset reads are float64. A timestamp scaler is UTC nanoseconds.
    if pa.types.is_timestamp(y_col.type):
        y = np.asarray(y_col.to_numpy(zero_copy_only=False), dtype="datetime64[ns]")
    else:
        y = np.asarray(y_col.to_numpy(zero_copy_only=False))
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
        raise GotdmsError(
            "polars is required for as_polars. It is a gotdms dependency; "
            "reinstall gotdms or run `pip install polars`"
        ) from exc
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
        frame = pl.DataFrame([_value_series(names[i], series[i].y) for i in range(len(series))])
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
                    _value_series(name, item.y),
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


def _value_series(name, values):
    """Polars series for one channel. The numpy dtype is kept.

    Dataset reads of ordinary channels are float64. DAQmx reads are the
    stored scaler type, and digital lines are uint8. ``scaled=True`` is
    float64 because the scale result is engineering units.
    """
    import polars as pl

    arr = np.asarray(values)
    if np.issubdtype(arr.dtype, np.datetime64):
        return pl.Series(name, arr).cast(pl.Datetime("ns", "UTC"))
    return pl.Series(name, arr)


def _time_series(values, name: str, absolute: bool):
    import polars as pl

    if absolute:
        return pl.Series(name, np.asarray(values)).cast(pl.Datetime("ns", "UTC"))
    return pl.Series(name, np.asarray(values, dtype=np.float64))
