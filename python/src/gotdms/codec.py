"""Frame codec and Arrow IPC decode for gotdms-bridge (protocol version 2).

Every frame is little-endian:

    magic "GMDS" | version uint16 | flags uint16 | header_len uint32 | payload_len uint32
    header JSON | payload

Metadata replies and errors are one frame. A data reply (``read``,
``read_many``, ``read_values``) is a sequence of frames:

* ``result_begin``: the channel list, the clock table, and ``streams``, the
  exact byte size of every Arrow IPC stream that follows. No payload.
* ``part``: ``{"op": "part", "stream": k}`` and the next bytes of stream
  ``k`` (at most 64 MiB per frame). A stream may span many parts.
* ``result_end``: ``{"op": "result_end", "streams": n, "bytes": total}``, or
  ``"error": {"code", "message"}`` when the bridge failed after
  ``result_begin``. The session is still in sync after that error.

No frame is near the 4 GiB ``payload_len`` limit, so a reply has no size
ceiling. Each stream gets one preallocated, 8-byte-aligned buffer, filled in
place with ``readinto``; Arrow decodes it through ``pyarrow.py_buffer``
without copying, so fixed-width columns are aligned numpy views of that
buffer. Each buffer lives as long as an array from its stream.

Each stream has one schema and ``batches`` record batches. Channels and
clocks are columns: each names its ``stream``, ``batch``, and ``column``
(see :class:`ReplyColumns`), so many short channels share one stream and
one ``open_stream``. A clock column is ``time_ns`` int64 and is decoded
once. Channels with the same clock id get the same ``time_ns`` array object.
"""

from __future__ import annotations

import io
import json
import re
import struct

import numpy as np

from .errors import AlignmentError, ArgumentError, GotdmsError, ProtocolError, UntimedError

MAGIC = b"GMDS"
VERSION = 2
_PREFIX = struct.Struct("<4sHHII")
#: The largest part payload a bridge sends. A bigger part is a protocol
#: error, so a corrupt length cannot make the client allocate gigabytes.
MAX_PART = 64 << 20
MAX_HEADER = 32 << 20

#: Arrow IPC aligns every body buffer to 8 bytes from the start of its
#: stream, so a stream buffer that starts on an 8-byte boundary decodes to
#: aligned columns.
_ALIGN = 8


def _waiter(stream, events):
    """A function that blocks until ``stream``'s descriptor is ready.

    Polars installs a SIGINT handler with ``SA_RESTART``, so once it is
    imported the kernel restarts a blocked pipe ``read`` or ``write`` after
    Ctrl+C, and Python sees ``KeyboardInterrupt`` only when the bridge next
    moves bytes. ``poll`` is never restarted after a signal handler, so
    waiting in ``poll`` first keeps a read or write on the bridge pipe
    interruptible. Only an unbuffered ``FileIO`` is waited on: a buffered
    stream can hold bytes its descriptor no longer shows. Returns ``None`` on
    Windows (no ``poll`` on pipes) and for any other stream.
    """
    if not _POLLIN or not isinstance(stream, io.FileIO):
        return None
    try:
        poller = _select.poll()
        poller.register(stream.fileno(), events)
    except (OSError, ValueError):
        return None
    return poller.poll


try:
    import select as _select

    _POLLIN = _select.POLLIN
    _POLLOUT = _select.POLLOUT
except (ImportError, AttributeError):  # Windows has no select.poll
    _POLLIN = _POLLOUT = 0


def write_all(stream, data) -> None:
    """Write every byte of ``data``.

    The bridge pipes are unbuffered ``FileIO`` objects (``bufsize=0``).
    ``FileIO.write`` may write fewer bytes than asked, and on Windows one
    call is capped near 2 GiB, so a single ``write`` is not enough.
    """
    view = memoryview(data).cast("B")
    wait = _waiter(stream, _POLLOUT) if len(view) else None
    while len(view):
        if wait is not None:
            wait()
        n = stream.write(view)
        if n is None:
            raise ProtocolError("bridge pipe would block")
        if n <= 0:
            raise ProtocolError("bridge closed the stream")
        view = view[n:]


def write_frame(stream, header: dict, payload: bytes | None = None) -> None:
    """Write one protocol frame. ``payload`` is raw bytes, not JSON.

    ``write_chunk`` puts one Arrow IPC stream in the payload. Every other
    request has no payload.
    """
    body = json.dumps(header, separators=(",", ":")).encode()
    payload = payload or b""
    if len(payload) > 0xFFFFFFFF:
        raise ArgumentError("Arrow chunk exceeds the frame payload limit (4 GiB)")
    write_all(stream, _PREFIX.pack(MAGIC, VERSION, 0, len(body), len(payload)) + body)
    if payload:
        write_all(stream, payload)
    stream.flush()


def readinto_exact(stream, view: memoryview) -> None:
    """Fill ``view`` from ``stream``.

    ``readinto`` may return fewer bytes than asked (a pipe returns what is
    there, and Windows caps one call near 2 GiB), so this loops. A stream
    without ``readinto`` falls back to ``read`` and one copy per chunk.
    """
    n = len(view)
    got = 0
    readinto = getattr(stream, "readinto", None)
    wait = _waiter(stream, _POLLIN) if n else None
    while got < n:
        if wait is not None:
            wait()
        if readinto is not None:
            k = readinto(view[got:])
        else:
            chunk = stream.read(n - got)
            k = len(chunk) if chunk else 0
            view[got : got + k] = chunk or b""
        if not k:
            raise ProtocolError("bridge closed the stream")
        got += k


def read_exact(stream, n: int) -> bytearray:
    """Read exactly ``n`` bytes into one new ``bytearray``."""
    buf = bytearray(n)
    with memoryview(buf) as view:
        readinto_exact(stream, view)
    return buf


def read_head(stream):
    """Read one frame prefix and header. Returns ``(header, payload_len)``.

    The caller reads the payload, so a part can go straight into its
    stream's buffer.
    """
    magic, version, _flags, hlen, plen = _PREFIX.unpack(read_exact(stream, _PREFIX.size))
    if magic != MAGIC:
        raise ProtocolError(f"bad magic {magic!r}")
    if hlen > MAX_HEADER:
        raise ProtocolError(f"header length {hlen} exceeds {MAX_HEADER}")
    raw = read_exact(stream, hlen) if hlen else b""
    if version != VERSION:
        # Skip the payload so the pipe stays in sync, then refuse.
        if plen:
            read_exact(stream, plen)
        raise ProtocolError(
            f"gotdms-bridge speaks protocol version {version}, this client needs {VERSION}. "
            "Rebuild gotdms-bridge or unset GOTDMS_BRIDGE to use the bundled one."
        )
    return (json.loads(raw) if raw else {}), plen


def read_frame(stream):
    """Read one frame. The payload is a ``bytearray`` (empty when absent)."""
    header, plen = read_head(stream)
    payload = read_exact(stream, plen) if plen else bytearray()
    return header, payload


def aligned_buffer(n: int) -> np.ndarray:
    """A writable ``uint8`` array of ``n`` bytes whose first byte is 8-aligned."""
    raw = np.empty(n + _ALIGN, dtype=np.uint8)
    pad = (-raw.ctypes.data) % _ALIGN
    return raw[pad : pad + n]


class Reply:
    """One reply. ``header`` is the single frame, or ``result_begin`` of a
    data reply. ``streams`` is ``None`` for a single frame, else one
    read-only ``memoryview`` per stream id. ``error`` is the ``result_end``
    error, if any; its streams are partial and must not be decoded."""

    __slots__ = ("header", "payload", "streams", "end", "error")

    def __init__(self, header, payload=b"", streams=None, end=None, error=None):
        self.header = header
        self.payload = payload
        self.streams = streams
        self.end = end
        self.error = error


def _is_int(value) -> bool:
    # JSON true is a Python bool, which is an int; it is not a size or an id.
    return isinstance(value, int) and not isinstance(value, bool)


def read_reply(stream) -> Reply:
    """Read one reply: a single frame, or a whole data reply.

    A framing fault raises :class:`ProtocolError` and leaves the pipe out of
    sync; the caller must discard the bridge. A ``result_end`` error is
    returned in :attr:`Reply.error` with the pipe still in sync.

    Streams arrive in id order, each filled to exactly its declared size
    before the next one starts. No frame payload is over :data:`MAX_PART`,
    so a corrupt length cannot make the client allocate or wait for gigabytes
    outside a declared stream.
    """
    header, plen = read_head(stream)
    if header.get("op") != "result_begin":
        if plen > MAX_PART:
            raise ProtocolError(f"{header.get('op')!r} frame with a {plen}-byte payload")
        payload = read_exact(stream, plen) if plen else bytearray()
        return Reply(header, payload)
    if plen:
        raise ProtocolError("result_begin with a payload")
    specs = header.get("streams")
    if not isinstance(specs, list):
        raise ProtocolError("result_begin without a stream list")
    sizes = []
    for i, spec in enumerate(specs):
        if not isinstance(spec, dict) or spec.get("id") != i or not _is_int(spec.get("id")):
            raise ProtocolError(f"stream {i} is {spec!r}")
        size = spec.get("bytes")
        if not _is_int(size) or size < 0:
            raise ProtocolError(f"stream {i} has size {size!r}")
        sizes.append(size)
    bufs = [aligned_buffer(size) for size in sizes]
    filled = [0] * len(bufs)
    cur = 0  # the stream being filled; every earlier one is complete
    while True:
        part, plen = read_head(stream)
        op = part.get("op")
        if op == "part":
            k = part.get("stream")
            if not _is_int(k) or not 0 <= k < len(bufs):
                raise ProtocolError(f"part for unknown stream {k!r}")
            if plen == 0 or plen > MAX_PART:
                raise ProtocolError(f"part of {plen} bytes")
            if k < cur:
                raise ProtocolError(f"part for stream {k} after stream {cur} started")
            while cur < k:
                if filled[cur] != sizes[cur]:
                    raise ProtocolError(f"stream {k} started with stream {cur} at {filled[cur]} of {sizes[cur]} bytes")
                cur += 1
            off = filled[k]
            if off + plen > sizes[k]:
                raise ProtocolError(f"stream {k} overflows its {sizes[k]} bytes")
            with memoryview(bufs[k]) as whole:
                readinto_exact(stream, whole[off : off + plen])
            filled[k] = off + plen
        elif op == "result_end":
            if plen:
                raise ProtocolError("result_end with a payload")
            err = part.get("error")
            if err:
                if not isinstance(err, dict):
                    raise ProtocolError(f"result_end error {err!r}")
                return Reply(header, b"", None, part, err)
            for k, buf in enumerate(bufs):
                if filled[k] != sizes[k]:
                    raise ProtocolError(f"stream {k} has {filled[k]} of {sizes[k]} bytes")
            if part.get("streams") != len(bufs) or part.get("bytes") != sum(sizes):
                raise ProtocolError(
                    f"result_end says {part.get('streams')!r} streams, {part.get('bytes')!r} bytes; "
                    f"result_begin declared {len(bufs)} and {sum(sizes)}"
                )
            return Reply(header, b"", [memoryview(b).toreadonly() for b in bufs], part)
        else:
            raise ProtocolError(f"unexpected {op!r} frame inside a data reply")


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


def numpy_view(col) -> np.ndarray:
    """A read-only numpy view of one Arrow array's values, without a copy.

    Fixed-width integer, float, and nanosecond timestamp arrays without nulls
    are viewed straight from their data buffer (``np.frombuffer``), which is
    much cheaper per column than ``to_numpy`` on older pyarrow. Anything else
    goes through ``to_numpy``. A timestamp array is ``datetime64[ns]``.
    """
    import pyarrow as pa

    t = col.type
    dtype = None
    if col.null_count == 0:
        if pa.types.is_timestamp(t) and t.unit == "ns":
            dtype = np.dtype("datetime64[ns]")
        elif pa.types.is_integer(t):
            # Not DataType.to_pandas_dtype: pyarrow 14 imports pandas there.
            dtype = np.dtype(f"{'i' if pa.types.is_signed_integer(t) else 'u'}{t.bit_width // 8}")
        elif pa.types.is_floating(t):
            dtype = np.dtype(f"f{t.bit_width // 8}")
    if dtype is None:
        out = np.asarray(col.to_numpy(zero_copy_only=False))
        if pa.types.is_timestamp(t):
            out = out.astype("datetime64[ns]")
        return out
    buf = col.buffers()[1]
    if buf is None or len(col) == 0:
        out = np.empty(0, dtype=dtype)
        out.flags.writeable = False
        return out
    return np.frombuffer(buf, dtype=dtype, count=len(col), offset=col.offset * dtype.itemsize)


class ReplyColumns:
    """The Arrow streams of one data reply, decoded on first use.

    Protocol 2 packs many channels into one stream: each stream has one
    schema and ``batches`` record batches, and a channel or clock names its
    ``stream``, ``batch``, and ``column``. Each stream is opened once, so
    reading N short channels costs a few ``open_stream`` calls instead of N.
    Every reference is checked: the stream kind, the batch and column
    bounds, the column name, and that no column is claimed twice. After the
    last :meth:`take`, :meth:`finish` checks that every column of every
    stream was claimed, so a reply cannot carry columns nobody reads.
    """

    def __init__(self, specs, blobs):
        if not isinstance(specs, list) or len(specs) != len(blobs):
            raise ProtocolError("result_begin stream list does not match the streams read")
        for sid, spec in enumerate(specs):
            if not isinstance(spec, dict):
                raise ProtocolError(f"result_begin stream {sid} is {spec!r}")
            if "batches" not in spec:
                raise ProtocolError(
                    f"result_begin stream {sid} does not declare its batches; the bridge predates packed streams, rebuild gotdms-bridge"
                )
        self._specs = specs
        self._blobs = blobs
        self._batches = [None] * len(blobs)
        self._names = [None] * len(blobs)
        self._used = set()

    def batches(self, sid: int):
        got = self._batches[sid]
        if got is None:
            import pyarrow as pa
            import pyarrow.ipc as ipc

            want = self._specs[sid].get("batches")
            if not _is_int(want) or want < 1:
                raise ProtocolError(f"stream {sid} declares {want!r} batches")
            try:
                got = list(ipc.open_stream(pa.py_buffer(self._blobs[sid])))
            except (pa.ArrowException, OSError) as exc:
                raise ProtocolError(f"stream {sid} is not a valid Arrow IPC stream: {exc}") from exc
            if len(got) != want:
                raise ProtocolError(f"stream {sid} has {len(got)} batches, declared {want}")
            # Every batch of a stream has the stream's schema. Its names are
            # read once: RecordBatch.schema builds a new object per call.
            self._names[sid] = got[0].schema.names
            self._batches[sid] = got
        return got

    def take(self, what: str, sid, batch, column, base: str, kind: str):
        """The Arrow array at ``(sid, batch, column)``, named ``base`` or
        ``base_<n>``, in a stream of ``kind``."""
        for name, value in (("stream", sid), ("batch", batch), ("column", column)):
            if not _is_int(value) or value < 0:
                raise ProtocolError(f"{what} has {name} {value!r}")
        if sid >= len(self._blobs):
            raise ProtocolError(f"{what} names stream {sid} of {len(self._blobs)}")
        if self._specs[sid].get("kind") != kind:
            raise ProtocolError(f"{what} names {self._specs[sid].get('kind')!r} stream {sid}, want {kind!r}")
        batches = self.batches(sid)
        if batch >= len(batches):
            raise ProtocolError(f"{what} names batch {batch} of stream {sid}, which has {len(batches)}")
        rb = batches[batch]
        if column >= rb.num_columns:
            raise ProtocolError(f"{what} names column {column} of a {rb.num_columns}-column batch")
        name = self._names[sid][column]
        if name != base and not (name.startswith(base + "_") and name[len(base) + 1 :].isdigit()):
            raise ProtocolError(f"{what} names column {name!r}, want {base!r}")
        key = (sid, batch, column)
        if key in self._used:
            raise ProtocolError(f"{what} names column {column} of stream {sid} batch {batch}, already taken")
        self._used.add(key)
        return rb.column(column)

    def finish(self) -> None:
        total = 0
        for sid in range(len(self._blobs)):
            total += sum(rb.num_columns for rb in self.batches(sid))
        if total != len(self._used):
            raise ProtocolError(f"reply has {total} columns and {len(self._used)} were named")


def series_from_reply(header: dict, streams: list) -> list[Series]:
    """Build one :class:`Series` per channel of a ``result_begin`` header.

    Each stream is decoded once and each clock once. Channels with the same
    clock id share that ``time_ns`` array object. ``y`` is a read-only view
    of its stream's buffer, so a channel keeps alive the stream it was packed
    in (at most 16 MiB of other channels; a larger channel has a stream of
    its own).
    """
    import pyarrow as pa

    cols = ReplyColumns(header.get("streams"), streams)
    clocks = {}
    for clock in header.get("clocks") or []:
        if not isinstance(clock, dict):
            raise ProtocolError(f"clock entry {clock!r}")
        cid = clock.get("id")
        what = f"clock {cid!r}"
        arr = cols.take(what, clock.get("stream"), clock.get("batch"), clock.get("column"), "time_ns", "clock")
        if not pa.types.is_int64(arr.type):
            raise ProtocolError(f"{what} is {arr.type}, want int64")
        if len(arr) != clock.get("samples", -1):
            raise ProtocolError(f"clock {cid} has {len(arr)} samples")
        if cid in clocks:
            raise ProtocolError(f"clock {cid!r} appears twice")
        clocks[cid] = numpy_view(arr)
    out = []
    for meta in header.get("channels") or []:
        what = f"channel {meta.get('name')!r}"
        sid, batch = meta.get("stream"), meta.get("batch")
        y = numpy_view(cols.take(what, sid, batch, meta.get("column"), "y", "channel"))
        relative = None
        rc = meta.get("relative_column")
        if rc is not None:
            arr = cols.take(what, sid, batch, rc, "relative_s", "channel")
            if not pa.types.is_float64(arr.type):
                raise ProtocolError(f"{what} relative_s is {arr.type}, want double")
            relative = numpy_view(arr)
        elif "relative_s" in (meta.get("columns") or []):
            raise ProtocolError(f"{what} lists relative_s without a relative_column")
        if "samples" in meta and len(y) != meta["samples"]:
            raise ProtocolError(f"{what} has {len(y)} samples, header says {meta['samples']!r}")
        time_ns = None
        clock = meta.get("clock")
        if clock is not None:
            if clock not in clocks:
                raise ProtocolError(f"{what} names unknown clock {clock!r}")
            time_ns = clocks[clock]
            if len(time_ns) != len(y):
                raise ProtocolError(f"{what} has {len(y)} samples and clock {clock} {len(time_ns)}")
        out.append(Series(meta, y, time_ns, relative))
    cols.finish()
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
            keys.append(item.time_ns if isinstance(item.time_ns, np.ndarray) else np.asarray(item.time_ns))
        else:
            if item.relative_s is None:
                raise GotdmsError(f"{item.group}/{item.name} has no relative time")
            keys.append(np.asarray(item.relative_s, dtype=np.float64))
    names = [f"{item.group}/{item.name}" for item in series]
    key_name = "time" if absolute_time else "relative_s"
    if align == "strict":
        for key in keys[1:]:
            # Channels on one bridge clock hold the same array object.
            if key is not keys[0] and not np.array_equal(keys[0], key):
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
