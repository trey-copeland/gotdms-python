"""Relative time defaults, Python/Go equivalence, and zero-copy decode.

``relative`` defaults to False. ``Series.relative_s`` is computed in Python
from ``time_ns`` and ``basis``. These tests compare it bit for bit with the
``relative_s`` column the bridge computes with Go's ``RelativeTimes``.
"""

import io
import json
import os
import random
import struct
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))

try:
    import polars as pl
except ImportError:  # pragma: no cover - skipped only when polars is absent
    pl = None

_MIN = -(1 << 63)
_MAX = (1 << 63) - 1


def go_seconds(t: int, basis: int) -> float:
    """Reference for Go ``t.Sub(basis).Seconds()`` with Python ints."""
    d = min(max(t - basis, _MIN), _MAX)
    sec = abs(d) // 1_000_000_000 * (1 if d >= 0 else -1)
    nsec = d - sec * 1_000_000_000
    return float(sec) + float(nsec) / 1e9


def bits(a):
    return np.asarray(a, dtype=np.float64).view(np.int64)


class RelativeMathTest(unittest.TestCase):
    def test_matches_go_reference(self):
        from gotdms.codec import relative_seconds

        rng = random.Random(7)
        specials = [0, 1, -1, 999_999_999, -999_999_999, 1_000_000_000, -1_000_000_001, _MIN, _MAX, _MIN + 1, _MAX - 1]
        times = specials + [rng.randint(_MIN, _MAX) for _ in range(2000)]
        times += [rng.randint(-10**15, 10**15) for _ in range(2000)]
        bases = specials + [rng.randint(_MIN, _MAX) for _ in range(20)] + [10**18 * 9, -(10**18) * 9]
        # Bases outside int64 take the exact-int path.
        bases += [_MAX + 5, _MIN - 5, -(2**70)]
        arr = np.array(times, dtype=np.int64)
        for basis in bases:
            with self.subTest(basis=basis):
                want = np.array([go_seconds(t, basis) for t in times])
                got = relative_seconds(arr, basis)
                self.assertTrue(np.array_equal(bits(got), bits(want)))

    def test_blocks_match_one_pass(self):
        from gotdms import codec

        rng = np.random.default_rng(11)
        n = 2 * codec._RELATIVE_BLOCK + 5
        t = rng.integers(-(10**18), 10**18, size=n, dtype=np.int64)
        basis = 1_700_000_000_123_456_789
        got = codec.relative_seconds(t, basis)
        self.assertTrue(np.array_equal(bits(got), bits(codec._relative_block(t, basis))))
        for i in (0, codec._RELATIVE_BLOCK - 1, codec._RELATIVE_BLOCK, n - 1):
            self.assertEqual(bits(got[i]), bits(go_seconds(int(t[i]), basis)))

    def test_rfc3339_parse(self):
        from gotdms.codec import rfc3339_to_unix_ns

        self.assertEqual(rfc3339_to_unix_ns("1970-01-01T00:00:00Z"), 0)
        self.assertEqual(rfc3339_to_unix_ns("1970-01-01T00:00:00.000000001Z"), 1)
        self.assertEqual(rfc3339_to_unix_ns("1969-12-31T23:59:59.5Z"), -500_000_000)
        self.assertEqual(rfc3339_to_unix_ns("1904-01-01T00:00:00Z"), -2_082_844_800 * 10**9)
        self.assertEqual(rfc3339_to_unix_ns("0000-01-01T00:00:00Z"), -62_167_219_200 * 10**9)
        rng = random.Random(3)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        for _ in range(500):
            when = datetime(rng.randint(1, 9999), rng.randint(1, 12), rng.randint(1, 28),
                            rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59), tzinfo=timezone.utc)
            frac = rng.randint(0, 999_999_999)
            text = f"{when.year:04d}-" + when.strftime("%m-%dT%H:%M:%S") + (f".{frac:09d}".rstrip("0") if frac else "") + "Z"
            whole = (when - epoch) // timedelta(seconds=1)
            self.assertEqual(rfc3339_to_unix_ns(text), whole * 10**9 + frac, text)
        from gotdms import ProtocolError

        with self.assertRaises(ProtocolError):
            rfc3339_to_unix_ns("2024-01-01T00:00:00+01:00")
        # Go time.Time.Unix() for years outside 1..9999, as Go formats them.
        go_unix = {
            "-0401-01-01T12:00:00.000000005Z": -74821492800,
            "-0401-03-01T12:00:00.000000005Z": -74816395200,
            "-0400-01-01T12:00:00.000000005Z": -74789956800,
            "-0400-03-01T12:00:00.000000005Z": -74784772800,
            "-0002-01-01T12:00:00.000000005Z": -62230248000,
            "-0002-03-01T12:00:00.000000005Z": -62225150400,
            "-0001-01-01T12:00:00.000000005Z": -62198712000,
            "-0001-03-01T12:00:00.000000005Z": -62193614400,
            "0000-03-01T12:00:00.000000005Z": -62161992000,
            "10000-01-01T12:00:00.000000005Z": 253402344000,
            "10000-03-01T12:00:00.000000005Z": 253407528000,
        }
        for text, unix in go_unix.items():
            with self.subTest(text=text):
                self.assertEqual(rfc3339_to_unix_ns(text), unix * 10**9 + 5)


def _ipc(batch) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as w:
        w.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _v2_reply(header: dict, streams: list, part: int, end=None) -> bytes:
    """Frames of a protocol 2 data reply, each stream cut into parts."""
    from gotdms.codec import MAGIC, VERSION, _PREFIX

    def frame(h, payload=b""):
        body = json.dumps(h).encode()
        return _PREFIX.pack(MAGIC, VERSION, 0, len(body), len(payload)) + body + payload

    begin = dict(header, op="result_begin", streams=[{"id": i, "kind": "x", "bytes": len(s)} for i, s in enumerate(streams)])
    out = [frame(begin)]
    for i, s in enumerate(streams):
        for off in range(0, len(s), part):
            out.append(frame({"op": "part", "stream": i}, s[off : off + part]))
    out.append(frame(end or {"op": "result_end", "streams": len(streams), "bytes": sum(map(len, streams))}))
    return b"".join(out)


class ZeroCopyTest(unittest.TestCase):
    def test_parts_fill_one_aligned_buffer_per_stream(self):
        # Odd part sizes put every part boundary off 8 bytes. Each stream
        # still decodes from its own aligned buffer with no copy.
        import pyarrow as pa
        from gotdms.codec import decode_ipc, read_reply

        streams = []
        for k in range(5):
            batch = pa.record_batch(
                [pa.array(np.arange(100 + k, dtype=np.float64) + k), pa.array(np.arange(100 + k, dtype=np.int64))],
                names=["y", "time_ns"],
            )
            streams.append(_ipc(batch))
        for part in (1, 7, 13, 1 << 20):
            with self.subTest(part=part):
                reply = read_reply(io.BytesIO(_v2_reply({}, streams, part)))
                self.assertIsNone(reply.error)
                self.assertEqual([bytes(p) for p in reply.streams], streams)
                self.assertTrue(all(isinstance(p, memoryview) and p.readonly for p in reply.streams))
                for k, view in enumerate(reply.streams):
                    y, time_ns, rel = decode_ipc(view)
                    np.testing.assert_array_equal(y, np.arange(100 + k) + k)
                    np.testing.assert_array_equal(time_ns, np.arange(100 + k))
                    self.assertIsNone(rel)
                    self.assertTrue(y.flags.aligned and time_ns.flags.aligned, f"stream {k}")
                    self.assertEqual(y.ctypes.data % 8, 0)
                    self.assertFalse(y.flags.writeable)
                    self.assertTrue(np.shares_memory(y, np.frombuffer(view, dtype=np.uint8)), "y was copied")

    def test_shared_clock_is_one_array(self):
        import pyarrow as pa
        from gotdms.codec import as_polars, read_reply, series_from_reply

        n = 50
        clock = _ipc(pa.record_batch([pa.array(np.arange(n, dtype=np.int64) * 10)], names=["time_ns"]))
        other = _ipc(pa.record_batch([pa.array(np.arange(n, dtype=np.int64) * 7)], names=["time_ns"]))
        chans = [_ipc(pa.record_batch([pa.array(np.full(n, float(k)))], names=["y"])) for k in range(4)]
        meta = lambda name, clk, sid: {"name": name, "group": "g", "timed": clk is not None, "clock": clk, "stream": sid,
                                       "basis": "1970-01-01T00:00:00Z", "samples": n}
        header = {
            "clocks": [{"id": 0, "samples": n, "stream": 0}, {"id": 1, "samples": n, "stream": 1}],
            "channels": [meta("a", 0, 2), meta("b", 0, 3), meta("c", 1, 4), meta("u", None, 5)],
        }
        reply = read_reply(io.BytesIO(_v2_reply(header, [clock, other] + chans, 9)))
        a, b, c, u = series_from_reply(reply.header, reply.streams)
        self.assertIs(a.time_ns, b.time_ns)
        self.assertIsNot(a.time_ns, c.time_ns)
        np.testing.assert_array_equal(a.time_ns, np.arange(n) * 10)
        np.testing.assert_array_equal(c.time_ns, np.arange(n) * 7)
        self.assertIsNone(u.time_ns)
        np.testing.assert_array_equal(u.relative_s, np.arange(n))
        np.testing.assert_array_equal(b.relative_s, (np.arange(n) * 10) / 1e9)
        frame = as_polars([a, b])
        self.assertEqual(frame.columns, ["time", "g/a", "g/b"])
        from gotdms import AlignmentError

        with self.assertRaises(AlignmentError):
            as_polars([a, c])

    def test_bad_replies_raise(self):
        import pyarrow as pa
        from gotdms import ProtocolError
        from gotdms.codec import MAGIC, VERSION, _PREFIX, read_reply, series_from_reply

        blob = _ipc(pa.record_batch([pa.array(np.arange(4, dtype=np.float64))], names=["y"]))
        good = _v2_reply({}, [blob], 16)

        def frame(h, payload=b""):
            body = json.dumps(h).encode()
            return _PREFIX.pack(MAGIC, VERSION, 0, len(body), len(payload)) + body + payload

        begin = frame({"op": "result_begin", "streams": [{"id": 0, "kind": "x", "bytes": len(blob)}]})
        cases = {
            "short stream": begin + frame({"op": "part", "stream": 0}, blob[:-8]) + frame({"op": "result_end"}),
            "overflow": begin + frame({"op": "part", "stream": 0}, blob + b"x" * 8),
            "unknown stream": begin + frame({"op": "part", "stream": 3}, blob),
            "stray frame": begin + frame({"op": "result"}),
            "truncated": good[:-20],
            "old version": _PREFIX.pack(MAGIC, 1, 0, 2, 0) + b"{}",
            "bool size": frame({"op": "result_begin", "streams": [{"id": 0, "kind": "x", "bytes": True}]}),
            "bool stream id": begin + frame({"op": "part", "stream": False}, blob),
            "no stream list": frame({"op": "result_begin"}),
            "end miscounts streams": begin + frame({"op": "part", "stream": 0}, blob)
            + frame({"op": "result_end", "streams": 2, "bytes": len(blob)}),
            "end miscounts bytes": begin + frame({"op": "part", "stream": 0}, blob)
            + frame({"op": "result_end", "streams": 1, "bytes": len(blob) + 1}),
            "huge single frame": _PREFIX.pack(MAGIC, VERSION, 0, 2, (64 << 20) + 1) + b"{}",
        }
        two = frame({"op": "result_begin", "streams": [{"id": 0, "kind": "x", "bytes": len(blob)},
                                                       {"id": 1, "kind": "x", "bytes": len(blob)}]})
        cases["stream started early"] = two + frame({"op": "part", "stream": 0}, blob[:8]) + frame({"op": "part", "stream": 1}, blob)
        cases["stream after a later one"] = (two + frame({"op": "part", "stream": 0}, blob)
                                             + frame({"op": "part", "stream": 1}, blob[:8])
                                             + frame({"op": "part", "stream": 0}, blob[:8]))
        # The new checks must fire for their own reason, not because the
        # reader later runs out of bytes.
        why = {
            "bool size": "has size",
            "bool stream id": "unknown stream",
            "no stream list": "stream list",
            "end miscounts streams": "result_end says",
            "end miscounts bytes": "result_end says",
            "huge single frame": "payload",
            "stream started early": "started with stream 0",
            "stream after a later one": "after stream 1 started",
        }
        for name, data in cases.items():
            with self.subTest(name=name), self.assertRaisesRegex(ProtocolError, why.get(name, "")):
                read_reply(io.BytesIO(data))
        # A clock id that is not in the table.
        header = {"clocks": [], "channels": [{"name": "a", "clock": 4, "stream": 0}]}
        reply = read_reply(io.BytesIO(_v2_reply(header, [blob], 64)))
        with self.assertRaises(ProtocolError):
            series_from_reply(reply.header, reply.streams)

    def test_mid_stream_error_is_returned(self):
        import pyarrow as pa
        from gotdms.codec import read_reply

        blob = _ipc(pa.record_batch([pa.array(np.arange(4, dtype=np.float64))], names=["y"]))
        end = {"op": "result_end", "error": {"code": "corrupt", "message": "bad segment", "stream": 1}}
        data = _v2_reply({}, [blob], 8, end=end)
        stream = io.BytesIO(data + b"NEXT")
        reply = read_reply(stream)
        self.assertEqual(reply.error["code"], "corrupt")
        self.assertIsNone(reply.streams)
        self.assertEqual(stream.read(), b"NEXT", "reader did not stop at result_end")

    def test_write_all_and_readinto_handle_short_calls(self):
        # Unbuffered pipes (and Windows' per-call caps) can move fewer bytes
        # than asked; both directions must loop.
        from gotdms.codec import readinto_exact, write_all

        class Short:
            def __init__(self, data=b""):
                self.data, self.out = data, bytearray()

            def write(self, b):
                b = bytes(b[:5])
                self.out += b
                return len(b)

            def readinto(self, view):
                k = min(3, len(view), len(self.data))
                view[:k] = self.data[:k]
                self.data = self.data[k:]
                return k

        s = Short()
        write_all(s, b"0123456789abcdef")
        self.assertEqual(bytes(s.out), b"0123456789abcdef")
        buf = bytearray(10)
        readinto_exact(Short(b"abcdefghijXYZ"), memoryview(buf))
        self.assertEqual(bytes(buf), b"abcdefghij")

    def test_read_exact_without_readinto(self):
        from gotdms.codec import read_exact
        from gotdms import ProtocolError

        class Trickle:
            def __init__(self, data):
                self.data = data

            def read(self, n):
                out, self.data = self.data[:3], self.data[3:]
                return out

        self.assertEqual(bytes(read_exact(Trickle(b"abcdefgh"), 8)), b"abcdefgh")
        with self.assertRaises(ProtocolError):
            read_exact(Trickle(b"abc"), 8)


class BridgeRelativeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if pl is None:
            raise unittest.SkipTest("polars is not installed")
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.bin = root / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"], cwd=ROOT)
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        demo = subprocess.check_output([str(cls.bin), "demo", "--out", cls.tmp.name], text=True)
        cls.demo = json.loads(demo)
        cls.epoch = datetime(2024, 3, 1, tzinfo=timezone.utc)

        import gotdms

        # Waveform timing with an increment that is not a whole number of
        # nanoseconds, so time_ns has varied sub-second digits.
        cls.wave = root / "wave.tdms"
        n = 20000
        gotdms.write_tdms(
            cls.wave,
            pl.DataFrame({"v": np.linspace(-1, 1, n), "w": np.linspace(1, 2, n)}),
            group="wave",
            waveforms={"wave": {
                "v": {"wf_start_time": datetime(2025, 7, 4, 12, 0, 0, 123457, tzinfo=timezone.utc), "wf_increment": 1 / 3e4},
                "w": {"wf_start_time": datetime(2025, 7, 4, 12, 0, 0, 123457, tzinfo=timezone.utc), "wf_increment": 1 / 3e4},
            }},
        )

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def open_demo(self, **kw):
        import gotdms

        return gotdms.open(
            self.demo["files"],
            timestamp_channel="Time",
            timestamp_channels={"raw": ""},
            numeric_epoch=self.epoch,
            **kw,
        )

    def go_series(self, session, specs, **extra):
        """Ask the bridge for both columns, the way relative=True used to."""
        from gotdms.codec import series_from_reply

        header = {"op": "read_many", "channels": specs, "absolute": True, "relative": True, "scaled": False}
        header.update(extra)
        reply = session._rpc_raw(header)
        return series_from_reply(reply.header, reply.streams)

    def assert_python_matches_go(self, session, specs, **extra):
        from gotdms.codec import Series

        for go in self.go_series(session, specs, **extra):
            self.assertIsNotNone(go._relative_s, "bridge sent relative_s")
            py = Series(go.meta, go.y, go.time_ns, None)
            self.assertTrue(np.array_equal(bits(py.relative_s), bits(go._relative_s)), f"{go.group}/{go.name}")

    def test_equivalence_demo_and_waveform(self):
        import gotdms

        bases = [None, "2024-03-01T00:00:01Z", "2024-03-01T00:00:00.500000001Z", "1904-01-01T00:00:00Z",
                 "2261-12-31T23:59:59.999999999Z", "2025-07-04T12:00:00.123456789Z"]
        with self.open_demo() as session:
            specs = [{"group": c["group"], "name": c["name"]} for c in session._data_channels()]
            for basis in bases:
                with self.subTest(file="demo", basis=basis):
                    extra = {} if basis is None else {"basis": basis}
                    self.assert_python_matches_go(session, specs, **extra)
        with gotdms.open([str(self.wave)]) as session:
            specs = [{"group": "wave", "name": "v"}, {"group": "wave", "name": "w"}]
            for basis in bases:
                with self.subTest(file="wave", basis=basis):
                    extra = {} if basis is None else {"basis": basis}
                    self.assert_python_matches_go(session, specs, **extra)
            win = {"start": "2025-07-04T12:00:00.2Z", "end": "2025-07-04T12:00:00.4Z"}
            self.assert_python_matches_go(session, specs, **win)

    def test_untimed_relative_is_index(self):
        with self.open_demo() as session:
            for go in self.go_series(session, [{"group": "raw", "name": "x"}]):
                self.assertFalse(go.timed)
                from gotdms.codec import Series

                py = Series(go.meta, go.y, go.time_ns, None)
                self.assertTrue(np.array_equal(bits(py.relative_s), bits(go._relative_s)))
                np.testing.assert_array_equal(py.relative_s, [0.0, 1.0, 2.0])

    def test_default_sends_one_time_column(self):
        with self.open_demo() as session:
            series, header = session.read_channels([{"group": "acq", "name": "ai0"}])
            # Protocol 2: time_ns is in the clock stream the channel names,
            # so the channel stream itself is y only.
            self.assertEqual(header["channels"][0]["columns"], ["y"])
            self.assertEqual(header["channels"][0]["clock"], 0)
            self.assertEqual(len(header["clocks"]), 1)
            self.assertIsNone(series[0]._relative_s)
            np.testing.assert_array_equal(series[0].relative_s, [0, 1, 2, 3, 4, 5])

            series, header = session.read_channels([{"group": "acq", "name": "ai0"}], relative=True)
            self.assertEqual(header["channels"][0]["columns"], ["y"])
            self.assertIsNotNone(series[0]._relative_s, "relative=True computes eagerly")

            series, header = session.read_channels([{"group": "acq", "name": "ai0"}], absolute=False, relative=True)
            self.assertEqual(header["channels"][0]["columns"], ["y", "relative_s"])
            self.assertNotIn("clock", header["channels"][0])
            self.assertEqual(header["clocks"], [])
            self.assertIsNone(series[0].time_ns)
            np.testing.assert_array_equal(series[0].relative_s, [0, 1, 2, 3, 4, 5])

            # relative=None (the default) means "not absolute", so
            # absolute=False alone still gets relative time, as before.
            series, header = session.read_channels([{"group": "acq", "name": "ai0"}], absolute=False)
            self.assertEqual(header["channels"][0]["columns"], ["y", "relative_s"])
            np.testing.assert_array_equal(series[0].relative_s, [0, 1, 2, 3, 4, 5])

            series, header = session.read_channels([{"group": "acq", "name": "ai0"}], absolute=False, relative=False)
            self.assertEqual(header["channels"][0]["columns"], ["y"])
            self.assertIsNone(series[0].relative_s)

            series = session.read_all(absolute=False)
            self.assertTrue(all(s.relative_s is not None for s in series))

    def test_as_polars_and_time_track_request_one_column(self):
        with self.open_demo() as session:
            sent = []
            real = session._rpc_raw

            def spy(header):
                if header.get("op") == "read_many":
                    sent.append((header["absolute"], header["relative"]))
                return real(header)

            session._rpc_raw = spy
            frame = session.as_polars(group="acq")
            self.assertEqual(frame.columns[0], "time")
            frame = session.as_polars(group="acq", absolute_time=False)
            self.assertEqual(frame.columns[0], "relative_s")
            ai0 = session["acq"]["ai0"]
            ai0.time_track(absolute_time=True)
            np.testing.assert_array_equal(ai0.time_track(absolute_time=False), [0, 1, 2, 3, 4, 5])
            self.assertEqual(sent, [(True, False), (False, True), (True, False), (False, True)])


if __name__ == "__main__":
    unittest.main()
