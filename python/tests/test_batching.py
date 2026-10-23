"""Protocol 2 packs many channels into few Arrow streams.

A data reply's streams each hold one schema and one or more record batches,
and every channel and clock names its stream, batch, and column. These tests
read through a real bridge and check values, order, stream counts, and the
zero-copy views; the codec tests at the end hand-build bad references.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))

try:
    import polars as pl
except ImportError:  # pragma: no cover - skipped only when polars is absent
    pl = None

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _values(group: int, k: int, n: int) -> np.ndarray:
    return np.arange(n, dtype=np.float64) * (k + 1) + group * 1000.0


class BatchedReadTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if pl is None:
            raise unittest.SkipTest("polars is not installed")
        import gotdms

        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.bin = root / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"], cwd=ROOT)
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        # Many channels: 3 groups x 120 channels of 64 samples, one waveform
        # axis per group.
        cls.many = root / "many.tdms"
        data, waves = {}, {}
        for g in range(3):
            name = f"G{g}"
            data[name] = {f"c{k:03d}": pl.Series(_values(g, k, 64)) for k in range(120)}
            waves[name] = {f"c{k:03d}": {"wf_start_time": T0, "wf_increment": 0.001 * (g + 1)} for k in range(120)}
        gotdms.write_tdms(cls.many, data, waveforms=waves)
        # Ragged: 60 channels of 60 different lengths, plus 5 of one length,
        # on their own waveform axes, and an untimed group.
        cls.ragged = root / "ragged.tdms"
        rdata = {"r": {f"r{k:02d}": pl.Series(_values(0, k, k + 1)) for k in range(60)}}
        rdata["r"].update({f"s{k}": pl.Series(_values(1, k, 33)) for k in range(5)})
        rwaves = {"r": {name: {"wf_start_time": T0, "wf_increment": 0.01} for name in rdata["r"]}}
        rdata["u"] = {"u0": pl.Series(np.arange(7, dtype=np.float64)), "u1": pl.Series(np.arange(9, dtype=np.float64))}
        gotdms.write_tdms(cls.ragged, rdata, waveforms=rwaves)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_many_channels_few_streams(self):
        import gotdms

        with gotdms.open([self.many]) as s:
            series, header = s.read_channels(s._data_channels())
            self.assertEqual(len(series), 360)
            # One clock stream (three clocks of 64 samples side by side) and
            # one channel stream with all 360 channels as columns.
            kinds = [st["kind"] for st in header["streams"]]
            self.assertEqual(kinds, ["clock", "channel"], header["streams"])
            self.assertEqual(len(header["clocks"]), 3)
            for i, item in enumerate(series):
                g, k = divmod(i, 120)
                self.assertEqual((item.group, item.name), (f"G{g}", f"c{k:03d}"))
                np.testing.assert_array_equal(item.y, _values(g, k, 64))
                self.assertEqual(item.meta["column"], i)
                self.assertFalse(item.y.flags.writeable)
                self.assertEqual(item.y.ctypes.data % 8, 0)
            # Channels of one group share one time_ns object.
            self.assertIs(series[0].time_ns, series[119].time_ns)
            self.assertIsNot(series[0].time_ns, series[120].time_ns)
            self.assertEqual(int(series[120].time_ns[1] - series[120].time_ns[0]), 2_000_000)

    def test_request_order_is_kept(self):
        import gotdms

        rng = np.random.default_rng(7)
        with gotdms.open([self.many]) as s:
            sels = s._data_channels()
            order = rng.permutation(len(sels))[:150]
            got = s.read_many([sels[i] for i in order])
            for i, item in zip(order, got):
                g, k = divmod(int(i), 120)
                self.assertEqual((item.group, item.name), (f"G{g}", f"c{k:03d}"))
                np.testing.assert_array_equal(item.y, _values(g, k, 64))
            g1 = [sels[i] for i in order if sels[i]["group"] == "G1"]
            frame = s.as_polars(g1)
            self.assertEqual(frame.columns, ["time"] + [f"G1/{x['name']}" for x in g1])
            for x in g1[:5]:
                k = int(x["name"][1:])
                np.testing.assert_array_equal(frame[f"G1/{x['name']}"].to_numpy(), _values(1, k, 64))

    def test_group_frame_matches_per_channel_reads(self):
        import gotdms

        with gotdms.open([self.many]) as s:
            frame = s.as_polars(group="G2")
            self.assertEqual(frame.columns, ["time"] + [f"G2/c{k:03d}" for k in range(120)])
            for k in (0, 57, 119):
                one = s.read(f"c{k:03d}", "G2")
                np.testing.assert_array_equal(frame[f"G2/c{k:03d}"].to_numpy(), one.y)
            rel = s.as_polars(group="G0", absolute_time=False)
            np.testing.assert_array_equal(rel["relative_s"].to_numpy(), s.read("c000", "G0", absolute=False).relative_s)

    def test_ragged_channels_fall_back_to_few_streams(self):
        import gotdms

        with gotdms.open([self.ragged]) as s:
            sels = s._data_channels()
            self.assertEqual(len(sels), 67)
            for kw in ({}, {"absolute": False}, {"absolute": False, "relative": False}):
                with self.subTest(**kw):
                    series, header = s.read_channels(sels, **kw)
                    # Never one stream per channel: wide groups for the
                    # lengths that repeat, one ragged stream for the rest.
                    self.assertLessEqual(len(header["streams"]), 6, header["streams"])
                    for sel, item in zip(sels, series):
                        self.assertEqual(item.name, sel["name"])
                        one = s.read(sel["name"], sel["group"], **kw)
                        np.testing.assert_array_equal(item.y, one.y)
                        if kw.get("absolute", True):
                            if one.time_ns is None:
                                self.assertIsNone(item.time_ns)
                            else:
                                np.testing.assert_array_equal(item.time_ns, one.time_ns)
                        elif kw.get("relative", True):
                            np.testing.assert_array_equal(item.relative_s, one.relative_s)
                        else:
                            self.assertTrue(item.relative_s is None or not item.timed)
                    self.assertEqual(series[5].y.size, 6)
                    self.assertEqual(series[-1].y.size, 9)

    def test_ragged_stream_holds_one_batch_per_channel(self):
        import gotdms

        with gotdms.open([self.ragged]) as s:
            sels = [x for x in s._data_channels() if x["name"].startswith("r")]
            series, header = s.read_channels(sels, absolute=False, relative=False)
            self.assertEqual(len(header["streams"]), 1, header["streams"])
            self.assertEqual(header["streams"][0]["batches"], 60)
            self.assertEqual([x.meta["batch"] for x in series], list(range(60)))
            self.assertTrue(all(x.meta["column"] == 0 for x in series))

    def test_read_tdms_packs_channels(self):
        import gotdms

        out = gotdms.read_tdms(self.many)
        self.assertEqual(list(out.groups), ["G0", "G1", "G2"])
        for g in range(3):
            chans = out.groups[f"G{g}"].channels
            self.assertEqual(list(chans), [f"c{k:03d}" for k in range(120)])
            for k in (0, 64, 119):
                np.testing.assert_array_equal(chans[f"c{k:03d}"].values.to_numpy(), _values(g, k, 64))
        mixed = Path(self.tmp.name) / "types.tdms"
        gotdms.write_tdms(
            mixed,
            {
                "a": {"i": pl.Series([1, 2, 3], dtype=pl.Int16), "f": pl.Series([1.5, 2.5, 3.5], dtype=pl.Float32),
                      "s": pl.Series(["x", "yy", ""]), "b": pl.Series([True, False, True]),
                      "t": pl.Series([T0, T0, T0]).cast(pl.Datetime("ns", "UTC")), "i2": pl.Series([4, 5, 6], dtype=pl.Int16)},
                "z": {"long": pl.Series(np.arange(1000, dtype=np.uint32))},
            },
        )
        got = gotdms.read_tdms(mixed)
        a = got.groups["a"].channels
        self.assertEqual(list(a), ["i", "f", "s", "b", "t", "i2"])
        self.assertEqual(a["i"].values.dtype, pl.Int16)
        self.assertEqual(a["i"].values.to_list(), [1, 2, 3])
        self.assertEqual(a["i2"].values.to_list(), [4, 5, 6])
        self.assertEqual(a["f"].values.dtype, pl.Float32)
        self.assertEqual(a["s"].values.to_list(), ["x", "yy", ""])
        self.assertEqual(a["b"].values.to_list(), [True, False, True])
        self.assertEqual(a["t"].values.dtype, pl.Datetime("ns", "UTC"))
        self.assertEqual(a["t"].values.to_list()[0], T0)
        self.assertEqual(got.groups["z"].channels["long"].values.to_list(), list(range(1000)))


def _ipc(batches) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batches[0].schema) as w:
        for b in batches:
            w.write_batch(b)
    return sink.getvalue().to_pybytes()


class ReplyColumnsTest(unittest.TestCase):
    """Bad column references in a hand-built reply raise ProtocolError."""

    def setUp(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:  # pragma: no cover
            self.skipTest("pyarrow is not installed")

    def _series(self, channels, streams, kinds, batches=None, clocks=()):
        from gotdms.codec import read_reply, series_from_reply
        from test_relative import _v2_reply

        header = {"clocks": list(clocks), "channels": channels}
        reply = read_reply(io.BytesIO(_v2_reply(header, streams, 11, kinds=kinds, batches=batches)))
        return series_from_reply(reply.header, reply.streams)

    def test_wide_and_ragged_decode(self):
        import pyarrow as pa

        wide = _ipc([pa.record_batch([pa.array([1.0, 2.0]), pa.array([3.0, 4.0])], names=["y_0", "y_1"])])
        ragged = _ipc([pa.record_batch([pa.array(np.arange(k, dtype=np.float64))], names=["y"]) for k in (1, 3, 0)])
        ch = lambda n, s, b, c, rows: {"name": n, "stream": s, "batch": b, "column": c, "samples": rows}
        got = self._series(
            [ch("b", 0, 0, 1, 2), ch("r2", 1, 2, 0, 0), ch("a", 0, 0, 0, 2), ch("r0", 1, 0, 0, 1), ch("r1", 1, 1, 0, 3)],
            [wide, ragged], ["channel", "channel"], [1, 3])
        self.assertEqual([x.name for x in got], ["b", "r2", "a", "r0", "r1"])
        np.testing.assert_array_equal(got[0].y, [3.0, 4.0])
        np.testing.assert_array_equal(got[2].y, [1.0, 2.0])
        np.testing.assert_array_equal(got[4].y, [0.0, 1.0, 2.0])
        self.assertEqual(got[1].y.size, 0)

    def test_bad_references(self):
        import pyarrow as pa
        from gotdms import ProtocolError

        wide = _ipc([pa.record_batch([pa.array([1.0, 2.0]), pa.array([3.0, 4.0])], names=["y_0", "y_1"])])
        ch = lambda n, c, **kw: dict({"name": n, "stream": 0, "batch": 0, "column": c, "samples": 2}, **kw)
        cases = {
            "already taken": ([ch("a", 0), ch("b", 0)], ["channel"], [1]),
            "were named": ([ch("a", 0)], ["channel"], [1]),
            "column 2": ([ch("a", 0), ch("b", 2)], ["channel"], [1]),
            "batch 1": ([ch("a", 0), ch("b", 1, batch=1)], ["channel"], [1]),
            "want 'channel'": ([ch("a", 0), ch("b", 1)], ["clock"], [1]),
            "declared 2": ([ch("a", 0), ch("b", 1)], ["channel"], [2]),
            "stream 3": ([ch("a", 0), ch("b", 1, stream=3)], ["channel"], [1]),
            "has batch": ([ch("a", 0), ch("b", 1, batch=None)], ["channel"], [1]),
            "header says": ([ch("a", 0), ch("b", 1, samples=5)], ["channel"], [1]),
            "relative_s": ([ch("a", 0), ch("b", 1, relative_column=0)], ["channel"], [1]),
            "without a relative_column": ([ch("a", 0, columns=["y", "relative_s"]), ch("b", 1)], ["channel"], [1]),
        }
        for why, (chans, kinds, batches) in cases.items():
            with self.subTest(why), self.assertRaisesRegex(ProtocolError, why):
                self._series(chans, [wide], kinds, batches)
        clk = _ipc([pa.record_batch([pa.array([1.0, 2.0])], names=["time_ns"])])
        with self.assertRaisesRegex(ProtocolError, "want int64"):
            self._series([ch("a", 0), ch("b", 1, stream=1)], [clk, wide], ["clock", "channel"],
                         clocks=[{"id": 0, "samples": 2, "stream": 0, "batch": 0, "column": 0}])
        with self.assertRaisesRegex(ProtocolError, "want 'y'"):
            named = _ipc([pa.record_batch([pa.array([1.0, 2.0])], names=["time_ns"])])
            self._series([ch("a", 0)], [named], ["channel"])
        with self.assertRaisesRegex(ProtocolError, "not a valid Arrow IPC stream"):
            self._series([ch("a", 0)], [b"\x00" * 16], ["channel"])

    def test_stream_specs_are_checked(self):
        from gotdms import ProtocolError
        from gotdms.codec import ReplyColumns

        # A bridge from before packed streams declares no batches.
        with self.assertRaisesRegex(ProtocolError, "rebuild gotdms-bridge"):
            ReplyColumns([{"id": 0, "kind": "channel", "bytes": 1}], [b"x"])
        with self.assertRaisesRegex(ProtocolError, "stream 0 is"):
            ReplyColumns(["channel"], [b"x"])
        with self.assertRaisesRegex(ProtocolError, "does not match"):
            ReplyColumns([], [b"x"])


if __name__ == "__main__":
    unittest.main()
