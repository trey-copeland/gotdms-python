"""Lazy scan_tdms against the real gotdms-bridge binary."""

import json
import os
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


@unittest.skipIf(pl is None, "polars is not installed")
class ScanTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / "gotdms-bridge"
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        cls.demo = _demo(cls.bin, cls.tmp.name, "demo")
        cls.wave = _demo(cls.bin, str(Path(cls.tmp.name) / "wave"), "waveform")
        cls.gap = _demo(cls.bin, str(Path(cls.tmp.name) / "gap"), "gap")
        cls.epoch = datetime(2024, 3, 1, tzinfo=timezone.utc)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def scan(self, source=None, **kwargs):
        import gotdms

        kwargs.setdefault("timestamp_channel", "Time")
        kwargs.setdefault("timestamp_channels", {"raw": ""})
        kwargs.setdefault("numeric_epoch", self.epoch)
        if source is None:
            source = self.demo["files"]
        return gotdms.scan_tdms(source, **kwargs)

    def test_schema_is_metadata_and_collect_reads(self):
        import gotdms

        path = Path(self.demo["files"][0])
        trace = []
        frame = self.scan(path, trace=trace)
        self.assertEqual(frame.gotdms_sample_reads, 0)
        schema = frame.collect_schema()
        self.assertEqual(schema["time"], pl.Datetime("ns", "UTC"))
        self.assertEqual(list(schema), ["time", "acq/ai0", "acq/ai1", "fast/volt"])
        self.assertNotIn("acq/Time", schema)
        self.assertIn("gotdms", frame.select("acq/ai0").explain())
        selected = frame.select("acq/ai0").head(2)
        self.assertEqual(trace, [])

        original = path.read_bytes()
        try:
            path.write_bytes(original[: _data_pos(original) - 1])
            with self.assertRaises(gotdms.GotdmsError):
                selected.collect()
        finally:
            path.write_bytes(original)
        self.assertEqual(trace, [])

        got = selected.collect()
        self.assertEqual(got["acq/ai0"].to_list(), [10.0, 11.0])
        self.assertEqual(len(trace), 1)
        self.assertTrue(trace[0]["limited"])
        self.assertTrue(trace[0]["limit_pushed"])
        self.assertEqual(trace[0]["n_rows"], 2)
        self.assertFalse(trace[0]["predicate"])

        with gotdms.Session() as session:
            described = session.describe(
                self.demo["files"],
                timestamp_channel="Time",
                timestamp_channels={"raw": ""},
                numeric_epoch=self.epoch,
            )
        self.assertEqual(described["sample_reads"], 0)
        self.assertGreater(described["channels"][0]["samples"], 0)

    def test_schema_order_and_two_groups_with_one_name(self):
        schema = self.scan().collect_schema()
        self.assertEqual(
            list(schema),
            ["time", "acq/ai0", "acq/ai1", "fast/volt", "dups/d", "other/ai0", "other/poly", "raw/x"],
        )
        self.assertIn("acq/ai0", schema)
        self.assertIn("other/ai0", schema)
        self.assertNotIn("acq/Time", schema)
        self.assertNotIn("fast/Time", schema)

    def test_projection_and_limit_pushdown(self):
        trace = []
        frame = self.scan(trace=trace).select("time", "acq/ai0").collect()
        self.assertEqual(frame["acq/ai0"].to_list(), [10, 11, 12, 13, 14, 15])
        self.assertEqual(frame["time"].to_numpy()[0], np.datetime64("2024-03-01T00:00:00"))
        self.assertFalse(any(item["name"] in ("ai1", "volt", "poly", "x", "d") for item in trace[-1]["reads"]))
        self.assertTrue(any(item["name"] == "ai0" and item["role"] == "data" for item in trace[-1]["reads"]))
        self.assertTrue(any(item["name"] == "Time" and item["role"] == "timestamp" for item in trace[-1]["reads"]))
        self.assertFalse(trace[-1]["limit_pushed"])

        head = self.scan(trace=trace).select("acq/ai0").head(2).collect()
        self.assertEqual(head["acq/ai0"].to_list(), [10.0, 11.0])
        reads = trace[-1]["reads"]
        self.assertTrue(trace[-1]["limited"])
        self.assertEqual(_sum_len(reads, "ai0"), 2)
        self.assertTrue(all(item["length"] < item["channel_samples"] for item in reads if item["name"] == "ai0"))
        self.assertFalse(any(item["file"].endswith("b.tdms") for item in reads))
        self.assertFalse(any(item["name"] == "ai1" for item in reads))

        crossed = self.scan(trace=trace).select("acq/ai0").limit(5).collect()
        self.assertEqual(crossed["acq/ai0"].to_list(), [10, 11, 12, 13, 14])
        self.assertEqual(_sum_len(trace[-1]["reads"], "ai0"), 5)
        self.assertTrue(any(item["file"].endswith("b.tdms") and item["name"] == "ai0" for item in trace[-1]["reads"]))

        both = self.scan(trace=trace).select("acq/ai0", "acq/ai1").head(2).collect()
        self.assertEqual(both["acq/ai1"].to_list(), [1.0, 2.0])
        clocks = [item for item in trace[-1]["reads"] if item["name"] == "Time"]
        self.assertEqual(len(clocks), 1)
        self.assertEqual(clocks[0]["length"], 2)
        self.assertEqual(clocks[0]["channel_samples"], 4)

        scaled = self.scan(scaled=True, trace=trace).select("acq/ai1").head(2).collect()
        self.assertEqual(scaled["acq/ai1"].to_list(), [2.5, 4.5])

    def test_slice_is_not_pushed_and_predicate_blocks_limit(self):
        trace = []
        sliced = self.scan(trace=trace).select("acq/ai0").slice(2, 2).collect()
        self.assertEqual(sliced["acq/ai0"].to_list(), [12.0, 13.0])
        self.assertIsNone(trace[-1]["n_rows"])
        self.assertFalse(trace[-1]["limit_pushed"])
        self.assertFalse(trace[-1]["limited"])
        ai0 = [item for item in trace[-1]["reads"] if item["name"] == "ai0" and item["role"] == "data"]
        self.assertTrue(ai0)
        self.assertTrue(all(item["offset"] == 0 and item["length"] == item["channel_samples"] for item in ai0))

        filtered = self.scan(trace=trace).select("acq/ai0").filter(pl.col("acq/ai0") > 12).collect()
        self.assertEqual(filtered["acq/ai0"].to_list(), [13.0, 14.0, 15.0])
        self.assertTrue(trace[-1]["predicate"])
        self.assertFalse(trace[-1]["limit_pushed"])
        self.assertIsNone(trace[-1]["n_rows"])
        self.assertEqual(trace[-1]["with_columns"], ["acq/ai0"])
        self.assertFalse(any(item["name"] == "ai1" for item in trace[-1]["reads"]))

        gap = self.scan(self.gap["files"], timestamp_channels=None, trace=trace)
        headed = gap.filter(pl.col("acq/ai0") > 10).head(1).collect()
        self.assertEqual(headed["acq/ai0"].to_list(), [11.0])
        self.assertTrue(trace[-1]["predicate"])
        self.assertIsNone(trace[-1]["n_rows"])
        self.assertFalse(trace[-1]["limit_pushed"])
        self.assertEqual(_sum_len(trace[-1]["reads"], "ai0"), 4)

    def test_multi_file_directory_glob_and_gaps(self):
        import gotdms

        with gotdms.open(
            self.demo["files"],
            timestamp_channel="Time",
            timestamp_channels={"raw": ""},
            numeric_epoch=self.epoch,
        ) as session:
            eager = session.as_polars(
                channels=[session.channel("ai0", "acq"), session.channel("ai1", "acq")],
                time="column",
                align="strict",
            )
        lazy = self.scan().select("time", "acq/ai0", "acq/ai1").collect()
        self.assertTrue(lazy.equals(eager))

        directory = self.scan(self.demo["dir"]).select("acq/ai0").collect()
        globbed = self.scan(str(Path(self.demo["dir"]) / "*.tdms")).select("acq/ai0").collect()
        self.assertEqual(directory["acq/ai0"].to_list(), lazy["acq/ai0"].to_list())
        self.assertEqual(globbed["acq/ai0"].to_list(), lazy["acq/ai0"].to_list())

        with self.assertRaises(gotdms.UntimedError):
            self.scan().collect()
        with self.assertRaises(gotdms.AlignmentError):
            self.scan().select("acq/ai0", "fast/volt").collect()

        trace = []
        gap = self.scan(self.gap["files"], timestamp_channels=None, trace=trace).select("acq/ai0").collect()
        self.assertEqual(gap["acq/ai0"].to_list(), [10.0, 11.0, 14.0, 15.0])
        self.assertEqual(gap["acq/ai0"].null_count(), 0)
        self.assertEqual(len(trace[-1]["channels"][0]["gaps"]), 1)
        prefix = self.scan(self.gap["files"], timestamp_channels=None, trace=trace).select("acq/ai0").head(3).collect()
        self.assertEqual(prefix["acq/ai0"].to_list(), [10.0, 11.0, 14.0])
        self.assertEqual(prefix["acq/ai0"].null_count(), 0)
        self.assertEqual(len(trace[-1]["channels"][0]["gaps"]), 1)
        self.assertEqual(_sum_len(trace[-1]["reads"], "ai0"), 3)

    def test_timestamp_semantics(self):
        import gotdms

        with self.assertRaises(gotdms.ArgumentError) as missing:
            self.scan(numeric_epoch=None)
        self.assertIn("numeric_epoch", str(missing.exception))

        unnamed = self.scan(self.demo["files"][0], timestamp_channel=None, timestamp_channels=None, numeric_epoch=None)
        self.assertIn("acq/Time", unnamed.collect_schema())
        with self.assertRaises(gotdms.UntimedError):
            unnamed.select("acq/ai0").collect()

        raw = self.scan(absolute_time=False, trace=[]).select("raw/x").collect()
        self.assertEqual(raw["raw/x"].to_list(), [9.0, 8.0, 7.0])

        basis = self.epoch + timedelta(seconds=1)
        relative = self.scan(absolute_time=False, basis=basis).select("relative_s", "acq/ai0").head(3).collect()
        self.assertEqual(relative["relative_s"].to_list(), [-1.0, 0.0, 1.0])
        self.assertEqual(relative["acq/ai0"].to_list(), [10.0, 11.0, 12.0])

        wave = gotdms.scan_tdms(self.wave["files"])
        self.assertEqual(list(wave.collect_schema()), ["time", "w/Time", "w/ai0", "w/ai1"])
        with self.assertRaises(gotdms.UntimedError):
            wave.select("w/Time").collect()
        trace = []
        timed = gotdms.scan_tdms(self.wave["files"], trace=trace).select("time", "w/ai0").head(2).collect()
        self.assertEqual(timed["w/ai0"].to_list(), [10.0, 11.0])
        self.assertEqual(timed["time"].to_numpy()[0], np.datetime64("2024-03-01T00:00:00"))
        self.assertEqual(timed["time"].to_numpy()[1], np.datetime64("2024-03-01T00:00:01"))
        self.assertTrue(all(item["name"] == "ai0" for item in trace[-1]["reads"]))
        self.assertEqual(_sum_len(trace[-1]["reads"], "ai0"), 2)

        stored = gotdms.scan_tdms(self.wave["files"], absolute_time=False, trace=trace).select("w/Time").collect()
        self.assertEqual(stored["w/Time"].to_list(), [100.0, 101.0, 102.0, 103.0])
        self.assertTrue(all(item["name"] == "Time" for item in trace[-1]["reads"]))

        clock = self.scan(self.gap["files"], timestamp_channels=None, trace=trace).select("time").head(2).collect()
        self.assertEqual(clock["time"].to_numpy()[0], np.datetime64("2024-03-01T00:00:00"))
        self.assertEqual(len(clock), 2)
        self.assertTrue(all(item["name"] == "Time" for item in trace[-1]["reads"]))
        self.assertFalse(any(item["name"] == "ai0" for item in trace[-1]["reads"]))

    def test_argument_errors(self):
        import gotdms

        with self.assertRaises(gotdms.ArgumentError):
            self.scan(time="index")
        with self.assertRaises(gotdms.ArgumentError):
            gotdms.scan_tdms(self.demo["dir"], pattern="**/*.tdms", timestamp_channel="Time", numeric_epoch=self.epoch)


def _demo(binary, directory, kind):
    Path(directory).mkdir(parents=True, exist_ok=True)
    out = subprocess.check_output([str(binary), "demo", "--set", kind, "--out", directory], text=True)
    return json.loads(out)


def _data_pos(blob: bytes) -> int:
    if blob[:4] != b"TDSm":
        raise AssertionError("demo file is not TDMS")
    toc = int.from_bytes(blob[4:8], "little")
    endian = "big" if toc & (1 << 6) else "little"
    raw = int.from_bytes(blob[20:28], endian)
    return 28 + raw


def _sum_len(reads, name):
    return sum(item["length"] for item in reads if item["name"] == name and item["role"] == "data")


if __name__ == "__main__":
    unittest.main()
