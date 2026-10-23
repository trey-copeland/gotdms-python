"""Launch the real gotdms-bridge binary. The pipe is the product under test."""

import json
import os
import shutil
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


class BridgeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        demo = subprocess.check_output([str(cls.bin), "demo", "--out", cls.tmp.name], text=True)
        cls.demo = json.loads(demo)
        cls.epoch = datetime(2024, 3, 1, tzinfo=timezone.utc)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def open_demo(self):
        import gotdms

        return gotdms.open(
            self.demo["files"],
            timestamp_channel="Time",
            timestamp_channels={"raw": ""},
            numeric_epoch=self.epoch,
        )

    def test_binary_resolution(self):
        import gotdms
        from gotdms.client import bundled_binary

        explicit = gotdms.locate_binary("/tmp/does-not-need-to-exist")
        self.assertEqual(explicit, "/tmp/does-not-need-to-exist")
        self.assertEqual(gotdms.locate_binary(), os.environ["GOTDMS_BRIDGE"])

        dest_dir = Path(gotdms.__file__).resolve().parent / "_bin"
        dest_dir.mkdir(exist_ok=True)
        dest = dest_dir / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        shutil.copy(self.bin, dest)
        dest.chmod(0o755)
        saved = os.environ.pop("GOTDMS_BRIDGE")
        try:
            self.assertEqual(Path(bundled_binary()).resolve(), dest.resolve())
            self.assertEqual(Path(gotdms.locate_binary()).resolve(), dest.resolve())
            with gotdms.open(
                self.demo["files"],
                timestamp_channel="Time",
                timestamp_channels={"raw": ""},
                numeric_epoch=self.epoch,
            ) as session:
                got = session["acq"]["ai0"].data()
            np.testing.assert_array_equal(got, [10, 11, 12, 13, 14, 15])
            dest.unlink()
            with self.assertRaises(gotdms.GotdmsError) as raised:
                gotdms.locate_binary()
            self.assertIn("pip install .", str(raised.exception))
            self.assertIn("GOTDMS_BRIDGE", str(raised.exception))
            self.assertNotIn("pip install gotdms", str(raised.exception))
        finally:
            os.environ["GOTDMS_BRIDGE"] = saved
            if dest.exists():
                dest.unlink()

    def test_arrays_times_and_errors(self):
        import gotdms

        with self.open_demo() as session:
            ai0 = session["acq"]["ai0"]
            self.assertEqual(ai0.data_type, "tdsTypeDoubleFloat")
            y = ai0.data()
            self.assertIsInstance(y, np.ndarray)
            np.testing.assert_array_equal(y, [10, 11, 12, 13, 14, 15])
            absolute = ai0.time_track(absolute_time=True)
            self.assertEqual(absolute.dtype, np.dtype("datetime64[ns]"))
            self.assertEqual(absolute[0], np.datetime64("2024-03-01T00:00:00"))
            self.assertEqual(absolute[-1], np.datetime64("2024-03-01T00:00:05"))
            relative = ai0.time_track(absolute_time=False)
            np.testing.assert_array_equal(relative, [0, 1, 2, 3, 4, 5])

            basis = self.epoch + timedelta(seconds=1)
            window = session.read(
                "ai0",
                "acq",
                start=self.epoch + timedelta(seconds=2),
                end=self.epoch + timedelta(seconds=5),
                basis=basis,
            )
            np.testing.assert_array_equal(window.y, [12, 13, 14])
            np.testing.assert_array_equal(window.relative_s, [1, 2, 3])

            many = session.read_many(
                [{"name": "ai0", "group": "acq"}, {"name": "ai1", "group": "acq"}],
                start=self.epoch + timedelta(seconds=2),
                end=self.epoch + timedelta(seconds=5),
            )
            np.testing.assert_array_equal(many[0].time_ns, many[1].time_ns)
            np.testing.assert_array_equal(many[1].y, [3, 4, 5])
            self.assertFalse(many[1].scaled)
            scaled = session.read(
                "ai1",
                "acq",
                start=self.epoch + timedelta(seconds=2),
                end=self.epoch + timedelta(seconds=5),
                scaled=True,
            )
            np.testing.assert_array_equal(scaled.y, [6.5, 8.5, 10.5])
            self.assertTrue(scaled.scaled)
            self.assertEqual(session["acq"]["ai1"].scale["slope"], 2)
            self.assertEqual(session["acq"]["ai1"].unit, "V")

            with self.assertRaises(gotdms.NoScaleError):
                session.read("ai0", "acq", scaled=True)
            with self.assertRaises(gotdms.NonlinearScaleError):
                session.read("poly", "other", scaled=True)
            with self.assertRaises(gotdms.AmbiguousChannelError) as amb:
                session.channel("ai0")
            self.assertGreaterEqual(len(amb.exception.candidates), 2)
            with self.assertRaises(gotdms.UntimedError):
                session.read(
                    "x",
                    "raw",
                    start=self.epoch,
                    end=self.epoch + timedelta(seconds=1),
                )
            raw = session["raw"]["x"]
            np.testing.assert_array_equal(raw.data(), [9, 8, 7])
            np.testing.assert_array_equal(raw.time_track(absolute_time=False), [0, 1, 2])
            with self.assertRaises(gotdms.UntimedError):
                raw.time_track(absolute_time=True)

            names = [channel.name for channel in session.group("acq")]
            self.assertIn("ai0", names)
            self.assertIn("Time", names)
            self.assertTrue(any(item["name"] == "ai1" for item in session.search("ai1")))

    @unittest.skipIf(pl is None, "polars is not installed")
    def test_as_polars(self):
        import gotdms

        with self.open_demo() as session:
            frame = session.as_polars(
                channels=[session.channel("ai0", "acq"), session.channel("ai1", "acq")],
                time="column",
                absolute_time=True,
                align="strict",
            )
            self.assertIsInstance(frame, pl.DataFrame)
            self.assertEqual(frame.schema["time"], pl.Datetime("ns", "UTC"))
            self.assertEqual(frame.columns[0], "time")
            np.testing.assert_array_equal(frame["acq/ai0"].to_numpy(), [10, 11, 12, 13, 14, 15])
            np.testing.assert_array_equal(frame["acq/ai1"].to_numpy(), [1, 2, 3, 4, 5, 6])
            self.assertEqual(frame["acq/ai0"].null_count(), 0)
            with self.assertRaises(gotdms.ArgumentError):
                session.as_polars(channels=[session.channel("ai0", "acq")], time="index")
            grouped = session["acq"].as_polars(time="column", absolute_time=True, align="strict")
            self.assertNotIn("Time", "".join(grouped.columns))
            self.assertEqual(grouped.schema["time"], pl.Datetime("ns", "UTC"))

            with self.assertRaises(gotdms.AlignmentError):
                session.as_polars(
                    channels=[session.channel("ai0", "acq"), session.channel("volt", "fast")],
                    align="strict",
                )
            outer = session.as_polars(
                channels=[session.channel("ai0", "acq"), session.channel("volt", "fast")],
                align="nptdms",
                time="column",
            )
            self.assertGreater(outer["acq/ai0"].null_count(), 0)
            self.assertGreater(outer["fast/volt"].null_count(), 0)
            half = datetime(2024, 3, 1, 0, 0, 0, 500000, tzinfo=timezone.utc)
            at_half = outer.filter(pl.col("time") == half)
            self.assertEqual(at_half.height, 1)
            self.assertTrue(at_half["acq/ai0"].is_null().item())
            self.assertEqual(at_half["fast/volt"].item(), 1)

            with self.assertRaises(gotdms.AlignmentError):
                session.channel("d", "dups").as_polars(align="nptdms")

            relative = session.as_polars(
                channels=[session.channel("ai0", "acq")],
                time="column",
                absolute_time=False,
                basis=self.epoch + timedelta(seconds=1),
            )
            self.assertEqual(relative.columns[0], "relative_s")
            self.assertEqual(relative["relative_s"].to_list(), [float(v) for v in range(-1, 5)])

    def test_convert_bundle(self):
        import gotdms

        out = Path(self.tmp.name) / "bundle"
        start = "2024-03-01T00:00:02Z"
        end = "2024-03-01T00:00:05Z"
        subprocess.check_call(
            [
                str(self.bin),
                "convert",
                "--path",
                self.demo["files"][0],
                "--path",
                self.demo["files"][1],
                "--path",
                self.demo["files"][2],
                "--timestamp-channel",
                "Time",
                "--group-timestamp",
                "raw=",
                "--numeric-epoch",
                "2024-03-01T00:00:00Z",
                "--channel",
                "acq/ai0",
                "--channel",
                "acq/ai1",
                "--start",
                start,
                "--end",
                end,
                "--basis",
                "2024-03-01T00:00:01Z",
                "--out",
                str(out),
            ],
            stdout=subprocess.DEVNULL,
        )
        bundle = gotdms.load_bundle(out)
        self.assertEqual(bundle.format, "arrow-ipc-stream")
        self.assertEqual(len(bundle.channels), 2)
        self.assertEqual(bundle.channels[0].group, "acq")
        self.assertTrue(bundle.channels[0].meta.get("data_type"))
        self.assertTrue(bundle.basis)
        np.testing.assert_array_equal(bundle.channels[0].y, [12, 13, 14])
        np.testing.assert_array_equal(bundle.channels[0].relative_s, [1, 2, 3])
        if pl is not None:
            frame = bundle.as_polars(align="strict", time="column")
            self.assertEqual(frame.columns[0], "time")
            self.assertEqual(frame.schema["time"], pl.Datetime("ns", "UTC"))
            np.testing.assert_array_equal(frame["acq/ai1"].to_numpy(), [3, 4, 5])


if __name__ == "__main__":
    unittest.main()
