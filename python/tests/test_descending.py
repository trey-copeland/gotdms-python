"""Descending waveforms (negative ``wf_increment``) with gotdms v0.3.0.

Core v0.3.0 reports a negative ``dt`` for a descending waveform, orders the
files of a descending channel latest start first, records gaps with ``from``
after ``to``, and refuses files whose increments disagree in sign. The
bridge's own offset/limit reads must agree with the dataset path.
"""

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

import gotdms  # noqa: E402

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
T0_NS = 1767225600 * 10**9
S = 10**9


class DescendingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if pl is None:
            raise unittest.SkipTest("polars is not installed")
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"], cwd=ROOT)
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def _dir(self, starts, incs, n=4):
        d = tempfile.mkdtemp(dir=self.tmp.name)
        for k, (start, inc) in enumerate(zip(starts, incs)):
            gotdms.write_tdms(
                os.path.join(d, f"f{k}.tdms"),
                pl.DataFrame({"v": np.arange(n, dtype=np.float64) + n * k}),
                waveforms={"data": {"v": {"wf_start_time": start, "wf_increment": inc}}},
            )
        return d

    def test_single_file(self):
        d = self._dir([T0], [-1.0])
        with gotdms.open(directory=d) as s:
            r = s.read("v")
            self.assertEqual(r.meta["dt"], -1)
            self.assertEqual(r.time_ns.tolist(), [T0_NS - i * S for i in range(4)])
            self.assertEqual(r.y.tolist(), [0.0, 1.0, 2.0, 3.0])
            df = s.as_polars()
            self.assertEqual(df["time"].dt.epoch("ns").to_list(), r.time_ns.tolist())
            part = s.read("v", offset=1, limit=2)
            self.assertEqual(part.meta["dt"], -1)
            self.assertEqual(part.y.tolist(), [1.0, 2.0])
        lazy = gotdms.scan_tdms(d)
        self.assertEqual(lazy.collect()["data/v"].to_list(), [0.0, 1.0, 2.0, 3.0])

    def test_contiguous_files_merge_descending(self):
        # f0 covers T0..T0-3s, f1 covers T0-4s..T0-7s.
        d = self._dir([T0, T0 - timedelta(seconds=4)], [-1.0, -1.0])
        with gotdms.open(directory=d) as s:
            full = s.read("v")
            self.assertEqual(full.meta["dt"], -1)
            self.assertEqual(full.y.tolist(), [float(i) for i in range(8)])
            self.assertEqual(full.time_ns.tolist(), [T0_NS - i * S for i in range(8)])
            for off, lim in [(0, 8), (1, 5), (3, 2), (6, 10)]:
                part = s.read("v", offset=off, limit=lim)
                self.assertEqual(part.y.tolist(), full.y.tolist()[off : off + lim], (off, lim))
                self.assertEqual(part.time_ns.tolist(), full.time_ns.tolist()[off : off + lim])
                self.assertEqual(part.meta["dt"], -1)

    def test_gap_from_after_to(self):
        d = self._dir([T0, T0 - timedelta(seconds=10)], [-1.0, -1.0])
        with gotdms.open(directory=d) as s:
            full = s.read("v")
            self.assertEqual(full.y.tolist(), [float(i) for i in range(8)])
            self.assertEqual(full.meta["gaps"], [{"from": "2025-12-31T23:59:56Z", "to": "2025-12-31T23:59:50Z"}])
            part = s.read("v", offset=0, limit=8)
            self.assertEqual(part.y.tolist(), full.y.tolist())
            self.assertEqual(part.meta.get("gaps"), full.meta["gaps"])

    def test_keep_last_descending(self):
        # f1 starts 2s before T0 and overlaps f0's last two samples.
        d = self._dir([T0, T0 - timedelta(seconds=2)], [-1.0, -1.0])
        with gotdms.open(directory=d, overlap="keep_last") as s:
            full = s.read("v")
            self.assertEqual(full.time_ns.tolist(), [T0_NS - i * S for i in range(6)])
            self.assertEqual(full.y.tolist(), [0.0, 1.0, 4.0, 5.0, 6.0, 7.0])
            part = s.read("v", offset=0, limit=6)
            self.assertEqual(part.y.tolist(), full.y.tolist())

    def test_mixed_signs_refused(self):
        d = self._dir([T0, T0 + timedelta(seconds=100)], [1.0, -1.0])
        with gotdms.open(directory=d) as s:
            with self.assertRaises(gotdms.ArgumentError):
                s.read("v")
            with self.assertRaises(gotdms.ArgumentError):
                s.read("v", offset=1, limit=3)


if __name__ == "__main__":
    unittest.main()
