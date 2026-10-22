"""Interleaved channels that continue in a later segment.

The bytes are built here. npTDMS is an optional oracle and is not copied
into this package.
"""

import os
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))

try:
    import polars as pl
except ImportError:  # pragma: no cover - skipped only when polars is absent
    pl = None

try:
    from nptdms import TdmsFile
except ImportError:  # pragma: no cover - CI installs nptdms; local runs may not
    TdmsFile = None


def _u32(v):
    return struct.pack("<I", v)


def _u64(v):
    return struct.pack("<Q", v)


def _f64(v):
    return struct.pack("<d", v)


def _prop(name, type_code, value):
    return _u32(len(name)) + name.encode() + _u32(type_code) + value


def _wave():
    # Epoch 1904-01-01 and a one-second step, so open and scan have a clock.
    return [
        _prop("wf_start_time", 0x44, bytes(16)),
        _prop("wf_increment", 10, _f64(1.0)),
    ]


def _channel(path, count, props):
    # Standard raw index: length 20, float64, one dimension, then the count.
    index = _u32(20) + _u32(10) + _u32(1) + _u64(count)
    body = _u32(len(path)) + path.encode() + index + _u32(len(props))
    for prop in props:
        body += prop
    return body


def _segment(pairs, props):
    count = len(pairs[0][1])
    meta = _u32(len(pairs))
    for path, _vals in pairs:
        meta += _channel(path, count, props)
    raw = b""
    for i in range(count):
        for _path, vals in pairs:
            raw += _f64(vals[i])
    # Metadata, a new object list, raw data, and interleaved samples.
    toc = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 5)
    lead = b"TDSm" + _u32(toc) + _u32(4713) + _u64(len(meta) + len(raw)) + _u64(len(meta))
    return lead + meta + raw


def interleaved_two_segments():
    """Two interleaved segments. The second segment is one sample.

    A scatter that drops the channel's base index writes 4 and 40 at the
    start of each channel instead of after 1, 2, 3 and 10, 20, 30.
    """
    first = _segment(
        [
            ("/'acq'/'a'", [1.0, 2.0, 3.0]),
            ("/'acq'/'b'", [10.0, 20.0, 30.0]),
        ],
        _wave(),
    )
    second = _segment(
        [
            ("/'acq'/'a'", [4.0]),
            ("/'acq'/'b'", [40.0]),
        ],
        _wave(),
    )
    return first + second


@unittest.skipIf(pl is None, "polars is not installed")
class InterleavedMultiSegmentTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        cls.path = Path(cls.tmp.name) / "ileave.tdms"
        cls.path.write_bytes(interleaved_two_segments())

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_read_tdms_keeps_later_segment(self):
        import gotdms

        doc = gotdms.read_tdms(self.path)
        a = doc.groups["acq"].channels["a"]
        b = doc.groups["acq"].channels["b"]
        self.assertEqual(a.ni_type, "tdsTypeDoubleFloat")
        self.assertEqual(a.values.to_list(), [1.0, 2.0, 3.0, 4.0])
        self.assertEqual(b.values.to_list(), [10.0, 20.0, 30.0, 40.0])

    def test_open_read_and_scan(self):
        import gotdms

        with gotdms.open(paths=[self.path]) as session:
            ay = session.read("a", "acq").y
            by = session.read("b", "acq").y
            np.testing.assert_array_equal(ay, np.array([1.0, 2.0, 3.0, 4.0]))
            np.testing.assert_array_equal(by, np.array([10.0, 20.0, 30.0, 40.0]))
            frame = session["acq"].as_polars()
            self.assertEqual(frame["acq/a"].to_list(), [1.0, 2.0, 3.0, 4.0])
            self.assertEqual(frame["acq/b"].to_list(), [10.0, 20.0, 30.0, 40.0])

        scanned = gotdms.scan_tdms(self.path).collect()
        self.assertEqual(scanned["acq/a"].to_list(), [1.0, 2.0, 3.0, 4.0])
        self.assertEqual(scanned["acq/b"].to_list(), [10.0, 20.0, 30.0, 40.0])

    def test_nptdms_oracle_when_installed(self):
        if TdmsFile is None:
            self.skipTest("nptdms is not installed")
        tdms = TdmsFile.read(self.path)
        np.testing.assert_array_equal(
            np.asarray(tdms["acq"]["a"].read_data(scaled=False), dtype=np.float64),
            [1.0, 2.0, 3.0, 4.0],
        )
        np.testing.assert_array_equal(
            np.asarray(tdms["acq"]["b"].read_data(scaled=False), dtype=np.float64),
            [10.0, 20.0, 30.0, 40.0],
        )


if __name__ == "__main__":
    unittest.main()
