"""DAQmx raw reads. The fixture is hand-built bytes.

npTDMS is imported only as an oracle when it is installed. Its source is not
copied into this repository, and the package does not depend on it.
"""

import os
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


def _prop_str(name, value):
    raw = value.encode()
    return _prop(name, 0x20, _u32(len(raw)) + raw)


def _prop_f64(name, value):
    return _prop(name, 10, _f64(value))


def _prop_u32(name, value):
    return _prop(name, 7, _u32(value))


def _scaler_format(typ, buf, off, scale):
    return _u32(typ) + _u32(buf) + _u32(off) + _u32(0) + _u32(scale)


def _scaler_digital(typ, buf, bit, scale):
    return _u32(typ) + _u32(buf) + _u32(bit) + bytes([0]) + _u32(scale)


def _index(header, count, scaler, n_scaler, widths):
    body = _u32(header) + _u32(0xFFFFFFFF) + _u32(1) + _u64(count) + _u32(n_scaler)
    body += scaler
    body += _u32(len(widths))
    for width in widths:
        body += _u32(width)
    return body


def _wave():
    return [
        _prop("wf_start_time", 0x44, bytes(16)),
        _prop_f64("wf_increment", 1.0),
    ]


def _linear_wave():
    # Scale 1 is the output. Input source 0 has no NI_Scale type, so it is
    # the raw scaler whose scale id is 0. That is the NI linear example.
    return _wave() + [
        _prop_str("NI_Scaling_Status", "unscaled"),
        _prop_u32("NI_Number_Of_Scales", 2),
        _prop_str("NI_Scale[1]_Scale_Type", "Linear"),
        _prop_f64("NI_Scale[1]_Linear_Slope", 2.0),
        _prop_f64("NI_Scale[1]_Linear_Y_Intercept", 0.5),
        _prop_u32("NI_Scale[1]_Linear_Input_Source", 0),
    ]


def _object(path, index, props):
    body = _u32(len(path)) + path.encode() + index + _u32(len(props))
    for prop in props:
        body += prop
    return body


def daqmx_analog_digital():
    """int16 ai0 with a linear DAQmx scale, and a digital line0 of 0/1.

    Raw buffer 0 is three little-endian int16 values. Buffer 1 is three
    bytes whose low bit is the digital sample. This layout is the NI raw
    index, written here rather than taken from npTDMS.
    """
    widths = (2, 1)
    ai = _object(
        "/'acq'/'ai0'",
        _index(0x1269, 3, _scaler_format(3, 0, 0, 0), 1, widths),
        _linear_wave(),
    )
    line = _object(
        "/'acq'/'line0'",
        _index(0x126A, 3, _scaler_digital(0, 1, 0, 0), 1, widths),
        _wave(),
    )
    meta = _u32(2) + ai + line
    raw = bytes([0x01, 0x00, 0x02, 0x00, 0xFF, 0xFF, 0x01, 0x00, 0x03])
    toc = (1 << 1) | (1 << 2) | (1 << 3) | (1 << 7)
    lead = b"TDSm" + _u32(toc) + _u32(4713) + _u64(len(meta) + len(raw)) + _u64(len(meta))
    return lead + meta + raw


@unittest.skipIf(pl is None, "polars is not installed")
class DAQmxTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        cls.path = Path(cls.tmp.name) / "daq.tdms"
        cls.path.write_bytes(daqmx_analog_digital())
        cls.t0 = datetime(1904, 1, 1, tzinfo=timezone.utc)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_read_tdms_native_types(self):
        import gotdms

        doc = gotdms.read_tdms(self.path)
        ai = doc.groups["acq"].channels["ai0"]
        line = doc.groups["acq"].channels["line0"]
        self.assertEqual(ai.ni_type, "tdsTypeI16")
        self.assertEqual(ai.values.dtype, pl.Int16)
        self.assertEqual(ai.values.to_list(), [1, 2, -1])
        self.assertEqual(line.ni_type, "tdsTypeU8")
        self.assertEqual(line.values.dtype, pl.UInt8)
        self.assertEqual(line.values.to_list(), [1, 0, 1])

    def test_open_read_scan_keep_raw_native_types(self):
        import gotdms

        with gotdms.open(paths=[self.path]) as session:
            ai = session.channel("ai0", "acq")
            line = session.channel("line0", "acq")
            self.assertTrue(ai.daqmx)
            self.assertFalse(ai.digital)
            self.assertEqual(ai.data_type, "tdsTypeI16")
            self.assertTrue(ai.has_scale)
            self.assertTrue(line.daqmx)
            self.assertTrue(line.digital)
            self.assertEqual(line.data_type, "tdsTypeU8")
            self.assertFalse(line.has_scale)

            raw = ai.data()
            self.assertEqual(raw.dtype, np.dtype("int16"))
            np.testing.assert_array_equal(raw, np.array([1, 2, -1], dtype=np.int16))
            self.assertFalse(session.read("ai0", "acq").scaled)
            bits = line.data()
            self.assertEqual(bits.dtype, np.dtype("uint8"))
            np.testing.assert_array_equal(bits, np.array([1, 0, 1], dtype=np.uint8))

            frame = session["acq"].as_polars()
            self.assertEqual(frame.schema["acq/ai0"], pl.Int16)
            self.assertEqual(frame.schema["acq/line0"], pl.UInt8)
            self.assertEqual(frame["acq/ai0"].to_list(), [1, 2, -1])
            self.assertEqual(frame["acq/line0"].to_list(), [1, 0, 1])
            self.assertEqual(frame.schema["time"], pl.Datetime("ns", "UTC"))

            window = session.read(
                "ai0",
                "acq",
                start=self.t0 + timedelta(seconds=1),
                end=self.t0 + timedelta(seconds=3),
            )
            np.testing.assert_array_equal(window.y, np.array([2, -1], dtype=np.int16))
            limited = session.read("line0", "acq", limit=2)
            np.testing.assert_array_equal(limited.y, np.array([1, 0], dtype=np.uint8))

            scaled = session.read("ai0", "acq", scaled=True)
            self.assertTrue(scaled.scaled)
            self.assertEqual(scaled.y.dtype, np.dtype("float64"))
            np.testing.assert_allclose(scaled.y, [2.5, 4.5, -1.5])
            with self.assertRaises(gotdms.NoScaleError):
                session.read("line0", "acq", scaled=True)

        scanned = gotdms.scan_tdms(self.path)
        self.assertEqual(scanned.gotdms_sample_reads, 0)
        collected = scanned.collect()
        self.assertEqual(collected.schema["acq/ai0"], pl.Int16)
        self.assertEqual(collected.schema["acq/line0"], pl.UInt8)
        self.assertEqual(collected["acq/line0"].to_list(), [1, 0, 1])
        head = gotdms.scan_tdms(self.path).select("acq/line0").head(2).collect()
        self.assertEqual(head.schema["acq/line0"], pl.UInt8)
        self.assertEqual(head["acq/line0"].to_list(), [1, 0])
        scaled_scan = gotdms.scan_tdms(self.path, scaled=True).select("acq/ai0").collect()
        self.assertEqual(scaled_scan.schema["acq/ai0"], pl.Float64)
        self.assertEqual(scaled_scan["acq/ai0"].to_list(), [2.5, 4.5, -1.5])

    def test_nptdms_oracle_when_installed(self):
        if TdmsFile is None:
            self.skipTest("nptdms is not installed")
        tdms = TdmsFile.read(self.path)
        # npTDMS returns one array per scaler id. The bridge column is the
        # primary scaler, which is scale id 0 in this file.
        ai = tdms["acq"]["ai0"].read_data(scaled=False)[0]
        line = tdms["acq"]["line0"].read_data(scaled=False)[0]
        np.testing.assert_array_equal(ai, np.array([1, 2, -1], dtype=np.int16))
        np.testing.assert_array_equal(line, np.array([1, 0, 1], dtype=np.uint8))
        scaled = np.asarray(tdms["acq"]["ai0"].read_data(scaled=True), dtype=np.float64)
        np.testing.assert_allclose(scaled, [2.5, 4.5, -1.5])


if __name__ == "__main__":
    unittest.main()
