"""npTDMS reads files this package wrote.

npTDMS is LGPL-3.0-only. It is a CI oracle for the bytes on disk. It is not
imported by the gotdms package, it is not a runtime dependency, and it is
not shipped. This module skips when npTDMS is not installed.
"""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))

try:
    from nptdms import TdmsFile
    from nptdms.timestamp import TdmsTimestamp
except ImportError:
    TdmsFile = None
    TdmsTimestamp = None

try:
    from test_write import goban_groups, goban_write
except ImportError:  # loaded as python.tests.test_write_nptdms
    from .test_write import goban_groups, goban_write


def _plain(value):
    if isinstance(value, np.generic):
        return value.item()
    if TdmsTimestamp is not None and isinstance(value, TdmsTimestamp):
        return value
    return value


@unittest.skipIf(TdmsFile is None, "npTDMS is a CI-only LGPL oracle and is not a runtime dependency")
class NptdmsOracleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / "gotdms-bridge"
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_nonfinite_properties_read(self):
        """NaN and infinite float properties come back as floats, not a dead bridge."""
        import math

        import gotdms
        from nptdms import ChannelObject, GroupObject, RootObject, TdmsWriter

        path = str(Path(self.tmp.name) / "nonfinite.tdms")
        with TdmsWriter(path) as w:
            w.write_segment(
                [
                    RootObject(properties={"nan": float("nan")}),
                    GroupObject("data", properties={"low": float("-inf")}),
                    ChannelObject(
                        "data",
                        "v",
                        np.array([1.0, 2.0]),
                        properties={"high": float("inf"), "gain": 1.5},
                    ),
                ]
            )
        got = gotdms.read_tdms(path)
        self.assertTrue(math.isnan(got.properties["nan"]))
        self.assertEqual(got.groups["data"].properties["low"], float("-inf"))
        ch = got.groups["data"].channels["v"]
        self.assertEqual(ch.properties["high"], float("inf"))
        self.assertEqual(ch.properties["gain"], 1.5)
        self.assertEqual(ch.values.to_list(), [1.0, 2.0])
        with gotdms.open([path]) as session:
            props = session.channel("v").properties
            self.assertEqual(props["high"], float("inf"))
            self.assertEqual(session.read("v").samples, 2)

    def test_goban_layout_matches_nptdms(self):
        path = str(Path(self.tmp.name) / "goban.tdms")
        _result, when, frac = goban_write(path)
        expect = goban_groups()

        with TdmsFile.read(path, raw_timestamps=True) as tdms:
            self.assertEqual(tdms.tdms_version, 4713)
            props = tdms.properties
            self.assertEqual(props["title"], "cleaned run")
            self.assertEqual(_plain(props["revision"]), 4)
            self.assertIs(_plain(props["approved"]), False)
            self.assertEqual(_plain(props["gain"]), 0.125)
            self.assertEqual(_plain(props["code"]), 7)
            self.assertEqual(_plain(props["ratio"]), 1.25)
            self.assertIsInstance(props["when"], TdmsTimestamp)
            self.assertEqual(props["when"].as_datetime(), when.replace(tzinfo=None))
            self.assertIsInstance(props["exact"], TdmsTimestamp)
            self.assertEqual(props["exact"].seconds, 5)
            self.assertEqual(props["exact"].second_fractions, frac)

            self.assertEqual(
                [group.name for group in tdms.groups()],
                ["data", "uncertainties", "design", "log"],
            )
            self.assertEqual(_plain(tdms["data"].properties["role"]), "samples")
            self.assertEqual(_plain(tdms["uncertainties"].properties["role"]), "uncertainty")
            self.assertEqual(_plain(tdms["design"].properties["role"]), "design")
            self.assertEqual(_plain(tdms["log"].properties["role"]), "event")

            voltage = tdms["data"]["voltage"]
            current = tdms["data"]["current"]
            np.testing.assert_array_equal(voltage.read_data(scaled=False), expect["data"]["voltage"])
            np.testing.assert_array_equal(current.read_data(scaled=False), expect["data"]["current"])
            self.assertEqual(_plain(voltage.properties["unit_string"]), "V")
            self.assertEqual(_plain(current.properties["unit_string"]), "A")
            self.assertEqual(_plain(voltage.properties["wf_increment"]), 0.001)
            self.assertEqual(_plain(voltage.properties["wf_start_offset"]), 0.0)
            self.assertEqual(_plain(voltage.properties["wf_samples"]), 4)
            self.assertEqual(voltage.properties["wf_start_time"].as_datetime(), when.replace(tzinfo=None))

            unc_v = tdms["uncertainties"]["voltage"]
            unc_i = tdms["uncertainties"]["current"]
            np.testing.assert_array_equal(unc_v.read_data(scaled=False), expect["uncertainties"]["voltage"])
            np.testing.assert_array_equal(unc_i.read_data(scaled=False), expect["uncertainties"]["current"])
            self.assertEqual(_plain(unc_v.properties["unit_string"]), "V")
            self.assertEqual(_plain(unc_i.properties["unit_string"]), "A")
            self.assertNotEqual(len(voltage), len(tdms["design"]["setpoint"]))

            setpoint = tdms["design"]["setpoint"]
            label = tdms["design"]["label"]
            np.testing.assert_array_equal(setpoint.read_data(scaled=False), expect["design"]["setpoint"])
            self.assertEqual(label.read_data(scaled=False).tolist(), expect["design"]["label"])
            self.assertEqual(_plain(setpoint.properties["unit_string"]), "V")
            self.assertNotEqual(len(setpoint), len(label))

            message = tdms["log"]["message"]
            level = tdms["log"]["level"]
            self.assertEqual(message.read_data(scaled=False).tolist(), expect["log"]["message"])
            np.testing.assert_array_equal(level.read_data(scaled=False), expect["log"]["level"])
            self.assertEqual(_plain(level.properties["unit_string"]), "1")
            self.assertNotEqual(len(message), len(voltage))
