"""Round-trip TDMS writes through the real gotdms-bridge binary."""

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


class WriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if pl is None:
            raise unittest.SkipTest("polars is not installed")
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)

    @classmethod
    def tearDownClass(cls):
        if pl is not None:
            cls.tmp.cleanup()

    def path(self, name="out.tdms"):
        return str(Path(self.tmp.name) / name)

    def test_dtypes_properties_waveforms_and_chunks(self):
        import gotdms

        when = datetime(2024, 6, 1, 12, 30, 15, 250000, tzinfo=timezone.utc)
        # A nanosecond that is not a whole microsecond. Polars keeps it; a
        # property datetime cannot.
        stamp = pl.Series(
            "when",
            [1_717_238_215_123_456_789, 1_717_238_216_000_000_000],
            dtype=pl.Datetime("ns", "UTC"),
        )
        naive = pl.Series(
            "naive",
            [
                datetime(2024, 1, 2, 3, 4, 5, 123456),
                datetime(2024, 1, 2, 3, 4, 6, 1),
            ],
        )
        micros = pl.Series(
            "micros",
            [
                datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc),
                datetime(2024, 1, 2, 3, 4, 6, 1, tzinfo=timezone.utc),
            ],
            dtype=pl.Datetime("us", "UTC"),
        )
        wide = 2**60 + 3
        types = pl.DataFrame(
            {
                "i8": pl.Series("i8", [-128, 127], dtype=pl.Int8),
                "note": pl.Series("note", ["", "keep"]),
                "i16": pl.Series("i16", [-32768, 32767], dtype=pl.Int16),
                "i32": pl.Series("i32", [-(2**31), 2**31 - 1], dtype=pl.Int32),
                "i64": pl.Series("i64", [-(2**63), wide], dtype=pl.Int64),
                "u8": pl.Series("u8", [0, 255], dtype=pl.UInt8),
                "u16": pl.Series("u16", [0, 65535], dtype=pl.UInt16),
                "u32": pl.Series("u32", [0, 2**32 - 1], dtype=pl.UInt32),
                "u64": pl.Series("u64", [0, 2**64 - 1], dtype=pl.UInt64),
                "f32": pl.Series("f32", [1.5, -2.0], dtype=pl.Float32),
                "f64": pl.Series("f64", [1.25, -0.5], dtype=pl.Float64),
                "flag": pl.Series("flag", [True, False], dtype=pl.Boolean),
                "when": stamp,
                "naive": naive,
                "micros": micros,
            }
        )
        log = pl.DataFrame({"message": ["start", "done"]})
        frac = (1 << 63) + 1
        result = gotdms.write_tdms(
            self.path("types.tdms"),
            {"types": types, "log": log},
            properties={
                "title": "cleaned",
                "revision": 3,
                "wide": wide,
                "flag": True,
                "gain": 1.5,
                "code": gotdms.Property(7, "uint16"),
                "narrow": gotdms.Property(-4, "int8"),
                "when": when,
                "exact": {"seconds": 5, "fraction": frac},
            },
            group_properties={"types": {"kind": "samples"}, "log": {"kind": "event"}},
            channel_properties={
                "types": {"f64": {"unit_string": "V", "note": "raw"}},
                "log": {"message": {"unit_string": "1"}},
            },
            waveforms={
                "types": {
                    "f64": {
                        "wf_start_time": when,
                        "wf_increment": 0.5,
                        "wf_start_offset": 0.25,
                    }
                }
            },
            chunk_bytes=8,
        )
        self.assertGreater(result.stream_chunks, 1)
        self.assertEqual(result.buffered_channels, 2)
        self.assertEqual(result.segments, 2)
        self.assertEqual(result.channel_count, types.width + log.width)

        got = gotdms.read_tdms(result.path)
        self.assertEqual(list(got.groups), ["types", "log"])
        self.assertEqual(got.properties["title"], "cleaned")
        self.assertEqual(got.properties["revision"], 3)
        self.assertEqual(got.properties["wide"], wide)
        self.assertIs(got.properties["flag"], True)
        self.assertEqual(got.properties["gain"], 1.5)
        self.assertEqual(got.properties["code"], 7)
        self.assertEqual(got.properties["narrow"], -4)
        self.assertEqual(got.properties["when"], when)
        typed = {item["name"]: item for item in got.typed_properties}
        self.assertEqual(typed["revision"]["type"], "int32")
        self.assertEqual(typed["wide"]["type"], "int64")
        self.assertEqual(typed["code"]["type"], "uint16")
        self.assertEqual(typed["narrow"]["type"], "int8")
        self.assertEqual(typed["flag"]["type"], "bool")
        self.assertEqual(typed["gain"]["type"], "float64")
        self.assertEqual(typed["exact"]["value"]["seconds"], 5)
        self.assertEqual(typed["exact"]["value"]["fraction"], frac)
        self.assertEqual(got.groups["types"].properties["kind"], "samples")

        channels = got.groups["types"].channels
        self.assertEqual(list(channels), list(types.columns))
        for name in ("i8", "i16", "i32", "i64", "u8", "u16", "u32", "u64", "flag", "note"):
            self.assertEqual(channels[name].values.to_list(), types.get_column(name).to_list())
            self.assertEqual(channels[name].values.dtype, types.get_column(name).dtype)
        self.assertEqual(channels["f32"].values.dtype, pl.Float32)
        self.assertEqual(channels["f32"].values.to_list(), [1.5, -2.0])
        self.assertEqual(channels["f64"].values.to_list(), [1.25, -0.5])
        self.assertEqual(channels["f64"].properties["unit_string"], "V")
        self.assertEqual(channels["f64"].properties["note"], "raw")
        self.assertEqual(channels["f64"].properties["wf_increment"], 0.5)
        self.assertEqual(channels["f64"].properties["wf_start_offset"], 0.25)
        self.assertEqual(channels["f64"].properties["wf_samples"], 2)
        self.assertEqual(channels["f64"].properties["wf_start_time"], when)
        wave = {item["name"]: item for item in channels["f64"].typed_properties}
        self.assertEqual(wave["wf_samples"]["type"], "int32")
        self.assertEqual(channels["when"].values.dtype, pl.Datetime("ns", "UTC"))
        self.assertEqual(channels["when"].values.cast(pl.Int64).to_list(), stamp.cast(pl.Int64).to_list())
        naive_ns = naive.dt.replace_time_zone("UTC").dt.cast_time_unit("ns")
        self.assertEqual(channels["naive"].values.cast(pl.Int64).to_list(), naive_ns.cast(pl.Int64).to_list())
        micros_ns = micros.dt.cast_time_unit("ns")
        self.assertEqual(channels["micros"].values.cast(pl.Int64).to_list(), micros_ns.cast(pl.Int64).to_list())
        self.assertEqual(channels["i64"].values.to_list()[1], wide)
        self.assertEqual(got.groups["log"].channels["message"].values.to_list(), ["start", "done"])
        self.assertEqual(got.groups["log"].channels["message"].properties["unit_string"], "1")

        with gotdms.open([result.path]) as session:
            y = session["types"]["f64"].data()
            np.testing.assert_array_equal(y, [1.25, -0.5])
            absolute = session["types"]["f64"].time_track(absolute_time=True)
            self.assertEqual(absolute[0], np.datetime64("2024-06-01T12:30:15.250000000"))
            self.assertEqual(absolute[1], np.datetime64("2024-06-01T12:30:15.750000000"))
            with self.assertRaises(gotdms.GotdmsError):
                session["log"]["message"].data()

    def test_goban_layout(self):
        import gotdms

        path = self.path("goban.tdms")
        result, when, frac = goban_write(path)
        self.assertEqual(result.buffered_channels, 2)
        self.assertEqual(result.segments, 2)
        got = gotdms.read_tdms(path)
        expect = goban_groups()
        self.assertEqual(list(got.groups), ["data", "uncertainties", "design", "log"])
        self.assertEqual(got.properties["title"], "cleaned run")
        self.assertEqual(got.properties["revision"], 4)
        self.assertIs(got.properties["approved"], False)
        self.assertEqual(got.properties["gain"], 0.125)
        self.assertEqual(got.properties["code"], 7)
        self.assertEqual(got.properties["when"], when)
        typed = {item["name"]: item for item in got.typed_properties}
        self.assertEqual(typed["code"]["type"], "uint16")
        self.assertEqual(typed["ratio"]["type"], "float32")
        self.assertEqual(typed["ratio"]["value"], 1.25)
        self.assertEqual(typed["exact"]["value"]["seconds"], 5)
        self.assertEqual(typed["exact"]["value"]["fraction"], frac)
        self.assertEqual(got.groups["data"].properties["role"], "samples")
        self.assertEqual(got.groups["log"].properties["role"], "event")
        data = got.groups["data"]
        self.assertEqual(data.channels["voltage"].values.to_list(), expect["data"]["voltage"])
        self.assertEqual(data.channels["current"].values.to_list(), expect["data"]["current"])
        self.assertEqual(data.channels["voltage"].properties["unit_string"], "V")
        self.assertEqual(data.channels["current"].properties["unit_string"], "A")
        self.assertEqual(data.channels["voltage"].properties["wf_increment"], 0.001)
        unc = got.groups["uncertainties"]
        self.assertEqual(unc.channels["voltage"].values.to_list(), expect["uncertainties"]["voltage"])
        self.assertEqual(unc.channels["voltage"].properties["unit_string"], "V")
        self.assertNotEqual(
            data.channels["voltage"].values.len(),
            got.groups["design"].channels["setpoint"].values.len(),
        )
        design = got.groups["design"]
        self.assertEqual(design.channels["setpoint"].values.to_list(), [10.0, 20.0])
        self.assertEqual(design.channels["label"].values.to_list(), ["low", "mid", "high"])
        self.assertEqual(design.channels["setpoint"].properties["unit_string"], "V")
        with self.assertRaises(gotdms.ArgumentError) as raised:
            design.frame()
        self.assertIn("different lengths", str(raised.exception))
        self.assertEqual(data.frame().columns, ["voltage", "current"])
        log = got.groups["log"]
        self.assertEqual(log.channels["message"].values.to_list(), ["opened", "cleaned", ""])
        self.assertEqual(log.channels["level"].values.to_list(), [1, 2, 2])
        self.assertEqual(log.channels["level"].values.dtype, pl.Int32)
        self.assertEqual(log.channels["message"].type, "string")
        self.assertNotEqual(log.channels["message"].values.len(), data.channels["voltage"].values.len())

    def test_append_keeps_earlier_samples_and_properties(self):
        import gotdms

        path = self.path("append.tdms")
        frame = pl.DataFrame({"v": pl.Series("v", [1.0, 2.0, 3.0])})
        gotdms.write_tdms(
            path,
            frame,
            properties={"title": "first"},
            channel_properties={"data": {"v": {"unit_string": "V"}}},
        )
        gotdms.write_tdms(path, pl.DataFrame({"v": [4.0, 5.0]}), mode="append")
        got = gotdms.read_tdms(path)
        self.assertEqual(got.groups["data"].channels["v"].values.to_list(), [1.0, 2.0, 3.0, 4.0, 5.0])
        self.assertEqual(got.groups["data"].channels["v"].properties["unit_string"], "V")
        self.assertEqual(got.properties["title"], "first")

        with self.assertRaises(gotdms.ArgumentError):
            gotdms.write_tdms(
                path,
                pl.DataFrame({"v": pl.Series("v", [9], dtype=pl.Int32)}),
                mode="append",
            )
        again = gotdms.read_tdms(path)
        self.assertEqual(again.groups["data"].channels["v"].values.to_list(), [1.0, 2.0, 3.0, 4.0, 5.0])

        missing = self.path("missing.tdms")
        with self.assertRaises(gotdms.ArgumentError) as raised:
            gotdms.write_tdms(missing, frame, mode="append")
        self.assertIn("existing", str(raised.exception).lower())

        junk = self.path("junk.tdms")
        Path(junk).write_bytes(b"this is not a tdms file")
        with self.assertRaises(gotdms.CorruptFileError):
            gotdms.write_tdms(junk, frame, mode="append")
        self.assertEqual(Path(junk).read_bytes(), b"this is not a tdms file")

    def test_rejections_leave_an_existing_file_alone(self):
        import gotdms

        path = self.path("keep.tdms")
        Path(path).write_bytes(b"original")
        nulls = pl.DataFrame({"v": [1.0, None]})
        with self.assertRaises(gotdms.ArgumentError) as raised:
            gotdms.write_tdms(path, nulls)
        self.assertIn("not nullable", str(raised.exception))
        self.assertIn("not written as zero", str(raised.exception))
        self.assertEqual(Path(path).read_bytes(), b"original")

        blanks = pl.DataFrame({"note": ["ok", None]})
        with self.assertRaises(gotdms.ArgumentError) as raised:
            gotdms.write_tdms(path, blanks)
        self.assertIn("empty strings", str(raised.exception))
        self.assertEqual(Path(path).read_bytes(), b"original")

        nested = pl.DataFrame({"v": [[1, 2], [3]]})
        with self.assertRaises(gotdms.ArgumentError) as raised:
            gotdms.write_tdms(path, nested)
        self.assertIn("does not coerce", str(raised.exception))
        self.assertEqual(Path(path).read_bytes(), b"original")

        with self.assertRaises(gotdms.ArgumentError):
            gotdms.write_tdms(
                path,
                {"data": pl.DataFrame({"v": [1.0]})},
                group="other",
            )
        with self.assertRaises(gotdms.ArgumentError):
            gotdms.write_tdms(
                path,
                pl.DataFrame({"v": [1.0]}),
                channel_properties={"nope": {"v": {"unit_string": "V"}}},
            )
        with self.assertRaises(gotdms.ArgumentError):
            gotdms.write_tdms(
                path,
                pl.DataFrame({"note": ["x"]}),
                waveforms={"data": {"note": {"wf_increment": 1.0}}},
            )
        with self.assertRaises(gotdms.ArgumentError) as raised:
            gotdms.write_tdms(
                path,
                pl.DataFrame({"v": [1.0]}),
                channel_properties={"data": {"v": {"wf_increment": 1.0}}},
                waveforms={"data": {"v": {"wf_increment": 2.0}}},
            )
        self.assertIn("both", str(raised.exception))
        with self.assertRaises(gotdms.ArgumentError):
            gotdms.write_tdms(path, pl.DataFrame({"v": [float("nan")]}), properties={"gain": float("nan")})
        self.assertEqual(Path(path).read_bytes(), b"original")

    def test_empty_column_is_void(self):
        import gotdms

        path = self.path("empty.tdms")
        gotdms.write_tdms(
            path,
            {
                "data": {
                    "empty": pl.Series("empty", [], dtype=pl.Float64),
                    "v": pl.Series("v", [1.0], dtype=pl.Float64),
                }
            },
        )
        got = gotdms.read_tdms(path)
        empty = got.groups["data"].channels["empty"]
        self.assertEqual(empty.type, "void")
        self.assertEqual(empty.values.len(), 0)
        self.assertEqual(empty.values.dtype, pl.Null)
        self.assertEqual(got.groups["data"].channels["v"].values.to_list(), [1.0])

    def test_interrupted_append_leaves_file_unchanged(self):
        import gotdms
        from gotdms import tdmsfile

        path = self.path("interrupted.tdms")
        gotdms.write_tdms(path, pl.DataFrame({"v": [1.0, 2.0, 3.0]}))
        before = Path(path).read_bytes()
        real = tdmsfile._series_ipc
        calls = []

        def fail_second(series):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError("caller gave up")
            return real(series)

        tdmsfile._series_ipc = fail_second
        try:
            with self.assertRaises(RuntimeError):
                gotdms.write_tdms(path, pl.DataFrame({"v": [4.0, 5.0, 6.0]}), mode="append", chunk_bytes=8)
        finally:
            tdmsfile._series_ipc = real
        self.assertEqual(Path(path).read_bytes(), before)
        self.assertEqual(gotdms.read_tdms(path).groups["data"].channels["v"].values.to_list(), [1.0, 2.0, 3.0])
        gotdms.write_tdms(path, pl.DataFrame({"v": [4.0]}), mode="append")
        self.assertEqual(
            gotdms.read_tdms(path).groups["data"].channels["v"].values.to_list(), [1.0, 2.0, 3.0, 4.0]
        )

    @unittest.skipIf(os.name == "nt", "POSIX file modes")
    def test_create_follows_umask_and_keeps_mode(self):
        import gotdms

        path = self.path("mode.tdms")
        old = os.umask(0o022)
        try:
            gotdms.write_tdms(path, pl.DataFrame({"v": [1.0]}))
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o644)
            os.chmod(path, 0o640)
            gotdms.write_tdms(path, pl.DataFrame({"v": [2.0]}))
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o640)
        finally:
            os.umask(old)

    def test_single_frame_group_name(self):
        import gotdms

        path = self.path("one.tdms")
        gotdms.write_tdms(path, pl.DataFrame({"v": [1]}), group="acq")
        got = gotdms.read_tdms(path)
        self.assertEqual(list(got.groups), ["acq"])
        self.assertEqual(got.groups["acq"].channels["v"].values.to_list(), [1])


def goban_groups():
    return {
        "data": {
            "voltage": [1.0, 1.5, 1.25, 1.75],
            "current": [0.10, 0.20, 0.15, 0.25],
        },
        "uncertainties": {
            "voltage": [0.01, 0.02, 0.01],
            "current": [0.001, 0.002, 0.001],
        },
        "design": {
            "setpoint": [10.0, 20.0],
            "label": ["low", "mid", "high"],
        },
        "log": {
            "message": ["opened", "cleaned", ""],
            "level": [1, 2, 2],
        },
    }


def goban_write(path):
    """Write data + uncertainties + design + log, the cleaned-export layout.

    Uncertainties are shorter than data. Design channels do not share a
    length. The log group holds strings. ``unit_string`` is set per channel.
    """
    import gotdms

    groups = goban_groups()
    data = pl.DataFrame(
        {
            "voltage": pl.Series("voltage", groups["data"]["voltage"], dtype=pl.Float64),
            "current": pl.Series("current", groups["data"]["current"], dtype=pl.Float64),
        }
    )
    uncertainties = pl.DataFrame(
        {
            "voltage": pl.Series("voltage", groups["uncertainties"]["voltage"], dtype=pl.Float64),
            "current": pl.Series("current", groups["uncertainties"]["current"], dtype=pl.Float64),
        }
    )
    design = {
        "setpoint": pl.Series("setpoint", groups["design"]["setpoint"], dtype=pl.Float64),
        "label": pl.Series("label", groups["design"]["label"]),
    }
    log = pl.DataFrame(
        {
            "message": groups["log"]["message"],
            "level": pl.Series("level", groups["log"]["level"], dtype=pl.Int32),
        }
    )
    when = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    frac = (1 << 63) + 1
    result = gotdms.write_tdms(
        path,
        {
            "data": data,
            "uncertainties": uncertainties,
            "design": design,
            "log": log,
        },
        properties={
            "title": "cleaned run",
            "revision": 4,
            "approved": False,
            "gain": 0.125,
            "when": when,
            "code": gotdms.Property(7, "uint16"),
            "ratio": gotdms.Property(1.25, "float32"),
            "exact": {"seconds": 5, "fraction": frac},
        },
        group_properties={
            "data": {"role": "samples"},
            "uncertainties": {"role": "uncertainty"},
            "design": {"role": "design"},
            "log": {"role": "event"},
        },
        channel_properties={
            "data": {
                "voltage": {"unit_string": "V"},
                "current": {"unit_string": "A"},
            },
            "uncertainties": {
                "voltage": {"unit_string": "V"},
                "current": {"unit_string": "A"},
            },
            "design": {"setpoint": {"unit_string": "V"}},
            "log": {"level": {"unit_string": "1"}},
        },
        waveforms={
            "data": {
                "voltage": {
                    "wf_start_time": when,
                    "wf_increment": 0.001,
                    "wf_start_offset": 0.0,
                }
            }
        },
    )
    return result, when, frac
