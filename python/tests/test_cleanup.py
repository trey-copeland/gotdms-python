"""Bad-file errors, session cleanup, stderr bounds, and timestamp parsing.

Fixtures are written by gotdms itself and then damaged byte by byte.
"""

import gc
import os
import struct
import subprocess
import sys
import tempfile
import unittest
import weakref
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))

try:
    import polars as pl
except ImportError:  # pragma: no cover - skipped only when polars is absent
    pl = None

_LEAD = 28


class CleanupTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if pl is None:
            raise unittest.SkipTest("polars is not installed")
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        cls.bin = root / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"], cwd=ROOT)
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)

        import gotdms

        good = root / "good.tdms"
        gotdms.write_tdms(
            good,
            pl.DataFrame({"ai0": [1.0, 2.0, 3.0, 4.0], "ai1": [5.0, 6.0, 7.0, 8.0]}),
            group="g",
        )
        cls.good = good
        data = good.read_bytes()
        cls.bad = {}

        truncated = root / "truncated.tdms"
        truncated.write_bytes(data[: len(data) - 5])
        cls.bad["truncated"] = truncated

        bad_tag = root / "bad_tag.tdms"
        bad_tag.write_bytes(b"XXXX" + data[4:])
        cls.bad["bad_tag"] = bad_tag

        # The first uint32 of the metadata is the object count. A huge count
        # runs the object list past the raw-data offset.
        count = bytearray(data)
        struct.pack_into("<I", count, _LEAD, 0x7FFFFFFF)
        bad_count = root / "bad_count.tdms"
        bad_count.write_bytes(bytes(count))
        cls.bad["bad_count"] = bad_count

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_good_fixture_reads(self):
        import gotdms

        got = gotdms.read_tdms(self.good)
        self.assertEqual(got.groups["g"].channels["ai0"].values.to_list(), [1.0, 2.0, 3.0, 4.0])

    def test_bad_files_raise_corrupt_file_error_everywhere(self):
        import gotdms

        calls = {
            "open": lambda p: gotdms.open([p]).close(),
            "scan_tdms": lambda p: gotdms.scan_tdms(p).collect(),
            "read_tdms": lambda p: gotdms.read_tdms(p),
        }
        for kind, path in self.bad.items():
            for name, call in calls.items():
                with self.subTest(file=kind, call=name):
                    with self.assertRaises(gotdms.CorruptFileError) as raised:
                        call(str(path))
                    self.assertEqual(raised.exception.code, "corrupt")
                    self.assertIsInstance(raised.exception, gotdms.GotdmsError)
                    self.assertNotIsInstance(raised.exception, gotdms.ArgumentError)

    def test_append_to_bad_file_is_corrupt(self):
        import gotdms

        with self.assertRaises(gotdms.CorruptFileError):
            gotdms.write_tdms(self.bad["bad_tag"], pl.DataFrame({"x": [1.0]}), group="g", mode="append")

    def test_missing_file_is_still_an_argument_error(self):
        import gotdms

        missing = str(Path(self.tmp.name) / "nope.tdms")
        calls = {
            "open": lambda p: gotdms.open([p]).close(),
            "scan_tdms": lambda p: gotdms.scan_tdms(p).collect(),
            "read_tdms": lambda p: gotdms.read_tdms(p),
        }
        for name, call in calls.items():
            with self.subTest(call=name):
                with self.assertRaises(gotdms.ArgumentError):
                    call(missing)

    def test_unclosed_session_stops_its_bridge(self):
        import gotdms

        def leak():
            session = gotdms.open([str(self.good)])
            session.catalog()
            return session._proc, weakref.ref(session)

        proc, ref = leak()
        gc.collect()
        self.assertIsNone(ref(), "an unclosed Session must be collectable")
        self.assertIsNotNone(proc.wait(timeout=10), "the bridge must exit after the Session is collected")

    def test_close_is_idempotent_and_reaps(self):
        import gotdms

        session = gotdms.open([str(self.good)])
        proc = session._proc
        session.close()
        session.close()
        self.assertIsNotNone(proc.poll())
        with self.assertRaises(gotdms.GotdmsError):
            session.catalog()


    def test_finalizer_does_not_race_an_in_flight_request(self):
        # At interpreter exit a daemon thread can still hold the pipe lock.
        # The finalizer must not write a close frame under it, and must not
        # wait for it forever.
        import time

        import gotdms
        from gotdms._process import LOCK_TIMEOUT

        session = gotdms.open([str(self.good)])
        proc = session._proc
        session._lock.acquire()
        try:
            start = time.monotonic()
            session._finalizer()
            elapsed = time.monotonic() - start
        finally:
            session._lock.release()
        self.assertIsNotNone(proc.poll(), "the bridge must exit when its stdin closes")
        self.assertLess(elapsed, LOCK_TIMEOUT + 5)
        session.close()

    def test_unclosed_session_exits_quietly_at_interpreter_exit(self):
        script = (
            "import sys; sys.path.insert(0, sys.argv[1]); import gotdms; "
            "s = gotdms.open([sys.argv[2]]); s.catalog(); print(s._proc.pid, flush=True)"
        )
        out = subprocess.run(
            [sys.executable, "-c", script, str(ROOT / "python" / "src"), str(self.good)],
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
        # Exit must be quiet: no exception from the finalizer or the drain
        # thread during shutdown, and no bridge left behind.
        self.assertEqual(out.stderr, "")
        pid = int(out.stdout.split()[0])
        if os.name != "nt":
            with self.assertRaises(ProcessLookupError):
                os.kill(pid, 0)


class StderrTailTest(unittest.TestCase):
    def test_keeps_the_last_64_kib(self):
        from gotdms._process import STDERR_LIMIT, StderrTail

        self.assertEqual(STDERR_LIMIT, 64 * 1024)
        tail = StderrTail()
        for i in range(40):
            tail.append(bytes([ord("a") + i % 26]) * 4096)
        tail.append(b"last words")
        self.assertEqual(len(tail), STDERR_LIMIT)
        self.assertTrue(tail.text().endswith("last words"))

    def test_small_output_is_kept_whole(self):
        from gotdms._process import StderrTail

        tail = StderrTail()
        tail.append(b"  panic: boom\n")
        self.assertEqual(tail.text(), "panic: boom")


class TimestampWireTest(unittest.TestCase):
    def test_fraction_lengths(self):
        from gotdms.tdmsfile import _datetime_from_wire

        utc = timezone.utc
        cases = {
            "2024-06-01T12:30:15Z": datetime(2024, 6, 1, 12, 30, 15, tzinfo=utc),
            "2024-06-01T12:30:15.25Z": datetime(2024, 6, 1, 12, 30, 15, 250000, tzinfo=utc),
            "1904-01-01T00:00:05.5+00:00": datetime(1904, 1, 1, 0, 0, 5, 500000, tzinfo=utc),
            "2024-06-01T12:30:15.1234Z": datetime(2024, 6, 1, 12, 30, 15, 123400, tzinfo=utc),
            "2024-06-01T12:30:15.123456Z": datetime(2024, 6, 1, 12, 30, 15, 123456, tzinfo=utc),
            "2024-06-01T12:30:15.123456789Z": datetime(2024, 6, 1, 12, 30, 15, 123456, tzinfo=utc),
        }
        for text, want in cases.items():
            with self.subTest(text=text):
                got = _datetime_from_wire(text)
                self.assertEqual(got, want)
                self.assertEqual(got.utcoffset().total_seconds(), 0)

    def test_fraction_with_offsets(self):
        from datetime import timedelta

        from gotdms.tdmsfile import _datetime_from_wire

        minus5 = timezone(timedelta(hours=-5))
        plus530 = timezone(timedelta(hours=5, minutes=30))
        cases = {
            "2024-06-01T07:30:15.25-05:00": datetime(2024, 6, 1, 7, 30, 15, 250000, tzinfo=minus5),
            "2024-06-01T07:30:15-05:00": datetime(2024, 6, 1, 7, 30, 15, tzinfo=minus5),
            "2024-06-01T18:00:15.123456789+05:30": datetime(2024, 6, 1, 18, 0, 15, 123456, tzinfo=plus530),
        }
        for text, want in cases.items():
            with self.subTest(text=text):
                got = _datetime_from_wire(text)
                self.assertEqual(got, want)
                self.assertEqual(got.utcoffset(), want.utcoffset())


if __name__ == "__main__":
    unittest.main()
