"""Protocol 2 data replies: parts, shared clocks, mid-stream errors, and a
session that stops cleanly when a reply is cut off.

The bridge tests run the real binary with ``GOTDMS_BRIDGE_PART_BYTES`` set
small, so a tiny read still spans hundreds of part frames. The fault tests
use a fake bridge script, since the real one has no way to fail on cue.
"""

import json
import signal
import threading
import time
import os
import stat
import struct
import subprocess
import sys
import tempfile
import textwrap
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


class BridgePartsTest(unittest.TestCase):
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

        n = 5000
        start = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        wave = {"wf_start_time": start, "wf_increment": 1e-3}
        cls.paths = []
        for k in range(2):
            path = root / f"run{k}.tdms"
            gotdms.write_tdms(
                path,
                pl.DataFrame({
                    "a": np.arange(n, dtype=np.float64) + k * n,
                    "b": -np.arange(n, dtype=np.float64),
                    "c": np.arange(n, dtype=np.int16),
                    "slow": np.arange(n, dtype=np.float64),
                }),
                group="g",
                waveforms={"g": {
                    "a": dict(wave, wf_start_time=start.replace(second=5 + 10 * k)),
                    "b": dict(wave, wf_start_time=start.replace(second=5 + 10 * k)),
                    "c": dict(wave, wf_start_time=start.replace(second=5 + 10 * k)),
                    "slow": dict(wave, wf_start_time=start.replace(second=5 + 10 * k), wf_increment=2e-3),
                }},
            )
            cls.paths.append(path)

    @classmethod
    def tearDownClass(cls):
        os.environ.pop("GOTDMS_BRIDGE_PART_BYTES", None)
        cls.tmp.cleanup()

    def read(self, part_bytes=None, **kw):
        import gotdms

        if part_bytes is None:
            os.environ.pop("GOTDMS_BRIDGE_PART_BYTES", None)
        else:
            os.environ["GOTDMS_BRIDGE_PART_BYTES"] = str(part_bytes)
        try:
            with gotdms.open([str(p) for p in self.paths]) as session:
                return session.read_channels(["a", "b", "c", "slow"], **kw)
        finally:
            os.environ.pop("GOTDMS_BRIDGE_PART_BYTES", None)

    def test_small_parts_match_one_part(self):
        want, _ = self.read()
        for part in (1, 61, 4096):
            with self.subTest(part=part):
                got, header = self.read(part)
                for g, w in zip(got, want):
                    np.testing.assert_array_equal(g.y, w.y)
                    np.testing.assert_array_equal(g.time_ns, w.time_ns)
                    self.assertEqual(g.y.dtype, w.y.dtype)
                    self.assertEqual(g.y.ctypes.data % 8, 0)
                    self.assertEqual(g.time_ns.ctypes.data % 8, 0)

    def test_shared_clock_mapping(self):
        series, header = self.read()
        a, b, c, slow = series
        # a, b, c are merged from two files on the same waveform axis; the
        # bridge proves the merged clocks equal and sends one.
        self.assertIs(a.time_ns, b.time_ns)
        self.assertIs(a.time_ns, c.time_ns)
        self.assertIsNot(a.time_ns, slow.time_ns)
        self.assertEqual(len(header["clocks"]), 2)
        self.assertEqual([ch["clock"] for ch in header["channels"]], [0, 0, 0, 1])
        self.assertEqual(len(a.meta["gaps"]), 1)
        self.assertEqual(len(a.time_ns), 10000)
        # Every channel's clock is what that channel reads alone.
        import gotdms

        with gotdms.open([str(p) for p in self.paths]) as session:
            for s in series:
                alone = session.read(s.name, s.group)
                np.testing.assert_array_equal(alone.time_ns, s.time_ns)
        # relative_s is still computed from the shared clock, per channel basis.
        np.testing.assert_array_equal(a.relative_s, b.relative_s)

    def test_no_absolute_sends_no_clock(self):
        series, header = self.read(absolute=False)
        self.assertEqual(header["clocks"], [])
        for s in series:
            self.assertIsNone(s.time_ns)
            self.assertEqual(len(s.relative_s), 10000)

    def test_raw_frames_respect_the_cap(self):
        from gotdms.codec import read_head, read_exact, write_frame

        env = dict(os.environ, GOTDMS_BRIDGE_PART_BYTES="100")
        proc = subprocess.Popen([str(self.bin), "serve"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env, bufsize=0)
        try:
            write_frame(proc.stdin, {"op": "open", "paths": [str(p) for p in self.paths]})
            header, plen = read_head(proc.stdout)
            read_exact(proc.stdout, plen)
            self.assertEqual(header["op"], "result")
            write_frame(proc.stdin, {"op": "read_many", "channels": [{"name": "a", "group": "g"}],
                                     "absolute": True, "relative": False})
            begin, plen = read_head(proc.stdout)
            self.assertEqual((begin["op"], plen), ("result_begin", 0))
            sizes = [0] * len(begin["streams"])
            parts = 0
            while True:
                h, plen = read_head(proc.stdout)
                if h["op"] == "result_end":
                    break
                self.assertEqual(h["op"], "part")
                self.assertTrue(0 < plen <= 100, plen)
                sizes[h["stream"]] += len(read_exact(proc.stdout, plen))
                parts += 1
            self.assertEqual(sizes, [s["bytes"] for s in begin["streams"]])
            self.assertEqual(h["bytes"], sum(sizes))
            self.assertGreater(parts, 1500)
            self.assertEqual([s["kind"] for s in begin["streams"]], ["clock", "channel"])
        finally:
            proc.stdin.close()
            proc.wait(timeout=10)
            proc.stdout.close()

    def test_bad_part_cap_is_refused(self):
        env = dict(os.environ, GOTDMS_BRIDGE_PART_BYTES="0")
        out = subprocess.run([str(self.bin), "serve"], input=b"", env=env, capture_output=True)
        self.assertEqual(out.returncode, 2)
        self.assertIn(b"GOTDMS_BRIDGE_PART_BYTES", out.stderr)

    def test_read_tdms_in_small_parts(self):
        import gotdms

        os.environ["GOTDMS_BRIDGE_PART_BYTES"] = "33"
        try:
            got = gotdms.read_tdms(self.paths[0])
        finally:
            os.environ.pop("GOTDMS_BRIDGE_PART_BYTES", None)
        want = gotdms.read_tdms(self.paths[0])
        for name in ("a", "b", "c", "slow"):
            self.assertTrue(got.groups["g"].channels[name].values.equals(want.groups["g"].channels[name].values))


_FAKE = r'''
import json, struct, sys, time
P = struct.Struct("<4sHHII")
inp, out = sys.stdin.buffer, sys.stdout.buffer

def frame(h, payload=b""):
    body = json.dumps(h).encode()
    out.write(P.pack(b"GMDS", 2, 0, len(body), len(payload)) + body + payload)
    out.flush()

def read():
    pre = inp.read(16)
    if len(pre) < 16:
        sys.exit(0)
    _, _, _, hl, pl = P.unpack(pre)
    h = json.loads(inp.read(hl)) if hl else {}
    inp.read(pl)
    return h

while True:
    h = read()
    op = h.get("op")
    if op == "close":
        frame({"op": "result", "closed": True})
        sys.exit(0)
    if op == "open":
        frame({"op": "result", "channels": [], "files": [], "groups": [], "basis": "2024-01-01T00:00:00Z"})
        continue
    if op == "catalog":
        frame({"op": "result", "channels": []})
        continue
    frame({"op": "result_begin", "channels": [], "clocks": [], "reads": [], "limited": False,
           "streams": [{"id": 0, "kind": "channel", "bytes": 64}, {"id": 1, "kind": "channel", "bytes": 64}]})
    frame({"op": "part", "stream": 0}, b"\0" * 40)
    if MODE == "error":
        frame({"op": "result_end", "error": {"code": "corrupt", "message": "segment 3 is bad", "stream": 0}})
    elif MODE == "overflow":
        frame({"op": "part", "stream": 0}, b"\0" * 40)
    elif MODE == "die":
        sys.exit(3)
    elif MODE == "hang":
        time.sleep(60)
'''


@unittest.skipIf(os.name == "nt", "the fake bridge is a script with a shebang")
class FakeBridgeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def fake(self, mode):
        path = Path(self.tmp.name) / f"fake-{mode}"
        path.write_text(f"#!{sys.executable}\nMODE = {mode!r}\n" + textwrap.dedent(_FAKE))
        path.chmod(path.stat().st_mode | stat.S_IXUSR)
        return str(path)

    def test_mid_stream_error_keeps_the_session(self):
        import gotdms

        with gotdms.Session(self.fake("error")) as session:
            session.open(["x.tdms"])
            with self.assertRaises(gotdms.CorruptFileError) as cm:
                session.read_many([{"name": "a", "group": "g"}])
            self.assertIn("segment 3 is bad", str(cm.exception))
            # The reply ended in sync, so the same bridge answers again.
            self.assertEqual(session.catalog().get("channels"), [])
            with self.assertRaises(gotdms.CorruptFileError):
                session.read_many([{"name": "a", "group": "g"}])

    def test_cut_off_reply_breaks_the_session(self):
        import gotdms

        for mode in ("overflow", "die"):
            with self.subTest(mode=mode):
                session = gotdms.Session(self.fake(mode))
                session.open(["x.tdms"])
                with self.assertRaises(gotdms.ProtocolError):
                    session.read_many([{"name": "a", "group": "g"}])
                self.assertIsNotNone(session._proc.poll(), "bridge was not stopped")
                with self.assertRaises(gotdms.ProtocolError) as cm:
                    session.catalog()
                self.assertIn("unusable", str(cm.exception))
                session.close()

    def test_interrupt_mid_reply_kills_the_bridge(self):
        import gotdms
        from gotdms import codec

        session = gotdms.Session(self.fake("hang"))
        session.open(["x.tdms"])
        real = codec.readinto_exact
        calls = []

        def interrupt(stream, view):
            calls.append(len(view))
            if len(calls) == 6:  # inside the first part payload
                raise KeyboardInterrupt
            return real(stream, view)

        codec.readinto_exact = interrupt
        try:
            with self.assertRaises(KeyboardInterrupt):
                session.read_many([{"name": "a", "group": "g"}])
        finally:
            codec.readinto_exact = real
        self.assertIsNotNone(session._proc.poll())
        with self.assertRaises(gotdms.ProtocolError):
            session.catalog()
        session.close()

    @unittest.skipUnless(hasattr(signal, "pthread_kill"), "needs signal.pthread_kill")
    def test_real_sigint_in_a_blocked_read_kills_the_bridge(self):
        # A real SIGINT to this thread while it is blocked in the pipe read,
        # mid-reply: the read is interrupted (PEP 475 retries only when the
        # handler returns), KeyboardInterrupt propagates, and the bridge,
        # which is still alive and sleeping, is killed rather than waited for.
        import gotdms

        if signal.getsignal(signal.SIGINT) is not signal.default_int_handler:
            self.skipTest("SIGINT does not raise KeyboardInterrupt here")
        session = gotdms.Session(self.fake("hang"))
        self.addCleanup(session.close)
        session.open(["x.tdms"])
        main = threading.get_ident()
        timer = threading.Timer(0.5, signal.pthread_kill, (main, signal.SIGINT))
        began = time.monotonic()
        timer.start()
        try:
            with self.assertRaises(KeyboardInterrupt):
                session.read_many([{"name": "a", "group": "g"}])
        finally:
            timer.cancel()
        self.assertLess(time.monotonic() - began, 20)
        self.assertIsNotNone(session._proc.poll(), "bridge was not stopped")
        with self.assertRaises(gotdms.ProtocolError) as cm:
            session.catalog()
        self.assertIn("KeyboardInterrupt", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
