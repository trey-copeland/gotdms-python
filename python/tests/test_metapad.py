"""A segment may pad between its object list and the raw-data offset.

NI HSAI DAQmx captures do this. The file here is a normal write with zero
bytes inserted, the same rewrite as gotdms metapad_test.go. npTDMS is not
used and nothing from it is copied.
"""

import os
import struct
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))

try:
    import polars as pl
except ImportError:  # pragma: no cover - skipped only when polars is absent
    pl = None

_LEAD = 28
_TOC_META = 1 << 1
_INCOMPLETE = (1 << 64) - 1


def pad_segments(data: bytes, pad: int) -> bytes:
    """Insert pad zero bytes after each segment's metadata.

    The lead-in's next-segment length and raw-data offset both grow by pad
    when the segment has metadata. A raw-only segment is left alone. This
    matches padSegments in gotdms metapad_test.go.
    """
    out = bytearray()
    pos = 0
    while pos < len(data):
        if len(data) - pos < _LEAD:
            raise AssertionError(f"short lead-in at {pos}")
        toc, nxt, raw = struct.unpack_from("<I4xQQ", data, pos + 4)
        if nxt == _INCOMPLETE:
            end = len(data)
        else:
            end = pos + _LEAD + nxt
        if end > len(data):
            raise AssertionError(f"segment at {pos} runs past EOF")
        meta_end = pos + _LEAD + raw
        if meta_end > end:
            raise AssertionError(f"raw-data offset {raw} past the segment")
        extra = pad if toc & _TOC_META else 0
        hdr = bytearray(data[pos : pos + _LEAD])
        if nxt != _INCOMPLETE:
            struct.pack_into("<Q", hdr, 12, nxt + extra)
        struct.pack_into("<Q", hdr, 20, raw + extra)
        out += hdr
        out += data[pos + _LEAD : meta_end]
        out += bytes(extra)
        out += data[meta_end:end]
        pos = end
    return bytes(out)


@unittest.skipIf(pl is None, "polars is not installed")
class MetadataPaddingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(
            ["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"],
            cwd=ROOT,
        )
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_read_tdms_skips_metadata_padding(self):
        import gotdms

        path = Path(self.tmp.name) / "pad.tdms"
        gotdms.write_tdms(path, pl.DataFrame({"x": [1.5, 2.5, 3.5]}))
        plain = path.read_bytes()
        padded = pad_segments(plain, 333)
        self.assertGreater(len(padded), len(plain))
        raw_before = struct.unpack_from("<Q", plain, 20)[0]
        raw_after = struct.unpack_from("<Q", padded, 20)[0]
        self.assertEqual(raw_after, raw_before + 333)
        path.write_bytes(padded)

        doc = gotdms.read_tdms(path)
        self.assertEqual(doc.groups["data"].channels["x"].values.to_list(), [1.5, 2.5, 3.5])


if __name__ == "__main__":
    unittest.main()
