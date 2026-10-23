"""Time columns need an IANA time zone database on every platform.

Every time column this package returns is ``Datetime("ns", "UTC")``. Polars
turns such values into Python datetimes (``to_list``, ``rows``, iteration)
through ``zoneinfo``, and Windows has no system database, so the package
depends on ``tzdata`` there. Without it those calls panic with
``ZoneInfoNotFoundError: No time zone found with key UTC``.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

_CONVERT = """
import polars as pl
from datetime import datetime, timezone
t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
s = pl.Series([t0]).cast(pl.Datetime("ns", "UTC"))
assert s.to_list() == [t0], s.to_list()
print("ok")
"""


class TimeZoneDataTest(unittest.TestCase):
    def test_utc_zone_loads(self):
        import zoneinfo

        try:
            zoneinfo.ZoneInfo("UTC")
        except zoneinfo.ZoneInfoNotFoundError as exc:  # pragma: no cover - a broken install
            self.fail(f"no IANA time zone database ({exc}); install tzdata, a Windows dependency in pyproject.toml")

    def test_utc_column_converts(self):
        try:
            import polars as pl
        except ImportError:  # pragma: no cover
            self.skipTest("polars is not installed")
        t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
        s = pl.Series([t0]).cast(pl.Datetime("ns", "UTC"))
        self.assertEqual(s.to_list(), [t0])

    def test_tzdata_alone_is_enough(self):
        # PYTHONTZPATH="" hides the system database, as on Windows. With
        # tzdata installed the conversion still works.
        if importlib.util.find_spec("tzdata") is None:
            self.skipTest("tzdata is not installed (it is a dependency on Windows only)")
        try:
            import polars  # noqa: F401
        except ImportError:  # pragma: no cover
            self.skipTest("polars is not installed")
        env = dict(os.environ, PYTHONTZPATH="")
        out = subprocess.run([sys.executable, "-c", _CONVERT], env=env, capture_output=True, text=True)
        self.assertEqual(out.returncode, 0, out.stderr[-2000:])
        self.assertEqual(out.stdout.strip(), "ok")

    def test_pyproject_requires_tzdata_on_windows(self):
        text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn("\"tzdata; sys_platform == 'win32'\"", text)


if __name__ == "__main__":
    unittest.main()
