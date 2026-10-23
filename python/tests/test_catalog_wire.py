"""The compact catalog and read-header references (protocol 2).

On the wire, the catalog interns file paths and property pairs, and a read
header points into the catalog instead of repeating them. The client
expands both, so the public shapes are the ones callers always had.
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "python" / "src"))


class CatalogWireTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.bin = Path(cls.tmp.name) / ("gotdms-bridge.exe" if os.name == "nt" else "gotdms-bridge")
        subprocess.check_call(["go", "build", "-o", str(cls.bin), "./cmd/gotdms-bridge"], cwd=ROOT)
        os.environ["GOTDMS_BRIDGE"] = str(cls.bin)
        cls.demo = json.loads(subprocess.check_output([str(cls.bin), "demo", "--out", cls.tmp.name], text=True))
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

    def test_catalog_shape_is_unchanged(self):
        with self.open_demo() as session:
            cat = session.catalog()
            raw = session._rpc({"op": "catalog"})
        self.assertNotIn("property_table", cat)
        paths = [f["path"] for f in cat["files"]]
        self.assertEqual(len(paths), len(self.demo["files"]))
        for g in cat["groups"]:
            self.assertIn(g["file"], paths)
            self.assertIsInstance(g["properties"], dict)
        for c in cat["channels"]:
            self.assertIn(c["file"], paths)
            if "properties" in c:
                self.assertIsInstance(c["properties"], dict)
                self.assertTrue(c["properties"])
        # The wire form is compact: indexes into files and property_table.
        self.assertIn("property_table", raw)
        names = [(e["name"], json.dumps(e["value"], sort_keys=True)) for e in raw["property_table"]]
        self.assertEqual(len(names), len(set(names)))
        for wire, full in zip(raw["channels"], cat["channels"]):
            self.assertIsInstance(wire["file"], int)
            self.assertEqual(paths[wire["file"]], full["file"])
            props = {raw["property_table"][i]["name"]: raw["property_table"][i]["value"] for i in wire.get("properties", [])}
            self.assertEqual(props, full.get("properties", {}))

    def test_rows_do_not_share_mutable_values(self):
        with self.open_demo() as session:
            rows = [c for c in session.catalog()["channels"] if c.get("properties")]
            series = session.read_all()
        seen = set()
        for c in rows:
            self.assertNotIn(id(c["properties"]), seen)
            seen.add(id(c["properties"]))
            for v in c["properties"].values():
                if isinstance(v, dict):
                    self.assertNotIn(id(v), seen)
                    seen.add(id(v))
        for s in series:
            if "properties" in s.meta:
                self.assertNotIn(id(s.meta["properties"]), seen)

    def test_series_meta_resolves_to_paths_and_properties(self):
        with self.open_demo() as session:
            cat = session.catalog()
            series, header = session.read_channels(session._data_channels())
            channels = {}
            for name in {(c["group"], c["name"]) for c in cat["channels"] if not c["is_timestamp"]}:
                channels[name] = session[name[0]][name[1]]
            paths = {f["path"] for f in cat["files"]}
            for s in series:
                ch = channels[(s.meta["group"], s.meta["name"])]
                self.assertTrue(s.meta["files"])
                self.assertTrue(set(s.meta["files"]) <= paths)
                first = next(o for o in ch.occurrences if o["file"] == s.meta["files"][0])
                self.assertEqual(s.meta.get("properties"), first.get("properties"))
                # The bridge's properties_match agrees with the catalog rows of
                # the files this read touched.
                dumps = {
                    json.dumps(o.get("properties") or {}, sort_keys=True, default=str)
                    for o in ch.occurrences
                    if o["file"] in s.meta["files"]
                }
                self.assertEqual(s.meta["properties_match"], len(dumps) == 1, s.meta["name"])
            for r in header["reads"]:
                self.assertIn(r["file"], paths)

    def test_properties_match_across_files(self):
        import gotdms
        import polars as pl

        paths = []
        for name in ("a", "b"):
            path = Path(self.tmp.name) / f"props_{name}.tdms"
            gotdms.write_tdms(
                path,
                pl.DataFrame({"x": [1.0, 2.0], "y": [3.0, 4.0]}),
                group="g",
                channel_properties={"g": {"x": {"unit_string": "V", "gain": 3}, "y": {"note": name}}},
            )
            paths.append(str(path))
        with gotdms.open(paths) as session:
            got = {s.meta["name"]: s for s in session.read_all()}
            self.assertTrue(got["x"].meta["properties_match"])
            self.assertFalse(got["y"].meta["properties_match"])
            self.assertTrue(session["g"]["x"].properties_match)
            self.assertFalse(session["g"]["y"].properties_match)
            self.assertEqual(got["y"].meta["properties"]["note"], "a")
            self.assertEqual(session["g"]["y"].properties["note"], "a")
            self.assertEqual(got["x"].meta["properties"], {"unit_string": "V", "gain": 3})
            self.assertEqual(len(got["x"].meta["files"]), 2)

    def test_bad_references_are_protocol_errors(self):
        from gotdms import ProtocolError
        from gotdms.client import _expand_catalog, _resolve_read_header

        def cat(**over):
            base = {
                "files": [{"path": "/a.tdms", "properties": {}}],
                "groups": [{"name": "g", "file": 0, "properties": {}}],
                "property_table": [{"name": "unit_string", "value": "V"}],
                "channels": [{"name": "x", "group": "g", "file": 0, "properties": [0]}],
            }
            base.update(over)
            return base

        good = _expand_catalog(cat())
        self.assertEqual(good["channels"][0], {"name": "x", "group": "g", "file": "/a.tdms", "properties": {"unit_string": "V"}})
        for bad in (
            cat(channels=[{"name": "x", "group": "g", "file": 1}]),
            cat(channels=[{"name": "x", "group": "g", "file": -1}]),
            cat(channels=[{"name": "x", "group": "g", "file": True}]),
            cat(channels=[{"name": "x", "group": "g", "file": 0, "properties": [1]}]),
            cat(channels=[{"name": "x", "group": "g", "file": 0, "properties": [-1]}]),
            cat(groups=[{"name": "g", "file": "/a.tdms"}]),
            {k: v for k, v in cat().items() if k != "property_table"},
        ):
            with self.assertRaises(ProtocolError):
                _expand_catalog(bad)

        def header(**over):
            ch = {"name": "x", "group": "g", "files": [0], "properties": 0}
            ch.update(over)
            return {"channels": [ch], "reads": [{"file": 0}]}

        h = header()
        _resolve_read_header(h, good)
        self.assertEqual(h["channels"][0]["files"], ["/a.tdms"])
        self.assertEqual(h["channels"][0]["properties"], {"unit_string": "V"})
        self.assertEqual(h["reads"][0]["file"], "/a.tdms")
        for bad in (header(files=[1]), header(properties=1), header(properties=-1), header(name="y")):
            with self.assertRaises(ProtocolError):
                _resolve_read_header(bad, good)
        with self.assertRaises(ProtocolError):
            _resolve_read_header(header(), None)

        # A properties index must name the channel in a file of this read.
        two = _expand_catalog(cat(
            files=[{"path": "/a.tdms", "properties": {}}, {"path": "/b.tdms", "properties": {}}],
            channels=[{"name": "x", "group": "g", "file": 0, "properties": [0]}, {"name": "x", "group": "g", "file": 1}],
        ))
        with self.assertRaisesRegex(ProtocolError, "not a file of this read"):
            _resolve_read_header(header(files=[1], properties=0), two)
        h = header(files=[1], properties=1)
        _resolve_read_header(h, two)
        self.assertNotIn("properties", h["channels"][0])

    def test_key_order_is_unchanged(self):
        # Expanded rows keep the key order the bridge's full rows always had:
        # file right after group, properties last; in Series.meta, files
        # after basis and properties before properties_match; reads start
        # with file.
        with self.open_demo() as session:
            cat = session.catalog()
            found = session.find("ai0")
            pick = next(c for c in cat["channels"] if c.get("properties") and not c["is_timestamp"])
            series, header = session.read_channels([{"name": pick["name"], "group": pick["group"]}])
        for row in found:
            same = [c for c in cat["channels"] if (c["name"], c["group"], c["file"]) == (row["name"], row["group"], row["file"])]
            self.assertEqual(len(same), 1)
            self.assertEqual(list(same[0]), list(row))
        keys = list(series[0].meta)
        self.assertEqual(keys[: keys.index("files") + 2][-3:], ["basis", "files", "columns"])
        self.assertEqual(keys[keys.index("properties") + 1], "properties_match")
        self.assertEqual(list(header["reads"][0])[:3], ["file", "group", "name"])


if __name__ == "__main__":
    unittest.main()
