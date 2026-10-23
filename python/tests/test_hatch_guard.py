"""The packaging guard in hatch_build.py: no wheel or sdist from a go.mod
that replaces the gotdms core with a local directory."""

import importlib.util
import sys
import types
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
G = "github.com/trey-copeland/gotdms"


def _load():
    # hatch_build imports hatchling, which the test environments do not
    # install. The guard itself needs none of it.
    names = ["hatchling", "hatchling.builders", "hatchling.builders.hooks",
             "hatchling.builders.hooks.plugin", "hatchling.builders.hooks.plugin.interface"]
    saved = {n: sys.modules.get(n) for n in names}
    for n in names:
        sys.modules[n] = types.ModuleType(n)
    sys.modules[names[-1]].BuildHookInterface = object
    try:
        spec = importlib.util.spec_from_file_location("_hatch_build_under_test", ROOT / "hatch_build.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod
    finally:
        for n, m in saved.items():
            if m is None:
                sys.modules.pop(n, None)
            else:
                sys.modules[n] = m


class HatchGuardTest(unittest.TestCase):
    def test_local_replace_is_found(self):
        find = _load().local_gotdms_replace
        cases = {
            f"replace {G} => /workspace/v-go-clk": "/workspace/v-go-clk",
            f"replace {G} v0.1.2 => ../x": "../x",
            f"replace {G} => ..": "..",
            f"replace {G} => ./core": "./core",
            f"replace {G} => C:\\src": "C:\\src",
            f"replace (\n\t{G} => /abs\n)": "/abs",
            f"replace (\n\tgolang.org/x/sys => ../sys\n\t{G} v0.1.2 => ../g // dev\n)": "../g",
            f'replace "{G}" => "/q"': "/q",
            f"replace {G} => github.com/fork/gotdms v0.2.0": None,
            f"replace {G}-python => ../p": None,
            f"// replace {G} => /abs": None,
            f"require {G} v0.1.2": None,
            f"replace golang.org/x/sys => ../sys": None,
        }
        for text, want in cases.items():
            with self.subTest(text=text):
                self.assertEqual(find(text), want)

    def test_this_checkout(self):
        # The dev-only commit replaces gotdms with a local core; the guard
        # must refuse that tree, and accept go.mod without the replace.
        find = _load().local_gotdms_replace
        text = (ROOT / "go.mod").read_text(encoding="utf-8")
        lines = [line for line in text.splitlines() if not line.startswith("replace ")]
        self.assertIsNone(find("\n".join(lines)))


if __name__ == "__main__":
    unittest.main()
