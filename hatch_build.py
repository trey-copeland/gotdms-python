"""Hatchling hook for gotdms wheels and source distributions.

A wheel embeds one gotdms-bridge binary and is tagged for that platform.
The tag is not inferred from a C extension, because the binary is package
data. Cross-compiles set GOTDMS_PLAT_TAG (for example manylinux_2_17_x86_64)
plus GOOS and GOARCH. A source install with those unset tags the wheel for
the host (linux/amd64 -> manylinux_2_17_x86_64, and the four other pairs
.github/workflows/build-and-publish-python-package.yml builds). CGO_ENABLED=0.

go.mod requires github.com/trey-copeland/gotdms v0.1.2. ``go build``
downloads that module.
This hook does not rewrite go.mod and does not vendor a third_party/gotdms
tree. That vendoring belonged to the in-tree bridge, whose only core
requirement was ``replace => ../``.
"""

from __future__ import annotations

import os
import platform
import stat
import subprocess
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface

# Static binary, so the manylinux tag is a platform label, not a glibc build.
_HOST_TAGS = {
    ("linux", "amd64"): "manylinux_2_17_x86_64",
    ("linux", "arm64"): "manylinux_2_17_aarch64",
    ("darwin", "amd64"): "macosx_11_0_x86_64",
    ("darwin", "arm64"): "macosx_11_0_arm64",
    ("windows", "amd64"): "win_amd64",
}


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        if self.target_name == "sdist":
            # The sdist ships go.mod and go.sum. A source install resolves
            # gotdms from those files. Nothing is copied from a parent checkout.
            return
        if self.target_name != "wheel":
            return
        root = Path(self.root)
        self._reject_parent_replace(root)
        binary = self._build_binary(root)
        name = binary.name
        build_data.setdefault("force_include", {})
        build_data["force_include"][str(binary)] = f"gotdms/_bin/{name}"
        plat = os.environ.get("GOTDMS_PLAT_TAG", "").strip() or _default_plat_tag()
        build_data["infer_tag"] = False
        build_data["pure_python"] = False
        build_data["tag"] = f"py3-none-{plat}"

    def _build_binary(self, root: Path) -> Path:
        goos = os.environ.get("GOOS") or _host_goos()
        name = "gotdms-bridge.exe" if goos == "windows" else "gotdms-bridge"
        dest_dir = root / "python" / "src" / "gotdms" / "_bin"
        dest_dir.mkdir(parents=True, exist_ok=True)
        for stale in dest_dir.iterdir():
            if stale.is_file() and stale.name != ".gitignore":
                stale.unlink()
        dest = dest_dir / name
        env = os.environ.copy()
        env["CGO_ENABLED"] = "0"
        subprocess.check_call(
            ["go", "build", "-trimpath", "-o", str(dest), "./cmd/gotdms-bridge"],
            cwd=root,
            env=env,
        )
        dest.chmod(dest.stat().st_mode | stat.S_IEXEC)
        return dest

    def _reject_parent_replace(self, root: Path) -> None:
        text = (root / "go.mod").read_text(encoding="utf-8")
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped.startswith("replace "):
                continue
            if "github.com/trey-copeland/gotdms" in stripped and "=>" in stripped:
                dest = stripped.split("=>", 1)[1].strip()
                if dest in {"../", ".."}:
                    raise RuntimeError(
                        "go.mod still replaces gotdms with ../. "
                        "This repository requires a module version instead."
                    )


def _default_plat_tag() -> str:
    goos = os.environ.get("GOOS") or _host_goos()
    goarch = os.environ.get("GOARCH") or _host_goarch()
    tag = _HOST_TAGS.get((goos, goarch))
    if tag:
        return tag
    raise RuntimeError(
        f"set GOTDMS_PLAT_TAG; {goos}/{goarch} has no default wheel tag"
    )


def _host_goarch() -> str:
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        return "amd64"
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    raise RuntimeError(f"set GOTDMS_PLAT_TAG; unknown machine {machine}")


def _host_goos() -> str:
    if os.name == "nt":
        return "windows"
    return "linux" if os.uname().sysname != "Darwin" else "darwin"
