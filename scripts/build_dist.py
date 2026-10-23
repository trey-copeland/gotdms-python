#!/usr/bin/env python3
"""Build an sdist or one platform wheel for gotdms.

The wheel build is hatchling plus hatch_build.py. This script only sets
GOOS, GOARCH, CGO_ENABLED, and GOTDMS_PLAT_TAG, then runs `python -m build`.

Platform tags used by .github/workflows/build-and-publish-python-package.yml:

    linux/amd64   manylinux_2_17_x86_64
    linux/arm64   manylinux_2_17_aarch64
    darwin/amd64  macosx_11_0_x86_64
    darwin/arm64  macosx_11_0_arm64
    windows/amd64 win_amd64

The Go binary is static (CGO_ENABLED=0), so the manylinux tag is a
platform label rather than a glibc build. Run this from the repository
root. It does not edit go.mod. The core module is the version required
there (gotdms v0.1.2).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sdist", action="store_true")
    parser.add_argument("--goos", default="")
    parser.add_argument("--goarch", default="")
    parser.add_argument("--platform-tag", default="")
    parser.add_argument("--outdir", default="dist")
    args = parser.parse_args()
    env = os.environ.copy()
    cmd = [sys.executable, "-m", "build", "--outdir", args.outdir]
    if args.sdist:
        cmd.append("--sdist")
    else:
        if not args.platform_tag:
            parser.error("a wheel build needs --platform-tag")
        cmd.append("--wheel")
        env["CGO_ENABLED"] = "0"
        env["GOTDMS_PLAT_TAG"] = args.platform_tag
        if args.goos:
            env["GOOS"] = args.goos
        if args.goarch:
            env["GOARCH"] = args.goarch
    subprocess.check_call(cmd, cwd=ROOT, env=env)


if __name__ == "__main__":
    main()
