"""Load a directory written by ``gotdms-bridge convert``.

Layout:

    manifest.json
    data/0.arrow
    data/1.arrow

``manifest.json`` is the self-description a Polars load needs: channel path, group,
data type, time basis, scale, and properties. Each ``.arrow`` file is one
Arrow IPC stream with columns ``y`` and, when requested, ``time_ns`` and
``relative_s``. Samples are not CSV.
"""

from __future__ import annotations

import json
from pathlib import Path

from .codec import Series, as_polars, decode_ipc


class Bundle:
    """A converted file set. ``channels`` are :class:`Series` values."""

    def __init__(self, path, manifest: dict, channels: list[Series]):
        self.path = str(path)
        self.manifest = manifest
        self.channels = channels
        self.basis = manifest.get("basis")
        self.format = manifest.get("format")

    def as_polars(self, *, time="column", absolute_time=True, align="strict", channels=None):
        """Same alignment rules as :meth:`gotdms.Session.as_polars`.

        The bundle is already scaled or not, according to the convert
        command. This method does not scale again. ``scaled`` is not a
        parameter here; re-run convert with ``--scaled`` when you want the
        linear scale. npTDMS ``as_dataframe`` scales by default. Polars has
        no row index, so time is a column.
        """
        selected = self.channels
        if channels is not None:
            wanted = set(channels)
            selected = [item for item in self.channels if item.name in wanted or f"{item.group}/{item.name}" in wanted]
        return as_polars(selected, time=time, absolute_time=absolute_time, align=align)


def load_bundle(path) -> Bundle:
    """Read ``manifest.json`` and every Arrow stream it names."""
    root = Path(path)
    manifest_path = root / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("format") != "arrow-ipc-stream":
        raise ValueError(f"unsupported bundle format {manifest.get('format')!r}")
    channels = []
    for item in manifest.get("channels", []):
        blob = (root / item["path"]).read_bytes()
        y, time_ns, relative = decode_ipc(blob)
        channels.append(Series(item, y, time_ns, relative))
    return Bundle(root, manifest, channels)
