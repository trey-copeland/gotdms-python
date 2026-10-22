"""Python client for gotdms, the MIT TDMS reader and writer.

This package is the npTDMS-decoupling path. Analysis code stays in Python.
It launches the ``gotdms-bridge`` binary instead of importing npTDMS
(LGPL-3.0-only). Nothing in this package imports npTDMS.

The bridge speaks Arrow IPC, which Polars reads natively. numpy (BSD-3-Clause),
Polars, and pyarrow are required. This package does not depend on pandas.
A caller who wants a pandas object can use Polars' ``to_pandas()``.

``write_tdms`` writes Polars frames to a TDMS file. ``read_tdms`` reads
those samples back in the stored type. ``open`` still returns float64 and
is the dataset path.

Wheels bundle ``gotdms-bridge``. ``locate_binary`` uses an explicit path,
then ``GOTDMS_BRIDGE``, then that bundled file.

See the migration guide next to this package for the npTDMS mapping, the
license note, and the batch and interactive modes.
"""

from .bundle import Bundle, load_bundle
from .client import Channel, Group, Session, bundled_binary, locate_binary, open
from .codec import Series, as_polars
from .scan import scan_tdms
from .tdmsfile import (
    Property,
    TdmsChannel,
    TdmsContents,
    TdmsGroup,
    WriteResult,
    read_tdms,
    write_tdms,
)
from .errors import (
    AlignmentError,
    AmbiguousChannelError,
    ArgumentError,
    GotdmsError,
    MonotonicError,
    NoFilesError,
    NoScaleError,
    NonlinearScaleError,
    NotFoundError,
    OverlapError,
    ProtocolError,
    ScaleMismatchError,
    UntimedError,
)

__all__ = [
    "AlignmentError",
    "AmbiguousChannelError",
    "ArgumentError",
    "Bundle",
    "Channel",
    "GotdmsError",
    "Group",
    "MonotonicError",
    "NoFilesError",
    "NoScaleError",
    "NonlinearScaleError",
    "NotFoundError",
    "OverlapError",
    "Property",
    "ProtocolError",
    "ScaleMismatchError",
    "Series",
    "Session",
    "TdmsChannel",
    "TdmsContents",
    "TdmsGroup",
    "UntimedError",
    "WriteResult",
    "as_polars",
    "bundled_binary",
    "load_bundle",
    "locate_binary",
    "open",
    "read_tdms",
    "scan_tdms",
    "write_tdms",
]
