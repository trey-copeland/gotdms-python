"""Exceptions raised by the gotdms Python client.

Protocol errors from gotdms-bridge use these types. They are not npTDMS
exceptions. npTDMS is not imported.
"""


class GotdmsError(Exception):
    """Base error. ``code`` is the protocol code when the bridge sent one."""

    def __init__(self, message, code="error"):
        super().__init__(message)
        self.code = code


class ProtocolError(GotdmsError):
    def __init__(self, message):
        super().__init__(message, "protocol")


class ArgumentError(GotdmsError):
    def __init__(self, message):
        super().__init__(message, "argument")


class AmbiguousChannelError(GotdmsError):
    """The channel name exists in more than one group.

    npTDMS indexes by group first, so this situation does not arise there.
    Pass ``group=``. ``candidates`` is the list the bridge returned.
    """

    def __init__(self, message, candidates=None):
        super().__init__(message, "ambiguous")
        self.candidates = candidates or []


class NotFoundError(GotdmsError):
    def __init__(self, message):
        super().__init__(message, "not_found")


class NoFilesError(GotdmsError):
    def __init__(self, message):
        super().__init__(message, "no_files")


class UntimedError(GotdmsError):
    """A window or absolute time was requested for a channel with no time axis.

    gotdms does not invent waveform timing. A channel is timed only when its
    group has a declared timestamp channel or, with no declaration, waveform
    ``wf_start_time`` and ``wf_increment``.
    """

    def __init__(self, message):
        super().__init__(message, "untimed")


class NoScaleError(GotdmsError):
    """``scaled=True`` was requested and the channel has no NI linear scale.

    The raw samples are not returned in this case. npTDMS applies scaling by
    default and returns raw data only when asked; gotdms does the opposite
    and refuses to pretend a scale exists.
    """

    def __init__(self, message):
        super().__init__(message, "no_scale")


class NonlinearScaleError(GotdmsError):
    """The channel has a non-linear NI scale. gotdms does not apply it."""

    def __init__(self, message):
        super().__init__(message, "nonlinear_scale")


class ScaleMismatchError(GotdmsError):
    """Files of one merged channel do not share a linear scale."""

    def __init__(self, message):
        super().__init__(message, "scale_mismatch")


class OverlapError(GotdmsError):
    def __init__(self, message):
        super().__init__(message, "overlap")


class MonotonicError(GotdmsError):
    def __init__(self, message):
        super().__init__(message, "monotonic")


class AlignmentError(GotdmsError):
    """``as_polars`` could not align channels under the requested mode."""

    def __init__(self, message):
        super().__init__(message, "alignment")
