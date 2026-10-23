"""Process plumbing shared by :class:`Session` and the one-shot bridge.

The stderr drain thread and the cleanup finalizer hold the process and a
bounded buffer, never the owning object. A Session that is dropped without
``close`` can then be collected, and ``weakref.finalize`` stops its bridge.
"""

from __future__ import annotations

import subprocess
import threading

from .codec import read_frame, write_frame

#: Bytes of bridge stderr kept for error messages. Older output is dropped.
STDERR_LIMIT = 64 * 1024


class StderrTail:
    """The last :data:`STDERR_LIMIT` bytes a bridge wrote to stderr."""

    def __init__(self, limit: int = STDERR_LIMIT):
        self._limit = limit
        self._buf = bytearray()
        self._lock = threading.Lock()

    def append(self, chunk: bytes) -> None:
        with self._lock:
            self._buf += chunk
            extra = len(self._buf) - self._limit
            if extra > 0:
                del self._buf[:extra]

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)

    def text(self) -> str:
        with self._lock:
            data = bytes(self._buf)
        return data.decode(errors="replace").strip()


def drain_stderr(stream, tail: StderrTail) -> None:
    """Copy ``stream`` into ``tail`` until EOF. Runs on a daemon thread."""
    if stream is None:
        return
    try:
        while True:
            chunk = stream.read(4096)
            if not chunk:
                return
            tail.append(chunk)
    except Exception:
        return


def start_drain(proc: subprocess.Popen, tail: StderrTail) -> threading.Thread:
    thread = threading.Thread(
        target=drain_stderr,
        args=(proc.stderr, tail),
        name="gotdms-bridge-stderr",
        daemon=True,
    )
    thread.start()
    return thread


#: Seconds a finalizer waits for an in-flight request before it skips the
#: polite ``close`` frame and just closes the bridge's stdin.
LOCK_TIMEOUT = 1.0


def shutdown(proc: subprocess.Popen, lock=None) -> None:
    """Ask the bridge to close, reap the process, then close the pipes.

    Safe to call on a process that already exited. ``close`` calls this while
    it already holds the owner's pipe lock and passes no ``lock``.

    The ``weakref.finalize`` of an object that was never closed passes the
    owner's pipe lock. After garbage collection nothing holds it. At
    interpreter exit a daemon thread can still be inside a request. Then the
    lock is not free within :data:`LOCK_TIMEOUT`, and writing a ``close``
    frame and reading its reply would race that thread on the pipes, so the
    finalizer only closes stdin. The bridge exits at EOF.

    Pipes that another thread may be blocked reading (stdout, and stderr in
    the drain thread) are closed only after the process has exited. On
    Windows, closing a pipe handle that has a pending read can block.
    """
    polite = True
    held = False
    if lock is not None:
        held = lock.acquire(timeout=LOCK_TIMEOUT)
        polite = held
    try:
        _stop(proc, polite)
    finally:
        if held:
            lock.release()


def _stop(proc: subprocess.Popen, polite: bool) -> None:
    try:
        if polite and proc.poll() is None and proc.stdin is not None:
            write_frame(proc.stdin, {"op": "close"})
            read_frame(proc.stdout)
    except Exception:
        pass
    finally:
        _close_quietly(proc.stdin)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        _close_quietly(proc.stdout)
        _close_quietly(proc.stderr)


def _close_quietly(stream) -> None:
    if stream is None:
        return
    try:
        stream.close()
    except Exception:
        pass
