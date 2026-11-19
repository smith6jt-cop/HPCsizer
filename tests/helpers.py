"""Small helpers shared by test modules."""

import contextlib
import signal


@contextlib.contextmanager
def deadline(seconds=20):
    """Fail, rather than hang the whole test run, when the block does not come back.

    For code that must not block on something another user left in a shared
    directory (a pipe, for instance): if it did block, the test would never end.
    """

    def expired(_signum, _frame):
        raise AssertionError("still running after %d s" % seconds)

    before = signal.signal(signal.SIGALRM, expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, before)
