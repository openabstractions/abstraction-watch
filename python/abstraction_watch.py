"""watch -- the present, and whether it has stopped changing.

A Notice carries what is true now. ``quiet`` says nothing visible has changed
for at least the budget, and ``silence`` says for how long by this observer's
clock. The listener's own wait is the only timer: quiet is judged by a listener
that is waiting, after it has read, so a change the source already made is
always delivered ahead of a quiet that would postdate it.

``unavailable`` says the last read of a polled source failed: ``now`` is the
last good present and ``unread`` is the seconds since the last successful read.
It is said once per outage, quiet is never said while reads fail, and the first
successful read after an outage is said even when its stamp is unchanged.
"""

import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Generic, Optional, Tuple, TypeVar

T = TypeVar("T")

DEFAULT_EVERY = 1.0


class Closed(Exception):
    pass


@dataclass
class Notice(Generic[T]):
    now: T
    quiet: bool = False
    silence: float = 0.0
    unavailable: bool = False
    unread: float = 0.0


Source = Callable[[], Tuple[Any, str]]


class Subscription(Generic[T]):
    def __init__(self, read: Optional[Source], every: float, budget: float, first: T = None, stamp: str = ""):
        self._read = read
        self._every = every
        self._budget = budget
        self._cond = threading.Condition()
        self._now = first
        self._stamp = stamp
        self._pending = True
        self._changed = time.monotonic()
        self._told = 0.0
        self._closed = False
        self._last_read = None
        self._failing = False
        self._told_failed = False
        if read is not None:
            self._refresh()
            self._pending = True

    def post(self, value: T, stamp: str) -> bool:
        """A pushed source's new present. Reports whether it differed."""
        with self._cond:
            changed = self._take(value, stamp)
            if changed:
                self._cond.notify_all()
        return changed

    def _take(self, value: T, stamp: str) -> bool:
        if stamp == self._stamp:
            return False
        self._now, self._stamp, self._pending, self._changed = value, stamp, True, time.monotonic()
        return True

    def _refresh(self) -> None:
        if self._read is None:
            return
        try:
            value, stamp = self._read()
        except Exception:
            with self._cond:
                if not self._failing:
                    self._failing, self._told_failed = True, False
            return
        with self._cond:
            self._last_read = time.monotonic()
            recovered = self._failing
            self._failing = False
            if not self._take(value, stamp) and recovered:
                self._pending, self._told = True, self._last_read

    def _unread(self) -> float:
        since = self._changed if self._last_read is None else self._last_read
        return time.monotonic() - since

    def current(self) -> T:
        with self._cond:
            return self._now

    def next(self, timeout: Optional[float] = None) -> Notice:
        """Block until there is something to say: a changed present, a quiet one
        once the budget passed with nothing, an unavailable one once per outage
        of a polled source, or Closed. TimeoutError after
        ``timeout`` seconds with nothing to say."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            self._refresh()
            with self._cond:
                if self._closed:
                    raise Closed()
                if self._pending:
                    self._pending = False
                    if self._failing:
                        self._told_failed = True
                        return Notice(self._now, unavailable=True, unread=self._unread())
                    return Notice(self._now)
                if self._failing and not self._told_failed:
                    self._told_failed = True
                    return Notice(self._now, unavailable=True, unread=self._unread())
                wait = self._every if self._read is not None else None
                if self._budget > 0 and not self._failing:
                    left = self._budget - (time.monotonic() - max(self._changed, self._told))
                    if left <= 0:
                        self._told = time.monotonic()
                        return Notice(self._now, True, time.monotonic() - self._changed)
                    wait = left if wait is None else min(wait, left)
                if deadline is not None:
                    left = deadline - time.monotonic()
                    if left <= 0:
                        raise TimeoutError()
                    wait = left if wait is None else min(wait, left)
                self._cond.wait(wait)

    def __iter__(self):
        return self

    def __next__(self) -> Notice:
        try:
            return self.next()
        except Closed:
            raise StopIteration

    def close(self) -> None:
        with self._cond:
            self._closed = True
            self._cond.notify_all()


def poll(read: Source, every: float = DEFAULT_EVERY, budget: float = 0.0) -> Subscription:
    """Watch a source that has to be asked: read on entry to ``next`` and then
    every ``every`` seconds while a listener waits."""
    return Subscription(read, every if every > 0 else DEFAULT_EVERY, budget)


def push(first: T, stamp: str, budget: float = 0.0) -> Subscription:
    """Watch a source that says when it moved, through ``post``."""
    return Subscription(None, 0.0, budget, first, stamp)


def settle(events: "queue.Queue", period: float, settled: Callable[[], None]) -> None:
    """Call ``settled`` once ``events`` has been still for ``period`` seconds.

    A platform's notification says something moved and not what, and one edit
    produces several of them: a text editor writes, truncates and renames, and
    a subscriber wants one notice. ``period`` is the deliberate wait between
    the last signal and the read, and nothing else waits.

    Returns when ``None`` is put on the queue, which is how a caller stops it.
    """
    if period <= 0:
        period = DEFAULT_EVERY
    pending = False
    while True:
        try:
            if events.get(timeout=period if pending else None) is None:
                return
            pending = True
        except queue.Empty:
            pending = False
            settled()
