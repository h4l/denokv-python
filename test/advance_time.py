from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Callable
from typing import Final
from typing import Generator
from unittest.mock import patch

import pytest


@pytest.fixture
def advance_time_time() -> Generator[None]:
    """Advance time.time() when advance_time() is used to advance loop time."""

    def on_loop_time_advanced(seconds: float) -> None:
        time_advanced_time.advance_time(seconds)

    time_advanced_time = AdvancedTime(time.time)
    with patch("time.time", AdvancedTime(time.time)):
        advance_time_listeners.add(on_loop_time_advanced)
        try:
            yield None
        finally:
            advance_time_listeners.remove(on_loop_time_advanced)


@dataclass
class AdvancedTime:
    """Wrapper for a time() function that allows its time to be moved forward."""

    unadvanced: Callable[[], float]
    _offset: float = field(default=0)

    def __call__(self) -> float:
        return self.unadvanced() + self._offset

    @property
    def offset(self) -> float:
        return self._offset

    def advance_time(self, offset: float) -> None:
        if not (offset >= 0):
            raise ValueError(f"offset must be >= 0: {offset=}")
        self._offset += offset

    def __repr__(self) -> str:
        return f"<AdvancedTime of {self.unadvanced!r} by offset={self._offset}>"


advance_time_listeners: Final[set[Callable[[float], None]]] = set()


async def advance_time(seconds: float) -> None:
    """
    Fast-forward event loop time, running any tasks/timers that have been created.

    The current event loop's time() method will be wrapped with `AdvancedTime`
    to offset its time into the future and the loop's tasks will be executed up
    to the new time.

    Notes
    -----
    This does not work in uvloop, as patching loop.time() is not observed by the
    uvloop c/cython implementation. But it doesn't break it either, time just
    advances at the normal rate.

    The AdvancedTime wrapper for `loop.time()` is not intended to be removed,
    because event loops use `time.monotonic()` which isn't directly convertible
    to wall-clock time, but is guaranteed to only increase. This means it should
    be OK to jump it forwards, but not to move it back by un-patching time().
    """
    if not (seconds >= 0):
        raise ValueError(f"seconds must be >= 0: {seconds=}")
    loop = asyncio.get_running_loop()
    if not isinstance(loop.time, AdvancedTime):
        loop.time = AdvancedTime(unadvanced=loop.time)  # type: ignore[method-assign]

    end = asyncio.create_task(asyncio.sleep(seconds))
    # Creating a task is not enough to have it scheduled as a timer yet. It must
    # be scheduled before advancing the loop's time, otherwise it gets scheduled
    # after the time has advanced and then sleeps the full amount.
    await asyncio.sleep(0)
    loop.time.advance_time(seconds)

    for listener in advance_time_listeners:
        listener(seconds)

    await end  # should take 0 wall-clock time
