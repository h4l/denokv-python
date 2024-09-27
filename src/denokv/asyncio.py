from __future__ import annotations

import asyncio
import time
from datetime import datetime


def loop_time(wall_time: datetime | float | None = None) -> float:
    """
    Get time in the running event loop's arbitrary time offset.

    If `wall_time` is set, the returned time is the absolute loop time that this
    wall-clock time will occur at. If `wall_time` is not set, the current loop
    time is returned.

    Notes
    -----
    Loop time does not correspond to wall-clock time because it uses
    time.monotonic, which has an arbitrary epoch/offset.
    """
    if wall_time is None:
        return asyncio.get_running_loop().time()
    if isinstance(wall_time, datetime):
        wall_time = wall_time.timestamp()
    return wall_time - loop_time_offset()


def loop_time_offset() -> float:
    """
    Get the number of seconds to be added to loop_time() to get time.time().

    loop_time() + loop_time_offset() should equal time.time()
    """
    return time.time() - loop_time()
