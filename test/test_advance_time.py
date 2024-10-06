from __future__ import annotations

import asyncio
import sys

import pytest

from test.advance_time import advance_time


@pytest.mark.asyncio(loop_scope="module")
async def test_advance_time_with_sleep() -> None:
    sleep = asyncio.create_task(asyncio.sleep(3))
    await advance_time(3)
    await sleep


# asyncio.timeout added in 3.11
if sys.version_info > (3, 11):

    @pytest.mark.asyncio(loop_scope="module")
    async def test_advance_time_with_timeout() -> None:
        async def should_timeout() -> bool:
            try:
                async with asyncio.timeout(2):
                    await asyncio.sleep(3)
                return False
            except asyncio.TimeoutError:
                return True

        timed_out = asyncio.create_task(should_timeout())
        await advance_time(2)
        assert timed_out
