from __future__ import annotations

from v8serialize.constants import FLOAT64_SAFE_INT_RANGE
from v8serialize.jstypes import JSBigInt

from denokv._kv_values import KvU64
from denokv._kv_writes import LIMIT_KVU64
from denokv._kv_writes import LIMIT_UNLIMITED
from denokv._kv_writes import Limit
from denokv._kv_writes import LimitExceededPolicy


def test_constructor() -> None:
    assert Limit(1, 5, "clamp").limit_exceeded is LimitExceededPolicy.CLAMP
    assert Limit(1, 5, "abort").limit_exceeded is LimitExceededPolicy.ABORT
    assert Limit(1, 5).limit_exceeded is LimitExceededPolicy.ABORT
    assert (
        Limit(1, 5, LimitExceededPolicy.CLAMP).limit_exceeded
        is LimitExceededPolicy.CLAMP
    )
    assert Limit(1, 5).min == 1
    assert type(Limit(1, 5).min) is int
    assert Limit(1, 5).max == 5
    assert type(Limit(1, 5).max) is int


def test_contains() -> None:
    assert 3 in Limit(max=5)
    assert -10 in Limit(max=5)
    assert 5 in Limit(max=5)
    assert 6 not in Limit(max=5)
    assert 1 in Limit(1, 5)
    assert -10 not in Limit(0, 5)
    assert 10 not in Limit(0, 5)
    assert 5 in Limit()
    # Non-numbers are not contained
    assert object() not in Limit()

    # contains works across types
    assert 1.0 in Limit(FLOAT64_SAFE_INT_RANGE.start, FLOAT64_SAFE_INT_RANGE.stop - 1)
    assert JSBigInt(1) in Limit(
        float(FLOAT64_SAFE_INT_RANGE.start), float(FLOAT64_SAFE_INT_RANGE.stop - 1)
    )


def test_LIMIT_KVU64() -> None:
    assert LIMIT_KVU64.limit_exceeded is LimitExceededPolicy.WRAP
    assert KvU64.RANGE[0] in LIMIT_KVU64
    assert KvU64.RANGE[-1] in LIMIT_KVU64


def test_LIMIT_UNLIMITED() -> None:
    assert -(2**256) in LIMIT_UNLIMITED
    assert 0 in LIMIT_UNLIMITED
    assert 2**256 in LIMIT_UNLIMITED
    _limit1: Limit[int] = LIMIT_UNLIMITED
    _limit2: Limit[float] = LIMIT_UNLIMITED
