"""
Proof-of-concepts for extended atomic mutations for KvU64 numbers.

The default KvU64 supports sum() with positive delta and wrapping at 2**64.
This module also implements:

- sum() with negative delta and wrapping at 2**64
- sum() with positive and negative delta, with clamping at user-defined bounds
    - This is the same as BigInt
    - Which makes KvU64 more powerful than BigInt to some extent

It does not support wrapping on custom bounds, or error limits.
"""

from __future__ import annotations

from hypothesis import example
from hypothesis import given
from hypothesis import strategies as st

from denokv._kv_values import KvU64

u64 = st.integers(min_value=0, max_value=KvU64.RANGE.stop - 1)
neg_u64 = st.integers(min_value=-(KvU64.RANGE.stop - 1), max_value=0)


def sum_with_clamp__no_overflow(
    value: int, delta: int, limit_min: int | None, limit_max: int | None
) -> int:
    if limit_min is None:
        limit_min = 0
    if limit_max is None:
        limit_max = KvU64.RANGE.stop - 1

    assert all(x in KvU64.RANGE for x in [value, abs(delta), limit_min, limit_max])

    result = value + delta
    if limit_min is not None:
        result = max(limit_min, result)
    if limit_max is not None:
        result = min(limit_max, result)
    return result


def sum_with_clamp__overflow(
    value: int, delta: int, limit_min: int | None, limit_max: int | None
) -> int:
    if limit_min is None:
        limit_min = 0
    if limit_max is None:
        limit_max = KvU64.RANGE.stop - 1
    assert all(
        x in KvU64.RANGE for x in [value, delta, limit_min, limit_max] if x is not None
    )

    # When the upper limit is <= the delta, the result is always clamped at the
    # upper limit. Likewise if the lower limit pushes the result above the upper
    # limit, the upper limit is used (it's applied last).
    min_result = 0 + delta
    if limit_max <= min_result or limit_max <= limit_min:
        return limit_max  # set to max as mutation

    if limit_min >= limit_max or limit_min <= delta:
        limit_min = None

    # Can be < 0 which is not allowed in practice.
    # We can use high positive numbers like negative and rely on wrapping
    max_start = limit_max - delta
    if max_start < 0:
        start = max(max_start % 2**64, value)
    else:
        start = min(max_start, value)
    assert start in KvU64.RANGE
    result = start + delta
    result = result % KvU64.RANGE.stop
    if limit_min is not None:
        result = max(limit_min, result)
    return result


@given(value=u64, delta=u64, limit_min=u64 | st.none(), limit_max=u64 | st.none())
def test_sum_min_max(
    value: int, delta: int, limit_min: int | None, limit_max: int | None
) -> None:
    """Implement sum() with clamp for KvU64 (which can only wrap normally)."""
    expected = sum_with_clamp__no_overflow(value, delta, limit_min, limit_max)
    actual = sum_with_clamp__overflow(value, delta, limit_min, limit_max)

    assert actual == expected


# ---------------------------


def neg_sum_with_clamp__overflow(
    value: int,
    delta: int,
    limit_min: int | None,
    limit_max: int | None,
) -> int:
    if limit_max is None:
        limit_max = KvU64.RANGE.stop - 1
    if limit_min is None:
        limit_min = 0
    assert delta <= 0

    assert all(x in KvU64.RANGE for x in [value, abs(delta), limit_min, limit_max])

    # If value after adding the delta is always <= the lower limit, the lower
    # limit is always the result. However the upper limit applies last, so if
    # the upper limit is lower than the lower limit, it applies instead.
    if limit_max <= limit_min:
        return limit_max  # set to limit_max as mutation
    max_result = (KvU64.RANGE.stop - 1) + delta
    if limit_min >= max_result:
        assert limit_max > limit_min
        return limit_min  # set to limit_min as mutation

    if limit_max >= max_result:
        assert limit_max > limit_min
        # limit_max can have no effect on the result
        limit_max = None

    # Offset the start to prevent it going negative after adding the delta
    min_start = abs(delta) + limit_min
    if min_start >= KvU64.RANGE.stop:
        start = min(min_start % KvU64.RANGE.stop, value)
    else:
        start = max(min_start, value)
    assert start in KvU64.RANGE

    # Make the negative delta to a positive value that overflows to the original
    # negative delta offset.
    if delta < 0:
        delta = KvU64.RANGE.stop + delta
    assert delta in KvU64.RANGE

    # Apply the delta (effectively subtracting)
    result = (start + delta) % (KvU64.RANGE.stop)
    assert result in KvU64.RANGE

    if limit_max is not None:
        result = min(limit_max, result)
    return result


@given(value=u64, delta=neg_u64, limit_min=u64 | st.none(), limit_max=u64 | st.none())
@example(value=2**64 - 1, delta=-1, limit_min=0, limit_max=2**64 - 3)
def test_negative_sum_min_max(
    value: int, delta: int, limit_min: int | None, limit_max: int | None
) -> None:
    """
    Implement sum() with negative delta for KvU64 with clamp min/max.

    UvU64 sum can only add positive values with wrapping normally.
    """
    expected = sum_with_clamp__no_overflow(value, delta, limit_min, limit_max)
    actual = neg_sum_with_clamp__overflow(value, delta, limit_min, limit_max)

    assert actual == expected


# ---------------------------


def neg_sum_with_wrap__no_overflow(value: int, delta: int) -> int:
    assert delta <= 0
    assert all(x in KvU64.RANGE for x in [value, abs(delta)] if x is not None)

    result = (value + delta) % (2**64)
    assert result in KvU64.RANGE
    return result


def neg_sum_with_wrap__overflow(
    value: int,
    delta: int,
) -> int:
    assert delta <= 0

    assert all(x in KvU64.RANGE for x in [value, abs(delta)])

    if delta == 0:
        return value  # no mutation

    delta = 2**64 + delta
    assert delta in KvU64.RANGE

    result = (value + delta) % (2**64)
    assert result in KvU64.RANGE

    return result


# TODO: can we do wrapping on custom limits, not just 0 and 2**64?
@given(value=u64, delta=neg_u64)
def test_negative_sum_with_wrap(value: int, delta: int) -> None:
    """Implement sum() with negative delta for KvU64 (with wrapping)."""
    expected = neg_sum_with_wrap__no_overflow(value, delta)
    actual = neg_sum_with_wrap__overflow(value, delta)

    assert actual == expected
