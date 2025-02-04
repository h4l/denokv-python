from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from math import isnan
from typing import Literal

import pytest
from hypothesis import example
from hypothesis import given
from hypothesis import strategies as st
from v8serialize.constants import FLOAT64_SAFE_INT_RANGE
from v8serialize.jstypes import JSBigInt

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_values import KvU64
from denokv._kv_writes import LIMIT_KVU64
from denokv._kv_writes import LIMIT_UNLIMITED
from denokv._kv_writes import BigIntSum
from denokv._kv_writes import FloatSum
from denokv._kv_writes import KvNumber
from denokv._kv_writes import KvNumberIdentifier
from denokv._kv_writes import KvNumberInfo
from denokv._kv_writes import KvNumberNameT
from denokv._kv_writes import KvNumberTypeT
from denokv._kv_writes import Limit
from denokv._kv_writes import LimitExceededPolicy
from denokv._kv_writes import NumberT
from denokv._kv_writes import Sum
from denokv._kv_writes import U64Sum
from denokv._pycompat.typing import Any
from denokv._pycompat.typing import NewType
from denokv._pycompat.typing import assert_type
from denokv._pycompat.typing import cast
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.datapath import read_range_single
from denokv.kv_keys import KvKey
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result
from denokv.result import is_err
from test.denokv_testing import MockKvDb
from test.denokv_testing import SumLimitExceeded
from test.denokv_testing import add_entries
from test.denokv_testing import create_dataclass_slots_test
from test.denokv_testing import typeval
from test.denokv_testing import unsafe_parse_protobuf_kv_entry

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()

u64 = st.integers(min_value=0, max_value=KvU64.RANGE.stop - 1)
neg_u64 = st.integers(min_value=-(KvU64.RANGE.stop - 1), max_value=0)


@pytest.fixture
def instance() -> Sum:
    return Sum(KvKey("a"), 1)


test_instances_dont_have_dict_because_of_slots = create_dataclass_slots_test()


def test_init__limits() -> None:
    sum1 = Sum(KvKey("a"), 1)
    assert sum1.limit == LIMIT_UNLIMITED

    sum2 = Sum(KvKey("a"), 1, number_type="u64")
    assert sum2.limit == LIMIT_KVU64

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Limit keyword arguments in conflict: "
            "Options abort_*=, clamp_*=, limit= cannot be used together.\n"
            "Use limit=Limit(limit_exceeded=..., ...) to create a limit with a "
            "dynamic type."
        ),
    ):
        Sum(KvKey("a"), 1, limit=Limit(), clamp_over=1, abort_under=3)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Limit keyword arguments in conflict: "
            "Options abort_*=, clamp_*= cannot be used together.\n"
            "Use limit=Limit(limit_exceeded=..., ...) to create a limit with a "
            "dynamic type."
        ),
    ):
        Sum(KvKey("a"), 1, clamp_over=1, abort_under=3)

    sum4 = Sum(KvKey("a"), 1, clamp_over=98, clamp_under=2)
    assert sum4.limit == Limit(min=2, max=98, limit_exceeded="clamp")

    sum4 = Sum(KvKey("a"), 1, abort_under=3, abort_over=100)
    assert sum4.limit == Limit(min=3, max=100, limit_exceeded="abort")

    sum6 = Sum(
        KvKey("a"),
        1,
        limit=Limit(min=3, limit_exceeded="clamp"),
        expire_at=datetime.now(),
    )
    assert sum6.limit == Limit(min=3, limit_exceeded="clamp")

    # Passing None as a clamp/abort enables that limit type with the default
    sum7 = Sum(KvKey("a"), 1, "u64", clamp_under=None)
    assert sum7.limit == Limit(min=None, max=None, limit_exceeded="clamp")

    sum8 = Sum(KvKey("a"), 1, abort_over=None)
    assert sum8.limit == Limit(min=None, max=None, limit_exceeded="abort")

    sum9 = Sum(KvKey("a"), 1, "u64", limit=None)
    assert sum9.limit == LIMIT_KVU64


def test_init__overloads() -> None:
    k = KvKey("a")
    bigint, float, u64 = KvNumber.bigint.value, KvNumber.float.value, KvNumber.u64.value
    assert assert_type(Sum(k, JSBigInt(1)), BigIntSum).number_type == bigint
    assert assert_type(Sum(k, 1, "bigint"), BigIntSum).number_type == bigint
    assert assert_type(Sum(k, KvU64(1)), U64Sum).number_type == u64
    assert assert_type(Sum(k, 1, "u64"), U64Sum).number_type == u64
    assert assert_type(Sum(k, 1), FloatSum).number_type == float
    assert assert_type(Sum(k, 1.0), FloatSum).number_type == float
    assert assert_type(Sum(k, 1.0, "float"), FloatSum).number_type == float

    FooInt = NewType("FooInt", int)
    BarInt = NewType("BarInt", int)
    number_info: KvNumberInfo[Literal["test"], FooInt, BarInt] = cast(Any, bigint)
    assert (
        assert_type(
            Sum(k, FooInt(1), number_info), Sum[Literal["test"], FooInt, BarInt]
        )
    ).number_type == number_info


@pytest.mark.parametrize(
    "delta,number_type,expected_delta,expected_number_type",
    [
        (1, None, (int, 1), KvNumber.float.value),
        (1.0, None, (float, 1.0), KvNumber.float.value),
        (1, "float", (int, 1), KvNumber.float.value),
        (1.0, float, (float, 1.0), KvNumber.float.value),
        (1, KvNumber.float, (int, 1), KvNumber.float.value),
        (1, KvNumber.float.value, (int, 1), KvNumber.float.value),
        (JSBigInt(1), None, (int, 1), KvNumber.bigint.value),
        (1, "bigint", (int, 1), KvNumber.bigint.value),
        (1, JSBigInt, (int, 1), KvNumber.bigint.value),
        (1, KvNumber.bigint, (int, 1), KvNumber.bigint.value),
        (1, KvNumber.bigint.value, (int, 1), KvNumber.bigint.value),
        (KvU64(1), None, (int, 1), KvNumber.u64.value),
        (1, "u64", (int, 1), KvNumber.u64.value),
        (1, KvU64, (int, 1), KvNumber.u64.value),
        (1, KvNumber.u64, (int, 1), KvNumber.u64.value),
        (1, KvNumber.u64.value, (int, 1), KvNumber.u64.value),
    ],
)
def test_init__number_types(
    delta: int | float | KvU64 | JSBigInt,
    number_type: KvNumberInfo | KvNumberIdentifier | None,
    expected_delta: tuple[type[int], int] | tuple[type[float], float],
    expected_number_type: KvNumberInfo,
) -> None:
    sum = Sum(KvKey("a"), delta, cast(KvNumberInfo, number_type))
    assert typeval(sum.delta) == expected_delta
    assert sum.number_type is expected_number_type


def test_init() -> None:
    k = KvKey("a")
    limit1 = Limit(0, 10, "abort")

    sum1 = Sum(k, 1.0)
    assert sum1.key is k
    assert typeval(sum1.delta) == (float, 1.0)
    assert sum1.number_type is KvNumber.float.value
    assert sum1.expire_at is None
    assert sum1.limit == LIMIT_UNLIMITED

    sum2 = Sum(k, 1, "float", expire_at=T1, limit=limit1)
    assert sum2.key == k
    assert typeval(sum2.delta) == (int, 1)
    assert sum2.number_type is KvNumber.float.value
    assert sum2.expire_at is T1
    assert sum2.limit is limit1

    sum3 = Sum(
        key=k, delta=JSBigInt(1), number_type="bigint", expire_at=T1, limit=limit1
    )
    assert sum3.key == k
    assert typeval(sum3.delta) == (int, 1)
    assert sum3.number_type is KvNumber.bigint.value
    assert sum3.expire_at is T1
    assert sum3.limit is limit1

    with pytest.raises(
        TypeError,
        match=re.escape("Sum.__init__() got an unexpected keyword argument 'foo'"),
    ):
        Sum(KvKey("a"), 0, foo="bar")  # type: ignore[call-overload]


def test_init__unsupported_value_type_is_type_error() -> None:
    with pytest.raises(
        TypeError,
        match=re.escape("number is not supported by any KvNumber: Decimal('42')"),
    ):
        Sum(KvKey("a"), Decimal(42))  # type: ignore[call-overload]

    with pytest.raises(
        TypeError,
        match=re.escape(
            "number is not compatible with bigint py number type\n"
            "number: Decimal('42') (<class 'decimal.Decimal'>), "
            "bigint=BigIntKvNumberInfo(name='bigint', py_type=<class 'int'>, "
            "kv_type=<class 'v8serialize.jstypes.jsbigint.JSBigInt'>)"
        ),
    ):
        Sum(KvKey("a"), Decimal(42), "bigint")  # type: ignore[call-overload]


def test_init__float_number_type_rejects_out_of_range_int_values() -> None:
    with pytest.raises(
        ValueError,
        match=re.escape(
            "number is not compatible with float py number type\n"
            "number: 9007199254740992 (<class 'int'>), "
            "float=FloatKvNumberInfo(name='float', py_type=<class 'float'>, "
            "kv_type=<class 'float'>)\n"
            "The int is too large to represent as a 64-bit floating point value."
        ),
    ):
        Sum(KvKey("a"), FLOAT64_SAFE_INT_RANGE.stop, "float")


def test_init__kvu64_limit_cannot_be_changed() -> None:
    assert Sum(KvKey("a"), KvU64(1)).limit == LIMIT_KVU64
    assert Sum(KvKey("a"), KvU64(1), limit=LIMIT_KVU64) == Sum(KvKey("a"), KvU64(1))

    custom_wrap_limit = Limit(max=42, limit_exceeded=LimitExceededPolicy.WRAP)  # type: ignore[arg-type]
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Number type 'u64' wrap limit's min, max bounds cannot be changed\n"
            "'u64' (KvU64) can only wrap at 0 and 2^64 - 1. It can use clamp "
            "with custom bounds through."
        ),
    ):
        Sum(KvKey("a"), KvU64(1), limit=custom_wrap_limit)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Number type 'bigint' does not support wrap limits\n"
            "Use 'u64' (KvU64) to wrap on 0, 2^64 - 1 bounds."
        ),
    ):
        Sum(KvKey("a"), 1, "bigint", limit=LIMIT_KVU64)


# delta values beyond +/-2^64 are wrapped to this range. We still include them
# as inputs, to ensure that we are handling them correctly though. We don't just
# use st.integers() as the input, as using the two separate u64 int classes
# should probe 64-bit boundary values more effectively than just using
# st.integers().
@given(value=u64, delta=u64 | neg_u64 | st.integers())
def test_as_protobuf__u64_wrap(value: int, delta: int) -> None:
    expected = KvU64((value + delta) % KvU64.RANGE.stop)
    sum = Sum(KvKey("a"), delta, "u64")

    actual = apply_sum_mutation(sum, value).value_or_raise()
    assert actual == expected


@given(
    value=u64,
    delta=u64 | neg_u64 | st.integers(),
    clamp_under=st.none() | u64,
    clamp_over=st.none() | u64,
)
# Include examples to always hit branches, to avoid random coverage misses.
# constant result as clamp_over <= clamp_under
@example(value=0, delta=-1, clamp_under=0, clamp_over=0)
# constant result as result always meets clamp_under
@example(value=0, delta=-1, clamp_under=KvU64.RANGE.stop - 2, clamp_over=None)
def test_as_protobuf__u64_clamp(
    value: int, delta: int, clamp_under: int | None, clamp_over: int | None
) -> None:
    expected = KvU64(
        min(
            KvU64.RANGE.stop - 1 if clamp_over is None else clamp_over,
            max(
                0 if clamp_under is None else clamp_under,
                value + delta,
            ),
        )
    )
    sum = Sum(KvKey("a"), delta, "u64", clamp_under=clamp_under, clamp_over=clamp_over)
    actual = apply_sum_mutation(sum, value).value_or_raise()
    assert actual == expected


floats = st.floats(allow_nan=True)
float_safe_integers = st.integers(
    min_value=FLOAT64_SAFE_INT_RANGE.start, max_value=FLOAT64_SAFE_INT_RANGE.stop - 1
)
v8_sum_limits_bigint: st.SearchStrategy[Limit[int]] = st.builds(
    Limit,
    min=st.none() | st.integers(),
    max=st.none() | st.integers(),
    limit_exceeded=st.sampled_from(
        [LimitExceededPolicy.ABORT, LimitExceededPolicy.CLAMP]
    ),
)
v8_sum_limits_float: st.SearchStrategy[Limit[float]] = st.builds(
    Limit,
    max=st.none() | float_safe_integers | floats,
    min=st.none() | float_safe_integers | floats,
    limit_exceeded=st.sampled_from(
        [
            LimitExceededPolicy.ABORT,
            LimitExceededPolicy.CLAMP,
        ]
    ),
)


@given(value=st.integers(), delta=st.integers(), limit=v8_sum_limits_bigint)
def test_as_protobuf__v8_bigint(value: int, delta: int, limit: Limit[int]) -> None:
    _test_as_protobuf__v8(KvNumber.bigint.value, value, delta, limit)


@given(
    value=float_safe_integers | floats,
    delta=float_safe_integers | floats,
    limit=v8_sum_limits_float,
)
def test_as_protobuf__v8_float(value: float, delta: float, limit: Limit[float]) -> None:
    _test_as_protobuf__v8(KvNumber.float.value, value, delta, limit)


def _test_as_protobuf__v8(
    number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
    value: NumberT,
    delta: NumberT,
    limit: Limit[NumberT],
) -> None:
    if limit.limit_exceeded == LimitExceededPolicy.ABORT:
        # Explicitly calculate the expected result in the kv type, as with
        # floats, we can add int values and get greater precision than we would
        # with actual floats. (Normally as_kv_type() preserves ints in
        # float-safe range.)
        expected_value = number_type.kv_type(value) + number_type.kv_type(delta)  # type: ignore[call-arg,operator]
        should_abort = False
        if limit.min is not None and expected_value < limit.min:
            should_abort = True
        if limit.max is not None and expected_value > limit.max:
            should_abort = True
    else:
        should_abort = False
        expected_value = number_type.kv_type(value) + number_type.kv_type(delta)  # type: ignore[call-arg,operator]
        if limit.min is not None and expected_value < limit.min:
            expected_value = limit.min
        if limit.max is not None and expected_value > limit.max:
            expected_value = limit.max

    sum = Sum(KvKey("a"), delta, number_type, limit=limit)
    actual_result = apply_sum_mutation(sum, value)

    if should_abort:
        assert is_err(actual_result)
        assert isinstance(actual_result.error, SumLimitExceeded)
    else:
        actual_value = actual_result.value_or_raise()
        assert actual_value == expected_value or all(
            # We allow nan as an input, and nan can occur independently as a
            # result, e.g. inf + -inf = nan.
            isinstance(x, float) and isnan(x)
            for x in (actual_value, expected_value)
        )


def apply_sum_mutation(
    sum: Sum[str, NumberT, KvNumberTypeT], value: NumberT
) -> Result[KvNumberTypeT, SumLimitExceeded]:
    db = MockKvDb()
    add_entries(db, {sum.key: sum.number_type.as_kv_number(value)})
    mutations = sum.as_protobuf()

    try:
        write_result = db.atomic_write(datapath_pb2.AtomicWrite(mutations=mutations))
    except Exception as e:
        if isinstance((cause := e.__cause__), SumLimitExceeded):
            return Err(cause)
        raise e

    assert write_result.status == datapath_pb2.AW_SUCCESS
    raw_entry = db.snapshot_read_range(read_range_single(sum.key)).values[0]
    entry = unsafe_parse_protobuf_kv_entry(raw_entry)
    assert sum.number_type.is_kv_number(entry.value)
    return Ok(entry.value)
