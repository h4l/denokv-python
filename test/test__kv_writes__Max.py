import builtins
from typing import Literal

import pytest
from v8serialize import Encoder
from v8serialize.jstypes import JSBigInt

from denokv import _datapath_pb2 as pb2
from denokv._kv_values import KvU64
from denokv._kv_writes import BigIntMax
from denokv._kv_writes import FloatMax
from denokv._kv_writes import KvNumber
from denokv._kv_writes import KvNumberInfo
from denokv._kv_writes import Max
from denokv._kv_writes import U64Max
from denokv._pycompat.typing import Any
from denokv._pycompat.typing import NewType
from denokv._pycompat.typing import assert_type
from denokv._pycompat.typing import cast
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.kv_keys import KvKey
from test.denokv_testing import typeval

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()
k = KvKey("a")


def test_init__float() -> None:
    float_max = Max(k, 9, KvNumber.float.value)
    assert float_max.key is k
    assert typeval(float_max.value) == (int, 9)
    assert float_max.number_type is KvNumber.float.value

    assert typeval(Max(k, 9.0).value) == (float, 9.0)
    assert Max(k, 9) == float_max
    assert Max(k, 9.0) == float_max

    for nt in ("float", KvNumber.float, builtins.float, KvNumber.float.value):
        assert Max(k, 9, nt) == float_max

    assert Max(k, 9, "float", expire_at=T1).expire_at == T1


def test_init__bigint() -> None:
    bigint_max = Max(k, 9, KvNumber.bigint.value)
    assert bigint_max.key is k
    assert typeval(bigint_max.value) == (int, 9)
    assert bigint_max.number_type is KvNumber.bigint.value

    assert Max(k, JSBigInt(9)) == bigint_max

    for nt in ("bigint", KvNumber.bigint, JSBigInt, KvNumber.bigint.value):
        assert Max(k, 9, nt) == bigint_max

    assert Max(k, 9, "bigint", expire_at=T1).expire_at == T1


def test_init__u64() -> None:
    u64_max = Max(k, 9, KvNumber.u64.value)
    assert u64_max.key is k
    assert typeval(u64_max.value) == (int, 9)
    assert u64_max.number_type is KvNumber.u64.value

    assert Max(k, KvU64(9)) == u64_max

    for nt in ("u64", KvNumber.u64, KvU64, KvNumber.u64.value):
        assert Max(k, 9, nt) == u64_max

    assert Max(k, 9, "u64", expire_at=T1).expire_at == T1


def test_init__overloads() -> None:
    k = KvKey("a")
    bigint, float, u64 = KvNumber.bigint.value, KvNumber.float.value, KvNumber.u64.value
    assert assert_type(Max(k, 9), FloatMax).number_type == float
    assert assert_type(Max(k, 9.0), FloatMax).number_type == float
    assert assert_type(Max(k, 9, "float"), FloatMax).number_type == float
    assert assert_type(Max(k, 9, KvNumber.float), FloatMax).number_type == float
    assert assert_type(Max(k, 9, builtins.float), FloatMax).number_type == float
    assert assert_type(Max(k, 9, float), FloatMax).number_type == float

    assert assert_type(Max(k, 9, "bigint"), BigIntMax).number_type == bigint
    assert assert_type(Max(k, 9, KvNumber.bigint), BigIntMax).number_type == bigint
    assert assert_type(Max(k, 9, JSBigInt), BigIntMax).number_type == bigint
    assert assert_type(Max(k, 9, bigint), BigIntMax).number_type == bigint
    assert assert_type(Max(k, JSBigInt(9)), BigIntMax).number_type == bigint

    assert assert_type(Max(k, 9, "u64"), U64Max).number_type == u64
    assert assert_type(Max(k, 9, KvNumber.u64), U64Max).number_type == u64
    assert assert_type(Max(k, 9, KvU64), U64Max).number_type == u64
    assert assert_type(Max(k, 9, u64), U64Max).number_type == u64
    assert assert_type(Max(k, KvU64(9)), U64Max).number_type == u64

    FooInt = NewType("FooInt", int)
    BarInt = NewType("BarInt", int)
    number_info: KvNumberInfo[Literal["test"], FooInt, BarInt] = cast(Any, bigint)
    assert (
        assert_type(
            Max(k, FooInt(1), number_info), Max[Literal["test"], FooInt, BarInt]
        )
    ).number_type == number_info


@pytest.mark.parametrize("number_type", KvNumber)
def test_as_protobuf__float(number_type: KvNumber, v8_encoder: Encoder) -> None:
    mutations = Max(k, 9, number_type.value, expire_at=T1).as_protobuf(
        v8_encoder=v8_encoder
    )
    assert len(mutations) > 0
    # We test the effect of Max mutations elsewhere, e.g. in test_kv.
    assert all(isinstance(m, pb2.Mutation) for m in mutations)
