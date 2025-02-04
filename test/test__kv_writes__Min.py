import builtins
from typing import Literal  # noqa: TID251

import pytest
from v8serialize import Encoder
from v8serialize.jstypes import JSBigInt

from denokv import _datapath_pb2 as pb2
from denokv._kv_values import KvU64
from denokv._kv_writes import BigIntMin
from denokv._kv_writes import FloatMin
from denokv._kv_writes import KvNumber
from denokv._kv_writes import KvNumberInfo
from denokv._kv_writes import Min
from denokv._kv_writes import U64Min
from denokv._pycompat.typing import Any
from denokv._pycompat.typing import NewType
from denokv._pycompat.typing import assert_type
from denokv._pycompat.typing import cast
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.kv_keys import KvKey
from test.denokv_testing import create_dataclass_slots_test
from test.denokv_testing import typeval

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()
k = KvKey("a")


@pytest.fixture
def instance() -> Min:
    return Min(k, 9, KvNumber.float)


test_instances_dont_have_dict_because_of_slots = create_dataclass_slots_test()


def test_init__float() -> None:
    float_min = Min(k, 9, KvNumber.float.value)
    assert float_min.key is k
    assert typeval(float_min.value) == (int, 9)
    assert float_min.number_type is KvNumber.float.value

    assert typeval(Min(k, 9.0).value) == (float, 9.0)
    assert Min(k, 9) == float_min
    assert Min(k, 9.0) == float_min

    for nt in ("float", KvNumber.float, builtins.float, KvNumber.float.value):
        assert Min(k, 9, nt) == float_min

    assert Min(k, 9, "float", expire_at=T1).expire_at == T1


def test_init__bigint() -> None:
    bigint_min = Min(k, 9, KvNumber.bigint.value)
    assert bigint_min.key is k
    assert typeval(bigint_min.value) == (int, 9)
    assert bigint_min.number_type is KvNumber.bigint.value

    assert Min(k, JSBigInt(9)) == bigint_min

    for nt in ("bigint", KvNumber.bigint, JSBigInt, KvNumber.bigint.value):
        assert Min(k, 9, nt) == bigint_min

    assert Min(k, 9, "bigint", expire_at=T1).expire_at == T1


def test_init__u64() -> None:
    u64_min = Min(k, 9, KvNumber.u64.value)
    assert u64_min.key is k
    assert typeval(u64_min.value) == (int, 9)
    assert u64_min.number_type is KvNumber.u64.value

    assert Min(k, KvU64(9)) == u64_min

    for nt in ("u64", KvNumber.u64, KvU64, KvNumber.u64.value):
        assert Min(k, 9, nt) == u64_min

    assert Min(k, 9, "u64", expire_at=T1).expire_at == T1


def test_init__overloads() -> None:
    k = KvKey("a")
    bigint, float, u64 = KvNumber.bigint.value, KvNumber.float.value, KvNumber.u64.value
    assert assert_type(Min(k, 9), FloatMin).number_type == float
    assert assert_type(Min(k, 9.0), FloatMin).number_type == float
    assert assert_type(Min(k, 9, "float"), FloatMin).number_type == float
    assert assert_type(Min(k, 9, KvNumber.float), FloatMin).number_type == float
    assert assert_type(Min(k, 9, builtins.float), FloatMin).number_type == float
    assert assert_type(Min(k, 9, float), FloatMin).number_type == float

    assert assert_type(Min(k, 9, "bigint"), BigIntMin).number_type == bigint
    assert assert_type(Min(k, 9, KvNumber.bigint), BigIntMin).number_type == bigint
    assert assert_type(Min(k, 9, JSBigInt), BigIntMin).number_type == bigint
    assert assert_type(Min(k, 9, bigint), BigIntMin).number_type == bigint
    assert assert_type(Min(k, JSBigInt(9)), BigIntMin).number_type == bigint

    assert assert_type(Min(k, 9, "u64"), U64Min).number_type == u64
    assert assert_type(Min(k, 9, KvNumber.u64), U64Min).number_type == u64
    assert assert_type(Min(k, 9, KvU64), U64Min).number_type == u64
    assert assert_type(Min(k, 9, u64), U64Min).number_type == u64
    assert assert_type(Min(k, KvU64(9)), U64Min).number_type == u64

    FooInt = NewType("FooInt", int)
    BarInt = NewType("BarInt", int)
    number_info: KvNumberInfo[Literal["test"], FooInt, BarInt] = cast(Any, bigint)
    assert (
        assert_type(
            Min(k, FooInt(1), number_info), Min[Literal["test"], FooInt, BarInt]
        )
    ).number_type == number_info


@pytest.mark.parametrize("number_type", KvNumber)
def test_as_protobuf__float(number_type: KvNumber, v8_encoder: Encoder) -> None:
    mutations = Min(k, 9, number_type.value, expire_at=T1).as_protobuf(
        v8_encoder=v8_encoder
    )
    assert len(mutations) > 0
    # We test the effect of Min mutations elsewhere, e.g. in test_kv.
    assert all(isinstance(m, pb2.Mutation) for m in mutations)
