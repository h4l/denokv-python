from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Literal

import pytest
from v8serialize.jstypes import JSBigInt

from denokv._kv_values import KvU64
from denokv._kv_writes import BigIntKvNumberInfo
from denokv._kv_writes import FloatKvNumberInfo
from denokv._kv_writes import KvNumber
from denokv._kv_writes import KvNumberInfo
from denokv._kv_writes import U64KvNumberInfo
from denokv._pycompat.typing import assert_type


def test_dataclass_behaviours() -> None:
    assert KvNumber.bigint < KvNumber.float
    assert KvNumber.float < KvNumber.u64
    assert {KvNumber.bigint: "foo"}[KvNumber.bigint] == "foo"

    assert sorted(KvNumber) == [
        KvNumber.bigint,
        KvNumber.float,
        KvNumber.u64,
    ]

    with pytest.raises(FrozenInstanceError):
        KvNumber.bigint.foo = "bar"  # type: ignore[attr-defined]


def test_resolve() -> None:
    assert KvNumber.bigint.name == "bigint"
    assert (
        assert_type(KvNumber.resolve("bigint"), Literal[KvNumber.bigint])
        is KvNumber.bigint
    )
    assert (
        assert_type(KvNumber.resolve("float"), Literal[KvNumber.float])
        is KvNumber.float
    )
    assert assert_type(KvNumber.resolve("u64"), Literal[KvNumber.u64]) is KvNumber.u64
    assert (
        assert_type(KvNumber.resolve(JSBigInt), Literal[KvNumber.bigint])
        is KvNumber.bigint
    )
    assert (
        assert_type(KvNumber.resolve(float), Literal[KvNumber.float]) is KvNumber.float
    )
    assert assert_type(KvNumber.resolve(KvU64), Literal[KvNumber.u64]) is KvNumber.u64


def test_types() -> None:
    assert_type(KvNumber.bigint.value, BigIntKvNumberInfo)
    assert_type(KvNumber.float.value, FloatKvNumberInfo)
    assert_type(KvNumber.u64.value, U64KvNumberInfo)

    _t1: KvNumberInfo[Literal["bigint"], int, JSBigInt] = KvNumber.bigint.value
    _t2: KvNumberInfo[Literal["float"], float, float] = KvNumber.float.value
    _t3: KvNumberInfo[Literal["u64"], int, KvU64] = KvNumber.u64.value

    # name is covariant — can treat the name as str
    _t4: KvNumberInfo[str, int, KvU64] = KvNumber.u64.value
    # number params are invariant — cannot broaden the types
    _t_err1: KvNumberInfo[str, int | float, KvU64] = KvNumber.u64.value  # type: ignore[assignment]
    _t_err2: KvNumberInfo[str, int, KvU64 | float] = KvNumber.u64.value  # type: ignore[assignment]
