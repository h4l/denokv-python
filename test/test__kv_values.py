from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from denokv._kv_values import KvEntry
from denokv._kv_values import KvU64
from denokv._kv_values import VersionStamp
from denokv._pycompat.typing import Callable
from denokv.kv_keys import KvKey
from test.denokv_testing import create_dataclass_slots_test


@pytest.fixture(
    params=[
        pytest.param(lambda: KvEntry(KvKey("a"), 42, VersionStamp(1)), id="KvEntry"),
        pytest.param(lambda: VersionStamp(1), id="VersionStamp"),
        pytest.param(lambda: KvU64(1), id="KvU64"),
    ]
)
def instance(request: pytest.FixtureRequest) -> object:
    param: Callable[[], object] = request.param
    return param()


test_instances_dont_have_dict_because_of_slots = create_dataclass_slots_test()


@given(v=st.integers(min_value=0, max_value=2**80 - 1))
def test_VersionStamp_init(v: int) -> None:
    vs_int = VersionStamp(v)
    assert int(vs_int) == v
    assert VersionStamp(str(vs_int)) == vs_int
    assert VersionStamp(bytes(vs_int)) == vs_int
    assert bytes(vs_int) == vs_int
    assert isinstance(vs_int, bytes)


@given(i=st.integers(min_value=0, max_value=2**64 - 1))
def test_KvU64_init(i: int) -> None:
    u64 = KvU64(i)
    assert int(u64) == i
    assert KvU64(bytes(u64)) == u64
    assert u64.to_bytes() == bytes(u64)
    assert u64.to_bytes() == i.to_bytes(8, "little")


@given(
    v1=st.integers(min_value=0, max_value=2**80 - 1),
    v2=st.integers(min_value=0, max_value=2**80 - 1),
)
def test_VersionStamp_ordering(v1: int, v2: int) -> None:
    vs1, vs2 = VersionStamp(v1), VersionStamp(v2)
    if v1 < v2:
        assert vs1 < vs2
    elif v1 > v2:
        assert vs1 > vs2
    else:
        assert vs1 == vs2


def test_KVU64__bytes() -> None:
    assert KvU64(bytes(KvU64(123456789))).value == 123456789
    assert KvU64(KvU64(123456789).to_bytes()).value == 123456789
