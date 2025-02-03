from datetime import datetime

import pytest
from yarl import URL

from denokv._kv_writes import Check
from denokv._kv_writes import ConflictedWrite
from denokv._kv_writes import Enqueue
from denokv._kv_writes import Set
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.kv_keys import KvKey
from denokv.result import is_err

T1 = datetime.fromisoformat("2000-01-02T03:04:05.6Z")
EP = EndpointInfo(URL("https://example.com/"), consistency=ConsistencyLevel.STRONG)


@pytest.fixture
def instance() -> ConflictedWrite:
    checks = [
        Check.for_key_not_set(KvKey("a")),
        Check.for_key_not_set(KvKey("b")),
        Check.for_key_not_set(KvKey("c")),
    ]
    return ConflictedWrite(
        failed_checks=[0, 2],
        checks=list(checks),
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )


def test_constructor(instance: ConflictedWrite) -> None:
    checks = [
        Check.for_key_not_set(KvKey("a")),
        Check.for_key_not_set(KvKey("b")),
        Check.for_key_not_set(KvKey("c")),
    ]
    instance = ConflictedWrite(
        failed_checks=[0, 2],
        checks=list(checks),
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )

    assert not instance.ok
    assert instance.versionstamp is None
    assert instance.checks == tuple(checks)
    assert instance.mutations == (Set(KvKey("a"), 42),)
    assert instance.enqueues == (Enqueue("Hi"),)
    assert instance.endpoint is EP

    assert dict(instance.conflicts) == {KvKey("a"): checks[0], KvKey("c"): checks[2]}
    assert instance.conflicts[KvKey("a")] is checks[0]

    # conflicts is immutable
    with pytest.raises(TypeError):
        del instance.conflicts[KvKey("a")]  # type: ignore[attr-defined]

    with pytest.raises(ValueError, match=r"failed_checks contains out-of-bounds index"):
        ConflictedWrite(
            failed_checks=[0, 10],
            checks=list(checks),
            mutations=[Set(KvKey("a"), 42)],
            enqueues=[],
            endpoint=EP,
        )


def test_is_AnyFailure(instance: ConflictedWrite) -> None:
    assert is_err(instance)


def test_str_repr(instance: ConflictedWrite) -> None:
    assert (
        str(instance) == "<ConflictedWrite NOT APPLIED to 'https://example.com/' "
        "with 2/3 checks CONFLICTING, 1 mutations, 1 enqueues>"
    )
    assert str(instance) == repr(instance)
