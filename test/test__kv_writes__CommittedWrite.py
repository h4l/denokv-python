from __future__ import annotations

from datetime import datetime

from yarl import URL

from denokv._kv_values import VersionStamp
from denokv._kv_writes import Check
from denokv._kv_writes import CommittedWrite
from denokv._kv_writes import Enqueue
from denokv._kv_writes import Set
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.kv_keys import KvKey
from denokv.result import is_ok

T1 = datetime.fromisoformat("2000-01-02T03:04:05.6Z")
EP = EndpointInfo(URL("https://example.com/"), consistency=ConsistencyLevel.STRONG)


def test_is_AnySuccess() -> None:
    assert is_ok(
        CommittedWrite(
            VersionStamp(1), checks=[], mutations=[], enqueues=[], endpoint=EP
        )
    )


def test_constructors() -> None:
    instance = CommittedWrite(
        VersionStamp(1), checks=[], mutations=[], enqueues=[], endpoint=EP
    )
    assert instance.ok
    assert instance.versionstamp == VersionStamp(1)
    assert instance.checks == ()
    assert instance.mutations == ()
    assert instance.enqueues == ()
    assert instance.endpoint is EP

    instance = CommittedWrite(
        VersionStamp(1),
        checks=[Check.for_key_not_set(key=KvKey("a"))],
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )
    assert instance.ok
    assert instance.versionstamp == VersionStamp(1)
    assert instance.checks == (Check.for_key_not_set(key=KvKey("a")),)
    assert instance.mutations == (Set(KvKey("a"), 42),)
    assert instance.enqueues == (Enqueue("Hi"),)
    assert instance.endpoint is EP


def test_str_repr() -> None:
    instance = CommittedWrite(
        VersionStamp(1),
        checks=[Check.for_key_not_set(key=KvKey("a"))],
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )
    assert (
        str(instance) == "<CommittedWrite version 0x00000000000000000001 "
        "to 'https://example.com/' with 1 checks, 1 mutations, 1 enqueues>"
    )
    assert str(instance) == repr(instance)
