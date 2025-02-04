from __future__ import annotations

import pytest
from yarl import URL

from denokv._kv_values import VersionStamp
from denokv._kv_writes import Check
from denokv._kv_writes import CommittedWrite
from denokv._kv_writes import Enqueue
from denokv._kv_writes import Set
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.kv_keys import KvKey
from denokv.result import is_ok
from test.denokv_testing import create_dataclass_slots_test

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()
EP = EndpointInfo(URL("https://example.com/"), consistency=ConsistencyLevel.STRONG)


@pytest.fixture
def instance() -> CommittedWrite:
    return CommittedWrite(
        VersionStamp(1), checks=[], mutations=[], enqueues=[], endpoint=EP
    )


test_instances_dont_have_dict_because_of_slots = create_dataclass_slots_test()


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

    assert instance.conflicts == {}
    assert not instance.has_unknown_conflicts


def test_str_repr() -> None:
    instance = CommittedWrite(
        VersionStamp(1),
        checks=[Check.for_key_not_set(key=KvKey("a"))],
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )
    assert (
        str(instance) == "Write committed version 0x00000000000000000001 "
        "to 'https://example.com/' with 1 checks, 1 mutations, 1 enqueues"
    )
    assert (
        repr(instance) == "<CommittedWrite version 0x00000000000000000001 "
        "to 'https://example.com/' with 1 checks, 1 mutations, 1 enqueues>"
    )
