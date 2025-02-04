from __future__ import annotations

import traceback
from datetime import datetime

import pytest
from yarl import URL

from denokv._kv_writes import Check
from denokv._kv_writes import Enqueue
from denokv._kv_writes import FailedWrite
from denokv._kv_writes import Set
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.datapath import ProtocolViolation
from denokv.kv_keys import KvKey
from denokv.result import is_err

T1 = datetime.fromisoformat("2000-01-02T03:04:05.6Z")
EP = EndpointInfo(URL("https://example.com/"), consistency=ConsistencyLevel.STRONG)


@pytest.fixture
def instance() -> FailedWrite:
    checks = [
        Check.for_key_not_set(KvKey("a")),
        Check.for_key_not_set(KvKey("b")),
        Check.for_key_not_set(KvKey("c")),
    ]
    return FailedWrite(
        checks=list(checks),
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
        cause=ProtocolViolation("Server misbehaved", data=None, endpoint=EP),
    )


@pytest.mark.parametrize(
    "cause", [None, ProtocolViolation("Server misbehaved", data=None, endpoint=EP)]
)
def test_constructor(cause: BaseException | None) -> None:
    checks = [
        Check.for_key_not_set(KvKey("a")),
        Check.for_key_not_set(KvKey("b")),
        Check.for_key_not_set(KvKey("c")),
    ]
    instance = FailedWrite(
        checks=list(checks),
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
        cause=cause,
    )

    assert not instance.ok
    assert instance.versionstamp is None
    assert instance.checks == tuple(checks)
    assert instance.mutations == (Set(KvKey("a"), 42),)
    assert instance.enqueues == (Enqueue("Hi"),)
    assert instance.endpoint is EP
    assert instance.__cause__ is cause

    assert instance.conflicts == {}
    assert not instance.has_unknown_conflicts


def test_exception_attributes(instance: FailedWrite) -> None:
    assert instance.args == ()


def test_changes_to_conflicts_do_not_persist(instance: FailedWrite) -> None:
    conflicts = instance.conflicts
    assert isinstance(conflicts, dict)
    # Changes to conflicts do not persist
    conflicts[KvKey("a")] = instance.checks[0]
    assert instance.conflicts == {}


def test_is_AnyFailure(instance: FailedWrite) -> None:
    assert is_err(instance)


def test_str(instance: FailedWrite) -> None:
    assert (
        str(instance) == "Write failed to 'https://example.com/' "
        "due to ProtocolViolation, with 3 checks, 1 mutations, 1 enqueues"
    )

    instance.__cause__ = None
    assert (
        str(instance) == "Write failed to 'https://example.com/' "
        "due to unspecified cause, with 3 checks, 1 mutations, 1 enqueues"
    )


def test_repr(instance: FailedWrite) -> None:
    assert (
        repr(instance) == "<FailedWrite to 'https://example.com/' "
        "due to ProtocolViolation, with 3 checks, 1 mutations, 1 enqueues>"
    )

    instance.__cause__ = None
    assert (
        repr(instance) == "<FailedWrite to 'https://example.com/' "
        "due to unspecified cause, with 3 checks, 1 mutations, 1 enqueues>"
    )


def test_traceback_presentation(instance: FailedWrite) -> None:
    assert "\n".join(
        traceback.format_exception_only(type(instance), instance)
    ).strip() == (
        "denokv._kv_writes.FailedWrite: Write failed "
        "to 'https://example.com/' "
        "due to ProtocolViolation, with 3 checks, 1 mutations, 1 enqueues"
    )
