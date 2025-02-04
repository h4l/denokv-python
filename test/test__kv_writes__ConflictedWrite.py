from __future__ import annotations

import traceback
from datetime import datetime

import pytest
from yarl import URL

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_writes import Check
from denokv._kv_writes import ConflictedWrite
from denokv._kv_writes import Enqueue
from denokv._kv_writes import Set
from denokv._pycompat.typing import Iterable
from denokv._pycompat.typing import Sequence
from denokv._pycompat.typing import cast
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.datapath import CheckFailure
from denokv.kv_keys import KvKey
from denokv.result import is_err

T1 = datetime.fromisoformat("2000-01-02T03:04:05.6Z")
EP = EndpointInfo(URL("https://example.com/"), consistency=ConsistencyLevel.STRONG)


@pytest.fixture
def checks() -> tuple[Check, Check, Check]:
    return (
        Check.for_key_not_set(KvKey("a")),
        Check.for_key_not_set(KvKey("b")),
        Check.for_key_not_set(KvKey("c")),
    )


@pytest.fixture
def instance(checks: Iterable[Check]) -> ConflictedWrite:
    pb_checks = [
        datapath_pb2.Check(key=bytes(KvKey("a")), versionstamp=None),
        datapath_pb2.Check(key=bytes(KvKey("b")), versionstamp=None),
        datapath_pb2.Check(key=bytes(KvKey("c")), versionstamp=None),
    ]
    failed_checks = [0, 2]

    cause = CheckFailure(
        "Not all checks required by the Atomic Write passed",
        all_checks=pb_checks,
        failed_check_indexes=failed_checks,
        endpoint=EP,
    )

    return ConflictedWrite(
        failed_checks=failed_checks,
        checks=checks,
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
        cause=cause,
    )


def test_constructor(checks: Sequence[Check]) -> None:
    instance = ConflictedWrite(
        failed_checks=[0, 2],
        checks=cast(Iterable[Check], checks),
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

    assert instance.conflicts == {KvKey("a"): checks[0], KvKey("c"): checks[2]}
    assert instance.conflicts[KvKey("a")] is checks[0]


@pytest.mark.parametrize("failed_checks", [None, [], [0]])
def test_constructor__conflicts_are_always_known_with_single_check(
    failed_checks: Iterable[int] | None,
) -> None:
    instance = ConflictedWrite(
        failed_checks=failed_checks,
        checks=iter([Check.for_key_not_set(KvKey("a"))]),
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )

    assert KvKey("a") in instance.conflicts
    assert instance.conflicts[KvKey("a")].key == KvKey("a")
    assert not instance.has_unknown_conflicts


@pytest.mark.parametrize("failed_checks", [None, []])
def test_constructor__conflicts_are_unknown_with_multiple_checks_without_failed_checks(
    failed_checks: Iterable[int] | None, checks: Iterable[Check]
) -> None:
    instance = ConflictedWrite(
        failed_checks=failed_checks,
        checks=checks,
        mutations=[Set(KvKey("a"), 42)],
        enqueues=[Enqueue("Hi")],
        endpoint=EP,
    )

    assert len(instance.conflicts) == 0
    assert instance.has_unknown_conflicts


def test_constructor__rejects_out_of_bounds_failed_checks(
    checks: tuple[Check, Check, Check],
) -> None:
    assert len(checks) == 3
    with pytest.raises(ValueError, match=r"failed_checks contains out-of-bounds index"):
        ConflictedWrite(
            failed_checks=[0, 10],
            checks=checks,
            mutations=[Set(KvKey("a"), 42)],
            enqueues=[],
            endpoint=EP,
        )


def test_changes_to_conflicts_do_not_persist(instance: ConflictedWrite) -> None:
    assert isinstance(instance.conflicts, dict)
    # Changes to conflicts do not persist
    assert KvKey("a") in instance.conflicts
    del instance.conflicts[KvKey("a")]
    assert KvKey("a") in instance.conflicts


def test_is_AnyFailure(instance: ConflictedWrite) -> None:
    assert is_err(instance)


@pytest.mark.parametrize("with_cause", [True, False])
def test_str(instance: ConflictedWrite, with_cause: bool) -> None:
    assert instance.__cause__
    if not with_cause:
        instance.__cause__ = None
    assert (
        str(instance) == "Write NOT APPLIED to 'https://example.com/' "
        "with 2/3 checks CONFLICTING, 1 mutations, 1 enqueues"
    )


@pytest.mark.parametrize("with_cause", [True, False])
def test_repr(instance: ConflictedWrite, with_cause: bool) -> None:
    assert instance.__cause__
    if not with_cause:
        instance.__cause__ = None

    assert (
        repr(instance) == "<ConflictedWrite NOT APPLIED to 'https://example.com/' "
        "with 2/3 checks CONFLICTING, 1 mutations, 1 enqueues>"
    )


@pytest.mark.parametrize("with_cause", [True, False])
def test_traceback_presentation(instance: ConflictedWrite, with_cause: bool) -> None:
    assert instance.__cause__
    if not with_cause:
        instance.__cause__ = None

    assert "\n".join(
        traceback.format_exception_only(type(instance), instance)
    ).strip() == (
        "denokv._kv_writes.ConflictedWrite: "
        "Write NOT APPLIED to 'https://example.com/' "
        "with 2/3 checks CONFLICTING, 1 mutations, 1 enqueues"
    )
