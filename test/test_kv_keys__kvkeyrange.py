from typing import Literal
from typing import cast

import pytest

from denokv.kv_keys import Exclude
from denokv.kv_keys import Include
from denokv.kv_keys import IncludeAll
from denokv.kv_keys import IncludePrefix
from denokv.kv_keys import KvKey
from denokv.kv_keys import KvKeyRange
from denokv.kv_keys import StartBoundary
from denokv.kv_keys import StopBoundary


def test_types() -> None:
    _bound1: Include[KvKey[Literal["a"], Literal[1]]] = Include(KvKey("a", 1))
    _bound2: Include[KvKey[Literal["a"], Literal[1]]] = Include("a", 1)
    _bound3: Exclude[KvKey[Literal["a"], Literal[1]]] = Exclude(KvKey("a", 1))
    _bound4: Exclude[KvKey[Literal["a"], Literal[10]]] = Exclude("a", 10)
    _bound5: IncludePrefix[KvKey[Literal["a"], Literal[1]]] = IncludePrefix(
        KvKey("a", 1)
    )
    _bound6: IncludePrefix[KvKey[Literal["a"], Literal[1]]] = IncludePrefix("a", 1)

    start_boundary = cast(StartBoundary, IncludeAll())
    stop_boundary = cast(StopBoundary, IncludeAll())

    _range_unspecified: KvKeyRange = KvKeyRange(
        start=start_boundary, stop=stop_boundary
    )

    _range_open: KvKeyRange[IncludeAll, IncludeAll] = KvKeyRange()
    _range_open = IncludeAll.range()
    _range_open = IncludeAll().range()
    _range_open = KvKeyRange(IncludeAll())
    _range_open = KvKeyRange(stop=IncludeAll())

    _range_include_exclude1: KvKeyRange[
        Include[KvKey[Literal["a"], Literal[1]]],
        Exclude[KvKey[Literal["a"], Literal[10]]],
    ] = KvKeyRange(_bound1, _bound4)

    _range_include_exclude2: KvKeyRange[
        Include[KvKey[Literal["a"], Literal[1]]],
        Exclude[KvKey[Literal["a"], Literal[10]]],
    ] = KvKeyRange(Include("a", 1), Exclude("a", 10))

    _range_exclude_includeprefix1: KvKeyRange[
        Exclude[KvKey[str, int]], IncludePrefix[KvKey[str, int]]
    ] = IncludePrefix("a", 10).range_start(Exclude("a", 0))

    _range_exclude_includeprefix2: KvKeyRange[
        Exclude[KvKey[str, int]], IncludePrefix[KvKey[str, int]]
    ] = _bound6.range_start(Exclude("a", 0))


def test_KvKey__range() -> None:
    assert KvKey("a", 1).range() == KvKeyRange(Include("a", 1), IncludePrefix("a", 1))
    assert KvKey("a", 10).range_start(Include("a", 1)) == KvKeyRange(
        Include("a", 1), Exclude("a", 10)
    )
    assert KvKey("a", 1).range_stop(Exclude("a", 10)) == KvKeyRange(
        Include("a", 1), Exclude("a", 10)
    )


def test_Include() -> None:
    assert Include("a", 1) == Include(KvKey("a", 1))
    assert Include(("a", 1)) == Include(("a", 1))

    assert ("a", 1) in Include("a", 1).range()
    assert Include("a", 1).range() == KvKeyRange(Include(("a", 1)), Include(("a", 1)))
    assert Include("a", 1).range_stop(Exclude("a", 10)) == KvKeyRange(
        Include(("a", 1)), Exclude(("a", 10))
    )
    assert Include("a", 9).range_start(Include("a", 1)) == KvKeyRange(
        Include(("a", 1)), Include(("a", 9))
    )


def test_Exclude() -> None:
    assert Exclude("a", 1) == Exclude(KvKey("a", 1))
    assert Exclude(("a", 1)) == Exclude(("a", 1))

    assert ("a", 1) not in Exclude("a", 1).range()
    assert Exclude("a", 1).range() == KvKeyRange(Exclude(("a", 1)), Exclude(("a", 1)))
    assert Exclude("a", 1).range_stop(Exclude("a", 10)) == KvKeyRange(
        Exclude(("a", 1)), Exclude(("a", 10))
    )
    assert Exclude("a", 9).range_start(Exclude("a", 1)) == KvKeyRange(
        Exclude(("a", 1)), Exclude(("a", 9))
    )


def test_IncludeAll() -> None:
    a, b = IncludeAll(), IncludeAll()
    assert a is b, "IncludeAll() returns a singleton instance"

    assert ("a", 1) in IncludeAll().range()
    assert ("a", 1) in IncludeAll.range()
    assert IncludeAll.range() == KvKeyRange(IncludeAll(), IncludeAll())
    assert IncludeAll().range_stop(Exclude("a", 10)) == KvKeyRange(
        IncludeAll(), Exclude(("a", 10))
    )
    assert IncludeAll().range_start(Exclude("a", 1)) == KvKeyRange(
        Exclude(("a", 1)), IncludeAll()
    )


def test_IncludePrefix() -> None:
    assert IncludePrefix("a", 1) == IncludePrefix(KvKey("a", 1))
    assert IncludePrefix(("a", 1)) == IncludePrefix(("a", 1))

    assert IncludePrefix("a", 1).range() == KvKeyRange(
        Include(("a", 1)), IncludePrefix(("a", 1))
    )
    assert IncludePrefix("a", 1).range_stop(Exclude("a", 10)) == KvKeyRange(
        Include(("a", 1)), Exclude(("a", 10))
    )
    assert IncludePrefix("a", 9).range_start(Include("a", 1)) == KvKeyRange(
        Include(("a", 1)), IncludePrefix(("a", 9))
    )


def test_KvKeyRange() -> None:
    assert KvKeyRange() == KvKeyRange(IncludeAll(), IncludeAll())
    assert KvKeyRange(Include("a", 1)) == KvKeyRange(Include("a", 1), IncludeAll())
    assert KvKeyRange(Include("a", 1), Exclude("a", 10)) == KvKeyRange(
        Include("a", 1), Exclude("a", 10)
    )
    # IncludePrefix as start is not allowed by type, but normalised to Include()
    # at runtime.
    assert KvKeyRange(IncludePrefix("a", 1), IncludePrefix("a", 1)) == KvKeyRange(  # type: ignore[type-var]
        Include("a", 1), IncludePrefix("a", 1)
    )


@pytest.mark.parametrize(
    "start, key, key_included",
    [
        (IncludeAll(), KvKey("a"), True),
        (Include("b", 1), KvKey("a"), False),
        (Include("b", 1), KvKey("b", 0), False),
        (Include("b", 1), KvKey("b"), False),
        (Include("b", 1), KvKey("b", 1), True),
        (Include("b", 1), KvKey("b", 1, 2, 3), True),
        (Include("b", 1), KvKey("b", 2), True),
        (Exclude("b", 1), KvKey("a"), False),
        (Exclude("b", 1), KvKey("b", 0), False),
        (Exclude("b", 1), KvKey("b"), False),
        (Exclude("b", 1), KvKey("b", 1), False),
        (Include("b", 1), KvKey("b", 1, 2, 3), True),
        (Exclude("b", 1), KvKey("b", 2), True),
    ],
)
def test_contains__start(start: StartBoundary, key: KvKey, key_included: bool) -> None:
    key_range = KvKeyRange(start, IncludeAll())

    assert (key in key_range) == key_included


@pytest.mark.parametrize(
    "stop, key, key_included",
    [
        (IncludeAll(), KvKey("z"), True),
        (Include("b", 10), KvKey("z"), False),
        (Include("b", 10), KvKey("b", 11), False),
        (Exclude("b", 10), KvKey("b", 10, 1, 2), False),
        (Include("b", 10), KvKey("b", 10), True),
        (Include("b", 10), KvKey("b", 9, 1, 2), True),
        (Include("b", 10), KvKey("b", 9), True),
        (Exclude("b", 10), KvKey("z"), False),
        (Exclude("b", 10), KvKey("b", 11), False),
        (Exclude("b", 10), KvKey("b", 10, 1, 2), False),
        (Exclude("b", 10), KvKey("b", 10), False),
        (Exclude("b", 10), KvKey("b", 9, 1, 2), True),
        (Exclude("b", 10), KvKey("b", 9), True),
        (IncludePrefix("b", 10), KvKey("z"), False),
        (IncludePrefix("b", 10), KvKey("b", 11), False),
        (IncludePrefix("b", 10), KvKey("b", 10, 1, 2), True),
        (IncludePrefix("b", 10), KvKey("b", 10), True),
        (IncludePrefix("b", 10), KvKey("b", 9, 1, 2), True),
        (IncludePrefix("b", 10), KvKey("b", 9), True),
    ],
)
def test_contains__stop(
    stop: StopBoundary,
    key: KvKey,
    key_included: bool,
) -> None:
    key_range = KvKeyRange(IncludeAll(), stop)

    assert (key in key_range) == key_included
