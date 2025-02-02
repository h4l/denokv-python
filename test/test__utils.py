from dataclasses import FrozenInstanceError
from dataclasses import dataclass
from enum import Enum

import pytest

from denokv._pycompat.typing import Final
from denokv._utils import frozen


def test_frozen_decorator() -> None:
    @dataclass
    class Info:
        label: Final[str]  # type: ignore[misc]
        size: Final[int]  # type: ignore[misc]

    class Things(Info, Enum):
        FOO = "foo", 42
        BAR = "bar", 100

    # Enums prevent assigning to special enum fields
    with pytest.raises(AttributeError):
        Things.FOO.name = "XXX"  # type: ignore[misc]

    # But custom fields are writable
    assert Things.FOO.label == "foo"
    assert Things.FOO.size == 42

    Things.FOO.label = "lol"  # type: ignore[misc]
    del Things.FOO.size

    assert Things.FOO.label == "lol"
    assert not hasattr(Things.FOO, "size")

    # By not when using @frozen

    @frozen
    class FrozenThings(Info, Enum):
        FOO = "foo", 42
        BAR = "bar", 100

    with pytest.raises(FrozenInstanceError):
        FrozenThings.FOO.name = "XXX"  # type: ignore[misc]

    assert FrozenThings.FOO.label == "foo"
    assert FrozenThings.FOO.size == 42

    with pytest.raises(FrozenInstanceError):
        FrozenThings.FOO.label = "lol"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        del FrozenThings.FOO.size

    assert FrozenThings.FOO.label == "foo"
    assert FrozenThings.FOO.size == 42
