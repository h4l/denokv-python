from __future__ import annotations

from dataclasses import FrozenInstanceError

from denokv._pycompat.typing import TypeVar

TypeT = TypeVar("TypeT", bound=type)


def frozen_setattr(cls: type, name: str, value: object) -> None:
    raise FrozenInstanceError(f"Cannot assign to field {name!r}")


def frozen_delattr(cls: type, name: str) -> None:
    raise FrozenInstanceError(f"Cannot delete field {name!r}")


def frozen(cls: TypeT) -> TypeT:
    """Disable `__setattr__` and `__delattr__`, much like @dataclass(frozen=True)."""
    cls.__setattr__ = frozen_setattr  # type: ignore[method-assign,assignment]
    cls.__delattr__ = frozen_delattr  # type: ignore[method-assign,assignment]
    return cls
