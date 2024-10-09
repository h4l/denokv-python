from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Generic
from typing import TypeVar

from denokv._pycompat.dataclasses import slots_if310

if TYPE_CHECKING:
    from typing_extensions import TypeAlias
    from typing_extensions import TypeIs

T = TypeVar("T")
E = TypeVar("E")


@dataclass(frozen=True, **slots_if310())
class Ok(Generic[T]):
    value: T

    @property
    def value_or_none(self) -> T:
        return self.value

    @property
    def error_or_none(self) -> None:
        return None

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"


@dataclass(frozen=True, **slots_if310())
class Err(Generic[T]):
    error: T

    @property
    def value_or_none(self) -> None:
        return None

    @property
    def error_or_none(self) -> T:
        return self.error

    def __repr__(self) -> str:
        return f"Err({self.error!r})"


Result: TypeAlias = "Ok[T] | Err[E]"


def is_ok(result: Result[T, E]) -> TypeIs[Ok[T]]:
    return isinstance(result, Ok)


def is_err(result: Result[T, E]) -> TypeIs[Err[E]]:
    return isinstance(result, Err)
