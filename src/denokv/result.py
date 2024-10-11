from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Generic
from typing import TypeVar

from denokv._pycompat.dataclasses import slots_if310

if TYPE_CHECKING:
    from typing_extensions import TypeAlias
    from typing_extensions import TypeIs

T_co = TypeVar("T_co", covariant=True)
E_co = TypeVar("E_co", covariant=True)


@dataclass(frozen=True, **slots_if310())
class Ok(Generic[T_co]):
    value: T_co

    @property
    def value_or_none(self) -> T_co:
        return self.value

    @property
    def error_or_none(self) -> None:
        return None

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"


@dataclass(frozen=True, **slots_if310())
class Err(Generic[T_co]):
    error: T_co

    @property
    def value_or_none(self) -> None:
        return None

    @property
    def error_or_none(self) -> T_co:
        return self.error

    def __repr__(self) -> str:
        return f"Err({self.error!r})"


Result: TypeAlias = "Ok[T_co] | Err[E_co]"


def is_ok(result: Result[T_co, E_co]) -> TypeIs[Ok[T_co]]:
    return isinstance(result, Ok)


def is_err(result: Result[T_co, E_co]) -> TypeIs[Err[E_co]]:
    return isinstance(result, Err)
