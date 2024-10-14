from __future__ import annotations

from abc import ABCMeta
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Generic
from typing import Protocol
from typing import TypeVar
from typing import runtime_checkable

from denokv._pycompat.dataclasses import slots_if310

if TYPE_CHECKING:
    from typing_extensions import Never
    from typing_extensions import Self
    from typing_extensions import TypeAlias
    from typing_extensions import TypeIs


@runtime_checkable
class AnySuccess(Protocol, metaclass=ABCMeta):
    def _AnySuccess_marker(self, no_call: Never) -> Never: ...


@runtime_checkable
class AnyFailure(Protocol, metaclass=ABCMeta):
    def _AnyFailure_marker(self, no_call: Never) -> Never: ...


T_co = TypeVar("T_co", covariant=True)
E_co = TypeVar("E_co", covariant=True)


class Some(Generic[T_co]):
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    value: T_co


class Nothing:
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    def __new__(cls) -> Self:
        instance = object.__new__(cls)

        def __new__(cls) -> Self:
            return instance

        Nothing.__new__ = __new__
        return instance


Option: TypeAlias = "Some[T_co] | Nothing"


@AnySuccess.register
@dataclass(frozen=True, **slots_if310())
class Ok(Generic[T_co]):
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    value: T_co

    @property
    def value_or_none(self) -> T_co:
        return self.value

    @property
    def error_or_none(self) -> None:
        return None

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"


@AnyFailure.register
@dataclass(frozen=True, **slots_if310())
class Err(Generic[T_co]):
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

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


def is_ok(result: AnySuccess | AnyFailure) -> TypeIs[AnySuccess]:
    return isinstance(result, AnySuccess)


def is_err(result: AnySuccess | AnyFailure) -> TypeIs[AnyFailure]:
    return isinstance(result, AnyFailure)
