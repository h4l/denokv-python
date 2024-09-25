from dataclasses import dataclass
from typing import Generic
from typing import TypeAlias
from typing import TypeVar

T = TypeVar("T")
E = TypeVar("E")


@dataclass(slots=True, frozen=True)
class Ok(Generic[T]):
    value: T

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"


@dataclass(slots=True, frozen=True)
class Err(Generic[T]):
    error: T

    def __repr__(self) -> str:
        return f"Err({self.error!r})"


Result: TypeAlias = Ok[T] | Err[E]


# Better to use isinstance because mypy doesn't exclude the TypeGuard from the
# else case of an if using is_ok()...

# def is_ok(result: Result[T, E]) -> TypeGuard[Ok[T]]:
#     return isinstance(result, Ok)


# def is_err(result: Result[T, E]) -> TypeGuard[Err[E]]:
#     return isinstance(result, Err)
