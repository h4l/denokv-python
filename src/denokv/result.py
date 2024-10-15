from __future__ import annotations

from abc import ABC
from abc import ABCMeta
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import Protocol
from typing import TypeVar
from typing import overload
from typing import runtime_checkable

from denokv._pycompat.dataclasses import slots_if310

if TYPE_CHECKING:
    from typing_extensions import Never
    from typing_extensions import Self
    from typing_extensions import TypeAlias
    from typing_extensions import TypeGuard
    from typing_extensions import TypeIs


@runtime_checkable
class AnySuccess(Protocol, metaclass=ABCMeta):
    def _AnySuccess_marker(self, no_call: Never) -> Never: ...


@runtime_checkable
class AnyFailure(Protocol, metaclass=ABCMeta):
    def _AnyFailure_marker(self, no_call: Never) -> Never: ...


T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
E = TypeVar("E")
E_co = TypeVar("E_co", covariant=True)
U = TypeVar("U")


class Some(Generic[T_co]):
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    value: T_co


class Nothing:
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    def __new__(cls) -> Self:
        instance = object.__new__(cls)

        def __new__(cls: type[Nothing]) -> Nothing:
            return instance

        Nothing.__new__ = __new__  # type: ignore[method-assign,assignment]
        return instance


Option: TypeAlias = "Some[T_co] | Nothing"


class ResultMethods(Iterable[T_co], Protocol[T_co, E_co]):
    def and_(self, result: Result[U, E]) -> Result[U, E_co | E]:
        """
        Return result if this is Ok otherwise return this Err.

        Examples
        --------
        >>> assert Ok(2).and_(Ok(4)) == Ok(4)
        >>> assert Ok(2).and_(Ok(b'')) == Ok(b'')
        >>> assert Ok(2).and_(Err('x')) == Err('x')
        >>> assert Err('x').and_(Ok(4)) == Err('x')
        >>> assert Err('x').and_(Err('y')) == Err('x')
        """

    def and_then(self, fn: Callable[[T_co], Result[U, E]]) -> Result[U, E_co | E]:
        """
        Return the Result of `fn(self.value)` if this is Ok, otherwise return this Err.

        Examples
        --------
        >>> assert Ok(2).and_then(lambda x: Ok(x * 2)) == Ok(4)
        >>> assert Ok(2).and_then(lambda x: Ok([x])) == Ok([2])
        >>> assert Ok(2).and_then(lambda x: Err('x')) == Err('x')
        >>> assert Err('x').and_then(lambda x: Ok(x * 2)) == Err('x')
        """

    @property
    def error(self) -> E_co | Never:
        """
        Access the Err's error. Raises if this is Ok.

        Examples
        --------
        >>> assert Err('x').error == 'x'
        >>> Ok(1).error
        Traceback (most recent call last):
        TypeError: attempted to access error from Ok
        """

    def error_or(self, default: U) -> E_co | U:
        """
        Return the Err's value, or default if this is Ok.

        Examples
        --------
        >>> assert Ok(1).error_or(2) == 2
        >>> assert Err('x').error_or(2) == 'x'
        """

    def error_or_else(self, fn: Callable[[], U]) -> E_co | U:
        """
        Return the Err's value, or call `fn()` if this is Ok.

        Examples
        --------
        >>> assert Ok(1).error_or_else(lambda: 2) == 2
        >>> assert Err('x').error_or_else(lambda: 2) == 'x'
        """

    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        """
        Return as is after calling fn with the Ok's value only if this is Ok.

        Examples
        --------
        >>> assert Ok(2).inspect(lambda x: print('val:', x)) == Ok(2)
        val: 2
        >>> assert Err('x').inspect(lambda x: print('val:', x)) == Err('x')
        """

    def inspect_err(self, fn: Callable[[E_co], None]) -> Self:
        """
        Return as is after calling fn with the Err's error only if this is Err.

        Examples
        --------
        >>> assert Ok(2).inspect_err(lambda x: print('val:', x)) == Ok(2)
        >>> assert Err('x').inspect_err(lambda x: print('error:', x)) == Err('x')
        error: x
        """

    def map(self, fn: Callable[[T_co], U]) -> Result[U, E_co]:
        """
        Return `Ok(fn(self.value))` if this is Ok, or the Err as is.

        Examples
        --------
        >>> assert Ok(2).map(lambda x: x * 2) == Ok(4)
        >>> assert Err('x').map(lambda x: x * 2) == Err('x')
        """

    def map_or(self, default: U, fn: Callable[[T_co], U]) -> U:
        """
        Return `fn(self.value)` if this is Ok, or the default if this is Err.

        Examples
        --------
        >>> assert Ok(2).map_or(-1, lambda x: x * 2) == 4
        >>> assert Err('x').map_or(-1, lambda x: x * 2) == -1
        """

    def map_or_else(self, default: Callable[[E_co], U], fn: Callable[[T_co], U]) -> U:
        """
        Return `fn(self.value)` if this is Ok, or `default(self.error)` if this is Err.

        Examples
        --------
        >>> assert Ok(2).map_or_else(lambda e: -len(e), lambda x: x * 2) == 4
        >>> assert Err('x').map_or_else(lambda e: -len(e), lambda x: x * 2) == -1
        """

    def map_err(self, fn: Callable[[E_co], U]) -> Result[T_co, U]:
        """
        Return the Ok as is, or `Err(fn(self.error))` if this is Err.

        Examples
        --------
        >>> assert Ok(2).map_err(lambda e: f'Foo: {e}') == Ok(2)
        >>> assert Err('x').map_err(lambda e: f'Foo: {e}') == Err('Foo: x')
        """

    def ok(self) -> Option[T_co]:
        """
        Convert this Result to an Option of its value.

        Examples
        --------
        >>> Ok(2).ok()
        Some(2)
        >>> Err('fail').ok()
        Nothing()
        """

    def or_(self, default: Result[T, U]) -> Result[T_co | T, U]:
        """
        Return the Ok as-is, or the default Result if this is Err.

        Examples
        --------
        >>> assert Ok(1).or_(Ok(2)) == Ok(1)
        >>> assert Err('error a').or_(Ok(2)) == Ok(2)

        >>> assert Ok(1).or_(Err('error b')) == Ok(1)
        >>> assert Err('error a').or_(Err('error b')) == Err('error b')
        """

    def or_else(self, fn: Callable[[], Result[T, U]]) -> Result[T_co | T, U]:
        """
        Return the Ok as-is, or the Result from calling `fn()` if this is Err.

        Examples
        --------
        >>> assert Ok(1).or_else(lambda: Ok(2)) == Ok(1)
        >>> assert Err('error a').or_else(lambda: Ok(2)) == Ok(2)

        >>> assert Ok(1).or_else(lambda: Err('error b')) == Ok(1)
        >>> assert Err('error a').or_else(lambda: Err('error b')) == Err('error b')
        """

    @property
    def value(self) -> T_co | Never:
        """
        Access the Ok's value. Raises if this is Err.

        Examples
        --------
        >>> assert Ok(1).value == 1
        >>> Err('x').value
        Traceback (most recent call last):
        TypeError: attempted to access value from Err
        """

    def value_or(self, default: U) -> T_co | U:
        """
        Return the Ok's value, or default if this is Err.

        Examples
        --------
        >>> assert Ok(1).value_or(2) == 1
        >>> assert Err('x').value_or(2) == 2
        """

    def value_or_else(self, fn: Callable[[], U]) -> T_co | U:
        """
        Return the Ok's value, or call `fn()` if this is Err.

        Examples
        --------
        >>> assert Ok(1).value_or_else(lambda: 2) == 1
        >>> assert Err('x').value_or_else(lambda: 2) == 2
        """

    def __iter__(self) -> Iterator[T_co]:
        """
        Return an iterator containing the Ok's value or no values if this is Err.

        Examples
        --------
        >>> assert list(Ok(1)) == [1]
        >>> assert list(Err('error a')) == []
        """


@AnySuccess.register
@dataclass(frozen=True, **slots_if310())
class Ok(ResultMethods[T_co, Never]):
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    value: T_co

    def and_(self, result: Result[U, E]) -> Result[U, E]:
        return result

    def and_then(self, fn: Callable[[T_co], Result[U, E]]) -> Result[U, E]:
        return fn(self.value)

    @property
    def error(self) -> Never:
        raise TypeError("attempted to access error from Ok")

    def error_or(self, default: U) -> U:
        return default

    def error_or_else(self, fn: Callable[[], U]) -> U:
        return fn()

    def flatten(self: Ok[Result[U, E]]) -> Result[U, E]:
        """
        Unwrap a Result in a Result into a single Result.

        Examples
        --------
        >>> assert Ok(Ok(2)).flatten() == Ok(2)
        >>> assert Ok(Err('x')).flatten() == Err('x')
        >>> assert Err('x').flatten() == Err('x')

        >>> assert Ok(Ok(Ok(2))).flatten() == Ok(Ok(2))

        # Type error if type-checked
        >>> Ok(2).flatten()
        Traceback (most recent call last):
        TypeError: Ok value does not contain a Result to flatten
        """
        if isinstance(self.value, (Ok, Err)):
            return self.value
        raise TypeError("Ok value does not contain a Result to flatten")

    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        fn(self.value)
        return self

    def inspect_err(self, fn: Callable[[E_co], None]) -> Self:
        return self

    def map(self, fn: Callable[[T_co], U]) -> Ok[U]:
        return Ok(fn(self.value))

    def map_or(self, default: U, fn: Callable[[T_co], U]) -> U:
        return fn(self.value)

    def map_or_else(self, default: Callable[[E_co], U], fn: Callable[[T_co], U]) -> U:
        return fn(self.value)

    def map_err(self, fn: Callable[[E_co], U]) -> Self:
        return self

    def ok(self) -> Some[T_co]:
        return Some(self.value)

    def or_(self, default: Result[T, U]) -> Result[T_co, U]:
        return self

    def or_else(self, fn: Callable[[], Result[T, U]]) -> Result[T_co, U]:
        return self

    def value_or(self, default: U) -> T_co:
        return self.value

    def value_or_else(self, fn: Callable[[], U]) -> T_co:
        return self.value

    def __iter__(self) -> Iterator[T_co]:
        return iter((self.value,))

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"

    @staticmethod
    def is_ok(result: Result[T_co, Any]) -> TypeIs[Ok[T_co]]:
        return isinstance(result, Ok)

    # Note: It doesn't seem to be possible to use TypeIs as the return of
    # is_err_and. If we do, we have to make T object to satisfy the subtype
    # return requirement, and also mypy incorrectly over-narrows the non-matching
    # case to Ok, instead of keeping the non-matching Err.
    @staticmethod
    def is_ok_and(
        result: Result[T, object], check: Callable[[T], TypeIs[U]]
    ) -> TypeGuard[Ok[U]]:
        """Narrow the type of a result to Ok with a particular value type."""
        return isinstance(result, Ok) and check(result.value)


@AnyFailure.register
@dataclass(frozen=True, **slots_if310())
class Err(ResultMethods[Never, E_co]):
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    error: E_co

    def and_(self, result: Result[U, E]) -> Self:
        return self

    def and_then(self, fn: Callable[[T_co], Result[U, E]]) -> Self:
        return self

    def error_or(self, default: U) -> E_co:
        return self.error

    def error_or_else(self, fn: Callable[[], U]) -> E_co:
        return self.error

    def flatten(self) -> Self:
        return self

    flatten.__doc__ = Ok.flatten.__doc__

    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        return self

    def inspect_err(self, fn: Callable[[E_co], None]) -> Self:
        fn(self.error)
        return self

    def map(self, fn: Callable[[T_co], U]) -> Self:
        return self

    def map_or(self, default: U, fn: Callable[[T_co], U]) -> U:
        return default

    def map_or_else(self, default: Callable[[E_co], U], fn: Callable[[T_co], U]) -> U:
        return default(self.error)

    def map_err(self, fn: Callable[[E_co], U]) -> Err[U]:
        return Err(fn(self.error))

    def ok(self) -> Nothing:
        return Nothing()

    def or_(self, default: Result[T_co, U]) -> Result[T_co, U]:
        return default

    def or_else(self, fn: Callable[[], Result[T_co, U]]) -> Result[T_co, U]:
        return fn()

    @property
    def value(self) -> Never:
        raise TypeError("attempted to access value from Err")

    def value_or(self, x_default: U) -> U:
        return x_default

    def value_or_else(self, fn: Callable[[], U]) -> U:
        return fn()

    def __iter__(self) -> Iterator[Never]:
        return iter(())

    def __repr__(self) -> str:
        return f"Err({self.error!r})"

    @staticmethod
    def is_err(result: Result[Any, E_co]) -> TypeIs[Err[E_co]]:
        return isinstance(result, Err)

    # Note: It doesn't seem to be possible to use TypeIs as the return of
    # is_err_and. If we do, we have to make T object to satisfy the subtype
    # return requirement, and also mypy incorrectly over-narrows the non-matching
    # case to Ok, instead of keeping the non-matching Err.
    @staticmethod
    def is_err_and(
        result: Result[Any, T], check: Callable[[T], TypeIs[U]]
    ) -> TypeGuard[Err[U]]:
        """Narrow the type of a result to Err with a particular error type."""
        return isinstance(result, Err) and check(result.error)


Result: TypeAlias = "Ok[T_co] | Err[E_co]"


def is_ok(result: AnySuccess | AnyFailure) -> TypeIs[AnySuccess]:
    return isinstance(result, AnySuccess)


def is_err(result: AnySuccess | AnyFailure) -> TypeIs[AnyFailure]:
    return isinstance(result, AnyFailure)
