from __future__ import annotations

from abc import ABCMeta
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Final
from typing import Generic
from typing import Iterable
from typing import Iterator
from typing import Protocol
from typing import TypeVar
from typing import Union
from typing import cast
from typing import overload
from typing import runtime_checkable

from denokv._pycompat.dataclasses import slots_if310

if TYPE_CHECKING:
    from typing_extensions import Never
    from typing_extensions import ParamSpec
    from typing_extensions import Self
    from typing_extensions import TypeAlias
    from typing_extensions import TypeGuard
    from typing_extensions import TypeIs

    P = ParamSpec("P")


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
V = TypeVar("V")
F = TypeVar("F", bound=Callable[..., Any])


def doc_from(source: object) -> Callable[[F], F]:
    def copy_docstring(fn: F) -> F:
        try:
            fn_name = fn.__name__
            t_fn = getattr(source, fn_name)
            t_fn_doc = t_fn.__doc__
            assert (
                getattr(fn, "__doc__", None) is None
            ), "target already has a __doc__ value"
            fn.__doc__ = t_fn_doc
        except Exception as e:
            raise ValueError(
                f"Failed to copy docstring from source {source!r} for function {fn!r}"
            ) from e
        return fn

    return copy_docstring


class OptionMethods(Iterable[T_co], Protocol[T_co]):
    """Methods implemented by Some and Nothing."""

    def or_raise(
        self, exc: Callable[P, BaseException], *exc_args: P.args, **exc_kwargs: P.kwargs
    ) -> T_co:
        """
        Return the value of this Some or throw an exception if this is Nothing.

        If exc is an exception type and no arguments are provided, the type is
        passed a string message indicating that the option is Nothing.

        Notes
        -----
        - Use `value_or()` / `value_or_else()` to return a default instead of
          raising.
        - Use the `value` property to get the value while raising a TypeError if
          the Option is Nothing.

        Examples
        --------
        >>> Some(1).or_raise(AssertionError)
        1
        >>> Nothing().or_raise(AssertionError)
        Traceback (most recent call last):
        AssertionError: attempted to access value from Nothing
        >>> Nothing().or_raise(ValueError, 'No values were provided')
        Traceback (most recent call last):
        ValueError: No values were provided
        """

    def filter(
        self, check: type[U] | Callable[[T_co], bool] | None = None
    ) -> Option[T_co]:
        """
        Return Some if its value passes a check, otherwise Nothing.

        If no check is provided, values are kept if they are True according to
        `bool()`.

        This method cannot narrow the return type, use
        [Options.filter](`denokv.result.Options.is_some_and`) to narrow the return
        type.

        Examples
        --------
        >>> Some('a').filter(int)
        Nothing()
        >>> Some(1).filter(int)
        Some(1)
        >>> Some(-1).filter(lambda x: x > 0)
        Nothing()
        >>> Some(1).filter(lambda x: x > 0)
        Some(1)
        """

    def flatten(self) -> Self:
        """
        Flatten a Some containing an Option into a single level.

        Examples
        --------
        >>> Some(Some(2)).flatten()
        Some(2)
        >>> Some(2).flatten()
        Some(2)
        >>> Some(Nothing()).flatten()
        Nothing()
        >>> Nothing().flatten()
        Nothing()
        """

    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        """
        Call `fn(self.value)` only if this is Some, then return self.

        Examples
        --------
        >>> assert Some('Hello!').inspect(print) == Some('Hello!')
        Hello!
        >>> assert Nothing().inspect(print) == Nothing()
        """

    def map(self, fn: Callable[[T_co], U]) -> Option[U]:
        """
        Transform this Some's value with a function, otherwise Nothing.

        Examples
        --------
        >>> Some(2).map(lambda x: x * 2)
        Some(4)
        >>> Nothing().map(lambda x: x * 2)
        Nothing()
        """

    def map_or(self, default: U, fn: Callable[[T_co], V]) -> U | V:
        """
        Return `fn(this.value)` or default if this is Nothing.

        Examples
        --------
        >>> Some(2).map_or(-1, lambda x: x * 2)
        4
        >>> Nothing().map_or(-1, lambda x: x * 2)
        -1
        >>> Nothing().map_or('foo', lambda x: x * 2)
        'foo'
        """

    def map_or_else(
        self, default_fn: Callable[[], U], fn: Callable[[T_co], V]
    ) -> U | V:
        """
        Return `fn(this.value)` or `default_fn()` if this is Nothing.

        Examples
        --------
        >>> Some(2).map_or_else(lambda: -1, lambda x: x * 2)
        4
        >>> Nothing().map_or_else(lambda: -1, lambda x: x * 2)
        -1
        >>> Nothing().map_or_else(lambda: 'foo', lambda x: x * 2)
        'foo'
        """

    def ok_or(self, error: E) -> Result[T_co, E]:
        """
        Transform this into a Result of `Ok(self.value)` or `Err(error)` if Nothing.

        Examples
        --------
        >>> Some(1).ok_or('x')
        Ok(1)
        >>> Nothing().ok_or('x')
        Err('x')
        """

    def ok_or_else(self, fn: Callable[[], E]) -> Result[T_co, E]:
        """
        Transform this into a Result of `Ok(self.value)` or `Err(fn())` if Nothing.

        Examples
        --------
        >>> Some(1).ok_or_else(lambda: 'x')
        Ok(1)
        >>> Nothing().ok_or_else(lambda: 'x')
        Err('x')
        """

    def or_(self, other: Option[U]) -> Option[T_co | U]:
        """
        Return this if Some, otherwise other.

        Examples
        --------
        >>> Some(1).or_(Some(2))
        Some(1)
        >>> Some(1).or_(Nothing())
        Some(1)
        >>> Nothing().or_(Some(2))
        Some(2)
        """

    def or_else(self, fn: Callable[[], Option[U]]) -> Option[T_co | U]:
        """
        Return this if Some, otherwise the Option result of `fn()`.

        Examples
        --------
        >>> Some(1).or_else(lambda: Some(2))
        Some(1)
        >>> Some(1).or_else(lambda: Nothing())
        Some(1)
        >>> Nothing().or_else(lambda: Some(2))
        Some(2)
        """

    def value_or(self, default: U) -> T_co | U:
        """
        Return the value in this Some or default if this is Nothing.

        Examples
        --------
        >>> Some(1).value_or(2)
        1
        >>> Nothing().value_or(2)
        2
        """

    def value_or_else(self, fn: Callable[[], U]) -> T_co | U:
        """
        Return the value in this Some or the result of `fn()` if this is Nothing.

        Examples
        --------
        >>> Some(1).value_or_else(lambda: 2)
        1
        >>> Nothing().value_or_else(lambda: 2)
        2
        """

    def xor(self, other: Option[U]) -> Option[T_co | U]:
        """
        If just one of self and other is Some, return it, otherwise Nothing().

        Examples
        --------
        >>> Some(1).xor(Some(2))
        Nothing()
        >>> Some(1).xor(Nothing())
        Some(1)
        >>> Nothing().xor(Some(2))
        Some(2)
        """

    def zip(self, other: Option[U]) -> Option[tuple[T_co, U]]:
        """
        Pair this value with the value in other if both are Some.

        Examples
        --------
        >>> from denokv.result import Some, Nothing
        >>> Some(1).zip(Some(2))
        Some((1, 2))
        >>> Some(1).zip(Nothing())
        Nothing()
        >>> Nothing().zip(Some(2))
        Nothing()
        """

    def zip_with(self, other: Option[U], fn: Callable[[T_co, U], V]) -> Option[V]:
        """
        Call fn with the pair of this and the value in other if both are Some.

        Examples
        --------
        >>> Some('FF').zip_with(Some(16), int)
        Some(255)
        >>> Some('FF').zip_with(Nothing(), int)
        Nothing()
        >>> Nothing().zip_with(Some(16), int)
        Nothing()
        """


class Options(type):
    """
    Utility functions for Option (Some | None) types.

    Some and Nothing represent the presence or absence of a value as Some(_)
    and Nothing().
    """

    # mypy seems to ignore static methods if new is typed like this.
    if not TYPE_CHECKING:

        def __new__(self) -> Never:
            raise TypeError("cannot create instances of Options")

    @staticmethod
    def is_nothing(option: Option[T]) -> TypeIs[Nothing]:
        """
        Check if an Option is Nothing, narrowing its type.

        Example
        -------
        >>> Options.is_nothing(Some(1))
        False
        >>> Options.is_nothing(Nothing())
        True

        Notes
        -----
        This function is static because Python can't narrow the type of self
        with instance methods.
        """
        return isinstance(option, Nothing)

    @overload
    @staticmethod
    def is_nothing_or(option: Option[object], check: type[U]) -> TypeIs[Option[U]]: ...

    @overload
    @staticmethod
    def is_nothing_or(
        option: Option[T], check: Callable[[T], TypeIs[U]]
    ) -> TypeGuard[Option[U]]: ...

    # MyPy complains: "Overloaded function implementation does not accept all
    # possible arguments of signature 2". Seems fine to me...
    @staticmethod  # type: ignore[misc]
    def is_nothing_or(
        option: Option[T], check: type[U] | Callable[[T], TypeIs[U]]
    ) -> bool:
        """
        Narrow the type of an Option to Nothing, or Some whose value passes a test.

        Examples
        --------
        >>> is_negative = lambda x: x < 0
        >>> Options.is_nothing_or(Some(1), is_negative)
        False
        >>> Options.is_nothing_or(Some(-1), is_negative)
        True
        >>> Options.is_nothing_or(Nothing(), is_negative)
        True

        Notes
        -----
        This function is static because Python can't narrow the type of self
        with instance methods.
        """
        if isinstance(option, Nothing):
            return True
        if not isinstance(option, Some):
            raise TypeError(f"expected Some or Nothing, got: {option}")
        if isinstance(check, type):
            return isinstance(option.value, check)
        return check(option.value)

    @staticmethod
    def is_some(option: Option[T]) -> TypeIs[Some[T]]:
        """
        Check if an Option is Some, narrowing its type.

        Examples
        --------
        >>> Options.is_some(Some(1))
        True
        >>> Options.is_some(Nothing())
        False

        Notes
        -----
        This function is static because Python can't narrow the type of self
        with instance methods.
        """
        return isinstance(option, Some)

    @staticmethod
    def is_some_and(
        option: Option[T], check: Callable[[T], TypeIs[U]]
    ) -> TypeGuard[Some[U]]:
        """
        Narrow the type of an Option to Some whose value passes a test.

        Examples
        --------
        >>> is_positive = lambda x: x > 0

        >>> assert Options.is_some_and(Some(1), is_positive)
        >>> assert not Options.is_some_and(Some(-1), is_positive)
        >>> assert not Options.is_some_and(Nothing(), is_positive)

        Notes
        -----
        This function is static because Python can't narrow the type of self
        with instance methods.
        """
        return isinstance(option, Some) and check(option.value)

    @staticmethod
    def next(it: Iterable[T]) -> Option[T]:
        """
        Get the first value from an iterable as Some, or Nothing if it's empty.

        Examples
        --------
        >>> Options.next(())
        Nothing()
        >>> Options.next([3])
        Some(3)
        >>> it = iter([3])
        >>> Options.next(it), Options.next(it)
        (Some(3), Nothing())
        """
        try:
            return Some(next(iter(it)))
        except StopIteration:
            return Nothing()


@AnySuccess.register
@dataclass(frozen=True, init=False, **slots_if310())
class Some(Generic[T_co]):
    """
    A value — An Option representing the presence of a value.

    Examples
    --------
    >>> Some(2).value
    2
    >>> Some(2) == 2
    False
    >>> is_ok(Some(2))
    True
    >>> is_err(Some(2))
    False
    >>> is_err(Some(Exception()))
    False
    >>> is_ok(Some(Exception()))
    True
    """

    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    def __new__(cls, value: T_co) -> Some[T_co]:
        obj = object.__new__(Some)
        object.__setattr__(obj, "value", value)
        return obj

    value: Final[T_co]  # type: ignore[misc] # misc needed to ignore error: Final name must be initialized with a value. Final itself is needed because mypy under Python3.13 fail to detect Final via dataclass being frozen.
    """
    The value in this Some. Raises TypeError if accessed from Nothing.

    Examples
    --------
    >>> Some(1).value
    1
    >>> Nothing().value
    Traceback (most recent call last):
    TypeError: attempted to access value from Nothing
    """

    @doc_from(OptionMethods)
    def or_raise(
        self, exc: Callable[P, BaseException], *exc_args: P.args, **exc_kwargs: P.kwargs
    ) -> T_co:
        return self.value

    @doc_from(OptionMethods)
    def filter(
        self, check: type[U] | Callable[[T_co], bool] | None = None
    ) -> Option[T_co]:
        if isinstance(check, type):
            return self if isinstance(self.value, check) else Nothing()
        return self if (check or bool)(self.value) else Nothing()

    # Ignoring this is necessary to type this correctly, and seems fine in
    # practice. See https://stackoverflow.com/a/74567241/693728
    @overload
    def flatten(self: Some[Nothing]) -> Nothing: ...  # type: ignore[overload-overlap]
    @overload
    def flatten(self: Some[Some[U]]) -> Some[U]: ...
    @overload
    def flatten(self: Some[Option[U]]) -> Option[U]: ...  # type: ignore[overload-overlap]
    @overload
    def flatten(self) -> Self: ...

    def flatten(self: Some[Option[U] | T_co]) -> Option[U] | Some[T_co]:
        if isinstance(self.value, (Some, Nothing)):
            return self.value
        return cast(Some[T_co], self)

    doc_from(OptionMethods)(flatten)

    @doc_from(OptionMethods)
    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        fn(self.value)
        return self

    @doc_from(OptionMethods)
    def map(self, fn: Callable[[T_co], U]) -> Some[U]:
        return Some(fn(self.value))

    @doc_from(OptionMethods)
    def map_or(self, default: U, fn: Callable[[T_co], V]) -> V:
        return fn(self.value)

    @doc_from(OptionMethods)
    def map_or_else(self, default_fn: Callable[[], U], fn: Callable[[T_co], V]) -> V:
        return fn(self.value)

    @doc_from(OptionMethods)
    def ok_or(self, error: E) -> Ok[T_co]:
        return Ok(self.value)

    @doc_from(OptionMethods)
    def ok_or_else(self, fn: Callable[[], E]) -> Ok[T_co]:
        return Ok(self.value)

    @doc_from(OptionMethods)
    def or_(self, other: Option[U]) -> Option[T_co]:
        return self

    @doc_from(OptionMethods)
    def or_else(self, fn: Callable[[], Option[U]]) -> Option[T_co]:
        return self

    @doc_from(OptionMethods)
    def value_or(self, default: U) -> T_co:
        return self.value

    @doc_from(OptionMethods)
    def value_or_else(self, fn: Callable[[], U]) -> T_co:
        return self.value

    def unzip(self: Some[tuple[U, V]]) -> tuple[Option[U], Option[V]]:
        """
        Transform an Option containing a pair into a pair of Options.

        Examples
        --------
        >>> Some((1, 2)).unzip()
        (Some(1), Some(2))
        >>> Nothing().unzip()
        (Nothing(), Nothing())
        >>> Some(42).unzip()
        Traceback (most recent call last):
        TypeError: attempted to unzip a Some not containing a pair
        """
        try:
            left, right = self.value
        except TypeError as e:
            raise TypeError("attempted to unzip a Some not containing a pair") from e
        return Some(left), Some(right)

    @doc_from(OptionMethods)
    def xor(self, other: Option[U]) -> Option[T_co | U]:
        if isinstance(other, Nothing):
            return self
        if isinstance(other, Some):
            return Nothing()
        return other

    @doc_from(OptionMethods)
    def zip(self, other: Option[U]) -> Option[tuple[T_co, U]]:
        if isinstance(other, Some):
            return Some((self.value, other.value))
        return Nothing()

    @doc_from(OptionMethods)
    def zip_with(self, other: Option[U], fn: Callable[[T_co, U], V]) -> Option[V]:
        if isinstance(other, Some):
            return Some(fn(self.value, other.value))
        return Nothing()

    def __repr__(self) -> str:
        return f"Some({self.value!r})"

    def __iter__(self) -> Iterator[T_co]:
        return iter((self.value,))


@AnyFailure.register
@dataclass(frozen=True, **slots_if310())
class Nothing:
    """
    No value — An Option representing the absence of a value.

    Examples
    --------
    >>> Nothing()
    Nothing()
    >>> Nothing() is Nothing()
    True
    >>> Nothing() == Some(None)
    False
    >>> is_err(Nothing())
    True
    >>> is_ok(Nothing())
    False
    """

    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    def __new__(cls) -> Self:
        instance = object.__new__(cls)

        def __new__(cls: type[Nothing]) -> Nothing:
            return instance

        Nothing.__new__ = __new__  # type: ignore[method-assign,assignment]
        return instance

    @doc_from(OptionMethods)
    def or_raise(
        self, exc: Callable[P, BaseException], *exc_args: P.args, **exc_kwargs: P.kwargs
    ) -> Never:
        if (
            not (exc_args or exc_kwargs)
            and isinstance(exc, type)
            and issubclass(exc, BaseException)
        ):
            exc_args = ("attempted to access value from Nothing",)  # type: ignore[assignment]
        raise exc(*exc_args, **exc_kwargs)

    @doc_from(OptionMethods)
    def filter(self, check: type[Any] | Callable[[Any], bool] | None = None) -> Nothing:
        return self

    @doc_from(OptionMethods)
    def flatten(self) -> Nothing:
        return self

    @doc_from(OptionMethods)
    def inspect(self, fn: Callable[[Any], None]) -> Nothing:
        return self

    @doc_from(OptionMethods)
    def map(self, fn: Callable[[Any], Any]) -> Nothing:
        return self

    @doc_from(OptionMethods)
    def map_or(self, default: U, fn: Callable[[Any], Any]) -> U:
        return default

    @doc_from(OptionMethods)
    def map_or_else(self, default_fn: Callable[[], U], fn: Callable[[Any], Any]) -> U:
        return default_fn()

    @doc_from(OptionMethods)
    def ok_or(self, error: E) -> Err[E]:
        return Err(error)

    @doc_from(OptionMethods)
    def ok_or_else(self, fn: Callable[[], E]) -> Err[E]:
        return Err(fn())

    @doc_from(OptionMethods)
    def or_(self, other: Option[U]) -> Option[U]:
        return other

    @doc_from(OptionMethods)
    def or_else(self, fn: Callable[[], Option[U]]) -> Option[U]:
        return fn()

    if not TYPE_CHECKING:

        @property
        def value(self) -> Never:
            raise TypeError("attempted to access value from Nothing")

    @doc_from(OptionMethods)
    def value_or(self, default: U) -> U:
        return default

    @doc_from(OptionMethods)
    def value_or_else(self, fn: Callable[[], U]) -> U:
        return fn()

    @doc_from(Some)
    def unzip(self) -> tuple[Nothing, Nothing]:
        return self, self

    @doc_from(OptionMethods)
    def xor(self, other: Option[U]) -> Option[T_co | U]:
        if isinstance(other, Some):
            return other
        return self

    @doc_from(OptionMethods)
    def zip(self, other: Option[U]) -> Nothing:
        return self

    @doc_from(OptionMethods)
    def zip_with(self, other: Option[U], fn: Callable[[Any, Any], Any]) -> Nothing:
        return self

    def __iter__(self) -> Iterator[Never]:
        return iter(())


Option: TypeAlias = Union[Some[T_co], Nothing]
"""The presence or absence of a value as Some(_) and Nothing()."""


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

    # We have to type this quite broadly here, but can be more specific at the
    # Ok/Err implementations.
    def flatten(self) -> ResultMethods[object, object]:
        """
        Flatten an Ok containing a Result into a single Result.

        Ok not containing a Result are returned as is.

        Examples
        --------
        >>> assert Ok(Ok(2)).flatten() == Ok(2)
        >>> assert Ok(Err('x')).flatten() == Err('x')
        >>> assert Err('x').flatten() == Err('x')

        >>> assert Ok(Ok(Ok(2))).flatten() == Ok(Ok(2))

        >>> Ok(2).flatten()
        Ok(2)
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


class Results(ResultMethods[T_co, E_co]):
    """
    Utility functions for Result (Ok | Err) types.

    Ok and Err represent the presence of a value, or a reason the value could
    not be produced.

    Functions that may fail can return a Result rather than raising an Exception
    to signal failure.

    Examples
    --------
    >>> def parse(x: str) -> Result[int, ValueError]:
    ...     try:
    ...         return Ok(int(x))
    ...     except ValueError as e:
    ...         return Err(e)
    >>> parse('42')
    Ok(42)
    >>> parse('a4')
    Err(ValueError("invalid literal for int() with base 10: 'a4'"))
    """

    # mypy seems to ignore static methods if new is typed like this.
    if not TYPE_CHECKING:

        def __new__(self) -> Never:
            raise TypeError("cannot create instances of Results")

    @staticmethod
    def call(
        fn: Callable[P, T], *fn_args: P.args, **fn_kwargs: P.kwargs
    ) -> Result[T, Exception]:
        """
        Call a function and return Ok with its return value, or Err if it raises.

        Examples
        --------
        >>> Results.call(int, '42')
        Ok(42)
        >>> Results.call(int, 'a4')
        Err(ValueError("invalid literal for int() with base 10: 'a4'"))
        """
        try:
            return Ok(fn(*fn_args, **fn_kwargs))
        except Exception as e:
            return Err(e)

    def __init_subclass__(cls) -> None:
        module = globals()
        if "Ok" in module and "Err" in module:
            raise TypeError("cannot subclass Result")

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


@AnySuccess.register
@dataclass(frozen=True, **slots_if310())
class Ok(Generic[T_co]):
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    value: Final[T_co]  # type: ignore[misc] # misc needed to ignore error: Final name must be initialized with a value. Final itself is needed because mypy under Python3.13 fail to detect Final via dataclass being frozen.
    """
    Access the Ok's value. Raises TypeError if this is Err.

    Examples
    --------
    >>> assert Ok(1).value == 1
    >>> Err('x').value
    Traceback (most recent call last):
    TypeError: attempted to access value from Err
    """

    @doc_from(ResultMethods)
    def and_(self, result: Result[U, E]) -> Result[U, E]:
        return result

    @doc_from(ResultMethods)
    def and_then(self, fn: Callable[[T_co], Result[U, E]]) -> Result[U, E]:
        return fn(self.value)

    if not TYPE_CHECKING:

        @property
        def error(self) -> Never:
            raise TypeError("attempted to access error from Ok")

    @doc_from(ResultMethods)
    def error_or(self, default: U) -> U:
        return default

    @doc_from(ResultMethods)
    def error_or_else(self, fn: Callable[[], U]) -> U:
        return fn()

    @overload
    def flatten(self: Ok[Ok[U]]) -> Ok[U]: ...
    @overload
    def flatten(self: Ok[Err[E]]) -> Err[E]: ...
    @overload
    def flatten(self: Ok[Result[U, E]]) -> Result[U, E]: ...
    @overload
    def flatten(self) -> Result[T_co, E]: ...

    def flatten(self: Ok[Result[U, E] | T_co]) -> Ok[U] | Err[E] | Result[T_co, E]:
        if isinstance(self.value, (Ok, Err)):
            return self.value
        return cast(Ok[T_co], self)

    doc_from(ResultMethods)(flatten)

    @doc_from(ResultMethods)
    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        fn(self.value)
        return self

    @doc_from(ResultMethods)
    def inspect_err(self, fn: Callable[[E_co], None]) -> Self:
        return self

    @doc_from(ResultMethods)
    def map(self, fn: Callable[[T_co], U]) -> Ok[U]:
        return Ok(fn(self.value))

    @doc_from(ResultMethods)
    def map_or(self, default: U, fn: Callable[[T_co], U]) -> U:
        return fn(self.value)

    @doc_from(ResultMethods)
    def map_or_else(self, default: Callable[[E_co], U], fn: Callable[[T_co], U]) -> U:
        return fn(self.value)

    @doc_from(ResultMethods)
    def map_err(self, fn: Callable[[E_co], U]) -> Self:
        return self

    @doc_from(ResultMethods)
    def ok(self) -> Some[T_co]:
        return Some(self.value)

    @doc_from(ResultMethods)
    def or_(self, default: Result[T, U]) -> Result[T_co, U]:
        return self

    @doc_from(ResultMethods)
    def or_else(self, fn: Callable[[], Result[T, U]]) -> Result[T_co, U]:
        return self

    @doc_from(ResultMethods)
    def value_or(self, default: U) -> T_co:
        return self.value

    @doc_from(ResultMethods)
    def value_or_else(self, fn: Callable[[], U]) -> T_co:
        return self.value

    def __iter__(self) -> Iterator[T_co]:
        return iter((self.value,))

    def __repr__(self) -> str:
        return f"Ok({self.value!r})"


@AnyFailure.register
@dataclass(frozen=True, **slots_if310())
class Err(Generic[E_co]):
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    error: Final[E_co]  # type: ignore[misc] # misc needed to ignore error: Final name must be initialized with a value. Final itself is needed because mypy under Python3.13 fail to detect Final via dataclass being frozen.
    """
    Access the Err's error. Raises TypeError if this is Ok.

    Examples
    --------
    >>> assert Err('x').error == 'x'
    >>> Ok(1).error
    Traceback (most recent call last):
    TypeError: attempted to access error from Ok
    """

    @doc_from(ResultMethods)
    def and_(self, result: Result[U, E]) -> Self:
        return self

    @doc_from(ResultMethods)
    def and_then(self, fn: Callable[[T_co], Result[U, E]]) -> Self:
        return self

    @doc_from(ResultMethods)
    def error_or(self, default: U) -> E_co:
        return self.error

    @doc_from(ResultMethods)
    def error_or_else(self, fn: Callable[[], U]) -> E_co:
        return self.error

    @doc_from(ResultMethods)
    def flatten(self) -> Self:
        return self

    @doc_from(ResultMethods)
    def inspect(self, fn: Callable[[T_co], None]) -> Self:
        return self

    @doc_from(ResultMethods)
    def inspect_err(self, fn: Callable[[E_co], None]) -> Self:
        fn(self.error)
        return self

    @doc_from(ResultMethods)
    def map(self, fn: Callable[[T_co], U]) -> Self:
        return self

    @doc_from(ResultMethods)
    def map_or(self, default: U, fn: Callable[[T_co], U]) -> U:
        return default

    @doc_from(ResultMethods)
    def map_or_else(self, default: Callable[[E_co], U], fn: Callable[[T_co], U]) -> U:
        return default(self.error)

    @doc_from(ResultMethods)
    def map_err(self, fn: Callable[[E_co], U]) -> Err[U]:
        return Err(fn(self.error))

    @doc_from(ResultMethods)
    def ok(self) -> Nothing:
        return Nothing()

    @doc_from(ResultMethods)
    def or_(self, default: Result[T_co, U]) -> Result[T_co, U]:
        return default

    @doc_from(ResultMethods)
    def or_else(self, fn: Callable[[], Result[T_co, U]]) -> Result[T_co, U]:
        return fn()

    if not TYPE_CHECKING:

        @property
        def value(self) -> Never:
            raise TypeError("attempted to access value from Err")

    @doc_from(ResultMethods)
    def value_or(self, x_default: U) -> U:
        return x_default

    @doc_from(ResultMethods)
    def value_or_else(self, fn: Callable[[], U]) -> U:
        return fn()

    def __iter__(self) -> Iterator[Never]:
        return iter(())

    def __repr__(self) -> str:
        return f"Err({self.error!r})"


Result: TypeAlias = Union[Ok[T_co], Err[E_co]]
"""Represents the presence of a value, or a reason the value could not be produced."""


def is_ok(result: object) -> TypeIs[AnySuccess]:
    """Check if a value is a [successful type](`denokv.result.AnySuccess`)."""
    return isinstance(result, AnySuccess)


def is_err(result: object) -> TypeIs[AnyFailure]:
    """Check if a value is a [failure type](`denokv.result.AnyFailure`)."""
    return isinstance(result, AnyFailure)
