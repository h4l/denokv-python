from __future__ import annotations

import sys
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import Literal
from typing import Sequence
from typing import cast
from unittest.mock import Mock

import pytest
from typing_extensions import Never
from typing_extensions import TypeIs

from denokv.result import AnyFailure
from denokv.result import AnySuccess
from denokv.result import Err
from denokv.result import Nothing
from denokv.result import Ok
from denokv.result import Option
from denokv.result import OptionMethods
from denokv.result import Options
from denokv.result import Result
from denokv.result import ResultMethods
from denokv.result import Results
from denokv.result import Some
from denokv.result import is_err
from denokv.result import is_ok


@pytest.mark.skipif(
    sys.version_info < (3, 10), reason="<3.10 does not use slots for dataclass"
)
def test_Option__instances_use_slots_to_avoid_dict() -> None:
    with pytest.raises(AttributeError):
        print(Some(1).__dict__)

    with pytest.raises(AttributeError):
        print(Nothing().__dict__)


def test_Option__satisfies_OptionMethods() -> None:
    s = Some(1)
    n = Nothing()

    def use_option_like(o: OptionMethods[int]) -> int:
        return o.value_or(-1)

    assert use_option_like(s) == 1
    assert use_option_like(n) == -1


def test_Option__value__cannot_reference_value_from_Nothing() -> None:
    nothing = Nothing()
    with pytest.raises(TypeError, match=r"attempted to access value from Nothing"):
        print(nothing.value)  # type: ignore[attr-defined]

    option = cast(Option[int], nothing)
    with pytest.raises(TypeError, match=r"attempted to access value from Nothing"):
        print(option.value)  # type: ignore[union-attr]


def test_Option_is_some() -> None:
    """Check that is_some / is_some_and narrow types."""

    def is_token(s: str) -> TypeIs[Literal["foo", "bar"]]:
        return s in ("foo", "bar")

    assert Options.is_some(Some("foo"))
    assert not Options.is_some(Nothing())
    assert Options.is_some_and(Some("foo"), is_token)
    assert not Options.is_some_and(Nothing(), is_token)

    things: list[Option[str]] = [Nothing(), Some("abc"), Some("foo")]

    tokens: list[Literal["foo", "bar"]] = [
        t.value for t in things if Options.is_some_and(t, is_token)
    ]
    assert tokens == ["foo"]
    not_tokens: list[Option[str]] = [
        t for t in things if not Options.is_some_and(t, is_token)
    ]
    assert not_tokens == [Nothing(), Some("abc")]


def test_Option_is_nothing_or() -> None:
    def is_token(s: str) -> TypeIs[Literal["foo", "bar"]]:
        return s in ("foo", "bar")

    things: list[Option[Literal["foo", "abc"]]] = [
        Some("foo"),
        Some("abc"),
        Nothing(),
    ]
    nothings: list[Nothing] = [t for t in things if Options.is_nothing(t)]
    assert nothings == [Nothing()]

    tokens_or_nothing: list[Option[Literal["foo", "bar"]]] = [
        t for t in things if Options.is_nothing_or(t, is_token)
    ]
    assert tokens_or_nothing == [Some("foo"), Nothing()]


def test_Option_flatten() -> None:
    """Check that type are correctly inferred when flattening nested Options."""
    opts: list[Option[Option[int]]] = [Some(Some(1)), Nothing()]
    opts2: list[Option[int]] = [o.flatten() for o in opts]
    assert [o.value_or(None) for o in opts2] == [1, None]

    nothing: Nothing = Some(Nothing()).flatten()
    assert nothing == Nothing()

    something: Some[int] = Some(Some(1)).flatten()
    assert something == Some(1)
    something2: Some[int] = Some(2).flatten()
    assert something2 == Some(2)


def test_Option() -> None:
    assert Some(1).or_raise(AssertionError) == 1
    with pytest.raises(AssertionError, match=r"attempted to access value from Nothing"):
        Nothing().or_raise(AssertionError)

    with pytest.raises(ValueError, match=r"No values were provided"):
        Nothing().or_raise(ValueError, "No values were provided")

    assert Some("a").filter(int) == Nothing()
    assert Some(1).filter(int) == Some(1)
    assert Some(-1).filter(lambda x: x > 0) == Nothing()
    assert Some(1).filter(lambda x: x > 0) == Some(1)

    assert Some(Some(2)).flatten() == Some(2)
    assert Some(2).flatten() == Some(2)
    assert Some(Nothing()).flatten() == Nothing()
    assert Nothing().flatten() == Nothing()

    effect = Mock()
    Nothing().inspect(effect)
    effect.assert_not_called()
    Some(2).inspect(effect)
    effect.assert_called_once_with(2)

    assert Some(2).map(lambda x: x * 2) == Some(4)
    assert Nothing().map(lambda x: x * 2) == Nothing()

    assert Some(2).map_or(-1, lambda x: x * 2) == 4
    assert Nothing().map_or(-1, lambda x: x * 2) == -1
    assert Nothing().map_or("foo", lambda x: x * 2) == "foo"

    assert Some(2).map_or_else(lambda: -1, lambda x: x * 2) == 4
    assert Nothing().map_or_else(lambda: -1, lambda x: x * 2) == -1
    assert Nothing().map_or_else(lambda: "foo", lambda x: x * 2) == "foo"

    assert Some(1).ok_or("x") == Ok(1)
    assert Nothing().ok_or("x") == Err("x")

    assert Some(1).ok_or_else(lambda: "x") == Ok(1)
    assert Nothing().ok_or_else(lambda: "x") == Err("x")

    assert Some(1).or_(Some(2)) == Some(1)
    assert Some(1).or_(Nothing()) == Some(1)
    assert Nothing().or_(Some(2)) == Some(2)

    assert Some(1).or_else(lambda: Some(2)) == Some(1)
    assert Some(1).or_else(lambda: Nothing()) == Some(1)
    assert Nothing().or_else(lambda: Some(2)) == Some(2)

    assert Some(1).value == 1
    with pytest.raises(TypeError, match=r"attempted to access value from Nothing"):
        print(Nothing().value)  # type: ignore[attr-defined]

    assert Some(1).value_or(2) == 1
    assert Nothing().value_or(2) == 2

    assert Some(1).value_or_else(lambda: 2) == 1
    assert Nothing().value_or_else(lambda: 2) == 2

    assert Some((1, 2)).unzip() == (Some(1), Some(2))
    assert Nothing().unzip() == (Nothing(), Nothing())
    with pytest.raises(
        TypeError, match=r"attempted to unzip a Some not containing a pair"
    ):
        Some(1).unzip()  # type: ignore[misc]

    assert Some(1).xor(Some(2)) == Nothing()
    assert Some(1).xor(Nothing()) == Some(1)
    assert Nothing().xor(Some(2)) == Some(2)

    assert Some(1).zip(Some(2)) == Some((1, 2))
    assert Some(1).zip(Nothing()) == Nothing()
    assert Nothing().zip(Some(2)) == Nothing()

    assert int("FF", 16) == 255
    assert Some("FF").zip_with(Some(16), int) == Some(255)
    assert Some("FF").zip_with(Nothing(), int) == Nothing()
    assert Nothing().zip_with(Some(16), int) == Nothing()

    def type_check_zip_with(a: Option[str], b: Option[object]) -> Option[int]:
        # must be type error
        return a.zip_with(b, int)  # type: ignore[arg-type]


@pytest.mark.skipif(
    sys.version_info < (3, 10), reason="<3.10 does not use slots for dataclass"
)
def test_Result__instances_use_slots_to_avoid_dict() -> None:
    with pytest.raises(AttributeError):
        print(Ok(1).__dict__)

    with pytest.raises(AttributeError):
        print(Err("x").__dict__)


def test_Result__satisfies_ResultMethods() -> None:
    ok = Ok(1)
    err = Err("x")

    def use_result_like(r: ResultMethods[int, str]) -> tuple[int | None, str | None]:
        return r.value_or(None), r.error_or(None)

    assert use_result_like(ok) == (1, None)
    assert use_result_like(err) == (None, "x")


def test_Result__error__cannot_reference_value_from_Err() -> None:
    err: Err[str] = Err("x")
    with pytest.raises(TypeError, match=r"attempted to access value from Err"):
        print(err.value)  # type: ignore[attr-defined]

    res = cast(Result[int, str], err)
    with pytest.raises(TypeError, match=r"attempted to access value from Err"):
        print(res.value)  # type: ignore[union-attr]


def test_Result__error__cannot_reference_error_from_Ok() -> None:
    ok: Ok[int] = Ok(1)
    with pytest.raises(TypeError, match=r"attempted to access error from Ok"):
        print(ok.error)  # type: ignore[attr-defined]

    res = cast(Result[int, str], ok)
    with pytest.raises(TypeError, match=r"attempted to access error from Ok"):
        print(res.error)  # type: ignore[union-attr]


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_result_value_or_none(result: Result[int, ValueError]) -> None:
    if value := result.value_or(None):
        type_check: int = value
        assert isinstance(result, Ok)
        assert type_check == result.value
    else:
        assert result.value_or(None) is None
        assert isinstance(result, Err)


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_result_error_or_none(result: Result[int, ValueError]) -> None:
    if error := result.error_or(None):
        type_check: ValueError = error
        assert isinstance(result, Err)
        assert type_check == result.error
    else:
        assert result.error_or(None) is None
        assert isinstance(result, Ok)


@pytest.mark.parametrize("thing", [Some(1), Nothing()])
def test_is_ok__Some(thing: Option[int]) -> None:
    if is_ok(thing):
        ok: Some[int] = thing
        i: int = ok.value
        assert i == 1
    else:
        not_ok: Nothing = thing
        assert is_err(not_ok)


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_is_ok(result: Result[int, ValueError]) -> None:
    if is_ok(result):
        ok: Ok[int] = result
        i: int = ok.value
        assert i == 1
    else:
        not_ok: Err[ValueError] = result
        e: ValueError = not_ok.error
        assert e.args[0] == "example"


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_is_err(result: Result[int, ValueError]) -> None:
    if is_err(result):
        err: Err[ValueError] = result
        e: ValueError = err.error
        assert e.args[0] == "example"
    else:
        ok: Ok[int] = result
        i: int = ok.value
        assert i == 1


@pytest.mark.parametrize("result", [Ok(3), Err("x")])
def test_value_or(result: Result[int, str]) -> None:
    val: int = result.value_or(42)
    if is_ok(result):
        assert val == 3
    else:
        assert val == 42


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Results_is_ok(ok: bool, result: Result[int, str]) -> None:
    if ok:
        assert Results.is_ok(result)
        val: int = result.value
        assert val == 3
    else:
        assert not Results.is_ok(result)


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Results_is_ok_and(ok: bool, result: Result[object, str]) -> None:
    def is_int(x: object) -> TypeIs[int]:
        return isinstance(x, int)

    if ok:
        assert Results.is_ok_and(result, is_int)
        val: int = result.value
        assert val == 3
    else:
        assert not Results.is_ok_and(result, is_int)
        type_error: Err[Any] = result  # type: ignore[assignment]  # noqa: F841


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Results_is_err(ok: bool, result: Result[int, str]) -> None:
    if ok:
        assert not Results.is_err(result)
    else:
        assert Results.is_err(result)
        error: str = result.error
        assert error == "x"


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Results_is_err_and(ok: bool, result: Result[int, str | bytes]) -> None:
    def is_str(x: str | bytes) -> TypeIs[str]:
        return isinstance(x, str)

    if ok:
        assert not Results.is_err_and(result, is_str)
        # This must be a type error — we only know the result does not contain a
        # str, not that it is an Ok.
        type_error: Ok[Any] = result  # type: ignore[assignment]  # noqa: F841
    else:
        assert Results.is_err_and(result, is_str)
        error: str = result.error
        assert error == "x"


def test_Result_flatten() -> None:
    flat_ok: Ok[int] = Ok(Ok(2)).flatten()
    assert flat_ok == Ok(2)
    flat_err: Err[str] = Ok(Err("x")).flatten()
    assert flat_err == Err("x")
    results: list[Result[int, str]] = [Ok(2), Err("x")]
    flat_other: list[Result[int, str]] = [r.flatten() for r in results]
    assert flat_other == [Ok(2), Err("x")]

    assert Ok(Ok(2)).flatten() == Ok(2)
    assert Ok(Err("x")).flatten() == Err("x")
    assert Err("x").flatten() == Err("x")
    assert Ok(2).flatten() == Ok(2)


def test_ok_covariance() -> None:
    """
    Test that Ok type annotations merge correctly.

    For types T1, T2, where T1 is a subtype of T2, Ok[T1] must be assignable to
    Ok[T2].

    For example, an Ok[Sequence] can be assigned to an Ok[Iterable].
    """

    def get_seq() -> Result[Sequence[int], Exception]:
        return Ok((1, 2, 3))

    def maybe_use_iterable(things: Result[Iterable[float], Exception]) -> None:
        pass

    seq_result = get_seq()
    maybe_use_iterable(seq_result)


def test_error_covariance() -> None:
    """
    Test that Err type annotations merge correctly.

    For types E1, E2, where E1 is a subtype of E2, Err[E1] must be assignable to
    Err[E2].

    For example, an Err[ValueError] can be assigned to an Err[Exception].
    """

    def do_thing(a: int) -> Result[int, ValueError]:
        return Err(ValueError("foo"))

    def do_thing_2(a: int | str) -> Result[int, TypeError | ValueError]:
        if isinstance(a, int):
            return do_thing(a)
        return Err(TypeError("a must be int"))

    def use_error(err: Err[Exception]) -> None:
        pass

    result = do_thing_2(1)
    assert is_err(result)
    # FIXME: is_err is not narrowing type correctly with Result as base type
    #   rather than union of Ok | Err. Make current Result type a metaclass?
    assert isinstance(result.error, ValueError) and str(result.error) == "foo"
    use_error(result)


@AnySuccess.register
class Yes:
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    bar: str


@AnyFailure.register
class No:
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    foo: int


@pytest.mark.parametrize("maybe", [True, False])
def test_arbitrary_type_registration(maybe: bool) -> None:
    def use_yes(x: Yes) -> None:
        assert isinstance(x, Yes)

    def use_no(x: No) -> None:
        assert isinstance(x, No)

    thing: Yes | No = Yes() if maybe else No()
    if maybe:
        assert is_ok(thing)
        use_yes(thing)
    else:
        assert is_err(thing)
        use_no(thing)
