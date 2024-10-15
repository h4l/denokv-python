from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import Sequence

import pytest
from typing_extensions import Never
from typing_extensions import TypeIs

from denokv.result import AnyFailure
from denokv.result import AnySuccess
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result
from denokv.result import is_err
from denokv.result import is_ok


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


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_is_ok(result: Result[int, ValueError]) -> None:
    if is_ok(result):
        i: int = result.value
        assert i == 1
    else:
        e: ValueError = result.error
        assert e.args[0] == "example"


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_is_err(result: Result[int, ValueError]) -> None:
    if is_err(result):
        e: ValueError = result.error
        assert e.args[0] == "example"
    else:
        i: int = result.value
        assert i == 1


@pytest.mark.parametrize("result", [Ok(3), Err("x")])
def test_value_or(result: Result[int, str]) -> None:
    val: int = result.value_or(42)
    if is_ok(result):
        assert val == 3
    else:
        assert val == 42


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Ok_is_ok(ok: bool, result: Result[int, str]) -> None:
    if ok:
        assert Ok.is_ok(result)
        val: int = result.value
        assert val == 3
    else:
        assert not Ok.is_ok(result)


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Ok_is_ok_and(ok: bool, result: Result[object, str]) -> None:
    def is_int(x: object) -> TypeIs[int]:
        return isinstance(x, int)

    if ok:
        assert Ok.is_ok_and(result, is_int)
        val: int = result.value
        assert val == 3
    else:
        assert not Ok.is_ok_and(result, is_int)
        type_error: Err[Any] = result  # type: ignore[assignment]


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Err_is_err(ok: bool, result: Result[int, str]) -> None:
    if ok:
        assert not Err.is_err(result)
    else:
        assert Err.is_err(result)
        error: str = result.error
        assert error == "x"


@pytest.mark.parametrize("ok,result", [(True, Ok(3)), (False, Err("x"))])
def test_Err_is_err_and(ok: bool, result: Result[int, str | bytes]) -> None:
    def is_str(x: str | bytes) -> TypeIs[str]:
        return isinstance(x, str)

    if ok:
        assert not Err.is_err_and(result, is_str)
        # This must be a type error — we only know the result does not contain a
        # str, not that it is an Ok.
        type_error: Ok[Any] = result  # type: ignore[assignment]
    else:
        assert Err.is_err_and(result, is_str)
        error: str = result.error
        assert error == "x"


def test_Result_flatten() -> None:
    with pytest.raises(
        TypeError, match=r"Ok value does not contain a Result to flatten"
    ):
        # This must be a type error
        Ok(2).flatten()  # type: ignore[misc]
    assert Ok(Ok(2)).flatten() == Ok(2)
    assert Ok(Err("x")).flatten() == Err("x")
    assert Err("x").flatten() == Err("x")


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
