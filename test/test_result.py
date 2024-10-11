from __future__ import annotations

from typing import Iterable
from typing import Sequence

import pytest

from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result
from denokv.result import is_err
from denokv.result import is_ok


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_result_value_or_none(result: Result[int, ValueError]) -> None:
    if value := result.value_or_none:
        type_check: int = value
        assert isinstance(result, Ok)
        assert type_check == result.value
    else:
        assert result.value_or_none is None
        assert isinstance(result, Err)


@pytest.mark.parametrize("result", [(Ok(1)), (Err(ValueError("example")))])
def test_result_error_or_none(result: Result[int, ValueError]) -> None:
    if error := result.error_or_none:
        type_check: ValueError = error
        assert isinstance(result, Err)
        assert type_check == result.error
    else:
        assert result.error_or_none is None
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
