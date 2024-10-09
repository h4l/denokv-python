from __future__ import annotations

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
