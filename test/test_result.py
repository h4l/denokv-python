from __future__ import annotations

import pytest

from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result


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
