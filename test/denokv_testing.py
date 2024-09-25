from typing import TypeVar

from denokv.result import Ok
from denokv.result import Result

T = TypeVar("T")
E = TypeVar("E")


def assume_ok(result: Result[T, E]) -> T:
    if isinstance(result, Ok):
        return result.value
    raise AssertionError(f"result is not Ok: {result}")
