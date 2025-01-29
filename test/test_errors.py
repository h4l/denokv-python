import pytest

from denokv.errors import DenoKvError


def test_errors_are_regular_exceptions() -> None:
    """Errors must be caught by generic Exception handlers — not BaseException."""
    with pytest.raises(Exception):  # noqa: B017
        raise DenoKvError("error")


def test_DenoKvError_message() -> None:
    assert DenoKvError().message == "DenoKvError"
    assert DenoKvError("Foo bar").message == "Foo bar"

    class CustomError(DenoKvError):
        pass

    assert CustomError().message == "CustomError"
    assert CustomError("Bar baz").message == "Bar baz"
