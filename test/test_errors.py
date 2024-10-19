import pytest

from denokv.errors import DenoKvError


def test_errors_are_regular_exceptions() -> None:
    """Errors must be caught by generic Exception handlers — not BaseException."""
    with pytest.raises(Exception):  # noqa: B017
        raise DenoKvError("error")
