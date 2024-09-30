from dataclasses import dataclass
from typing import cast


@dataclass(init=False)
class DenoKvError(BaseException):
    message: str

    def __init__(self, message: str, *args: object) -> None:
        super().__init__(message, *args)
        if not isinstance(message, str):
            raise TypeError(f"first argument must be a str message: {message!r}")

    @property  # type: ignore[no-redef]
    def message(self) -> str:
        return cast(str, self.args[0])


class DenoKvValidationError(ValueError, DenoKvError):
    pass


class DenoKvUserError(DenoKvError):
    """An error caused by bad user input."""


@dataclass(init=False)
class InvalidCursor(DenoKvUserError):
    """A cursor string passed to [kv.list()] is invalid."""

    cursor: str

    def __init__(self, message: str, *args: object, cursor: str) -> None:
        super().__init__(message, *args)
        self.cursor = cursor
