from dataclasses import dataclass

from denokv._pycompat.typing import TYPE_CHECKING


@dataclass(init=False)
class DenoKvError(Exception):
    # Define message for dataclass field metadata only, not type annotation.
    if not TYPE_CHECKING:
        message: str

    def __init__(self, *args: object) -> None:
        super(DenoKvError, self).__init__(*args)

    @property
    def message(self) -> str:
        if args := self.args:
            return str(args[0])
        return type(self).__name__


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
