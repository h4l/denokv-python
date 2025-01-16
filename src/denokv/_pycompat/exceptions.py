from __future__ import annotations

from denokv._pycompat.typing import Protocol
from denokv._pycompat.typing import TypeGuard
from denokv._pycompat.typing import TypeVar
from denokv._pycompat.typing import cast


class Notes(Protocol):
    __notes__: list[str]


def has_notes(exc: BaseException) -> TypeGuard[Notes]:
    return isinstance(getattr(exc, "__notes__", None), list)


def add_note(exc: BaseException, note: str) -> None:
    if not isinstance(note, str):
        raise TypeError("note must be a str")
    if not has_notes(exc):
        exc_with_notes = cast(Notes, exc)
        exc_with_notes.__notes__ = notes = []
    else:
        notes = exc.__notes__
    notes.append(note)


ExceptionT = TypeVar("ExceptionT", bound=BaseException)


def with_notes(
    exc: ExceptionT, *notes: str, from_exception: BaseException | None = None
) -> ExceptionT:
    if from_exception and has_notes(from_exception):
        for note in from_exception.__notes__:
            add_note(exc, note)
    for note in notes:
        add_note(exc, note)
    return exc
