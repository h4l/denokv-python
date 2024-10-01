import ctypes
from typing import Hashable
from typing import TypeAlias
from uuid import UUID

from _typeshed import SupportsAllComparisons

def pack(t: tuple[KeyPiece, ...]) -> bytes: ...
def unpack(b: bytes) -> tuple[KeyPiece, ...]: ...

class SingleFloat(Hashable, SupportsAllComparisons):
    value: ctypes.c_float
    # This is __bool__ from Python 2, SingleFloat doesn't implement __bool__
    def __nonzero__(self) -> bool: ...
    def __hash__(self) -> int: ...

KeyPiece: TypeAlias = (
    None | bytes | str | int | SingleFloat | float | UUID | bool | tuple[KeyPiece, ...]
)
