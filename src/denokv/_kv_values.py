from __future__ import annotations

from binascii import unhexlify
from dataclasses import dataclass

from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.typing import ClassVar
from denokv._pycompat.typing import Generic
from denokv._pycompat.typing import Self
from denokv._pycompat.typing import TypeVar
from denokv._pycompat.typing import TypeVarTuple
from denokv._pycompat.typing import Unpack
from denokv.datapath import AnyKvKeyT
from denokv.datapath import KvKeyPiece

T = TypeVar("T", default=object)
# Note that the default arg doesn't seem to work with MyPy yet. The
# DefaultKvKey alias is what this should behave as when defaulted.
Pieces = TypeVarTuple("Pieces", default=Unpack[tuple[KvKeyPiece, ...]])


@dataclass(frozen=True, **slots_if310())
class KvEntry(Generic[AnyKvKeyT, T]):
    """A value read from the Deno KV database, along with its key and version."""

    key: AnyKvKeyT
    value: T
    versionstamp: VersionStamp


class VersionStamp(bytes):
    r"""
    A 20-hex-char / (10 byte) version identifier.

    This value represents the relative age of a KvEntry. A VersionStamp that
    compares larger than another is newer.

    Examples
    --------
    >>> VersionStamp(0xff << 16)
    VersionStamp('00000000000000ff0000')
    >>> int(VersionStamp('000000000000000000ff'))
    255
    >>> bytes(VersionStamp('00000000000000ff0000'))
    b'\x00\x00\x00\x00\x00\x00\x00\xff\x00\x00'
    >>> VersionStamp(b'\x00\x00\x00\x00\x00\x00\x00\xff\x00\x00')
    VersionStamp('00000000000000ff0000')
    >>> isinstance(VersionStamp(0), bytes)
    True
    >>> str(VersionStamp(0xff << 16))
    '00000000000000ff0000'
    """

    __slots__ = ()

    RANGE: ClassVar = range(0, 2**80)

    def __new__(cls, value: str | bytes | int) -> Self:
        if isinstance(value, int):
            if value not in VersionStamp.RANGE:
                raise ValueError("value not in range for 80-bit unsigned int")
            # Unlike most others, versionstamp uses big-endian as it needs to
            # sort lexicographically as bytes.
            value = value.to_bytes(length=10, byteorder="big")
        if isinstance(value, str):
            try:
                value = unhexlify(value)
            except Exception:
                value = b""
            if len(value) != 10:
                raise ValueError("value is not a 20 char hex string")
        else:
            if len(value) != 10:
                raise ValueError("value is not 10 bytes long")
        return bytes.__new__(cls, value)

    def __index__(self) -> int:
        return int.from_bytes(self, byteorder="big")

    def __bytes__(self) -> bytes:
        return self[:]

    def __str__(self) -> str:
        return self.hex()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self)!r})"


@dataclass(frozen=True, **slots_if310())
class KvU64:
    """
    An special int value that supports operations like `sum`, `max`, and `min`.

    Notes
    -----
    This type is not an int subtype to avoid it being mistakenly flattened into
    a regular int and loosing its special meaning when written back to the DB.

    Examples
    --------
    >>> KvU64(bytes([0, 0, 0, 0, 0, 0, 0, 0]))
    KvU64(0)
    >>> KvU64(bytes([1, 0, 0, 0, 0, 0, 0, 0]))
    KvU64(1)
    >>> KvU64(bytes([1, 1, 0, 0, 0, 0, 0, 0]))
    KvU64(257)
    >>> KvU64(2**64 - 1)
    KvU64(18446744073709551615)
    >>> KvU64(2**64)
    Traceback (most recent call last):
    ...
    ValueError: value not in range for 64-bit unsigned int
    >>> KvU64(-1)
    Traceback (most recent call last):
    ...
    ValueError: value not in range for 64-bit unsigned int
    """

    RANGE: ClassVar[range] = range(0, 2**64)
    value: int

    def __init__(self, value: bytes | int) -> None:
        if isinstance(value, bytes):
            if len(value) != 8:
                raise ValueError("value must be a 8 bytes")
            value = int.from_bytes(value, byteorder="little")
        elif isinstance(value, int):
            if value not in KvU64.RANGE:
                raise ValueError("value not in range for 64-bit unsigned int")
        else:
            raise TypeError("value must be 8 bytes or a 64-bit unsigned int")
        object.__setattr__(self, "value", value)

    def __index__(self) -> int:
        return self.value

    def __bytes__(self) -> bytes:
        return self.to_bytes()

    def to_bytes(self) -> bytes:
        return self.value.to_bytes(8, byteorder="little")

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.value})"
