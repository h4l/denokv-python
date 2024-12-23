from __future__ import annotations

from typing import TYPE_CHECKING
from typing import overload

from fdb.tuple import pack
from fdb.tuple import unpack

from denokv.datapath import KV_KEY_PIECE_TYPES
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyEncodableT
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyTuple
from denokv.datapath import is_kv_key_tuple

if TYPE_CHECKING:
    from typing_extensions import TypeAlias
    from typing_extensions import TypeVar
    from typing_extensions import TypeVarTuple
    from typing_extensions import Unpack

    T = TypeVar("T", default=object)
    # Note that the default arg doesn't seem to work with MyPy yet. The
    # DefaultKvKey alias is what this should behave as when defaulted.
    Pieces = TypeVarTuple("Pieces", default=Unpack[tuple[KvKeyPiece, ...]])
else:
    from typing import TypeVar

    T = TypeVar("T")
    Unpack = tuple  # hack to support py39 at runtime w/o typing_extensions
    Pieces = TypeVar("Pieces")  # hack to support py39 at runtime w/o typing_extensions


class KvKey(KvKeyEncodable, tuple[Unpack[Pieces]]):
    """
    A key identifying a value in a Deno KV database.

    KvKey is a tuple of key pieces — str, bytes, int, float or bool. Unlike a
    plain tuple, KvKey's values are guaranteed to only be valid key values, and
    int values are not coerced to float for JavaScript comparability when used
    with [Kv] methods.

    [Kv]: `denokv.kv.Kv`
    """

    # The Pieces TypeVarTuple cannot be bounded to KvKeyPiece elements, so this
    # type can hold any element, but  only KvKeyPiece can exist at runtime.
    def __new__(cls, *pieces: Unpack[Pieces]) -> KvKey[Unpack[Pieces]]:
        if not is_kv_key_tuple(pieces):
            raise TypeError(
                f"key contains types other than "
                f"{', '.join(t.__name__ for t in KV_KEY_PIECE_TYPES)}: {pieces!r}"
            )
        return tuple.__new__(KvKey, pieces)

    @overload
    @classmethod
    def wrap_tuple_keys(cls, key: KvKeyTuple) -> DefaultKvKey: ...

    @overload
    @classmethod
    def wrap_tuple_keys(cls, key: KvKeyEncodableT) -> KvKeyEncodableT: ...

    @classmethod
    def wrap_tuple_keys(
        cls, key: KvKeyTuple | KvKeyEncodableT
    ) -> DefaultKvKey | KvKeyEncodableT:
        """
        Return simple tuples as KvKey but return [KvKeyEncodable] keys as-is.

        Notes
        -----
        By wrapping plain tuple keys as KvKey we preserve int as int if a key
        read from the DB is passed back into a Kv method which is normalising int
        to float for JS compatibility.
        """
        if isinstance(key, KvKeyEncodable):
            return key  # type: ignore[return-value,unused-ignore]
        return cls(*key)  # type: ignore[arg-type,return-value,unused-ignore]

    def kv_key_bytes(self) -> bytes:
        return pack(self)  # type: ignore[arg-type]

    @classmethod
    def from_kv_key_bytes(cls, packed_key: bytes) -> DefaultKvKey:
        """Create a KvKey by unpacking a packed key."""
        try:
            # If packed key contains types other than allowed by KvKeyPiece
            # then the constructor throws TypeError, so this is type-safe.
            return cls(*unpack(packed_key))  # type: ignore[arg-type,return-value,unused-ignore]
        except ValueError as e:
            raise ValueError(
                f"Cannot create {cls.__name__} from packed key: {packed_key!r}:"
                f" value is not a valid packed key"
            ) from e
        except TypeError as e:
            raise ValueError(
                f"Cannot create {cls.__name__} from packed key: {packed_key!r}: {e}"
            ) from e


# Ideally the default parameter of the Pieces KvKeyTuple would make this the
# default for KvKey (with no generic type), but mypy thinks it is
# KvKey[*tuple[Any, ...]] when used without generic type args.
DefaultKvKey: TypeAlias = "KvKey[Unpack[tuple[KvKeyPiece, ...]]]"
"""KvKey containing any number of key values of any allowed type."""
