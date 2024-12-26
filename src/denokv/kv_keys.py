from __future__ import annotations

import functools
import sys
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import Final
from typing import Generic
from typing import Iterator
from typing import Sequence
from typing import Union
from typing import cast
from typing import overload

from fdb.tuple import pack
from fdb.tuple import unpack

from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.typing import override
from denokv.datapath import KV_KEY_PIECE_TYPES
from denokv.datapath import AnyKvKey
from denokv.datapath import AnyKvKeyT
from denokv.datapath import AnyKvKeyT_co
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyEncodableT
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyRangeEncodable
from denokv.datapath import KvKeyTuple
from denokv.datapath import PackKeyRangeOptions
from denokv.datapath import is_any_kv_key
from denokv.datapath import is_kv_key_tuple
from denokv.datapath import pack_key
from denokv.datapath import pack_key_range
from denokv.result import Nothing
from denokv.result import Some

if TYPE_CHECKING:
    from typing_extensions import Self
    from typing_extensions import SupportsIndex
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

_T_co = TypeVar("_T_co", covariant=True)


class _KvKeyState:
    _unpacked: KvKeyTuple
    _packed: bytes | None
    __slots__ = ("_unpacked", "_packed")

    def __init__(self, *pieces: KvKeyPiece) -> None:
        self._unpacked = pieces
        if not is_kv_key_tuple(pieces):
            raise TypeError(
                f"key contains types other than "
                f"{', '.join(t.__name__ for t in KV_KEY_PIECE_TYPES)}: {pieces!r}"
            )
        self._packed = None

    def __eq__(self, value: object) -> bool:
        # We don't compare equal to plain key tuples, because the comparison
        # is not symmetric and tuple's hash does not follow this equality.
        if not isinstance(value, KvKeyEncodable):
            return NotImplemented
        # unpacked tuples don't compare correctly because python == and hash
        # treats int and integer floats as the same, but FDB keys don't.
        return self.kv_key_bytes() == pack_key(value)

    def __hash__(self) -> int:
        return hash((KvKeyEncodable, self.kv_key_bytes()))

    def __repr__(self) -> str:
        return f"KvKey{self._unpacked!r}"

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
        if (packed := self._packed) is None:
            self._packed = packed = pack(self._unpacked)
        return packed

    def __bytes__(self) -> bytes:
        return self.kv_key_bytes()

    @classmethod
    def from_kv_key_bytes(cls, packed_key: bytes) -> DefaultKvKey:
        """Create a KvKey by unpacking a packed key."""
        try:
            # If packed key contains types other than allowed by KvKeyPiece
            # then the constructor throws TypeError, so this is type-safe.
            kvkey = cls(*unpack(packed_key))  # type: ignore[arg-type]
        except ValueError as e:
            raise ValueError(
                f"Cannot create {cls.__name__} from packed key: {packed_key!r}:"
                f" value is not a valid packed key"
            ) from e
        except TypeError as e:
            raise ValueError(
                f"Cannot create {cls.__name__} from packed key: {packed_key!r}: {e}"
            ) from e
        kvkey._packed = packed_key  # pre-cache the packed representation
        return kvkey  # type: ignore[return-value]

    def range_start(self, start: StartT) -> KvKeyRange[StartT, Exclude[Self]]:
        """Get a key range from a `start` boundary up to but excluding this key."""
        return KvKeyRange(start, Exclude(self))

    def range_stop(self, stop: StopT) -> KvKeyRange[Include[Self], StopT]:
        """Get a key range starting with this key up to the `stop` boundary."""
        return KvKeyRange(Include(self), stop)

    def range(self) -> KvKeyRange[Include[Self], IncludePrefix[Self]]:
        """Get a key range starting with this key, including all suffixes of it."""
        return KvKeyRange(Include(self), IncludePrefix(self))

    def include(self) -> Include[Self]:
        """Get this key as an inclusive key range boundary."""
        return Include(self)

    def include_prefix(self) -> IncludePrefix[Self]:
        """Get this key as an inclusive prefix key range boundary."""
        return IncludePrefix(self)

    def exclude(self) -> Exclude[Self]:
        """Get this key as an exclusive key range boundary."""
        return Exclude(self)

    # Methods supported by tuple that we handle slightly differently
    def __lt__(self, value: AnyKvKey, /) -> bool:
        try:
            return self.kv_key_bytes() < pack_key(value)
        except TypeError:
            return NotImplemented

    def __le__(self, value: AnyKvKey, /) -> bool:
        try:
            return self.kv_key_bytes() <= pack_key(value)
        except TypeError:
            return NotImplemented

    def __gt__(self, value: AnyKvKey, /) -> bool:
        try:
            return self.kv_key_bytes() > pack_key(value)
        except TypeError:
            return NotImplemented

    def __ge__(self, value: AnyKvKey, /) -> bool:
        try:
            return self.kv_key_bytes() >= pack_key(value)
        except TypeError:
            return NotImplemented


if TYPE_CHECKING:
    # KvKey isn't actually a tuple subclass at runtime, because we need to store
    # extra state on each instance to cache the packed representation. The tuple
    # type does not allow adding extra __slots__. However we do want to use
    # tuple-like per-element typing, but the only way to do this is to make the
    # type checker think this is a tuple subtype — Python typing has no way to
    # type a __getitem__ call as returning the per-item types — tuple is
    # special-cased.

    # We need ignore[misc] as _KvKeyState order methods like __le__ are broader.
    class KvKey(_KvKeyState, tuple[Unpack[Pieces]]):  # type: ignore[misc]
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
        def __new__(cls, *pieces: Unpack[Pieces]) -> KvKey[Unpack[Pieces]]: ...

        @overload
        def __getitem__(self, index: SupportsIndex, /) -> KvKeyPiece: ...
        @overload
        def __getitem__(self, index: slice, /) -> KvKey[KvKeyTuple]: ...
        def __getitem__(
            self, index: slice | SupportsIndex, /
        ) -> KvKey | KvKeyPiece: ...

        def __add__(self, value: KvKeyTuple, /) -> KvKey: ...  # type: ignore[override]
        def __mul__(self, value: SupportsIndex, /) -> KvKey: ...
        def __rmul__(self, value: SupportsIndex, /) -> KvKey: ...

else:

    @Sequence.register
    class KvKey(_KvKeyState):
        """
        A key identifying a value in a Deno KV database.

        KvKey is a tuple of key pieces — str, bytes, int, float or bool. Unlike a
        plain tuple, KvKey's values are guaranteed to only be valid key values, and
        int values are not coerced to float for JavaScript comparability when used
        with [Kv] methods.

        [Kv]: `denokv.kv.Kv`
        """

        __slots__ = ("__weakref__",)

        def __len__(self) -> int:
            return len(self._unpacked)

        def __contains__(self, value: object, /) -> bool:
            return value in self._unpacked

        @overload
        def __getitem__(self, index: SupportsIndex, /) -> KvKeyPiece: ...

        @overload
        def __getitem__(self, index: slice, /) -> Self: ...

        def __getitem__(self, index: slice | SupportsIndex, /) -> Self | KvKeyPiece:
            if isinstance(index, slice):
                return KvKey(*self._unpacked[index])
            return self._unpacked[index]

        def __iter__(self) -> Iterator[KvKeyPiece]:
            return iter(self._unpacked)

        def __reversed__(self) -> Iterator[_T_co]:
            return reversed(self._unpacked)

        def __add__(self, value: AnyKvKey, /) -> KvKey[KvKeyTuple]:
            if isinstance(value, KvKey):
                return KvKey(*self._unpacked, *value._unpacked)
            if is_kv_key_tuple(value):
                return KvKey(*self._unpacked, *value)
            return KvKey(*self._unpacked, *unpack(pack_key(value)))

        def __mul__(self, value: SupportsIndex, /) -> KvKey[KvKeyTuple]:
            return KvKey(*(self._unpacked * value))

        def __rmul__(self, value: SupportsIndex, /) -> KvKey[KvKeyTuple]:
            return KvKey(*(self._unpacked * value))

        def count(self, value: Any, /) -> int:
            return self._unpacked.count(value)

        def index(
            self,
            value: object,
            start: int = 0,
            stop: int = sys.maxsize,
            /,
        ) -> int:
            return self._unpacked.index(value, start, stop)


# Ideally the default parameter of the Pieces KvKeyTuple would make this the
# default for KvKey (with no generic type), but mypy thinks it is
# KvKey[*tuple[Any, ...]] when used without generic type args.
DefaultKvKey: TypeAlias = "KvKey[Unpack[tuple[KvKeyPiece, ...]]]"
"""KvKey containing any number of key values of any allowed type."""


@dataclass(frozen=True, **slots_if310())
class _KeyBoundary(Generic[AnyKvKeyT_co]):
    key: Final[AnyKvKeyT_co]  # type: ignore[misc]

    @overload
    def __init__(self: _KeyBoundary[AnyKvKeyT_co], key: AnyKvKeyT_co) -> None: ...

    @overload
    def __init__(
        self: _KeyBoundary[KvKey[Unpack[Pieces]]], *pieces: Unpack[Pieces]
    ) -> None: ...

    def __init__(self, arg1: AnyKvKeyT_co | KvKeyPiece, *rest: KvKeyPiece) -> None:  # type: ignore[misc]
        key: AnyKvKey
        if is_any_kv_key(arg1):
            key = arg1
        else:
            key = KvKey(cast(KvKeyPiece, arg1), *rest)
        object.__setattr__(self, "key", key)

    @property
    def key_option(self) -> Some[AnyKvKeyT_co]:
        return Some(self.key)

    def range_start(self, start: StartT) -> KvKeyRange[StartT, Self]:  # type: ignore[type-var,unused-ignore]  # type-var because Self is not allowed to be the StopT type (but works anyway). unused-ignore because 3.9 does not detect the Self error.
        return KvKeyRange(start, self)  # type: ignore[type-var,unused-ignore]

    def range_stop(self, stop: StopT) -> KvKeyRange[Self, StopT]:  # type: ignore[type-var,unused-ignore]
        return KvKeyRange(self, stop)  # type: ignore[type-var,unused-ignore]

    def __repr__(self) -> str:
        if isinstance(self.key, KvKey):
            key = repr(tuple(self.key))[1:-1]
        else:
            key = repr(self.key)
        return f"{type(self).__name__}({key})"


class Include(_KeyBoundary[AnyKvKeyT_co]):
    """KvKeyRange boundary that includes its key in the range."""

    if TYPE_CHECKING:
        # For some reason mypy only infers types of Pieces using new not init
        @overload
        def __new__(cls, key: AnyKvKeyT, /) -> Include[AnyKvKeyT]: ...  # type: ignore[overload-overlap]

        @overload
        def __new__(cls, *pieces: Unpack[Pieces]) -> Include[KvKey[Unpack[Pieces]]]: ...

        def __new__(cls, *args: Any) -> Self: ...  # type: ignore[misc]

    def range(self) -> KvKeyRange[Self, Self]:
        """Get a key range including only this Include's key."""
        return KvKeyRange(self, self)


class IncludePrefix(_KeyBoundary[AnyKvKeyT_co]):
    """KvKeyRange boundary that includes keys prefixed by its key in the range."""

    if TYPE_CHECKING:
        # For some reason mypy only infers types of Pieces using new not init
        @overload
        def __new__(cls, key: AnyKvKeyT, /) -> IncludePrefix[AnyKvKeyT]: ...  # type: ignore[overload-overlap]

        @overload
        def __new__(
            cls, *pieces: Unpack[Pieces]
        ) -> IncludePrefix[KvKey[Unpack[Pieces]]]: ...

        def __new__(cls, *args: Any) -> Self: ...  # type: ignore[misc]

    @override
    def range_stop(self, stop: StopT) -> KvKeyRange[Include[AnyKvKeyT_co], StopT]:  # type: ignore[override]
        return KvKeyRange(Include(self.key), stop)

    def range(self) -> KvKeyRange[Include[AnyKvKeyT_co], Self]:
        """
        Create a key range that includes all child keys of this object's key.

        Examples
        --------
        >>> r = IncludePrefix('a', 1).range()
        >>> r
        KvKeyRange(start=Include('a', 1), stop=IncludePrefix('a', 1))
        >>> assert ('a', 0, 99) not in r
        >>> assert ('a', 1) in r
        >>> assert ('a', 1, 0) in r
        >>> assert ('a', 1, 99) in r
        >>> assert ('a', 2, 0) not in r
        """
        return KvKeyRange(Include(self.key), self)


class Exclude(_KeyBoundary[AnyKvKeyT_co]):
    """KvKeyRange boundary that excludes its key from the range."""

    if TYPE_CHECKING:
        # For some reason mypy only infers types of Pieces using new not init
        @overload
        def __new__(cls, key: AnyKvKeyT, /) -> Exclude[AnyKvKeyT]: ...  # type: ignore[overload-overlap]

        @overload
        def __new__(cls, *pieces: Unpack[Pieces]) -> Exclude[KvKey[Unpack[Pieces]]]: ...

        def __new__(cls, *args: Any) -> Self: ...  # type: ignore[misc]

    def range(self) -> KvKeyRange[Self, Self]:
        """Get an empty key range that excludes this Exclude's key at the boundaries."""
        return KvKeyRange(self, self)


@dataclass(frozen=True, **slots_if310())
class IncludeAll:
    """KvKeyRange boundary that includes any key in the range."""

    def __new__(cls) -> Self:
        # Always return the same unique instance from IncludeAll()
        instance = object.__new__(cls)

        def __new__(cls: type[IncludeAll]) -> IncludeAll:
            return instance

        cls.__new__ = __new__  # type: ignore[method-assign,assignment]
        return instance

    @property
    def key_option(self) -> Nothing:
        return Nothing()

    def range_start(self, start: StartT) -> KvKeyRange[StartT, Self]:
        return KvKeyRange(start, self)

    def range_stop(self, stop: StopT) -> KvKeyRange[Self, StopT]:
        return KvKeyRange(self, stop)

    @classmethod
    def range(cls) -> KvKeyRange[IncludeAll, IncludeAll]:
        """Get a key range that includes all keys."""
        return KvKeyRange(IncludeAll(), IncludeAll())


StartBoundary: TypeAlias = Union[
    IncludeAll, Include[AnyKvKeyT_co], Exclude[AnyKvKeyT_co]
]
StopBoundary: TypeAlias = Union[
    IncludeAll, Include[AnyKvKeyT], IncludePrefix[AnyKvKeyT], Exclude[AnyKvKeyT]
]

if TYPE_CHECKING:
    StartT = TypeVar(
        "StartT", bound=StartBoundary, default=StartBoundary, covariant=False
    )
    StartT_co = TypeVar(
        "StartT_co", bound=StartBoundary, default=StartBoundary, covariant=True
    )
    StopT = TypeVar("StopT", bound=StopBoundary, default=StopBoundary, covariant=False)
    StopT_co = TypeVar(
        "StopT_co", bound=StopBoundary, default=StopBoundary, covariant=True
    )
else:
    StartT = TypeVar("StartT", bound=StartBoundary, covariant=False)
    StopT = TypeVar("StopT", bound=StopBoundary, covariant=False)
    StartT_co = TypeVar("StartT_co", bound=StartBoundary, covariant=True)
    StopT_co = TypeVar("StopT_co", bound=StopBoundary, covariant=True)


@functools.total_ordering
@dataclass(frozen=True, eq=False, **slots_if310())
class KvKeyRange(KvKeyRangeEncodable, Generic[StartT_co, StopT_co]):
    """
    A range of KV key values, bounded by a start and end positions.

    Examples
    --------
    >>> key_range = Include('a', 0).range_stop(Exclude('a', 10))
    >>> key_range
    KvKeyRange(start=Include('a', 0), stop=Exclude('a', 10))
    >>> KvKey('a', 0) in key_range
    True
    >>> KvKey('a', 10) in key_range
    False
    >>> KvKey('a', 10) in key_range.range_stop(Include('a', 10))
    True
    """

    start: Final[StartT_co] = field(default=cast(StartT_co, IncludeAll()))  # type: ignore[misc]
    stop: Final[StopT_co] = field(default=cast(StopT_co, IncludeAll()))  # type: ignore[misc]
    _packed: tuple[bytes, bytes] | None = field(
        default=None, init=False, repr=False, hash=False, compare=False
    )

    def __post_init__(self) -> None:
        # Types don't allow IncludePrefix as start because it has the same
        # effect as Include at the start position. But if it is used, we
        # normalise it.
        if isinstance(self.start, IncludePrefix):
            object.__setattr__(self, "start", Include(self.start.key))

        if not isinstance(self.start, (IncludeAll, Include, Exclude)):
            raise TypeError("start must be IncludeAll, Include or Exclude")
        if not isinstance(self.stop, (IncludeAll, Include, IncludePrefix, Exclude)):
            raise TypeError(
                "stop must be IncludeAll, Include, IncludePrefix or Exclude"
            )

    def range_start(self, start: StartT) -> KvKeyRange[StartT, StopT_co]:
        """Get a key range with this range's stop and the provided `start`."""
        return KvKeyRange(start, self.stop)

    def range_stop(self, stop: StopT) -> KvKeyRange[StartT_co, StopT]:
        """Get a key range with this range's start and the provided `stop`."""
        return KvKeyRange(self.start, stop)

    @override
    def kv_key_range_bytes(self) -> tuple[bytes, bytes]:
        if (packed := self._packed) is not None:
            return packed

        options = PackKeyRangeOptions()

        start = self.start
        if isinstance(start, Include):
            options["start"] = start.key
        elif isinstance(start, Exclude):
            options["start"] = start.key
            options["exclude_start"] = True
        else:
            assert isinstance(start, IncludeAll)
            options["start"] = ()

        stop = self.stop
        if isinstance(stop, Include):
            options["end"] = stop.key
            options["exclude_end"] = False
        elif isinstance(stop, Exclude):
            options["end"] = stop.key
        elif isinstance(stop, IncludePrefix):
            options["prefix"] = stop.key
        else:
            assert isinstance(stop, IncludeAll)

        object.__setattr__(self, "_packed", (packed := pack_key_range(**options)))
        return packed

    def __contains__(self, x: object, /) -> bool:
        if not is_any_kv_key(x):
            return False
        packed = pack_key(x)
        packed_start, packed_stop = pack_key_range(self)
        return packed_start <= packed < packed_stop

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, KvKeyRange):
            return NotImplemented
        return self.kv_key_range_bytes() == value.kv_key_range_bytes()

    def __hash__(self) -> int:
        return hash(self.kv_key_range_bytes())

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, KvKeyRange):
            return NotImplemented
        return self.kv_key_range_bytes() < other.kv_key_range_bytes()
