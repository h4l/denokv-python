from __future__ import annotations

import asyncio
from abc import ABC
from abc import abstractmethod
from base64 import urlsafe_b64decode
from base64 import urlsafe_b64encode
from binascii import unhexlify
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from enum import Enum
from enum import Flag
from enum import auto
from functools import partial
from itertools import islice
from os import environ
from types import MappingProxyType
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import Container
from typing import Coroutine
from typing import Final
from typing import Generic
from typing import Iterable
from typing import Literal
from typing import Mapping
from typing import MutableSequence
from typing import Protocol
from typing import Sequence
from typing import TypedDict
from typing import Union
from typing import cast
from typing import overload
from typing import runtime_checkable

import aiohttp
import v8serialize
from fdb.tuple import unpack
from typing_extensions import TypeIs
from v8serialize import Decoder
from v8serialize import Encoder
from v8serialize.constants import SerializationTag
from v8serialize.decode import ReadableTagStream
from yarl import URL

from denokv import _datapath_pb2 as dp_protobuf
from denokv import datapath
from denokv._datapath_pb2 import AtomicWrite
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv._pycompat.dataclasses import FrozenAfterInitDataclass
from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.enum import EvalEnumRepr
from denokv._pycompat.protobuf import enum_name
from denokv.asyncio import loop_time
from denokv.auth import ConsistencyLevel
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.auth import MetadataExchangeDenoKvError
from denokv.auth import get_database_metadata
from denokv.backoff import Backoff
from denokv.backoff import ExponentialBackoff
from denokv.backoff import attempts
from denokv.datapath import AnyKvKey
from denokv.datapath import AnyKvKeyT
from denokv.datapath import AutoRetry
from denokv.datapath import CheckFailure
from denokv.datapath import DataPathDenoKvError
from denokv.datapath import DataPathError
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyTuple
from denokv.datapath import ProtocolViolation
from denokv.datapath import is_kv_key_tuple
from denokv.datapath import pack_key
from denokv.datapath import parse_protobuf_kv_entry
from denokv.datapath import read_range_multi
from denokv.datapath import read_range_single
from denokv.errors import InvalidCursor
from denokv.kv_keys import KvKey
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result

if TYPE_CHECKING:
    from typing_extensions import Self
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
_DataPathErrorT = TypeVar("_DataPathErrorT", bound=DataPathDenoKvError)

SAFE_FLOAT_INT_RANGE: Final = range(-(2**53 - 1), 2**53)  # 2**53 - 1 is max safe

CursorFormatType: TypeAlias = Callable[["ListContext"], "AnyCursorFormat"]


def v8_encode_int_as_bigint(
    value: object,
    ctx: v8serialize.encode.EncodeContext,
    next: v8serialize.encode.EncodeNextFn,
) -> None:
    if isinstance(value, int):
        ctx.stream.write_bigint(value)
    else:
        next(value)


# TODO: add an explicit tagged JSBigInt type to v8serialize
def create_default_v8_encoder() -> v8serialize.Encoder:
    """
    Create a new V8-serialization format Encoder.

    This encoder always encodes int as JavaScript BigInt. We use this by default
    for Kv instances to ensure consistent handling of int and float types.

    Notes
    -----
    In contrast, the `v8serialize` default encoder encodes int as Number when it
    fits in the +/- 2**53 - 1 range which float64 can represent exactly. This
    results in differing number representation for different number sizes, which
    is likely to be a footgun in the context of the Sum/Min/Max write
    operations.
    """
    return v8serialize.Encoder(
        encode_steps=[v8_encode_int_as_bigint, *v8serialize.default_encode_steps]
    )


class KvListOptions(TypedDict, total=False):
    """Keyword arguments of `Kv.list()`."""

    limit: int | None
    cursor: str | None
    reverse: bool | None  # order asc/desc?
    consistency: ConsistencyLevel | None
    batch_size: int | None
    cursor_format_type: CursorFormatType | None


@dataclass(frozen=True, **slots_if310())
class KvEntry(Generic[AnyKvKeyT, T]):
    """A value read from the Deno KV database, along with its key and version."""

    key: AnyKvKeyT
    value: T
    versionstamp: VersionStamp


@dataclass(frozen=True, **slots_if310())
class ListKvEntry(KvEntry[AnyKvKeyT, T]):
    """
    A value read from the Deno KV database with a list operation.

    In addition to a normal [KvEntry's] key, value and version, [ListKvEntry]
    provides a [cursor] that can be used to start another [Kv.list()] starting
    from the value after this.
    """

    listing: ListContext = field(repr=False)

    @property
    def cursor(self) -> str:
        result = self.listing.cursor_format.get_cursor_for_key(self.key)
        if isinstance(result, Err):
            raise result.error  # should never occur with entries from Kv.list()
        return result.value


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

    RANGE: ClassVar = range(0, 2**64)
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


def encode_kv_write_value(value: object, *, v8_encoder: Encoder) -> dp_protobuf.KvValue:
    if isinstance(value, KvU64):
        return dp_protobuf.KvValue(
            data=bytes(value),
            encoding=dp_protobuf.ValueEncoding.VE_LE64,
        )
    elif isinstance(value, bytes):
        return dp_protobuf.KvValue(
            data=value, encoding=dp_protobuf.ValueEncoding.VE_BYTES
        )
    else:
        return dp_protobuf.KvValue(
            data=bytes(v8_encoder.encode(value)),
            encoding=dp_protobuf.ValueEncoding.VE_V8,
        )


@dataclass
class PlannedWrite:
    kv: Kv | None = field(default=None)
    checks: MutableSequence[AnyKeyVersion] = field(default_factory=list)
    mutations: MutableSequence[Mutation] = field(default_factory=list)
    enqueues: MutableSequence[Enqueue] = field(default_factory=list)

    async def write(self, kv: Kv | None = None) -> CompletedWrite:
        kv = self.kv if kv is None else kv
        if kv is None:
            raise TypeError("No kv was provided to write")
        return await kv.write(self)

    def as_protobuf(self, *, v8_encoder: Encoder) -> AtomicWrite:
        return AtomicWrite(
            checks=[
                dp_protobuf.Check(
                    key=pack_key(check.key), versionstamp=check.versionstamp
                )
                for check in self.checks
            ],
            mutations=[
                mut.as_protobuf(v8_encoder=v8_encoder) for mut in self.mutations
            ],
            enqueues=[enq.as_protobuf(v8_encoder=v8_encoder) for enq in self.enqueues],
        )

    @overload
    def check(self, key: AnyKvKey, versionstamp: VersionStamp | None) -> Self: ...

    @overload
    def check(self, check: AnyKeyVersion, /) -> Self: ...

    def check(
        self, key: AnyKvKey | AnyKeyVersion, versionstamp: VersionStamp | None = None
    ) -> Self:
        if isinstance(key, AnyKeyVersion):
            self.checks.append(key)
            if versionstamp is not None:
                raise TypeError(
                    "versionstamp argument cannot be passed when first argument "
                    "is check object with a key and versionstamp"
                )
        else:
            self.checks.append(Check(key, versionstamp))
        return self

    def set(self, key: AnyKvKey, value: object, *, versioned: bool = False) -> Self:
        return self.mutate(Set(key, value, versioned=versioned))

    @overload
    def sum(self, sum: Sum, /) -> Self: ...

    @overload
    def sum(self, key: AnyKvKey, value: KvU64) -> Self: ...

    @overload
    def sum(
        self,
        key: AnyKvKey,
        value: int | float,
        *,
        limit_min: int | float | None = None,
        limit_max: int | float | None = None,
        limit_exceeded: LimitExceededInput | None = None,
        limit: Limit | None = None,
    ) -> Self: ...

    def sum(
        self,
        key: AnyKvKey | Sum,
        value: int | float | KvU64 | None = None,
        *,
        limit_min: int | float | None = None,
        limit_max: int | float | None = None,
        limit_exceeded: LimitExceededInput | None = None,
        limit: Limit | None = None,
    ) -> Self:
        if isinstance(key, Sum):
            if value is not None:
                raise TypeError("sum() takes no arguments after 'sum'")
            return self.mutate(key)

        if value is None:
            raise TypeError("sum() missing 1 required positional argument: 'value'")
        if limit is None:
            if not (limit_min is None and limit_max is None and limit_exceeded is None):
                limit = Limit(
                    min=limit_min, max=limit_max, limit_exceeded=limit_exceeded
                )
        else:
            limit_min = limit.min if limit_min is None else limit_min
            limit_max = limit.max if limit_max is None else limit_max
            limit_exceeded = (
                cast(LimitExceededInput, limit.limit_exceeded)
                if limit_exceeded is None
                else limit_exceeded
            )
            limit = Limit(limit_min, limit_max, limit_exceeded)

        return self.mutate(Sum(key, value, limit=limit))

    def min(self, key: AnyKvKey, value: int | KvU64) -> Self:
        return self.mutate(Min(key, value))

    def max(self, key: AnyKvKey, value: int | KvU64) -> Self:
        return self.mutate(Max(key, value))

    def delete(self, key: AnyKvKey) -> Self:
        return self.mutate(Delete(key))

    def mutate(self, mutation: Mutation) -> Self:
        self.mutations.append(mutation)
        return self

    @overload
    def enqueue(self, enqueue: Enqueue, /) -> Self: ...
    @overload
    def enqueue(
        self,
        message: object,
        *,
        delivery_time: datetime | None = None,
        retry_delays: Backoff | None = None,
        dead_letter_keys: Sequence[AnyKvKey] | None = None,
    ) -> Self: ...
    def enqueue(
        self,
        message: object | Enqueue,
        *,
        delivery_time: datetime | None = None,
        retry_delays: Backoff | None = None,
        dead_letter_keys: Sequence[AnyKvKey] | None = None,
    ) -> Self:
        if isinstance(message, Enqueue):
            enqueue = message
        else:
            enqueue = Enqueue(
                message,
                delivery_time=delivery_time,
                retry_delays=retry_delays,
                dead_letter_keys=dead_letter_keys,
            )
        self.enqueues.append(enqueue)
        return self


@dataclass(init=False, **slots_if310())
class ConflictedWrite(FrozenAfterInitDataclass):
    ok: Literal[False]
    conflicts: Mapping[AnyKvKey, Check]
    versionstamp: None
    checks: Sequence[Check]
    mutations: Sequence[Mutation]
    enqueues: Sequence[Enqueue]

    def __init__(
        self,
        failed_checks: Sequence[int],
        checks: Sequence[Check],
        mutations: Sequence[Mutation],
        enqueues: Sequence[Enqueue],
    ) -> None:
        self.ok = False
        try:
            self.conflicts = MappingProxyType(
                {checks[i].key: checks[i] for i in failed_checks}
            )
        except IndexError as e:
            raise ValueError("failed_checks contains out-of-bounds index") from e
        self.versionstamp = None
        self.checks = tuple(checks)
        self.mutations = tuple(mutations)
        self.enqueues = tuple(enqueues)


@dataclass(init=False, **slots_if310())
class CommittedWrite(FrozenAfterInitDataclass):
    ok: Literal[True]
    conflicts: Mapping[KvKey, Check]  # empty
    versionstamp: VersionStamp
    checks: Sequence[Check]
    mutations: Sequence[Mutation]
    enqueues: Sequence[Enqueue]

    def __init__(
        self,
        versionstamp: VersionStamp,
        checks: Sequence[Check],
        mutations: Sequence[Mutation],
        enqueues: Sequence[Enqueue],
    ) -> None:
        self.ok = True
        self.conflicts = MappingProxyType({})
        self.versionstamp = versionstamp
        self.checks = tuple(checks)
        self.mutations = tuple(mutations)
        self.enqueues = tuple(enqueues)


CompletedWrite: TypeAlias = Union[CommittedWrite, ConflictedWrite]


def is_applied(write: CompletedWrite) -> TypeIs[CommittedWrite]:
    return isinstance(write, CommittedWrite)


@runtime_checkable
class AnyKeyVersion(Protocol):
    @property
    def key(self) -> AnyKvKey: ...
    @property
    def versionstamp(self) -> VersionStamp | None: ...


@dataclass(frozen=True, **slots_if310())
class Check(AnyKeyVersion):
    key: AnyKvKey
    versionstamp: VersionStamp | None


@dataclass(init=False, **slots_if310())
class Mutation(FrozenAfterInitDataclass, ABC):
    key: AnyKvKey
    expire_at: datetime | None

    def __init__(self, key: AnyKvKey, expire_at: datetime | None) -> None:
        if type(self) is Mutation:
            raise TypeError("cannot create Mutation instances directly")
        self.key = key
        self.expire_at = expire_at

    @abstractmethod
    def as_protobuf(self, *, v8_encoder: Encoder) -> dp_protobuf.Mutation:
        pass

    def _expire_at_ms(self) -> int:
        return 0 if self.expire_at is None else int(self.expire_at.timestamp() * 1000)


@dataclass(init=False, **slots_if310())
class Set(Mutation):
    value: object
    versioned: bool

    def __init__(
        self,
        key: AnyKvKey,
        value: object,
        *,
        expire_at: datetime | None = None,
        versioned: bool = False,
    ) -> None:
        super(Set, self).__init__(key, expire_at=expire_at)
        self.value = value
        self.versioned = versioned

    def as_protobuf(self, *, v8_encoder: Encoder) -> dp_protobuf.Mutation:
        return dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_SET_SUFFIX_VERSIONSTAMPED_KEY
            if self.versioned
            else dp_protobuf.MutationType.M_SET,
            key=pack_key(self.key),
            value=encode_kv_write_value(self.value, v8_encoder=v8_encoder),
            expire_at_ms=self._expire_at_ms(),
        )


class LimitExceededPolicy(EvalEnumRepr, Enum):
    ERROR = "error"
    CLAMP = "clamp"
    WRAP = "wrap"


LimitExceededInput = Literal[
    "error",
    "clamp",
    LimitExceededPolicy.ERROR,
    LimitExceededPolicy.CLAMP,
]


@dataclass(init=False, frozen=True, **slots_if310())
class Limit(Container["int | float"]):
    """
    A range of numbers used to define the allowed range of Add operations.

    Examples
    --------
    >>> lim = Limit(0, 100, limit_exceeded='clamp')
    >>> lim
    Limit(min=0, max=100, limit_exceeded=LimitExceededPolicy.CLAMP)
    >>> -10 in lim
    False
    >>> 110 in lim
    False
    >>> 10 in lim
    True
    >>> 9000 in Limit(min=0)
    True
    """

    min: int | float | None
    max: int | float | None
    limit_exceeded: LimitExceededPolicy

    def __init__(
        self,
        min: int | float | None = None,
        max: int | float | None = None,
        limit_exceeded: LimitExceededInput | None = LimitExceededPolicy.ERROR,
    ) -> None:
        object.__setattr__(self, "min", min)
        object.__setattr__(self, "max", max)
        object.__setattr__(
            self,
            "limit_exceeded",
            LimitExceededPolicy(limit_exceeded or LimitExceededPolicy.ERROR),
        )

    def __contains__(self, x: object) -> bool:
        if not isinstance(x, (int, float)):
            return False
        return (self.min is None or self.min <= x) and (
            self.max is None or self.max >= x
        )

    def as_protobuf(
        self,
        mutation: dp_protobuf.Mutation,
        *,
        v8_encoder: Encoder,
        value_type: type[int | float],
    ) -> dp_protobuf.Mutation:
        if value_type not in (int, float):
            raise TypeError(f"value_type must be int or float: {value_type!r}")

        if self.min is not None:
            encoded_min = bytes(v8_encoder.encode(self.min))
            Limit._validate_encoded_type(
                "min",
                value=self.min,
                v8_value=encoded_min,
                required_encoding=value_type,
            )
            mutation.sum_min = encoded_min
        if self.max is not None:
            encoded_max = bytes(v8_encoder.encode(self.max))
            Limit._validate_encoded_type(
                "max",
                value=self.max,
                v8_value=encoded_max,
                required_encoding=value_type,
            )
            mutation.sum_max = encoded_max
        if self.limit_exceeded is LimitExceededPolicy.CLAMP:
            mutation.sum_clamp = True
        return mutation

    @staticmethod
    def _validate_encoded_type(
        field: Literal["min", "max"],
        value: int | float,
        v8_value: bytes,
        required_encoding: type[int | float],
    ) -> None:
        assert required_encoding in (int, float)
        try:
            value_type = _get_number_type(_get_v8_value_tag(v8_value))
        except ValueError as e:
            raise RuntimeError(
                f"Limit.{field} is not None so must encode to BigInt or "
                f"Number using the configured v8_encoder, but it didn't: "
                f"value={value}, v8_value={v8_value!r}, error={e}"
            ) from e
        if value_type is not required_encoding:
            raise ValueError(
                f"Limit.{field} encoded to {_js_type_name(value_type)} ({value_type}) "
                f"but the parent Sum's value encoded as "
                f"{_js_type_name(required_encoding)} ({required_encoding}). "
                "Both must encode to the same JavaScript type. Use int or "
                "float consistently for both. If a the V8 serializer is "
                "customised, check how it's encoding int and float values."
            )


def _js_type_name(py_type: type[int | float]) -> Literal["BigInt", "Number"]:
    return "BigInt" if py_type is int else "Number"


LIMIT_KVU64 = Limit(
    min=0,
    max=2**64 - 1,
    # Not normally allowed by types because only LIMIT_KVU64 can use WRAP.
    limit_exceeded=cast(LimitExceededInput, LimitExceededPolicy.WRAP),
)
LIMIT_UNLIMITED = Limit()


@dataclass(init=False, **slots_if310())
class Sum(Mutation):
    value: int | float | KvU64
    limit: Limit = field(default=Limit())

    def __init__(
        self,
        key: AnyKvKey,
        value: int | float | KvU64,
        *,
        limit: Limit | None = None,
        expire_at: datetime | None = None,
    ) -> None:
        super(Sum, self).__init__(key, expire_at=expire_at)

        # Only KvU64 supports wrapping on boundary (and this can't be changed).
        if isinstance(value, KvU64):
            if limit is not None and limit != LIMIT_KVU64:
                raise ValueError(
                    "limit for KvU64 cannot be changed, it must be None or LIMIT_KVU64"
                )
            limit = LIMIT_KVU64
        else:
            if limit is None:
                limit = LIMIT_UNLIMITED
            elif limit.limit_exceeded == LimitExceededPolicy.WRAP:
                raise ValueError(
                    "limit for JavaScript BigInt or Number cannot be WRAP, it "
                    "must be ERROR or CLAMP"
                )
        assert limit is not None

        self.value = value
        self.limit = limit

    def as_protobuf(self, *, v8_encoder: Encoder) -> dp_protobuf.Mutation:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_SUM,
            key=pack_key(self.key),
            value=encode_kv_write_value(self.value, v8_encoder=v8_encoder),
            expire_at_ms=self._expire_at_ms(),
        )

        v8_number_type = _validate_number_mutation_value(self, mutation)
        if v8_number_type is not None:
            assert mutation.value.encoding == dp_protobuf.ValueEncoding.VE_V8
            # Only V8 values use the min/max limits.
            self.limit.as_protobuf(
                mutation, v8_encoder=v8_encoder, value_type=v8_number_type
            )

        return mutation


def _validate_number_mutation_value(
    mut: Sum | Min | Max, mutation: dp_protobuf.Mutation
) -> type[int | float] | None:
    """
    Validate the encoded numeric value of a Sum/Min/Max mutation operation.

    If the operation value is a V8-encoded number, the return value is the int
    or float type, indicating if the encoded value is BigInt or Number.
    Otherwise the return value is None.
    """
    if mutation.value.encoding == dp_protobuf.ValueEncoding.VE_LE64:
        return None
    elif mutation.value.encoding == dp_protobuf.ValueEncoding.VE_V8:
        try:
            value_type = _get_number_type(_get_v8_value_tag(mutation.value.data))
        except ValueError as e:
            raise RuntimeError(
                f"{type(mut).__name__}.value is not KvU64 so it must encode to "
                f"BigInt or Number using the configured v8_encoder, but it didn't: "
                f"value={mut.value!r}, v8_value={mutation.value.data!r}, error={e}"
            ) from e

        return value_type

    raise ValueError(
        f"{type(mut).__name__}.value is not a KvU64 or number that "
        f"V8-serializes to BigInt or Number: value={mut.value!r}, ValueEncoding: "
        f"{enum_name(dp_protobuf.ValueEncoding, mutation.value.encoding)}"
    )


def _get_v8_value_tag(v8_value: bytes) -> SerializationTag:
    """Inspect a V8-serialized value to determine the type of value it holds."""
    try:
        rts = ReadableTagStream(v8_value)
        rts.read_header()
        return rts.read_tag()
    except v8serialize.V8SerializeError as e:
        raise ValueError("v8_value bytes does not contain a V8-encoded value") from e


def _get_number_type(tag: SerializationTag) -> type[int | float]:
    """Determine the JS number type of a V8-serialized value type tag."""
    if tag is SerializationTag.kBigInt or tag is SerializationTag.kBigIntObject:
        return int
    elif tag in {
        SerializationTag.kNumberObject,
        SerializationTag.kDouble,
        SerializationTag.kInt32,
        SerializationTag.kUint32,
    }:
        return float
    raise ValueError(f"tag is not a BigInt or Number: {tag}")


@dataclass(**slots_if310())
class Min(Mutation):
    value: KvU64

    def __init__(
        self,
        key: AnyKvKey,
        value: int | KvU64,
        *,
        expire_at: datetime | None = None,
    ) -> None:
        super(Min, self).__init__(key, expire_at=expire_at)
        self.value = value if isinstance(value, KvU64) else KvU64(value)

    def as_protobuf(self, *, v8_encoder: Encoder) -> dp_protobuf.Mutation:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_MIN,
            key=pack_key(self.key),
            value=encode_kv_write_value(self.value, v8_encoder=v8_encoder),
            expire_at_ms=self._expire_at_ms(),
        )
        _validate_number_mutation_value(self, mutation)
        return mutation


@dataclass(**slots_if310())
class Max(Mutation):
    value: int | float | KvU64

    def __init__(
        self,
        key: AnyKvKey,
        value: int | KvU64,
        *,
        expire_at: datetime | None = None,
    ) -> None:
        super(Max, self).__init__(key, expire_at=expire_at)
        self.value = value if isinstance(value, KvU64) else KvU64(value)

    def as_protobuf(self, *, v8_encoder: Encoder) -> dp_protobuf.Mutation:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_MAX,
            key=pack_key(self.key),
            value=encode_kv_write_value(self.value, v8_encoder=v8_encoder),
            expire_at_ms=self._expire_at_ms(),
        )
        _validate_number_mutation_value(self, mutation)
        return mutation


@dataclass(**slots_if310())
class Delete(Mutation):
    def __init__(self, key: AnyKvKey) -> None:
        super(Delete, self).__init__(key, expire_at=None)

    def as_protobuf(self, *, v8_encoder: Encoder | None = None) -> dp_protobuf.Mutation:
        return dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_DELETE, key=pack_key(self.key)
        )


DEFAULT_ENQUEUE_RETRY_DELAYS = ExponentialBackoff(
    initial_interval_seconds=1, multiplier=3
)
DEFAULT_ENQUEUE_RETRY_DELAY_COUNT = 10


@dataclass(init=False, **slots_if310())
class Enqueue(FrozenAfterInitDataclass):
    """
    A message to be async-delivered to a Deno app listening to the Kv's queue.

    Parameters
    ----------
    message:
        The message to deliver. Can be any value that can be written to the database.
    delivery_time:
        Delay the message delivery until this time.

        If the time is None or in the past, the message is delivered as soon as
        possible.
    retry_delays:
        Delivery attempts that fail will be retried after these delays.

        If the value is an Iterable, a fixed number of values will be drawn to retry
        with. Use a fixed-length Sequence to specify a precise number of retries.
        Default: DEFAULT_ENQUEUE_RETRY_DELAYS
    dead_letter_keys:
        Messages that cannot be delivered will be written to these keys.

    Notes
    -----
    See [Deno.Kv.listenQueue()](https://docs.deno.com/api/deno/~/Deno.Kv#method_listenqueue_0)
    """

    message: object
    delivery_time: datetime | None
    retry_delays: Backoff
    dead_letter_keys: Sequence[AnyKvKey]

    def __init__(
        self,
        message: object,
        *,
        delivery_time: datetime | None = None,
        retry_delays: Backoff | None = None,
        dead_letter_keys: Sequence[AnyKvKey] | None = None,
    ):
        self.message = message
        self.delivery_time = delivery_time
        self.retry_delays = (
            DEFAULT_ENQUEUE_RETRY_DELAYS if retry_delays is None else retry_delays
        )
        self.dead_letter_keys = () if dead_letter_keys is None else dead_letter_keys

    def as_protobuf(self, *, v8_encoder: Encoder) -> dp_protobuf.Enqueue:
        deadline_ms = None
        if self.delivery_time is not None:
            deadline_ms = int(self.delivery_time.timestamp() * 1000)
        return dp_protobuf.Enqueue(
            payload=bytes(v8_encoder.encode(self.message)),
            keys_if_undelivered=[pack_key(k) for k in self.dead_letter_keys],
            deadline_ms=deadline_ms,
            backoff_schedule=self._evaluate_backoff_schedule(),
        )

    def _evaluate_backoff_schedule(self) -> Sequence[int]:
        # Sample a fixed max number from unknown-length iterables.
        delay_seconds = (
            self.retry_delays
            if isinstance(self.retry_delays, Sequence)
            else islice(self.retry_delays, DEFAULT_ENQUEUE_RETRY_DELAY_COUNT)
        )
        # Backoff times are in seconds, but we need milliseconds
        return [int(delay * 1000) for delay in delay_seconds]


WriteOperation: TypeAlias = Union[Check, Set, Sum, Min, Max, Delete, Enqueue]


@dataclass(frozen=True, **slots_if310())
class EndpointSelector:
    # Right now this is very simple, which is fine for the local SQLite-backed
    # Deno KV server, but for distributed Deno KV we need to support selecting
    # endpoints based on latency, so this can be stateful.
    meta: DatabaseMetadata

    def __post_init__(self) -> None:
        # Require at least one strongly-consistent endpoint. Note that this is
        # a requirement of the KV Connect spec and we enforce it when fetching
        # metadata, so this requirement should only be broken as a result of a
        # programmer error.
        if not any(
            ep.consistency is ConsistencyLevel.STRONG for ep in self.meta.endpoints
        ):
            raise ValueError(f"no endpoint has {ConsistencyLevel.STRONG} consistency")

    def get_endpoints(self, consistency: ConsistencyLevel) -> Sequence[EndpointInfo]:
        return [
            e
            for e in self.meta.endpoints
            if consistency is ConsistencyLevel.EVENTUAL
            or e.consistency is ConsistencyLevel.STRONG
        ]

    def get_endpoint(self, consistency: ConsistencyLevel) -> EndpointInfo:
        candidates = self.get_endpoints(consistency)
        # DatabaseMetadata is required by the Metadata Exchange spec to have at
        # least one strongly consistent endpoint.
        assert len(candidates) > 0
        return candidates[0]


@dataclass(frozen=True, **slots_if310())
class CachedValue(Generic[T]):
    fresh_until: float
    value: T

    def is_fresh(self, time: float) -> bool:
        return time < self.fresh_until


AuthenticatorFn: TypeAlias = Callable[
    [], Awaitable["Result[DatabaseMetadata, MetadataExchangeDenoKvError]"]
]
"""
The type of a function that connects to a KV database and returns metadata.

The metadata contains the server URLs and access tokens needed to query the KV
database.
"""


@dataclass(frozen=True, **slots_if310())
class KvCredentials:
    server_url: URL
    access_token: str


@dataclass
class Authenticator:
    """
    Authenticates with a KV database server and returns its metadata.

    Notes
    -----
    This is an [AuthenticationFn](`denokv.kv.AuthenticationFn`).
    """

    session: aiohttp.ClientSession
    retry_delays: Backoff
    credentials: KvCredentials

    async def __call__(self) -> Result[DatabaseMetadata, MetadataExchangeDenoKvError]:
        result: Result[DatabaseMetadata, MetadataExchangeDenoKvError] | None = None
        credentials = self.credentials
        for delay in attempts(self.retry_delays):
            result = await get_database_metadata(
                session=self.session,
                server_url=credentials.server_url,
                access_token=credentials.access_token,
            )
            if isinstance(result, Err) and result.error.retryable:
                await asyncio.sleep(delay)
                continue
            return result
        else:  # backoff timed out
            assert isinstance(result, Err)
            return result


def _cached_database_metadata(value: DatabaseMetadata) -> CachedValue[DatabaseMetadata]:
    return CachedValue(fresh_until=loop_time(wall_time=value.expires_at), value=value)


def normalize_key(key: KvKeyTuple, *, bigints: bool = False) -> KvKeyTuple:
    """
    Coerce key types for JavaScript compatibility.

    Encoding of `int` values depends on the `bigints` argument:
    - by default and with `bigints=False` `int` is encoded as `float`, which is
        how JavaScript encodes `number`.
    - with `bigints=True` `int` is encoded as JavaScript encodes `BigInt` and
      `float` is encoded as JavaScript encodes `number`.

    [SAFE_FLOAT_INT_RANGE](`denokv.kv.SAFE_FLOAT_INT_RANGE`)

    Raises
    ------
    ValueError
        If `bigints=False` and an int value is outside [SAFE_FLOAT_INT_RANGE].

    Examples
    --------
    The `key` can contain int, but ints are packed as float because JavaScript
    int are floats.

    >>> normalize_key((1, 2.0), bigints=False)
    (1.0, 2.0)

    Pass `bigints=True` to encode int as JavaScript encodes BigInt:

    >>> normalize_key((1, 2.0), bigints=True)
    (1, 2.0)
    """
    if bigints or not any(isinstance(part, int) for part in key):
        return key
    normalised = list(key)
    for i, val in enumerate(normalised):
        # bool is an int subtype!
        if not (isinstance(val, int) and not isinstance(val, bool)):
            continue
        if val not in SAFE_FLOAT_INT_RANGE:
            raise ValueError(
                f"int value is too large to be losslessly normalized to a float: {val}."
            )
        normalised[i] = float(val)
    return tuple(normalised)


@dataclass(init=False)
class DatabaseMetadataCache:
    authenticator: AuthenticatorFn
    current: CachedValue[DatabaseMetadata] | None
    pending: (
        asyncio.Task[Result[CachedValue[DatabaseMetadata], MetadataExchangeDenoKvError]]
        | None
    )

    def __init__(
        self,
        *,
        initial: DatabaseMetadata | None = None,
        authenticator: AuthenticatorFn,
    ) -> None:
        # Can start as None, in which case reload happens on first access.
        self.current = _cached_database_metadata(initial) if initial else None
        self.pending = None
        self.authenticator = authenticator

    async def get(
        self, now: float | None = None
    ) -> Result[DatabaseMetadata, MetadataExchangeDenoKvError]:
        if now is None:
            now = loop_time()
        current = self.current
        if current is not None and current.is_fresh(now):
            return Ok(current.value)

        pending_task = self.pending
        if pending_task is None:
            self.pending = pending_task = asyncio.create_task(self.reload())
        pending = await pending_task

        # The first caller to await pending can handle swapping pending to current
        if self.pending is pending_task:
            self.pending = None
            # We don't cache error values. We expect that the auth fn called by
            # reload() is doing its own retry and backoff, and the Kv call that
            # triggers get() (e.g. Kv.get()) will also do retry with backoff on
            # retry-able failed auth.
            # TODO: It'd probably make sense for the auth fn to also have a rate
            #   limit on attempts after non-retry-able errors.
            if isinstance(pending, Ok):
                self.current = pending.value

        if isinstance(pending, Err):
            return pending
        return Ok(pending.value.value)

    async def reload(
        self,
    ) -> Result[CachedValue[DatabaseMetadata], MetadataExchangeDenoKvError]:
        result = await self.authenticator()
        if isinstance(result, Err):
            return result
        return Ok(_cached_database_metadata(result.value))

    def purge(self, metadata: DatabaseMetadata) -> None:
        # Only purge the cached version if the purged version is the current,
        # otherwise an async task operating on an expired previous version could
        # incorrectly expire a just-fetched fresh version.
        if self.current and self.current.value is metadata:
            self.current = None


class KvFlags(Flag):
    """Options that can be enabled/disabled to affect [Kv](`denokv.kv.Kv`) behaviour."""

    NoFlag = 0
    IntAsNumber = auto()
    """
    Treat `int` as `float` in KV keys.

    This causes ints to behave like JavaScript number values, as JavaScript uses
    floating-point numbers for integer literals. In JavaScript, Deno KV also
    supports `BigInt` keys. To use Python `int` like JavaScript `BigInt` keys,
    either don't use this flag (and manually convert to float only where
    needed), or pass keys as [KvKey](`denokv.kv.KvKey`) values, which maintain
    `int` values as-is, causing them to act like JavaScript `BigInt`.
    """


DEFAULT_KV_FLAGS: Final = KvFlags.IntAsNumber


@dataclass(init=False, **slots_if310())
class Kv:
    """
    Interface to perform requests against a Deno KV database.

    [DEFAULT_KV_FLAGS]: `denokv.kv.DEFAULT_KV_FLAGS`

    Parameters
    ----------
    flags
        Enable/disable flags that change Kv behaviour. Default: [DEFAULT_KV_FLAGS]
    """

    session: aiohttp.ClientSession
    retry_delays: Backoff
    metadata_cache: DatabaseMetadataCache
    v8_encoder: Encoder
    v8_decoder: Decoder
    flags: KvFlags

    def __init__(
        self,
        session: aiohttp.ClientSession,
        auth: AuthenticatorFn,
        retry: Backoff | None = None,
        v8_encoder: Encoder | None = None,
        v8_decoder: Decoder | None = None,
        flags: KvFlags | None = None,
    ) -> None:
        self.session = session
        self.metadata_cache = DatabaseMetadataCache(authenticator=auth)
        self.retry_delays = ExponentialBackoff() if retry is None else retry
        self.v8_encoder = v8_encoder or create_default_v8_encoder()
        self.v8_decoder = v8_decoder or Decoder()
        self.flags = KvFlags.IntAsNumber if flags is None else flags

    def _prepare_key(self, key: AnyKvKeyT) -> AnyKvKeyT:
        if self.flags & KvFlags.IntAsNumber and not isinstance(key, KvKeyEncodable):
            return normalize_key(key, bigints=False)  # type: ignore[return-value]
        return key

    # get(x), get(x, y), get(keys=[a, b, c])
    @overload
    async def get(
        self,
        /,
        *,
        keys: Iterable[AnyKvKeyT],
        consistency: ConsistencyLevel = ConsistencyLevel.STRONG,
    ) -> tuple[tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None], ...]: ...

    @overload
    async def get(
        self,
        key1: AnyKvKeyT,
        key2: AnyKvKeyT,
        /,
        *keys: AnyKvKeyT,
        consistency: ConsistencyLevel = ConsistencyLevel.STRONG,
    ) -> tuple[tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None], ...]: ...

    @overload
    async def get(
        self,
        key: AnyKvKeyT,
        /,
        *,
        consistency: ConsistencyLevel = ConsistencyLevel.STRONG,
    ) -> tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None]: ...

    async def get(
        self,
        *args: AnyKvKeyT,
        keys: Iterable[AnyKvKeyT] | None = None,
        consistency: ConsistencyLevel = ConsistencyLevel.STRONG,
    ) -> (
        tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None]
        | tuple[tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None], ...]
    ):
        """Get the value of one or more known keys from the database."""
        if keys is not None:
            if len(args) > 0:
                raise TypeError("cannot use positional keys and keys keyword argument")
            args = tuple(keys)
            return_unwrapped = False
        else:
            if len(args) == 0:
                raise TypeError("at least one key argument must be passed")
            return_unwrapped = len(args) == 1

        args = tuple(self._prepare_key(key) for key in args)
        ranges = [read_range_single(key) for key in args]
        snapshot_read_result = await self._snapshot_read(
            dp_protobuf.SnapshotRead(ranges=ranges), consistency=consistency
        )
        if isinstance(snapshot_read_result, Err):
            raise snapshot_read_result.error
        result, endpoint = snapshot_read_result.value

        assert len(args) == len(ranges)

        results: list[tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None]] = []
        decoder = self.v8_decoder
        for key, in_range, range in zip(args, ranges, result.ranges):
            if len(range.values) == 0:
                results.append((key, None))
                continue
            if len(range.values) != 1:
                raise ProtocolViolation(
                    f"Server responded with {len(range.values)} values to "
                    f"read for key {key!r} with limit 1",
                    data=result,
                    endpoint=endpoint,
                )
            raw_kv_entry = range.values[0]
            if raw_kv_entry.key != in_range.start:
                raise ProtocolViolation(
                    f"Server responded to read for exact key "
                    f"{in_range.start!r} with key {raw_kv_entry.key!r}",
                    data=result,
                    endpoint=endpoint,
                )

            parse_result = parse_protobuf_kv_entry(
                raw_kv_entry, v8_decoder=decoder, le64_type=KvU64
            )
            if isinstance(parse_result, Err):
                raise ProtocolViolation(
                    f"Server responded to Data Path request with invalid "
                    f"value: {parse_result.error}",
                    data=raw_kv_entry,
                    endpoint=endpoint,
                ) from parse_result.error
            parsed_key, parsed_value, parsed_versionstamp = parse_result.value

            kv_entry = KvEntry(
                key=key,  # We keep the caller's key which may be a custom type
                value=parsed_value,
                versionstamp=VersionStamp(parsed_versionstamp),
            )
            results.append((key, kv_entry))

        if return_unwrapped:
            assert len(results) == 1
            return results[0]
        return tuple(results)

    async def list(
        self,
        *,
        prefix: AnyKvKeyT | None = None,
        start: AnyKvKeyT | None = None,
        end: AnyKvKeyT | None = None,
        **options: Unpack[KvListOptions],
    ) -> AsyncIterator[ListKvEntry[KvKey]]:
        limit = options.get("limit")
        if limit is not None and limit < 0:
            raise ValueError(f"limit cannot be negative: {limit}")

        batch_size = options.get("batch_size")
        batch_size = min(500, (limit or 100) if batch_size is None else batch_size)
        if batch_size < 1:
            raise ValueError(f"batch_size cannot be < 1: {batch_size}")
        reverse = options.get("reverse") or False
        consistency = options.get("consistency") or ConsistencyLevel.STRONG
        cursor = options.get("cursor")

        prefix = None if prefix is None else self._prepare_key(prefix)
        start = None if start is None else self._prepare_key(start)
        end = None if end is None else self._prepare_key(end)

        read_range = read_range_multi(
            prefix=prefix,
            start=start,
            end=end,
            reverse=reverse,
            limit=batch_size,
        )

        context = ListContext(
            prefix=prefix,
            start=start,
            end=end,
            packed_start=read_range.start,
            packed_end=read_range.end,
            limit=limit,
            cursor=cursor,
            reverse=reverse,
            consistency=consistency,
            batch_size=batch_size,
            cursor_format_type=options.get("cursor_format_type")
            or Base64KeySuffixCursorFormat.from_list_context,
        )

        batch_start: KvKeyTuple | None = None
        if cursor is not None:
            cursor_result = context.cursor_format.get_key_for_cursor(cursor)
            if isinstance(cursor_result, Err):
                raise cursor_result.error
            if not (read_range.start <= pack_key(cursor_result.value) < read_range.end):
                raise InvalidCursor(
                    "cursor is not within the the start and end key range",
                    cursor=cursor,
                )
            batch_start = cursor_result.value

        decoder = self.v8_decoder
        if limit == 0:
            return
        count = 0
        while True:
            if batch_start is not None:
                # With a known limit we can reduce the batch size on the final
                # batch to avoid reading results we can't yield
                count_remaining = None if limit is None else limit - count
                required_batch_size = min(batch_size, count_remaining or batch_size)

                # re-calculate the range to start from the cursor position
                if reverse:
                    # start and end of reversed ranges remain in ascending
                    # order, the order results are returned in is reversed.
                    # So the batch start controls end bound in reverse order.
                    read_range = read_range_multi(
                        prefix=prefix,
                        start=start,
                        end=batch_start,
                        reverse=True,
                        limit=required_batch_size,
                    )
                else:
                    read_range = read_range_multi(
                        prefix=prefix,
                        # The batch_start is the key to start after, unlike the
                        # normal start key which is the (inclusive) key to start
                        # from.
                        exclude_start=True,
                        start=batch_start,
                        end=end,
                        reverse=False,
                        limit=required_batch_size,
                    )

            snapshot_read_result = await self._snapshot_read(
                dp_protobuf.SnapshotRead(ranges=[read_range]), consistency=consistency
            )
            if isinstance(snapshot_read_result, Err):
                raise snapshot_read_result.error
            result, endpoint = snapshot_read_result.value
            if len(result.ranges) != 1:
                raise ProtocolViolation(
                    f"Server responded with {len(result.ranges)} ranges to "
                    f"request for 1 range",
                    data=result,
                    endpoint=endpoint,
                )

            parsed_key: KvKeyTuple | None = None
            (result_range,) = result.ranges
            for raw_kv_entry in result_range.values:
                parse_result = parse_protobuf_kv_entry(
                    raw_kv_entry, v8_decoder=decoder, le64_type=KvU64
                )
                if isinstance(parse_result, Err):
                    raise ProtocolViolation(
                        f"Server responded to Data Path request with invalid "
                        f"value: {parse_result.error}",
                        data=raw_kv_entry,
                        endpoint=endpoint,
                    ) from parse_result.error
                parsed_key, parsed_value, parsed_versionstamp = parse_result.value

                kv_entry = ListKvEntry(
                    key=KvKey.wrap_tuple_keys(parsed_key),
                    value=parsed_value,
                    versionstamp=VersionStamp(parsed_versionstamp),
                    listing=context,
                )
                yield kv_entry

                count += 1
                if limit is not None and count >= limit:
                    return

            # If the read returned less results than the limit, we must have
            # read all the keys that exist within the key's range. Another read
            # would return an empty set.
            if len(result_range.values) < read_range.limit:
                return

            if parsed_key is None:
                assert len(result_range.values) == 0
                return
            batch_start = parsed_key

    async def _snapshot_read(
        self, read: SnapshotRead, *, consistency: ConsistencyLevel
    ) -> _KvSnapshotReadResult:
        return await self._datapath_request(
            partial(datapath.snapshot_read, read=read), consistency=consistency
        )

    async def _atomic_write(self, write: AtomicWrite) -> _KvAtomicWriteResult:
        return await self._datapath_request(
            partial(datapath.atomic_write, write=write),
            consistency=ConsistencyLevel.STRONG,
        )

    async def _datapath_request(
        self,
        datapath_request: partial[Coroutine[Any, Any, Result[T, _DataPathErrorT]]],
        *,
        consistency: ConsistencyLevel,
    ) -> Result[tuple[T, EndpointInfo], _DataPathErrorT]:
        result: Result[T, _DataPathErrorT]
        endpoint: EndpointInfo
        for delay in attempts(self.retry_delays):
            # return error from this?
            cached_meta = await self.metadata_cache.get()
            if isinstance(cached_meta, Err):
                # In the typical case, errors should only propagate from the
                # metadata cache if they're non-retryable, because the
                # metadata-fetching auth function is expected to be doing its
                # own retrying. However it's possible that its retries are
                # exhausted, or its not configured to retry itself, so it seems
                # reasonable to retry retry-able errors.
                if cached_meta.error.retryable:
                    await asyncio.sleep(delay)
                    continue
                raise cached_meta.error
            endpoints = EndpointSelector(meta=cached_meta.value)
            endpoint = endpoints.get_endpoint(consistency)

            result = await datapath_request(
                session=self.session, meta=cached_meta.value, endpoint=endpoint
            )
            if isinstance(result, Err):
                if result.error.auto_retry is AutoRetry.AFTER_BACKOFF:
                    await asyncio.sleep(delay)
                    continue
                elif result.error.auto_retry is AutoRetry.AFTER_METADATA_EXCHANGE:
                    self.metadata_cache.purge(cached_meta.value)
                    continue
                assert result.error.auto_retry is AutoRetry.NEVER
                return result
            break
        else:
            assert isinstance(result, Err)
            return result
        assert isinstance(result, Ok)
        return Ok((result.value, endpoint))

    def atomic(self, *operations: WriteOperation) -> PlannedWrite:
        write = PlannedWrite(kv=self)
        for op in operations:
            if isinstance(op, Check):
                write.check(op)
            elif isinstance(op, Mutation):
                write.mutate(op)
            else:
                assert isinstance(op, Enqueue)
                write.enqueue(op)
        return write

    @overload
    async def write(self, *operations: WriteOperation) -> CompletedWrite: ...

    @overload
    async def write(self, planned_write: PlannedWrite, /) -> CompletedWrite: ...

    async def write(
        self, arg: PlannedWrite | WriteOperation | None = None, *args: WriteOperation
    ) -> CompletedWrite:
        if arg is None:
            if args:
                raise TypeError("arguments cannot be None")
            # One None arg means 0 args were passed
            planned_write = PlannedWrite()
        elif isinstance(arg, PlannedWrite):
            planned_write = arg
            if args:
                raise TypeError("unexpected arguments after PlannedWrite")
        else:
            planned_write = self.atomic(arg, *args)

        # Note that it's OK to submit a write with no operations. We get a
        # versionstamp back. Submitting a write with only checks could be used
        # to check if a key has been changed without reading the value.
        result = await self._atomic_write(
            planned_write.as_protobuf(v8_encoder=self.v8_encoder)
        )

        concrete_checks = [
            Check(key=key_ver.key, versionstamp=key_ver.versionstamp)
            for key_ver in planned_write.checks
        ]

        if isinstance(result, Err):
            if isinstance(result.error, CheckFailure):
                check_failure = result.error
                return ConflictedWrite(
                    failed_checks=list(check_failure.failed_check_indexes),
                    checks=concrete_checks,
                    mutations=planned_write.mutations,
                    enqueues=planned_write.enqueues,
                )
            raise result.error

        raw_versionstamp, endpoint = result.value
        return CommittedWrite(
            versionstamp=VersionStamp(raw_versionstamp),
            checks=concrete_checks,
            mutations=planned_write.mutations,
            enqueues=planned_write.enqueues,
        )


_KvSnapshotReadResult: TypeAlias = (
    "Result[tuple[SnapshotReadOutput, EndpointInfo], DataPathError]"
)
_KvAtomicWriteResult: TypeAlias = (
    "Result[tuple[bytes, EndpointInfo], CheckFailure | DataPathError]"
)


@dataclass(frozen=True)
class ListContext:
    prefix: AnyKvKey | None
    start: AnyKvKey | None
    end: AnyKvKey | None
    packed_start: bytes
    packed_end: bytes
    limit: int | None
    cursor: str | None
    reverse: bool
    consistency: ConsistencyLevel
    batch_size: int
    cursor_format_type: Callable[[ListContext], AnyCursorFormat] = field(
        repr=False,
        compare=False,
    )
    cursor_format: AnyCursorFormat = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "cursor_format", self.cursor_format_type(self))


class AnyCursorFormat(Protocol):
    def get_key_for_cursor(self, cursor: str) -> Result[KvKeyTuple, InvalidCursor]: ...

    def get_cursor_for_key(self, key: AnyKvKey) -> Result[str, ValueError]: ...


@dataclass(frozen=True)
class Base64KeySuffixCursorFormat(AnyCursorFormat):
    r"""
    A cursor format that encodes keys as URL-safe base64.

    Packed key bytes that are common to both the start and end key range are
    omitted from the encoded cursor values. This matches the behaviour of Deno's
    built-in KV.list() cursors.

    Examples
    --------
    >>> from base64 import b64decode
    >>> from denokv.datapath import pack_key_range

    >>> start, end = pack_key_range(prefix=('foo',))
    >>> cf = Base64KeySuffixCursorFormat(packed_start=start, packed_end=end)

    The cursor encodes just the part of the key that isn't in both start and
    end.

    >>> cf.get_cursor_for_key(('foo', 'EXAMPLE'))
    Ok('AkVYQU1QTEUA')
    >>> b64decode('AkVYQU1QTEUA')
    b'\x02EXAMPLE\x00'
    """

    packed_start: bytes
    packed_end: bytes
    redundant_prefix_length: int = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "redundant_prefix_length",
            _common_prefix_length(self.packed_start, self.packed_end),
        )

    @classmethod
    def from_list_context(cls, list_context: ListContext) -> Self:
        return cls(
            packed_start=list_context.packed_start, packed_end=list_context.packed_end
        )

    def get_key_for_cursor(self, cursor: str) -> Result[KvKeyTuple, InvalidCursor]:
        try:
            significant_packed_key_bytes = urlsafe_b64decode(cursor)
        except Exception as e:
            err = InvalidCursor("cursor is not valid URL-safe base64", cursor=cursor)
            err.__cause__ = e
            return Err(err)

        packed_key = (
            self.packed_start[: self.redundant_prefix_length]
            + significant_packed_key_bytes
        )

        if not (self.packed_start <= packed_key < self.packed_end):
            return Err(
                InvalidCursor(
                    "cursor is not within the the start and end key range",
                    cursor=cursor,
                )
            )

        try:
            unpacked_key = unpack(packed_key)
            if not is_kv_key_tuple(unpacked_key):
                return Err(
                    InvalidCursor("cursor contains invalid part types", cursor=cursor)
                )
            return Ok(unpacked_key)
        except Exception as e:
            err = InvalidCursor(
                "cursor is not a valid suffix for the start and end keys", cursor=cursor
            )
            err.__cause__ = e
            return Err(err)

    def get_cursor_for_key(self, key: AnyKvKey) -> Result[str, ValueError]:
        packed_key = pack_key(key)
        if not (self.packed_start <= packed_key < self.packed_end):
            return Err(ValueError("key is not within the start and end keys"))

        significant_packed_key_bytes = packed_key[self.redundant_prefix_length :]
        return Ok(urlsafe_b64encode(significant_packed_key_bytes).decode())


def _common_prefix_length(a: Sequence[object], b: Sequence[object]) -> int:
    """
    Get the number of elements that are the same from the start of two sequences.

    >>> _common_prefix_length('abc', 'abd')
    2
    >>> _common_prefix_length('abc', 'xyz')
    0
    >>> _common_prefix_length('abc', 'abc')
    3
    >>> _common_prefix_length('', '')
    0
    """
    match_length = 0
    for match_length, (_a, _b) in enumerate(zip(a, b), start=1):
        if _a != _b:
            match_length -= 1
            break
    return match_length


async def open_kv(
    target: URL | str | KvCredentials,
    *,
    access_token: str | None = None,
    session: aiohttp.ClientSession | None = None,
    flags: KvFlags | None = None,
) -> Kv:
    """
    Create a connection to a KV database.

    [yarl]: `yarl.URL`
    [DEFAULT_KV_FLAGS]: `denokv.kv.DEFAULT_KV_FLAGS`

    Parameters
    ----------
    target
        The Deno KV database server to connect to. Can be a string or [yarl] URL.
    access_token
        The secret access token to authenticate to the target database with.
        Default: The environment variable `DENO_KV_ACCESS_TOKEN` is read.
    session
        The HTTP client session to use to communicate with the database.
        Default: A new session is created.
    flags
        Enable/disable flags that change Kv behaviour. Default: [DEFAULT_KV_FLAGS]

    Notes
    -----
    Although this not an async function, it must be run in the context of an
    asyncio event loop when `session` is not provided, because creating a
    aiohttp.ClientSession requires a loop.
    """
    if isinstance(target, str):
        try:
            target = URL(target)
        except ValueError as e:
            raise ValueError(
                f"Cannot open KV database: target argument str is not a "
                f"valid URL: {e}"
            ) from e
    if isinstance(target, URL):
        if access_token is None:
            access_token = environ.get("DENO_KV_ACCESS_TOKEN") or None
        if access_token is None:
            raise ValueError(
                "Cannot open KV database: access_token argument is None and "
                "DENO_KV_ACCESS_TOKEN environment variable is not set"
            )

        target = KvCredentials(server_url=target, access_token=access_token)

    session = session or aiohttp.ClientSession()
    retry = ExponentialBackoff()
    auth = Authenticator(session=session, retry_delays=retry, credentials=target)
    return Kv(session=session, auth=auth, retry=retry, flags=flags)
