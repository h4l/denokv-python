from __future__ import annotations

import asyncio
import time
from binascii import unhexlify
from dataclasses import dataclass
from enum import Flag
from enum import auto
from typing import TYPE_CHECKING
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import Final
from typing import Generic
from typing import Iterable
from typing import Self
from typing import Sequence
from typing import TypedDict
from typing import overload

import aiohttp
from fdb.tuple import pack
from v8serialize import Decoder

from denokv import datapath
from denokv._datapath_pb2 import KvEntry as RawKvEntry
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import ValueEncoding
from denokv.auth import ConsistencyLevel
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.auth import MetadataExchangeDenoKvError
from denokv.backoff import Backoff
from denokv.backoff import ExponentialBackoff
from denokv.backoff import attempts
from denokv.datapath import KV_KEY_PIECE_TYPES
from denokv.datapath import AnyKvKeyT
from denokv.datapath import AutoRetry
from denokv.datapath import CreateKvEntryFn
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyPieceT
from denokv.datapath import KvKeyTuple
from denokv.datapath import KvKeyTupleT
from denokv.datapath import ProtocolViolation
from denokv.datapath import SnapshotReadResult
from denokv.datapath import is_kv_key_tuple
from denokv.datapath import parse_protobuf_kv_entry
from denokv.datapath import read_range_single
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result

if TYPE_CHECKING:
    from typing_extensions import TypeVar
    from typing_extensions import Unpack

    T = TypeVar("T", default=object)
else:
    from typing import TypeVar

    T = TypeVar("T")

SAFE_FLOAT_INT_RANGE: Final = range(-(2**53 - 1), 2**53)  # 2**53 - 1 is max safe


class KvListOptions(TypedDict):
    """Keyword arguments of `Kv.list()`."""

    limit: int
    cursor: str
    reverse: bool  # order asc/desc?
    consistency: ConsistencyLevel
    batchSize: int


@dataclass(slots=True, frozen=True)
class KvEntry(Generic[AnyKvKeyT, T]):
    """A value read from the Deno KV database, along with its key and version."""

    key: AnyKvKeyT
    value: T
    versionstamp: VersionStamp


def _create_kv_entry(
    key: AnyKvKeyT,
    value: object,
    versionstamp: bytes,
    *,
    raw: RawKvEntry,
) -> KvEntry[AnyKvKeyT, object]:
    # Wrap VE_LE64 values with the KvU64 marker type so that they get re-encoded
    # as VE_LE64 rather than using V8 serialization if they're written back.
    if raw.encoding == ValueEncoding.VE_LE64:
        assert isinstance(value, int)
        value = KvU64(value)

    # Wrap plain tuple keys as KvKey so that we preserve int as int if a key
    # read from the DB is passed back into a Kv method which is normalising int
    # to float for JS compatibility.
    if not isinstance(key, KvKeyEncodable):
        # The type signature is not quite accurate as we convert raw key tuples
        # to our KvKey tuple subclass, but this case is only used internally in
        # situations like listing keys where we don't have a potential custom
        # key type to preserve.
        key = KvKey(*key)  # type: ignore[misc,assignment]
    return KvEntry(key=key, value=value, versionstamp=VersionStamp(versionstamp))


if TYPE_CHECKING:
    ___create_kv_entry: CreateKvEntryFn[KvEntry[KvKey, object], KvKey] = (
        _create_kv_entry
    )


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

    def __str__(self) -> str:
        return self.hex()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({str(self)!r})"


@dataclass(slots=True, frozen=True)
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


class KvKey(KvKeyEncodable, tuple[KvKeyPieceT]):
    def __new__(cls, *pieces: KvKeyPiece) -> Self:
        if not is_kv_key_tuple(pieces):
            raise TypeError(
                f"key contains types other than "
                f"{', '.join(t.__name__ for t in KV_KEY_PIECE_TYPES)}: {pieces!r}"
            )
        return tuple.__new__(cls, pieces)

    def kv_key_bytes(self) -> bytes:
        return pack(self)


@dataclass(slots=True, frozen=True)
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


@dataclass(slots=True, frozen=True)
class CachedValue(Generic[T]):
    fresh_until: float
    value: T

    def is_fresh(self, time: float) -> bool:
        return time < self.fresh_until


AuthenticatorFn = Callable[
    [], Awaitable[Result[DatabaseMetadata, MetadataExchangeDenoKvError]]
]
"""
The type of a function that connects to a KV database and returns metadata.

The metadata contains the server URLs and access tokens needed to query the KV
database.
"""

# get_database_metadata with retry:
# TODO: have auth module handle this
#
# async def reload(
#         self,
#     ) -> Result[CachedValue[DatabaseMetadata], MetadataExchangeDenoKvError]:
#     result: DatabaseMetadata | MetadataExchangeDenoKvError
#     for delay in attempts(self.backoff):
#         result = await get_database_metadata()
#         if isinstance(result, MetadataExchangeDenoKvError):
#             if result.retryable:
#                 await asyncio.sleep(delay)
#                 continue
#             return Err(result)
#         else:
#             return Ok(CachedValue(result.expires_at.timestamp(), value=result))
#     else:  # backoff timed out
#         assert isinstance(result, MetadataExchangeDenoKvError)
#         return Err(result)


def _cached_database_metadata(value: DatabaseMetadata) -> CachedValue[DatabaseMetadata]:
    return CachedValue(value.expires_at.timestamp(), value=value)


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
    get_database_metadata: AuthenticatorFn
    current: CachedValue[DatabaseMetadata] | None
    pending: (
        asyncio.Task[Result[CachedValue[DatabaseMetadata], MetadataExchangeDenoKvError]]
        | None
    )

    def __init__(
        self,
        *,
        initial: DatabaseMetadata | None = None,
        get_database_metadata: AuthenticatorFn,
    ) -> None:
        # Can start as None, in which case reload happens on first access.
        self.current = _cached_database_metadata(initial) if initial else None
        self.pending = None
        self.get_database_metadata = get_database_metadata

    async def get(
        self, now: float | None = None
    ) -> Result[DatabaseMetadata, MetadataExchangeDenoKvError]:
        if now is None:
            now = time.time()
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
        result = await self.get_database_metadata()
        if isinstance(result, Err):
            return result
        return Ok(_cached_database_metadata(result.value))

    def purge(self, metadata: DatabaseMetadata) -> None:
        # Only purge the cached version if the purged version is the current,
        # otherwise an async task operating on an expired previous version could
        # incorrectly expire a just-fetched fresh version.
        if self.current and self.current.value is metadata:
            self.current = None


class KVFlags(Flag):
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


DEFAULT_KV_FLAGS: Final = KVFlags.IntAsNumber


@dataclass(slots=True, init=False)
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
    v8_decoder: Decoder
    flags: KVFlags

    def __init__(
        self,
        session: aiohttp.ClientSession,
        auth: AuthenticatorFn,
        retry: Backoff | None = None,
        v8_decoder: Decoder | None = None,
        flags: KVFlags | None = None,
    ) -> None:
        self.session = session
        self.metadata_cache = DatabaseMetadataCache(get_database_metadata=auth)
        self.retry_delays = ExponentialBackoff() if retry is None else retry
        self.v8_decoder = v8_decoder or Decoder()
        self.flags = KVFlags.IntAsNumber if flags is None else flags

    def _prepare_key(self, key: AnyKvKeyT) -> AnyKvKeyT:
        if self.flags & KVFlags.IntAsNumber and not isinstance(key, KvKeyEncodable):
            return normalize_key(key, bigints=False)  # type: ignore[return-value,arg-type]
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
        read = SnapshotRead(ranges=[read_range_single(key) for key in args])
        result: SnapshotReadResult
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

            result = await datapath.snapshot_read(
                session=self.session,
                meta=cached_meta.value,
                endpoint=endpoint,
                read=read,
            )
            if isinstance(result, Err):
                if result.error.auto_retry is AutoRetry.AFTER_BACKOFF:
                    await asyncio.sleep(delay)
                    continue
                elif result.error.auto_retry is AutoRetry.AFTER_METADATA_EXCHANGE:
                    self.metadata_cache.purge(cached_meta.value)
                    continue
                assert result.error.auto_retry is AutoRetry.NEVER
                raise result.error
            break
        else:
            assert isinstance(result, Err)
            raise result.error

        assert len(args) == len(read.ranges)

        results: list[tuple[AnyKvKeyT, KvEntry[AnyKvKeyT] | None]] = []
        decoder = self.v8_decoder
        for key, in_range, range in zip(args, read.ranges, result.value.ranges):
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
            value = range.values[0]
            if value.key != in_range.start:
                raise ProtocolViolation(
                    f"Server responded to read for exact key "
                    f"{in_range.start!r} with key {value.key!r}",
                    data=result,
                    endpoint=endpoint,
                )

            value_result = parse_protobuf_kv_entry(
                value, decoder, _create_kv_entry, preserve_key=key
            )
            if isinstance(value_result, Err):
                raise ProtocolViolation(
                    f"Server responded to Data Path request with invalid "
                    f"value: {value_result.error}",
                    data=value,
                    endpoint=endpoint,
                ) from value_result.error
            results.append((key, value_result.value))

        if return_unwrapped:
            assert len(results) == 1
            return results[0]
        return tuple(results)

    @overload
    async def list(
        self,
        *,
        start: KvKey,
        end: KvKey,
        prefix: None = None,
        **options: Unpack[KvListOptions],
    ) -> AsyncIterator[tuple[KvKeyTupleT, KvEntry | None]]: ...

    @overload
    async def list(
        self,
        *,
        prefix: KvKey,
        start: None = None,
        end: None = None,
        **options: Unpack[KvListOptions],
    ) -> AsyncIterator[tuple[KvKeyTupleT, KvEntry | None]]: ...

    @overload
    async def list(
        self,
        *,
        prefix: KvKey,
        end: KvKey,
        start: None = None,
        **options: Unpack[KvListOptions],
    ) -> AsyncIterator[tuple[KvKeyTupleT, KvEntry | None]]: ...

    @overload
    async def list(
        self,
        *,
        prefix: KvKey,
        start: KvKey,
        end: None = None,
        **options: Unpack[KvListOptions],
    ) -> AsyncIterator[tuple[KvKeyTupleT, KvEntry | None]]: ...

    async def list(
        self,
        *,
        prefix: KvKey | None = None,
        start: KvKey | None = None,
        end: KvKey | None = None,
        **options: Unpack[KvListOptions],
    ) -> AsyncIterator[tuple[KvKeyTupleT, KvEntry | None]]:
        raise NotImplementedError
