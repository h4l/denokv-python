from __future__ import annotations

import asyncio
import weakref
from base64 import urlsafe_b64decode
from base64 import urlsafe_b64encode
from binascii import unhexlify
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from dataclasses import field
from enum import Flag
from enum import auto
from os import environ
from types import TracebackType
from typing import TYPE_CHECKING
from typing import AsyncIterator
from typing import Awaitable
from typing import Callable
from typing import ClassVar
from typing import Final
from typing import Generic
from typing import Iterable
from typing import Literal
from typing import Protocol
from typing import Sequence
from typing import TypedDict
from typing import overload

import aiohttp
from fdb.tuple import unpack
from v8serialize import Decoder
from yarl import URL

from denokv import datapath
from denokv._datapath_pb2 import ReadRange
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.typing import override
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
from denokv.datapath import DataPathError
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyTuple
from denokv.datapath import ProtocolViolation
from denokv.datapath import SnapshotReadResult
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

SAFE_FLOAT_INT_RANGE: Final = range(-(2**53 - 1), 2**53)  # 2**53 - 1 is max safe

CursorFormatType: TypeAlias = Callable[["ListContext"], "AnyCursorFormat"]


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


@dataclass(init=False)
class Kv(AbstractAsyncContextManager["Kv", None]):
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
    flags: KvFlags

    def __init__(
        self,
        session: aiohttp.ClientSession,
        auth: AuthenticatorFn,
        retry: Backoff | None = None,
        v8_decoder: Decoder | None = None,
        flags: KvFlags | None = None,
    ) -> None:
        self.session = session
        self.metadata_cache = DatabaseMetadataCache(authenticator=auth)
        self.retry_delays = ExponentialBackoff() if retry is None else retry
        self.v8_decoder = v8_decoder or Decoder()
        self.flags = KvFlags.IntAsNumber if flags is None else flags

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
        /,
    ) -> None:
        await self.aclose()

    @property
    def closed(self) -> bool:
        return self.session.closed

    async def aclose(self) -> None:
        if self.closed:
            return
        await self._aclose(self.session)

    @classmethod
    async def _aclose(cls, session: aiohttp.ClientSession) -> None:
        await session.close()

    @classmethod
    def _finalize(cls, session: aiohttp.ClientSession) -> None | asyncio.Future[None]:
        if session._loop.is_running():
            return session._loop.create_task(
                cls._aclose(session), name="denokv.Kv.create_finalizer"
            )
        else:
            return session._loop.run_until_complete(cls._aclose(session))

    def create_finalizer(self) -> weakref.finalize:
        """
        Automatically close the instance when it goes out of scope, or at exit.

        This creates a Finalizer (`weakref.finalize`) that closes the Kv
        instance automatically when garbage collected, or when Python exits.

        If the event loop of the Kv's session is running, the finalizer returns,
        an `asyncio.Task` that closes the Kv instance. Otherwise it runs the
        session's loop to close it and returns None after it's closed.

        Notes
        -----
        It's recommended to close Kv instances explicitly using async context
        manager blocks, but Finalizers can be used in situations where a context
        manager is not practical, like in an interactive environment.

        `open_kv()` automatically creates a Finalizer in interactive sessions.
        """
        return weakref.finalize(self, self._finalize, self.session)

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
            ranges, consistency=consistency
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
                ranges=[read_range], consistency=consistency
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
        self, ranges: Sequence[ReadRange], *, consistency: ConsistencyLevel
    ) -> _KvSnapshotReadResult:
        read = SnapshotRead(ranges=ranges)
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
                return result
            break
        else:
            assert isinstance(result, Err)
            return result
        assert isinstance(result, Ok)
        return Ok((result.value, endpoint))


_KvSnapshotReadResult: TypeAlias = (
    "Result[tuple[SnapshotReadOutput, EndpointInfo], DataPathError]"
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


def _is_python_running_in_interactive_environment() -> bool:
    import sys

    # sys.ps1 is only set in interactive environments:
    #   https://stackoverflow.com/a/64523765/693728
    return hasattr(sys, "ps1")


OpenKvFinalize: TypeAlias = Literal[True, False, "interactive"]


async def open_kv(
    target: URL | str | KvCredentials,
    *,
    access_token: str | None = None,
    session: aiohttp.ClientSession | None = None,
    flags: KvFlags | None = None,
    finalize: OpenKvFinalize | None = None,
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
    finalize
        Whether to create a finalizer to automatically close the Kv instance at
        exit, or when out of scope. If set to 'interactive', a finalizer is
        created only if Python is running as an interactive session.
        Default: ['interactive']

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
    if finalize not in (True, False, "interactive", None):
        raise ValueError("finalize must be True, False, None or 'interactive'")
    finalize = "interactive" if finalize is None else finalize

    session = session or aiohttp.ClientSession()
    retry = ExponentialBackoff()
    auth = Authenticator(session=session, retry_delays=retry, credentials=target)
    kv = Kv(session=session, auth=auth, retry=retry, flags=flags)
    if finalize is True or (
        finalize == "interactive" and _is_python_running_in_interactive_environment()
    ):
        kv.create_finalizer()
    return kv
