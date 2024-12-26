"""The Deno KV [Data Path Protocol].

[Data Path Protocol]: https://github.com/denoland/denokv/blob/main/proto/kv-connect.md#data-path-protocol
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import Enum
from enum import auto
from typing import TYPE_CHECKING
from typing import Awaitable
from typing import Callable
from typing import Container
from typing import Final
from typing import Protocol
from typing import Type
from typing import TypedDict
from typing import Union
from typing import overload
from typing import runtime_checkable

import aiohttp
import aiohttp.client_exceptions
from fdb.tuple import pack
from fdb.tuple import unpack
from google.protobuf.message import Error as ProtobufMessageError
from v8serialize import Decoder

from denokv._datapath_pb2 import KvEntry
from denokv._datapath_pb2 import ReadRange
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv._datapath_pb2 import SnapshotReadStatus
from denokv._datapath_pb2 import ValueEncoding
from denokv.auth import ConsistencyLevel
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.errors import DenoKvError
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result

KV_KEY_PIECE_TYPES: Final = (str, bytes, int, float, bool)


if TYPE_CHECKING:
    from typing_extensions import TypeAlias
    from typing_extensions import TypeGuard
    from typing_extensions import TypeVar
    from typing_extensions import Unpack

    KvKeyPiece: TypeAlias = Union[str, bytes, int, float, bool]
    KvKeyPieceT = TypeVar("KvKeyPieceT", bound=KvKeyPiece, default=KvKeyPiece)

    KvKeyTuple: TypeAlias = tuple[KvKeyPieceT, ...]
    KvKeyTupleT = TypeVar("KvKeyTupleT", bound=KvKeyTuple, default=KvKeyTuple)
    KvKeyTupleT_co = TypeVar(
        "KvKeyTupleT_co", bound=KvKeyTuple, default=KvKeyTuple, covariant=True
    )

    @runtime_checkable
    class KvKeyEncodable(Protocol):
        __slots__ = ()

        def kv_key_bytes(self) -> bytes: ...

    KvKeyEncodableT = TypeVar("KvKeyEncodableT", bound=KvKeyEncodable)
    AnyKvKey: TypeAlias = "KvKeyEncodable | KvKeyTuple"
    AnyKvKeyT = TypeVar("AnyKvKeyT", bound=AnyKvKey, default=AnyKvKey)
    AnyKvKeyT_co = TypeVar(
        "AnyKvKeyT_co", bound=AnyKvKey, default=AnyKvKey, covariant=True
    )
    AnyKvKeyT_con = TypeVar(
        "AnyKvKeyT_con", bound=AnyKvKey, default=AnyKvKey, contravariant=True
    )
else:
    from typing import TypeVar

    KvKeyPiece: TypeAlias = "str | bytes | int | float | bool"
    KvKeyPieceT = TypeVar("KvKeyPieceT", bound=KvKeyPiece)

    KvKeyTuple: TypeAlias = tuple[KvKeyPieceT, ...]
    KvKeyTupleT = TypeVar("KvKeyTupleT", bound=KvKeyTuple)
    KvKeyTupleT_co = TypeVar("KvKeyTupleT_co", bound=KvKeyTuple, covariant=True)

    @runtime_checkable
    class KvKeyEncodable(Protocol):
        def kv_key_bytes(self) -> bytes: ...

    KvKeyEncodableT = TypeVar("KvKeyEncodableT", bound=KvKeyEncodable)
    AnyKvKey: TypeAlias = "KvKeyEncodable | KvKeyTuple"
    AnyKvKeyT = TypeVar("AnyKvKeyT", bound=AnyKvKey)
    AnyKvKeyT_co = TypeVar("AnyKvKeyT", bound=AnyKvKey, covariant=True)
    AnyKvKeyT_con = TypeVar("AnyKvKeyT_con", bound=AnyKvKey, contravariant=True)

_T = TypeVar("_T")


@runtime_checkable
class KvKeyRangeEncodable(Container[AnyKvKey], Protocol):
    def kv_key_range_bytes(self) -> tuple[bytes, bytes]: ...


_LE64 = struct.Struct("<Q")
"""Little-endian 64-bit unsigned int format."""
assert _LE64.size == 8


class AutoRetry(Enum):
    NEVER = auto()
    AFTER_BACKOFF = auto()
    AFTER_METADATA_EXCHANGE = auto()


@dataclass(init=False)
class DataPathDenoKvError(DenoKvError):
    endpoint: EndpointInfo
    auto_retry: AutoRetry

    def __init__(
        self, message: str, *args: object, endpoint: EndpointInfo, auto_retry: AutoRetry
    ) -> None:
        self.endpoint = endpoint
        self.auto_retry = auto_retry
        super().__init__(message, *args)


class EndpointNotUsableReason(Enum):
    DISABLED = auto()
    CONSISTENCY_CHANGED = auto()


@dataclass(init=False)
class EndpointNotUsable(DataPathDenoKvError):
    """A Data Path endpoint is no longer able to fulfil our requests."""

    reason: EndpointNotUsableReason
    """The reason the endpoint is not usable."""

    def __init__(
        self,
        message: str,
        *args: object,
        endpoint: EndpointInfo,
        reason: EndpointNotUsableReason,
    ) -> None:
        self.reason = reason
        auto_retry = AutoRetry.AFTER_METADATA_EXCHANGE
        super().__init__(message, *args, endpoint=endpoint, auto_retry=auto_retry)


@dataclass(init=False)
class ProtocolViolation(DataPathDenoKvError):
    """Data Protocol response Data received from the KV server is not valid."""

    data: object | DatabaseMetadata
    """The invalid protobuf data, or a parsed-but-invalid data."""

    def __init__(
        self,
        message: str,
        *args: object,
        data: object | DatabaseMetadata,
        endpoint: EndpointInfo,
    ) -> None:
        self.data = data
        super().__init__(message, *args, endpoint=endpoint, auto_retry=AutoRetry.NEVER)


@dataclass(init=False)
class ResponseUnsuccessful(DataPathDenoKvError):
    """The KV server responded to the Data Path request unsuccessfully."""

    status: int
    body_text: str

    def __init__(
        self,
        message: str,
        status: int,
        body_text: str,
        *args: object,
        endpoint: EndpointInfo,
        auto_retry: AutoRetry,
    ) -> None:
        super().__init__(message, *args, endpoint=endpoint, auto_retry=auto_retry)
        self.status = status
        self.body_text = body_text


class RequestUnsuccessful(DataPathDenoKvError):
    """Unable to make a Data Path request to the KV server."""

    pass


DataPathError: TypeAlias = (
    "EndpointNotUsable | RequestUnsuccessful | ResponseUnsuccessful | ProtocolViolation"
)


class _DataPathRequestKind(Enum):
    SnapshotRead = "snapshot_read"
    SnapshotWrite = "snapshot_write"
    Watch = "watch"


async def _datapath_request(
    *,
    kind: _DataPathRequestKind,
    session: aiohttp.ClientSession,
    meta: DatabaseMetadata,
    endpoint: EndpointInfo,
    request_body: bytes,
    handle_response: Callable[[aiohttp.ClientResponse], Awaitable[_T]],
) -> _T | Err[DataPathError]:
    url = endpoint.url / kind.value
    try:
        db_id_header = (
            "x-transaction-domain-id" if meta.version == 1 else "x-denokv-database-id"
        )
        response = await session.post(
            url,
            data=request_body,
            allow_redirects=False,
            headers={
                "authorization": f"Bearer {meta.token}",
                "content-type": "application/x-protobuf",
                db_id_header: str(meta.database_id),
                "x-denokv-version": str(meta.version),
            },
        )
        if not response.status == 200:  # must be 200, not just 2xx.
            body_text = await response.text()
            if 400 <= response.status < 500:
                return Err(
                    ResponseUnsuccessful(
                        "Server rejected Data Path request indicating client error",
                        status=response.status,
                        body_text=body_text,
                        endpoint=endpoint,
                        auto_retry=AutoRetry.NEVER,
                    )
                )
            elif 500 <= response.status < 600:
                return Err(
                    ResponseUnsuccessful(
                        "Server failed to respond to Data Path request "
                        "indicating server error",
                        status=response.status,
                        body_text=body_text,
                        endpoint=endpoint,
                        auto_retry=AutoRetry.AFTER_BACKOFF,
                    )
                )
            else:
                return Err(
                    ResponseUnsuccessful(
                        "Server responded to Data Path request with "
                        "unexpected HTTP status",
                        status=response.status,
                        body_text=body_text
                        if response.content_type.startswith("text/")
                        else f"Response content-type: {response.content_type}",
                        endpoint=endpoint,
                        auto_retry=AutoRetry.NEVER,
                    )
                )

        content_type = response.headers.get("content-type")
        if content_type != "application/x-protobuf":
            e1 = ProtocolViolation(
                f"response content-type is not application/x-protobuf: "
                f"{content_type}",
                data=content_type,
                endpoint=endpoint,
            )
            return Err(e1)

        return await handle_response(response)
    except aiohttp.client_exceptions.ClientError as e:
        auto_retry = (
            AutoRetry.NEVER
            if isinstance(e, aiohttp.InvalidURL)
            else AutoRetry.AFTER_BACKOFF
        )
        e2 = RequestUnsuccessful(
            "Failed to make Data Path HTTP request to KV server",
            endpoint=endpoint,
            auto_retry=auto_retry,
        )
        e2.__cause__ = e
        return Err(e2)


async def _response_body_bytes(response: aiohttp.ClientResponse) -> Ok[bytes]:
    async with response:
        return Ok(await response.read())


SnapshotReadResult: TypeAlias = "Result[SnapshotReadOutput, DataPathError]"


async def snapshot_read(
    *,
    session: aiohttp.ClientSession,
    meta: DatabaseMetadata,
    endpoint: EndpointInfo,
    read: SnapshotRead,
) -> SnapshotReadResult:
    """
    Perform a Data Path snapshot_read request against a database endpoint.

    The request does not retry on error conditions, the caller is responsible
    for retrying if they wish. The Err results report whether retries are
    permitted by the Data Path protocol spec using their `auto_retry: AutoRetry`
    field.
    """
    result = await _datapath_request(
        kind=_DataPathRequestKind.SnapshotRead,
        session=session,
        meta=meta,
        endpoint=endpoint,
        request_body=read.SerializeToString(),
        handle_response=_response_body_bytes,
    )
    if isinstance(result, Err):
        return result
    response_bytes = result.value

    try:
        read_output = SnapshotReadOutput.FromString(response_bytes)
    except ProtobufMessageError as e:
        err = ProtocolViolation(
            "Server responded to Data Path request with invalid SnapshotReadOutput",
            data=response_bytes,
            endpoint=endpoint,
        )
        err.__cause__ = e
        return Err(err)
    if (
        read_output.read_disabled
        or read_output.status == SnapshotReadStatus.SR_READ_DISABLED
    ):
        return Err(
            EndpointNotUsable(
                "Server responded to Data Path request indicating it is disabled",
                endpoint=endpoint,
                reason=EndpointNotUsableReason.DISABLED,
            )
        )
    # Version 3 introduced the status field and it must be set to SR_SUCCESS
    if meta.version >= 3 and read_output.status != SnapshotReadStatus.SR_SUCCESS:
        try:
            status_desc = SnapshotReadStatus.Name(read_output.status)  # type: ignore[attr-defined]
        except Exception:
            status_desc = str(read_output.status)

        return Err(
            ProtocolViolation(
                f"v{meta.version} server responded to Data Path request with "
                f"status {status_desc}",
                data=read_output,
                endpoint=endpoint,
            )
        )

    if (
        endpoint.consistency is ConsistencyLevel.STRONG
        and not read_output.read_is_strongly_consistent
    ):
        # Server configuration has changed since our metadata was fetched. We
        # must stop using the server and re-fetch metadata to find new servers.
        return Err(
            EndpointNotUsable(
                "Server expected to be strongly-consistent responded to Data "
                "Path request with a non-strongly-consistent read",
                endpoint=endpoint,
                reason=EndpointNotUsableReason.CONSISTENCY_CHANGED,
            )
        )

    if len(read_output.ranges) != len(read.ranges):
        return Err(
            ProtocolViolation(
                f"Server responded to request with {len(read.ranges)} ranges "
                f"with {len(read_output.ranges)} ranges",
                data=read_output,
                endpoint=endpoint,
            )
        )

    return Ok(read_output)


def is_kv_key_tuple(tup: object) -> TypeGuard[KvKeyTuple]:
    """Check if a tuple only contains valid KV key tuple type values."""
    return isinstance(tup, tuple) and all(
        isinstance(part, KV_KEY_PIECE_TYPES) for part in tup
    )


def is_any_kv_key(obj: object) -> TypeGuard[AnyKvKey]:
    """Check if an object is an AnyKvKey type."""
    return isinstance(obj, KvKeyEncodable) or is_kv_key_tuple(obj)


def parse_protobuf_kv_entry(
    raw: KvEntry,
    *,
    v8_decoder: Decoder,
    le64_type: Callable[[int], object] = int,
) -> Result[tuple[KvKeyTuple, bytes | int | object, bytes], ValueError]:
    """
    Validate & decode the raw bytes of a protobuf KvEntry.

    If `preserve_key` is provided, it's passed to `create_kv_entry` instead of the key
    decoded from the `raw` `KvEntry`.

    Returns
    -------
    :
        A Result with Ok being tuple of (key, value, versionstamp) and Err
        being a ValueError.
    """
    try:
        key = unpack(raw.key)
    except Exception as e:
        err = ValueError(f"Invalid encoded key tuple: {raw.key!r}")
        err.__cause__ = e
        return Err(err)
    if not is_kv_key_tuple(key):
        return Err(ValueError(f"Key tuple contains invalid part type: {key!r}"))
    if len(raw.versionstamp) != 10:
        return Err(
            ValueError(f"versionstamp is not an 80-bit value: {raw.versionstamp!r}")
        )
    value: bytes | int | object
    if raw.encoding == ValueEncoding.VE_BYTES:
        value = raw.value
    elif raw.encoding == ValueEncoding.VE_LE64:
        if len(raw.value) != 8:
            return Err(ValueError(f"LE64 value is not a 64-bit value: {raw.value!r}"))
        value = le64_type(_LE64.unpack(raw.value)[0])
    elif raw.encoding == ValueEncoding.VE_V8:
        try:
            value = v8_decoder.decodes(raw.value)
        except Exception as e:
            err = ValueError(f"V8-serialized value is not decodable: {e}")
            err.__cause__ = e
            return Err(err)
    else:
        msg = (
            "UNSPECIFIED"
            if raw.encoding == ValueEncoding.VE_UNSPECIFIED
            else f"unknown: {raw.encoding}"
        )
        return Err(ValueError(f"Value encoding is {msg}"))
    return Ok((key, value, raw.versionstamp))


_PackedKeyCacheKey: TypeAlias = tuple[Type[KvKeyPiece], Union[str, bytes, int, bool]]
_ieee_binary64: Callable[[float], bytes] = struct.Struct("d").pack
_PACK_KEY_CACHE_LIMIT = 128
_PACK_KEY_CACHE: dict[tuple[_PackedKeyCacheKey, ...], bytes] = {}


def pack_key(key: AnyKvKey) -> bytes:
    r"""
    Encode a KV key tuple into its bytes form, enforcing type restrictions.

    Only pieces with types in
    [`KV_KEY_PIECE_TYPES`](`denokv.datapath.KV_KEY_PIECE_TYPES`) are allowed.

    >>> packed = pack_key(('foo', b'bar', True, 1, 1.23))
    >>> packed
    b"\x02foo\x00\x01bar\x00'\x15\x01!\xbf\xf3\xae\x14z\xe1G\xae"
    >>> from fdb.tuple import unpack
    >>> unpack(packed)
    ('foo', b'bar', True, 1, 1.23)

    Only supported types are allowed:

    >>> pack_key(('nested', ('a', 'b')))
    Traceback (most recent call last):
    ...
    TypeError: key contains types other than str, bytes, int, float, bool: ('nested', ('a', 'b'))
    """  # noqa: E501
    if isinstance(key, KvKeyEncodable):
        return key.kv_key_bytes()

    cache = _PACK_KEY_CACHE

    cache_key: tuple[_PackedKeyCacheKey, ...] = tuple(
        [
            # -0.0 and 0.0 needs to be cached separately, but Python treats them
            # as equal. So we use the binary float representation as the cache
            # key. (This is slightly faster than using copysign, and potentially
            # handles other non-canonical float equality differences.)
            (float, _ieee_binary64(x)) if isinstance(x, float) else (type(x), x)
            for x in key
        ]
    )
    packed_key = cache.get(cache_key)
    if packed_key:
        return packed_key

    for piece in key:
        if not isinstance(piece, KV_KEY_PIECE_TYPES):
            raise TypeError(
                f"key contains types other than "
                f"{', '.join(t.__name__ for t in KV_KEY_PIECE_TYPES)}: {key!r}"
            )
    packed_key = pack(key)
    if len(cache) >= _PACK_KEY_CACHE_LIMIT:
        del cache[next(iter(cache.keys()))]
    cache[cache_key] = packed_key
    return packed_key


class PackKeyRangeOptions(TypedDict, total=False):
    """Keyword arguments of `pack_key_range()`."""

    prefix: AnyKvKey | None
    start: AnyKvKey | None
    end: AnyKvKey | None
    exclude_start: bool
    exclude_end: bool


@overload
def pack_key_range(key_range: KvKeyRangeEncodable) -> tuple[bytes, bytes]: ...


@overload
def pack_key_range(**options: Unpack[PackKeyRangeOptions]) -> tuple[bytes, bytes]: ...


def pack_key_range(
    key_range: KvKeyRangeEncodable | None = None,
    *,
    prefix: AnyKvKey | None = None,
    start: AnyKvKey | None = None,
    end: AnyKvKey | None = None,
    exclude_start: bool = False,
    exclude_end: bool = True,
) -> tuple[bytes, bytes]:
    r"""
    Get the encoded key bytes bounding the start and end of a range of keys.

    Containment of a key in the range bounded by the packed keys is assumed to
    be evaluated with `start <= x < end`, regardless of the
    `exclude_start`/`exclude_end` arguments, because this is now Deno KV / Data
    Path Protocol evaluates ranges. The exclude arguments affect the encoding of
    the start and end to achieve the requested inclusion behaviour when
    evaluated using `start <= x < end`.

    Examples
    --------
    End is excluded by default

    >>> def in_range(key):
    ...     # This is how Data Path Protocol evaluates if a value is in a range
    ...     return start <= pack_key(key) < end

    >>> start, end = pack_key_range(start=(0,), end=(10,))
    >>> assert in_range((0,)) and in_range((5,)) and in_range((9,))
    >>> assert not in_range((-1,)) and not in_range((10,))

    >>> start, end = pack_key_range(start=(0,), end=(10,),
    ...                             exclude_start=True, exclude_end=False)
    >>> assert in_range((1,)) and in_range((5,)) and in_range((10,))
    >>> assert not in_range((0,)) and not in_range((11,))

    The `prefix` defines both the start and end. `start` and `end` take
    precedence over `prefix` if both are set.

    >>> start, end = pack_key_range(prefix=('b',))
    >>> assert in_range(('b', 1)) and in_range(('b', 'any'))
    >>> assert not in_range(('a', 'any')) and not in_range(('c', 'any'))

    >>> start, end = pack_key_range(prefix=('b',), start=('a', 3))
    >>> assert in_range(('a', 3)) and in_range(('b', 'any'))
    >>> assert not in_range(('a', 2)) and not in_range(('c', 'any'))

    >>> start, end = pack_key_range(prefix=('b',), end=('b', 3))
    >>> assert in_range(('b', 0)) and in_range(('b', 2))
    >>> assert not in_range(('a', 'any')) and not in_range(('b', 3))

    When no endpoints are specified, every key is included.

    >>> start, end = pack_key_range()
    >>> assert in_range(()) and in_range(('any',)) and in_range(('z',))

    Notes
    -----
    The packed start/end bytes are not necessarily an un-packable FoundationDB
    key values. It may not be possible to call `fdb.tuple.unpack(packed_end)`,
    but the byte values none the less satisfy the range described. They must
    only be used to evaluate start/end of range queries, not as actual key
    values.
    """
    if key_range is not None:
        return key_range.kv_key_range_bytes()

    packed_prefix: bytes | None = None
    packed_start = (
        pack_key(start)
        if start is not None
        else (packed_prefix := pack_key(prefix or ()))
    )
    packed_end = (
        (packed_start if start is end else pack_key(end))
        if end is not None
        else ((packed_prefix or pack_key(prefix or ())) + b"\xff")
    )
    # The datapath protocol includes the start, so if we want to exclude it we
    # need to increment the start key to start from the next key after it.
    if exclude_start:
        packed_start = increment_packed_key(packed_start)
    # The datapath protocol itself excludes the end key when evaluating a
    # range, so if we want it to be included, we need to increment the end to
    # have the db exclude the value after end instead.
    if not exclude_end:
        packed_end = increment_packed_key(packed_end)
    return packed_start, packed_end


def increment_packed_key(packed_key: bytes) -> bytes:
    r"""
    Get a value greater than a key but less or equal to the next higher key.

    The value is not necessarily un-packable back to a tuple of key values — it
    should only be used to specify an endpoint in a range query.
    """
    # Adding a null byte results in a byte string greater than the shorter
    # version. It'll be equal to the next-higher key after \xff, e.g.
    return packed_key + b"\x00"


def read_range_single(key: AnyKvKey) -> ReadRange:
    """Create a ReadRange that matches a unique key or nothing."""
    start, end = pack_key_range(start=key, end=key, exclude_end=False)
    return ReadRange(start=start, end=end, limit=1)


def read_range_multi(
    *,
    prefix: AnyKvKey | None,
    start: AnyKvKey | None,
    end: AnyKvKey | None,
    limit: int | None = None,
    reverse: bool = False,
    exclude_start: bool = False,
    exclude_end: bool = True,
) -> ReadRange:
    """Create a ReadRange that matches multiple keys."""
    packed_start, packed_end = pack_key_range(
        prefix=prefix,
        start=start,
        end=end,
        exclude_start=exclude_start,
        exclude_end=exclude_end,
    )
    return ReadRange(start=packed_start, end=packed_end, limit=limit, reverse=reverse)
