from __future__ import annotations

import difflib
import math
import re
import sys
from base64 import b16decode
from base64 import b16encode
from collections import deque
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from itertools import groupby
from typing import Literal
from typing import Never
from typing import overload
from unittest.mock import Mock
from uuid import UUID

import pytest
import v8serialize
import v8serialize.encode
from aiohttp import web
from fdb.tuple import pack
from fdb.tuple import unpack
from google.protobuf.message import Message
from v8serialize.constants import SerializationTag
from v8serialize.decode import DecodeContext
from v8serialize.decode import DecodeNextFn
from v8serialize.jstypes import JSBigInt
from yarl import URL

from denokv._datapath_pb2 import AtomicWrite
from denokv._datapath_pb2 import AtomicWriteOutput
from denokv._datapath_pb2 import AtomicWriteStatus
from denokv._datapath_pb2 import Enqueue
from denokv._datapath_pb2 import KvEntry as ProtobufKvEntry
from denokv._datapath_pb2 import KvValue
from denokv._datapath_pb2 import Mutation
from denokv._datapath_pb2 import MutationType
from denokv._datapath_pb2 import ReadRange
from denokv._datapath_pb2 import ReadRangeOutput
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv._datapath_pb2 import SnapshotReadStatus
from denokv._datapath_pb2 import ValueEncoding
from denokv._kv_values import KvEntry
from denokv._kv_values import KvU64
from denokv._kv_values import VersionStamp
from denokv._kv_writes import LimitExceededPolicy
from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.enum import EvalEnumRepr
from denokv._pycompat.protobuf import enum_name
from denokv._pycompat.typing import Any
from denokv._pycompat.typing import Callable
from denokv._pycompat.typing import ClassVar
from denokv._pycompat.typing import Final
from denokv._pycompat.typing import Iterable
from denokv._pycompat.typing import Mapping
from denokv._pycompat.typing import NamedTuple
from denokv._pycompat.typing import Sequence
from denokv._pycompat.typing import TypeIs
from denokv._pycompat.typing import TypeVar
from denokv._pycompat.typing import Union
from denokv._pycompat.typing import cast
from denokv.auth import ConsistencyLevel
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.datapath import AnyKvKey
from denokv.datapath import KvKeyTuple
from denokv.datapath import increment_packed_key
from denokv.datapath import is_kv_key_tuple
from denokv.datapath import pack_key
from denokv.datapath import parse_protobuf_kv_entry
from denokv.errors import InvalidCursor
from denokv.kv import AnyCursorFormat
from denokv.kv import ListContext
from denokv.kv_keys import KvKey
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result
from denokv.result import is_err
from denokv.result import is_ok

T = TypeVar("T")
E = TypeVar("E")
E2 = TypeVar("E2")
MessageT = TypeVar("MessageT", bound=Message)


def mocked(mocked_value: object) -> Mock:
    """Type-safely cast `mocked_value` to a Mock."""
    assert isinstance(mocked_value, Mock)
    return mocked_value


def diff_protobuf_messages(
    left: Message,
    right: Message,
    *,
    left_name: str | None = None,
    right_name: str | None = None,
    context_line_count: int = 3,
    lineterm: str = "\n",
) -> Sequence[str]:
    if left_name is None:
        left_name = f"left: {left.DESCRIPTOR.full_name}"
    if right_name is None:
        right_name = f"right: {right.DESCRIPTOR.full_name}"
    left_lines = str(left).splitlines(keepends=False)
    right_lines = str(right).splitlines(keepends=False)
    return [
        *difflib.unified_diff(
            left_lines,
            right_lines,
            fromfile=left_name,
            tofile=right_name,
            n=context_line_count,
            lineterm=lineterm,
        )
    ]


def decode_js_number_as_float(
    tag: SerializationTag, /, ctx: DecodeContext, next: DecodeNextFn
) -> object:
    if tag in {
        SerializationTag.kInt32,
        SerializationTag.kDouble,
        SerializationTag.kUint32,
        SerializationTag.kNumberObject,
    }:
        number = next(tag)
        if isinstance(number, int):
            return float(number)
        return number
    return next(tag)


default_v8_decoder = v8serialize.Decoder()
default_v8_encoder = v8serialize.Encoder()


def assume_ok(result: Result[T, E]) -> T:
    if is_ok(result):
        return result.value
    raise AssertionError(f"result is not Ok: {result}")


@overload
def assume_err(result: Result[T, E]) -> E: ...


@overload
def assume_err(result: Result[T, Any], type: type[E]) -> E: ...


def assume_err(result: Result[T, E], type: type[E2] | None = None) -> E | E2:
    if not is_err(result):
        raise AssertionError(f"result is not Err: {result}")
    if type is None or isinstance(result.error, type):
        return result.error
    raise AssertionError(
        f"result is an Err but its value is not instanceof {type.__name__}"
    )


@dataclass(repr=False)
class KvNumberEncoding:
    py_name: str
    js_name: str | None
    value_encoding: ValueEncoding


class KvNumber(KvNumberEncoding, EvalEnumRepr):
    """
    The number types supported by datapath Sum/Min/Max operations.

    Examples
    --------
    >>> f'Foo bar: {KvNumber.bigint}'
    'Foo bar: JSBigInt (VE_V8 BigInt)'
    >>> f'Foo bar: {KvNumber.u64}'
    'Foo bar: KvU64 (VE_LE64)'
    >>> repr(KvNumber.bigint)
    'KvNumber.bigint'
    """

    bigint = "JSBigInt", "BigInt", ValueEncoding.VE_V8
    float = "int/float", "Number", ValueEncoding.VE_V8
    u64 = "KvU64", None, ValueEncoding.VE_LE64

    def __str__(self) -> str:
        ve_name = enum_name(ValueEncoding, self.value_encoding)
        if self.js_name is None:
            return f"{self.py_name} ({ve_name})"
        return f"{self.py_name} ({ve_name} {self.js_name})"


@dataclass(**slots_if310(), frozen=True)
class KvWriteValue:
    data: bytes
    encoding: ValueEncoding
    expire_at_ms: int = field(default=0)

    @staticmethod
    def tombstone() -> KvWriteValue:
        return KvWriteValue(b"", ValueEncoding.VE_UNSPECIFIED, expire_at_ms=-1)


class MockKvDbEntry(NamedTuple):
    key: bytes
    versionstamp: int
    encoding: ValueEncoding
    data: bytes
    expire_at_ms: int


class MockKvDbMessage(NamedTuple):
    payload: object
    deadline_ms: int
    keys_if_undelivered: Sequence[KvKey]
    backoff_schedule: Sequence[int]


class SumLimitExceeded(ValueError):
    pass


@dataclass
class MockKvDb:
    entries: list[MockKvDbEntry]
    next_version: int
    queued_messages: deque[MockKvDbMessage]

    def __init__(self, entries: Iterable[tuple[bytes, KvWriteValue]] = ()) -> None:
        self.clear()
        self.extend(entries)

    def clear(self) -> None:
        self.entries = []
        self.next_version = 0
        self.queued_messages = deque()

    def extend(self, entries: Iterable[tuple[bytes, KvWriteValue]]) -> None:
        version = self.next_version
        self.next_version += 1

        self.entries.extend(
            MockKvDbEntry(
                key=key,
                versionstamp=version,
                encoding=kv_value.encoding,
                data=kv_value.data,
                expire_at_ms=kv_value.expire_at_ms,
            )
            for (key, kv_value) in entries
        )
        self.entries.sort(key=lambda e: (e.key, e.versionstamp))

    def _read_range(
        self, start: bytes, end: bytes, limit: int, reverse: bool, current_time_ms: int
    ) -> Sequence[MockKvDbEntry]:
        assert limit >= 0
        matches = [e for e in self.entries if start <= e.key < end]
        latest_matches = [
            ver
            for ver in (
                list(versions)[-1]
                for (k, versions) in groupby(matches, key=lambda m: m.key)
            )
            if (ver.expire_at_ms == 0 or ver.expire_at_ms > current_time_ms)
        ]
        if reverse:
            latest_matches = list(reversed(latest_matches))
        return latest_matches[:limit]

    def snapshot_read_range(
        self, read: ReadRange, current_time_ms: int = 0
    ) -> ReadRangeOutput:
        entries = self._read_range(
            start=read.start,
            end=read.end,
            limit=read.limit,
            reverse=read.reverse,
            current_time_ms=current_time_ms,
        )
        return ReadRangeOutput(
            values=[
                ProtobufKvEntry(
                    key=e.key,
                    versionstamp=bytes(VersionStamp(e.versionstamp)),
                    encoding=e.encoding,
                    value=e.data,
                )
                for e in entries
            ]
        )

    @overload
    def _read_single(
        self, key: bytes, current_time_ms: int, pending_entries: None = None
    ) -> MockKvDbEntry | None: ...

    @overload
    def _read_single(
        self,
        key: bytes,
        current_time_ms: int,
        pending_entries: Mapping[bytes, KvWriteValue],
    ) -> MockKvDbEntry | KvWriteValue | None: ...

    def _read_single(
        self,
        key: bytes,
        current_time_ms: int,
        pending_entries: Mapping[bytes, KvWriteValue] | None = None,
    ) -> MockKvDbEntry | KvWriteValue | None:
        if pending_entries and (pending := pending_entries.get(key)):
            if pending.expire_at_ms != 0 and pending.expire_at_ms <= current_time_ms:
                return None
            return pending
        matches = self._read_range(
            start=key,
            end=increment_packed_key(key),
            limit=1,
            reverse=False,
            current_time_ms=current_time_ms,
        )
        assert len(matches) < 2
        return matches[0] if matches else None

    def atomic_write(
        self, write: AtomicWrite, current_time_ms: int = 0
    ) -> AtomicWriteOutput:
        failed_checks: list[int] = []
        for i, check in enumerate(write.checks):
            checked_entry = self._read_single(
                check.key, current_time_ms=current_time_ms
            )

            if len(check.versionstamp) == 0:
                if checked_entry is not None:
                    failed_checks.append(i)
            elif len(check.versionstamp) == 10:
                if checked_entry is None:
                    failed_checks.append(i)
                else:
                    if VersionStamp(checked_entry.versionstamp) != VersionStamp(
                        check.versionstamp
                    ):
                        failed_checks.append(i)
            else:
                raise ValueError(
                    f"Check versionstamp is not valid: {check.versionstamp!r}"
                )

        if len(failed_checks) > 0:
            return AtomicWriteOutput(
                status=AtomicWriteStatus.AW_CHECK_FAILURE, failed_checks=failed_checks
            )

        messages = [decode_enqueue_message(enqueue) for enqueue in write.enqueues]

        versionstamp = VersionStamp(self.next_version)
        mutation_entries: dict[bytes, KvWriteValue] = {}
        for mut in write.mutations:
            cause: Exception | None = None
            try:
                key_tuple = unpack(mut.key)
                key_bytes = pack(unpack(mut.key))
            except Exception as e:
                key_tuple = None
                key_bytes = None
                cause = e
            if key_bytes != mut.key:
                raise ValueError(f"Mutation key is not valid: {mut.key!r}") from cause
            assert key_tuple is not None

            expires_at_ms = mut.expire_at_ms
            if expires_at_ms < 0:
                raise ValueError(
                    f"Mutation expire_at_ms cannot be negative: {mut.expire_at_ms}"
                )

            if mut.mutation_type == MutationType.M_SET:
                mutation_entries[key_bytes] = KvWriteValue(
                    data=mut.value.data,
                    encoding=mut.value.encoding,
                    expire_at_ms=expires_at_ms,
                )
            elif mut.mutation_type == MutationType.M_DELETE:
                mutation_entries[key_bytes] = KvWriteValue.tombstone()
            elif (
                mut.mutation_type == MutationType.M_SUM
                or mut.mutation_type == MutationType.M_MIN
                or mut.mutation_type == MutationType.M_MAX
            ):
                # FIXME: Check this again, I don't think KvU64 actually allows
                #        bigint operands with the sqlite implementation. Does
                #        the FoundationDB impl act differently?
                #
                # Deno KV allows sum(left, right) with certain combinations of
                # types:
                #
                # (Left is stored in the database, right is the value sent in
                # the atomic operation.)
                #
                #  Left  |         Right           |
                #  ————  | KvU64 | bigint | number |
                # KvU64  |  yes  |  yes   |   no   |
                # bigint |  no   |  yes   |   no   |
                # number |  no   |   no   |  yes   |
                #
                # min() and max() can only use KvU64 values.

                # We need to also read from mutation_entries to take into
                # account values changed by preceding mutations within this
                # AtomicWrite operation.
                current = self._read_single(
                    mut.key,
                    current_time_ms=current_time_ms,
                    pending_entries=mutation_entries,
                )
                operand_encoding, operand_value = decode_number_value(mut.value)
                if current is None:
                    # float operands must have 0.0 not 0 as the default as
                    # cross-type sum operations are not allowed.
                    current_encoding, current_value = None, (type(operand_value)(0))
                else:
                    current_encoding, current_value = decode_number_value(current)

                op = _get_number_operator(mut, operand_encoding=operand_encoding)

                if not _is_allowed_op_combination(
                    op,
                    (current_encoding, current_value),
                    (operand_encoding, operand_value),
                ):
                    raise ValueError(
                        f"Cannot apply operation "
                        f"{enum_name(MutationType, mut.mutation_type)}, "
                        f"number types are incompatible: "
                        f"current type: {current_encoding}, "
                        f"operand type: {operand_encoding}"
                    )

                try:
                    result = op(current_value, operand_value)
                except Exception as e:
                    raise ValueError(
                        f"Mutation is not a valid "
                        f"{enum_name(MutationType, mut.mutation_type)} operation: {e}"
                    ) from e
                result_encoding = current_encoding or operand_encoding
                assert result_encoding is not None

                mutation_entries[key_bytes] = KvWriteValue(
                    data=encode_number_value(result, result_encoding),
                    encoding=result_encoding.value_encoding,
                    expire_at_ms=expires_at_ms,
                )
            elif mut.mutation_type == MutationType.M_SET_SUFFIX_VERSIONSTAMPED_KEY:
                suffix_key = pack((*key_tuple, str(versionstamp)))
                mutation_entries[suffix_key] = KvWriteValue(
                    data=mut.value.data,
                    encoding=mut.value.encoding,
                    expire_at_ms=expires_at_ms,
                )
            else:
                raise ValueError(
                    f"Mutation mutation_type is not valid: {mut.mutation_type}"
                )
        self.extend(mutation_entries.items())
        self.queued_messages.extend(messages)

        return AtomicWriteOutput(
            status=AtomicWriteStatus.AW_SUCCESS, versionstamp=versionstamp
        )


def _is_allowed_op_combination(
    op: Callable[[int | float, int | float], int | float] | MutationSumOperator | None,
    left: tuple[KvNumber | None, int | float],
    right: tuple[KvNumber, int | float],
) -> TypeIs[Callable[[int | float, int | float], int | float] | MutationSumOperator]:
    left_encoding, left_value = left
    right_encoding, right_value = right

    def values_are(types: type | tuple[type, ...]) -> bool:
        return isinstance(left_value, types) and isinstance(right_value, types)

    if isinstance(op, MutationSumOperator):
        if left_encoding is KvNumber.u64:
            return (
                right_encoding is KvNumber.u64
                or (right_encoding is KvNumber.bigint)
                and values_are(int)
            )
        elif left_encoding is KvNumber.bigint:
            return right_encoding is KvNumber.bigint and values_are(JSBigInt)
        elif left_encoding is KvNumber.float:
            return right_encoding is KvNumber.float
        elif left_encoding is None:
            # Sum can be used with a missing left operand.
            return right_encoding is KvNumber.float or values_are(int)
    elif op is min or op is max:
        return (
            left_encoding in (KvNumber.u64, None)
            and right_encoding is KvNumber.u64
            and values_are(int)
        )
    raise AssertionError(f"Unexpected op combinations: {op=}, {left=}, {right=}")


def _get_number_operator(
    mut: Mutation, *, operand_encoding: KvNumber
) -> Callable[[float, float], float] | None:
    if mut.mutation_type == MutationType.M_SUM:
        if (
            mut.sum_min or mut.sum_max or mut.sum_clamp
        ) and mut.value.encoding != ValueEncoding.VE_V8:
            raise ValueError(
                "Mutation used sum_min/sum_max/sum_clamp with non-V8 encoding"
            )
        if mut.value.encoding == ValueEncoding.VE_LE64:
            return MutationSumOperator(0, 2**64 - 1, LimitExceededPolicy.WRAP)

        min_enc, min_ = decode_v8_number(mut.sum_min) if mut.sum_min else (None, None)
        max_enc, max_ = decode_v8_number(mut.sum_max) if mut.sum_max else (None, None)
        if (min_enc and min_enc is not operand_encoding) or (
            max_enc and max_enc is not operand_encoding
        ):
            raise ValueError(
                "Mutation used different number types for value/sum_min/sum_max"
            )
        boundary = (
            LimitExceededPolicy.CLAMP if mut.sum_clamp else LimitExceededPolicy.ABORT
        )
        return MutationSumOperator(min=min_, max=max_, boundary=boundary)
    elif mut.mutation_type == MutationType.M_MAX:
        return max
    elif mut.mutation_type == MutationType.M_MIN:
        return min
    return None


@dataclass
class MutationSumOperator:
    min: int | float | None
    max: int | float | None
    boundary: LimitExceededPolicy

    def __call__(self, left: int | float, right: int | float) -> int | float:
        min, max = self.min, self.max

        result = left + right
        if self.boundary is LimitExceededPolicy.WRAP:
            # wrap is only used for uint64
            assert min == 0
            assert max is not None and max >= 0
            result = result % (max + 1)
        elif min is not None and result < min:
            if self.boundary is LimitExceededPolicy.CLAMP:
                result = min
            else:
                assert self.boundary is LimitExceededPolicy.ABORT
                raise SumLimitExceeded(
                    f"result of sum({left}, {right}) = {result}, which is less "
                    f"than the minimum {min}"
                )
        if max is not None and result > max:
            if self.boundary is LimitExceededPolicy.CLAMP:
                result = max
            else:
                assert self.boundary is LimitExceededPolicy.ABORT
                raise SumLimitExceeded(
                    f"result of sum({left}, {right}) = {result}, which is "
                    f"greater than the maximum {max}"
                )
        return result


def decode_number_value(
    entry: MockKvDbEntry | KvWriteValue | KvValue,
) -> tuple[KvNumber, int | float]:
    if entry.encoding == ValueEncoding.VE_LE64:
        return KvNumber.u64, KvU64(entry.data).value
    elif entry.encoding == ValueEncoding.VE_V8:
        return decode_v8_number(entry.data)
    else:
        raise ValueError("entry value is not an LE64 or V8-encoded BigInt or Number")


def decode_v8_number(data: bytes) -> tuple[KvNumber, int | float]:
    value = default_v8_decoder.decodes(data)
    if type(value) is JSBigInt:
        return KvNumber.bigint, value
    if type(value) in (int, float):
        return KvNumber.float, cast(Union[int, float], value)
    raise ValueError("V8-serialized value is not a BigInt or Number")


def encode_number_value(value: int | float, encoding: KvNumber) -> bytes:
    if encoding is KvNumber.float:
        return bytes(default_v8_encoder.encode(float(value)))
    elif encoding is KvNumber.bigint:
        if isinstance(value, float):
            raise TypeError("Cannot encode float as V8 BigInt")
        return bytes(default_v8_encoder.encode(JSBigInt(value)))
    else:
        assert encoding is KvNumber.u64
        if isinstance(value, float):
            raise TypeError("Cannot encode float as LE64")
        return KvU64(value).to_bytes()


def encode_kv_write_value(value: object, expires_at_ms: int = 0) -> KvWriteValue:
    if isinstance(value, KvU64):
        return KvWriteValue(
            data=bytes(value),
            encoding=ValueEncoding.VE_LE64,
            expire_at_ms=expires_at_ms,
        )
    elif isinstance(value, bytes):
        return KvWriteValue(
            data=value, encoding=ValueEncoding.VE_BYTES, expire_at_ms=expires_at_ms
        )
    else:
        return KvWriteValue(
            data=v8serialize.dumps(value),
            encoding=ValueEncoding.VE_V8,
            expire_at_ms=expires_at_ms,
        )


def decode_enqueue_message(enqueue: Enqueue) -> MockKvDbMessage:
    try:
        payload_value = default_v8_decoder.decodes(enqueue.payload)
    except v8serialize.V8SerializeError as e:
        raise ValueError("Enqueue payload is not a valid V8-encoded value") from e
    keys_if_undelivered = list[KvKey]()
    for k in enqueue.keys_if_undelivered:
        try:
            keys_if_undelivered.append(KvKey.from_kv_key_bytes(k))
        except ValueError as e:
            raise ValueError(
                f"Enqueue keys_if_undelivered contains invalid key: {k!r}"
            ) from e
    return MockKvDbMessage(
        payload=payload_value,
        backoff_schedule=list(enqueue.backoff_schedule),
        deadline_ms=enqueue.deadline_ms,
        keys_if_undelivered=keys_if_undelivered,
    )


def mock_db_api(mock_db: MockKvDb) -> web.Application:
    """HTTP endpoints implementing the KV Data Path protocol against MockKvDb."""

    def get_server_version(request: web.Request) -> Literal[1, 2, 3]:
        match = re.match(r"^/v([123])/", request.path)
        version: Final = int(match.group(1)) if match else -1
        if version not in (1, 2, 3):
            raise AssertionError("handler is not registered at /v[123]/ URL path")
        return cast(Literal[1, 2, 3], version)

    def validate_request(request: web.Request) -> None:
        server_version = get_server_version(request)

        if request.method != "POST":
            raise web.HTTPBadRequest(text="method must be POST")
        if request.content_type != "application/x-protobuf":
            raise web.HTTPBadRequest(text="content-type must be application/x-protobuf")

        db_id_header = (
            "x-transaction-domain-id" if server_version == 1 else "x-denokv-database-id"
        )
        try:
            UUID(request.headers.get(db_id_header, ""))
        except Exception:
            raise web.HTTPBadRequest(
                text=f"client did not set a valid {db_id_header} when talking to a "
                f"v{server_version} server"
            ) from None

        if server_version > 2:
            try:
                client_version = int(request.headers.get("x-denokv-version", ""))
                if client_version not in (2, 3):
                    raise ValueError(f"invalid client_version: {client_version}")
            except Exception:
                raise web.HTTPBadRequest(
                    text=f"client did not set a valid x-denokv-version header when "
                    f"talking to a v{server_version} server"
                ) from None

    def parse_protobuf_body(
        body_bytes: bytes, message_type: type[MessageT]
    ) -> MessageT:
        message = message_type()
        try:
            count = message.ParseFromString(body_bytes)
            if len(body_bytes) != count:
                raise ValueError(
                    f"{len(body_bytes) - count} trailing bytes after "
                    f"{message_type.__name__}"
                )
        except Exception as e:
            raise web.HTTPBadRequest(
                text=f"body is not a valid {message_type.__name__} message: {e}"
            ) from e
        return message

    # Valid snapshot_read handler
    async def strong_snapshot_read(request: web.Request) -> web.Response:
        validate_request(request)
        read = parse_protobuf_body(await request.read(), SnapshotRead)

        read_result = SnapshotReadOutput(
            status=SnapshotReadStatus.SR_SUCCESS,
            read_is_strongly_consistent=True,
            ranges=[mock_db.snapshot_read_range(r) for r in read.ranges],
        )
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=read_result.SerializeToString(),
        )

    # Valid atomic_write handler
    async def atomic_write(request: web.Request) -> web.Response:
        validate_request(request)

        write = parse_protobuf_body(await request.read(), AtomicWrite)

        try:
            write_result = mock_db.atomic_write(write)
        except ValueError as e:
            raise web.HTTPBadRequest(text=f"SnapshotWrite is not valid: {e}") from e

        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=write_result.SerializeToString(),
        )

    app = web.Application()

    # Working endpoints
    app.router.add_post("/v1/consistency/strong/snapshot_read", strong_snapshot_read)
    app.router.add_post("/v2/consistency/strong/snapshot_read", strong_snapshot_read)
    app.router.add_post("/v3/consistency/strong/snapshot_read", strong_snapshot_read)
    app.router.add_post("/v1/consistency/strong/atomic_write", atomic_write)
    app.router.add_post("/v2/consistency/strong/atomic_write", atomic_write)
    app.router.add_post("/v3/consistency/strong/atomic_write", atomic_write)
    return app


def make_database_metadata(
    endpoints: URL | Sequence[EndpointInfo],
    *,
    endpoint_consistency: ConsistencyLevel | None = None,
    version: Literal[1, 2, 3] = 3,
    database_id: UUID | None = None,
    expires_at: datetime | None = None,
    token: str = "hunter2.123",
) -> DatabaseMetadata:
    if isinstance(endpoints, URL):
        if endpoint_consistency is None:
            endpoint_consistency = ConsistencyLevel.STRONG
        endpoints = [EndpointInfo(url=endpoints, consistency=endpoint_consistency)]
    else:
        if endpoint_consistency is not None:
            raise TypeError(
                "cannot set endpoint_consistency argument wen endpoints is a Sequence"
            )

    if database_id is None:
        database_id = UUID("00000000-0000-0000-0000-000000000000")
    if expires_at is None:
        expires_at = datetime.now() + timedelta(minutes=30)

    meta = DatabaseMetadata(
        version=version,
        database_id=database_id,
        endpoints=endpoints,
        expires_at=expires_at,
        token=token,
    )
    return meta


def meta_endpoint(meta: DatabaseMetadata) -> tuple[DatabaseMetadata, EndpointInfo]:
    return meta, meta.endpoints[0]


def add_entries(
    db: MockKvDb,
    entries: Mapping[AnyKvKey, object]
    | Mapping[KvKeyTuple, object]
    | Iterable[tuple[AnyKvKey, object]],
) -> VersionStamp:
    if isinstance(entries, Mapping):
        entries = entries.items()

    version = VersionStamp(db.next_version)
    encoded_entries = [
        (pack_key(key), encode_kv_write_value(value)) for (key, value) in entries
    ]
    db.extend(encoded_entries)
    return version


def unsafe_parse_protobuf_kv_entry(
    raw: ProtobufKvEntry, v8_decoder: v8serialize.Decoder | None = None
) -> KvEntry:
    if v8_decoder is None:
        v8_decoder = default_v8_decoder
    key, value, versionstamp = assume_ok(
        parse_protobuf_kv_entry(raw, v8_decoder=v8_decoder, le64_type=KvU64)
    )
    return KvEntry(KvKey.wrap_tuple_keys(key), value, VersionStamp(versionstamp))


class ExampleCursorFormat(AnyCursorFormat):
    """
    A cursor encoding format used for testing/example purposes.

    It contains the entire packed key, making it easy to generate values for
    testing.

    >>> ExampleCursorFormat.INSTANCE.get_cursor_for_key(('a', 1))
    Ok('0x0261001501')
    >>> ExampleCursorFormat.INSTANCE.get_key_for_cursor('0x0261001501')
    Ok(('a', 1))
    """

    INSTANCE: ClassVar[ExampleCursorFormat]

    def __init__(self, list_context: ListContext | None = None) -> None:
        pass

    def get_key_for_cursor(self, cursor: str) -> Result[KvKeyTuple, InvalidCursor]:
        cause: Exception | None = None
        if cursor.startswith("0x"):
            try:
                key = unpack(b16decode(cursor[2:]))
            except Exception as e:
                cause = e
            else:
                if is_kv_key_tuple(key):
                    return Ok(key)
        err = InvalidCursor(f"invalid cursor: {cursor}", cursor=cursor)
        err.__cause__ = cause
        return Err(err)

    def get_cursor_for_key(self, key: AnyKvKey) -> Result[str, ValueError]:
        try:
            packed_key = pack_key(key)
        except ValueError as e:
            return Err(e)
        return Ok(f"0x{b16encode(packed_key).decode()}")


ExampleCursorFormat.INSTANCE = ExampleCursorFormat()

if sys.version_info >= (3, 12):
    nextafter = math.nextafter
else:

    def nextafter(x: float, y: float, *, steps: int = 1) -> float:
        if steps < 0:
            raise ValueError("steps must be a non-negative integer")
        x = float(x)
        for _ in range(steps):
            x = math.nextafter(x, y)
        return x


def typeval(value: T) -> tuple[type[T], T]:
    return type(value), value


def create_dataclass_slots_test() -> Callable[[Never], None]:
    @pytest.mark.skipif(
        sys.version_info < (3, 10), reason="<3.10 does not use slots for dataclass"
    )
    def test_instances_dont_have_dict_because_of_slots(instance: object) -> None:
        with pytest.raises(AttributeError):
            _ = instance.__dict__

    return test_instances_dont_have_dict_because_of_slots
