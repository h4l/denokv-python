from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from enum import Enum
from itertools import islice
from types import MappingProxyType
from typing import Literal
from typing import overload

import v8serialize
from v8serialize import Encoder
from v8serialize.constants import SerializationTag
from v8serialize.decode import ReadableTagStream

from denokv import _datapath_pb2 as dp_protobuf
from denokv._datapath_pb2 import AtomicWrite
from denokv._kv_types import AtomicWriteRepresentation
from denokv._kv_types import KvWriter
from denokv._kv_values import KvEntry as KvEntry
from denokv._kv_values import KvU64 as KvU64
from denokv._kv_values import VersionStamp as VersionStamp
from denokv._pycompat.dataclasses import FrozenAfterInitDataclass
from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.enum import EvalEnumRepr
from denokv._pycompat.protobuf import enum_name
from denokv._pycompat.typing import TYPE_CHECKING
from denokv._pycompat.typing import Container
from denokv._pycompat.typing import Mapping
from denokv._pycompat.typing import MutableSequence
from denokv._pycompat.typing import Protocol
from denokv._pycompat.typing import Self
from denokv._pycompat.typing import Sequence
from denokv._pycompat.typing import TypeAlias
from denokv._pycompat.typing import TypeIs
from denokv._pycompat.typing import Union
from denokv._pycompat.typing import cast
from denokv._pycompat.typing import runtime_checkable
from denokv.backoff import Backoff
from denokv.backoff import ExponentialBackoff
from denokv.datapath import AnyKvKey
from denokv.datapath import pack_key
from denokv.kv_keys import KvKey


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
class PlannedWrite(AtomicWriteRepresentation):
    kv: KvWriter | None = field(default=None)
    checks: MutableSequence[AnyKeyVersion] = field(default_factory=list)
    mutations: MutableSequence[Mutation] = field(default_factory=list)
    enqueues: MutableSequence[Enqueue] = field(default_factory=list)

    async def write(self, kv: KvWriter | None = None) -> CompletedWrite:
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
    __slots__ = ()

    if TYPE_CHECKING:

        @property
        def key(self) -> AnyKvKey: ...
        @property
        def versionstamp(self) -> VersionStamp | None: ...
    else:
        key = ...
        versionstamp = ...


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
