from __future__ import annotations

from abc import abstractmethod
from builtins import float as float_
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from enum import Enum
from functools import total_ordering
from itertools import islice
from types import MappingProxyType
from typing import Literal  # noqa: TID251
from typing import overload  # noqa: TID251

from v8serialize import Encoder
from v8serialize.constants import FLOAT64_SAFE_INT_RANGE
from v8serialize.encode import WritableTagStream
from v8serialize.jstypes import JSBigInt

from denokv import _datapath_pb2 as dp_protobuf
from denokv._datapath_pb2 import AtomicWrite
from denokv._kv_types import AtomicWriteRepresentationWriter
from denokv._kv_types import KvWriter
from denokv._kv_types import ProtobufMessageRepresentation
from denokv._kv_types import SingleProtobufMessageRepresentation
from denokv._kv_types import get_v8_encoder
from denokv._kv_values import KvEntry as KvEntry
from denokv._kv_values import KvU64 as KvU64
from denokv._kv_values import VersionStamp as VersionStamp
from denokv._pycompat.dataclasses import FrozenAfterInitDataclass
from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.enum import EvalEnumRepr
from denokv._pycompat.exceptions import with_notes
from denokv._pycompat.typing import TYPE_CHECKING
from denokv._pycompat.typing import Any
from denokv._pycompat.typing import ClassVar
from denokv._pycompat.typing import Container
from denokv._pycompat.typing import Final
from denokv._pycompat.typing import Generic
from denokv._pycompat.typing import Iterable
from denokv._pycompat.typing import Mapping
from denokv._pycompat.typing import MutableSequence
from denokv._pycompat.typing import Never
from denokv._pycompat.typing import Protocol
from denokv._pycompat.typing import Self
from denokv._pycompat.typing import Sequence
from denokv._pycompat.typing import TypeAlias
from denokv._pycompat.typing import TypedDict
from denokv._pycompat.typing import TypeGuard
from denokv._pycompat.typing import TypeIs
from denokv._pycompat.typing import TypeVar
from denokv._pycompat.typing import Union
from denokv._pycompat.typing import Unpack
from denokv._pycompat.typing import assert_never
from denokv._pycompat.typing import cast
from denokv._pycompat.typing import override
from denokv._pycompat.typing import runtime_checkable
from denokv._utils import frozen
from denokv.auth import EndpointInfo
from denokv.backoff import Backoff
from denokv.backoff import ExponentialBackoff
from denokv.datapath import AnyKvKey
from denokv.datapath import CheckFailure
from denokv.datapath import pack_key
from denokv.errors import DenoKvError
from denokv.kv_keys import KvKey
from denokv.result import AnyFailure
from denokv.result import AnySuccess
from denokv.result import is_err

KvNumberNameT = TypeVar("KvNumberNameT", bound=str, default=str)
NumberT = TypeVar("NumberT", bound=Union[int, float], default=Union[int, float])
KvNumberTypeT = TypeVar("KvNumberTypeT", default=object)

KvNumberNameT_co = TypeVar("KvNumberNameT_co", bound=str, covariant=True, default=str)
NumberT_co = TypeVar(
    "NumberT_co", bound=Union[int, float], covariant=True, default=Union[int, float]
)
KvNumberTypeT_co = TypeVar("KvNumberTypeT_co", covariant=True, default=object)

U = TypeVar("U")
MutateResultT = TypeVar("MutateResultT")
EnqueueResultT = TypeVar("EnqueueResultT")
CheckResultT = TypeVar("CheckResultT")


@total_ordering
@dataclass(frozen=True, unsafe_hash=True, **slots_if310())
class KvNumberInfo(Generic[KvNumberNameT_co, NumberT, KvNumberTypeT]):
    name: KvNumberNameT_co = field(init=False)
    py_type: type[NumberT] = field(init=False)
    kv_type: type[KvNumberTypeT] = field(init=False)

    @property
    @abstractmethod
    def default_limit(self) -> Limit[NumberT]: ...

    @abstractmethod
    def validate_limit(self, limit: Limit[NumberT]) -> Limit[NumberT]: ...

    def __lt__(self, other: object) -> bool:
        if isinstance(other, KvNumberInfo):
            self_name: str = self.name  # mypy needs help with inferring str
            other_name: str = other.name

            if self_name == other_name and self != other:
                raise RuntimeError("KvNumberInfo instances must have unique names")
            return self_name < other_name
        return NotImplemented

    def as_py_number(self, number: KvNumberTypeT | NumberT | int) -> NumberT:
        if self.is_py_number(number):
            return number
        if self.is_kv_number(number) or self._is_compatible_int(number, target="py"):
            return self.py_type(number)  # type: ignore[arg-type,return-value]
        raise self._describe_invalid_number(number, target="py")

    def as_kv_number(self, number: KvNumberTypeT | NumberT | int) -> KvNumberTypeT:
        if self.is_kv_number(number):
            return number
        if self.is_py_number(number) or self._is_compatible_int(number, target="kv"):
            return self.kv_type(number)  # type: ignore[call-arg]
        raise self._describe_invalid_number(number, target="kv")

    def _is_compatible_int(
        self, number: object, *, target: Literal["py", "kv"]
    ) -> TypeGuard[int]:
        return type(number) is int

    def _describe_invalid_number(
        self, number: object, *, target: Literal["py", "kv"]
    ) -> Exception:
        return with_notes(
            TypeError(
                f"number is not compatible with {self.name} {target} number type"
            ),
            f"number: {number!r} ({type(number)}), " f"{self.name}={self}",
        )

    def is_py_number(self, value: object) -> TypeGuard[NumberT]:
        return isinstance(value, self.py_type)

    def is_kv_number(self, value: object) -> TypeGuard[KvNumberTypeT]:
        return isinstance(value, self.kv_type)

    @abstractmethod
    def get_sum_mutations(
        self,
        sum: Sum[KvNumberNameT_co, NumberT, KvNumberTypeT],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]: ...

    @abstractmethod
    def get_min_mutations(
        self,
        min: Min[KvNumberNameT_co, NumberT, KvNumberTypeT],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]: ...

    @abstractmethod
    def get_max_mutations(
        self,
        max: Max[KvNumberNameT_co, NumberT, KvNumberTypeT],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]: ...


class V8KvNumberInfo(KvNumberInfo[KvNumberNameT_co, NumberT, KvNumberTypeT]):
    @property
    def default_limit(self) -> Limit[NumberT]:
        return LIMIT_UNLIMITED

    def validate_limit(self, limit: Limit[NumberT]) -> Limit[NumberT]:
        if limit.limit_exceeded not in (
            LimitExceededPolicy.ABORT,
            LimitExceededPolicy.CLAMP,
        ):
            raise with_notes(
                ValueError(f"Number type {self.name!r} does not support wrap limits"),
                "Use 'u64' (KvU64) to wrap on 0, 2^64 - 1 bounds.",
            )
        return limit

    @abstractmethod
    def v8_encode_kv_number(self, value: KvNumberTypeT) -> bytes: ...

    @override
    def get_sum_mutations(
        self,
        sum: Sum[KvNumberNameT_co, NumberT, KvNumberTypeT],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]:
        encoded_min = b""
        encoded_max = b""
        self.validate_limit(sum.limit)
        if sum.limit.min is not None:
            encoded_min = self.v8_encode_kv_number(self.as_kv_number(sum.limit.min))
        if sum.limit.max is not None:
            encoded_max = self.v8_encode_kv_number(self.as_kv_number(sum.limit.max))

        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_SUM,
            key=pack_key(sum.key),
            value=dp_protobuf.KvValue(
                data=self.v8_encode_kv_number(self.as_kv_number(sum.delta)),
                encoding=dp_protobuf.ValueEncoding.VE_V8,
            ),
            expire_at_ms=sum.expire_at_ms(),
            sum_min=encoded_min,
            sum_max=encoded_max,
            sum_clamp=sum.limit.limit_exceeded is LimitExceededPolicy.CLAMP,
        )

        return [mutation]

    @override
    def get_min_mutations(
        self,
        min: Min[KvNumberNameT_co, NumberT, KvNumberTypeT],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_SUM,
            key=pack_key(min.key),
            value=dp_protobuf.KvValue(
                data=self.v8_encode_kv_number(self.as_kv_number(self.as_py_number(0))),
                encoding=dp_protobuf.ValueEncoding.VE_V8,
            ),
            sum_max=self.v8_encode_kv_number(self.as_kv_number(min.value)),
            sum_clamp=True,
            expire_at_ms=min.expire_at_ms(),
        )
        return [mutation]

    @override
    def get_max_mutations(
        self,
        max: Max[KvNumberNameT_co, NumberT, KvNumberTypeT],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_SUM,
            key=pack_key(max.key),
            value=dp_protobuf.KvValue(
                data=self.v8_encode_kv_number(self.as_kv_number(self.as_py_number(0))),
                encoding=dp_protobuf.ValueEncoding.VE_V8,
            ),
            sum_min=self.v8_encode_kv_number(self.as_kv_number(max.value)),
            sum_clamp=True,
            expire_at_ms=max.expire_at_ms(),
        )
        return [mutation]


class BigIntKvNumberInfo(V8KvNumberInfo[Literal["bigint"], int, JSBigInt]):
    __slots__ = ()
    name = "bigint"
    py_type = int
    kv_type = JSBigInt

    def v8_encode_kv_number(self, value: JSBigInt) -> bytes:
        return encode_v8_bigint(value)

    @override
    def is_py_number(self, value: object) -> TypeGuard[int]:
        # Don't treat JSBigInt instances as being py numbers so that we downcast
        # JSBigInt to plain int in as_py_number(). This is important to allow
        # other things to not treat JSBigInt as the same as int.
        return (not self.is_kv_number(value)) and super().is_py_number(value)


class FloatKvNumberInfo(V8KvNumberInfo[Literal["float"], float, float]):
    __slots__ = ()
    name = "float"
    py_type = float
    kv_type = float

    def v8_encode_kv_number(self, value: float) -> bytes:
        return encode_v8_number(value)

    def _is_int_in_float_safe_range(self, value: object) -> TypeGuard[int]:
        # int is assignable to float in Python's type system, but
        # isinstance(int(x), float) is False. We don't allow subclasses of int,
        # because JSBigInt is a subclass of int, and we don't want to treat them
        # as FloatKvNumberInfo values.
        return type(value) is int and value in FLOAT64_SAFE_INT_RANGE

    @override
    def is_kv_number(self, value: object) -> TypeGuard[float]:
        return self._is_int_in_float_safe_range(value) or super().is_kv_number(value)

    @override
    def is_py_number(self, value: object) -> TypeGuard[float]:
        return self._is_int_in_float_safe_range(value) or super().is_py_number(value)

    @override
    def _is_compatible_int(
        self, number: object, *, target: Literal["py", "kv"]
    ) -> TypeGuard[int]:
        # only allow conversions from plain int that is in the safe range.
        return self._is_int_in_float_safe_range(number)

    @override
    def _describe_invalid_number(
        self, number: object, *, target: Literal["py", "kv"]
    ) -> Exception:
        err = super()._describe_invalid_number(number, target=target)
        if type(number) is int and not self._is_int_in_float_safe_range(number):
            return with_notes(
                ValueError(*err.args),
                "The int is too large to represent as a 64-bit floating point value.",
                from_exception=err,
            )
        return err


class U64KvNumberInfo(KvNumberInfo[Literal["u64"], int, KvU64]):
    __slots__ = ()
    name = "u64"
    py_type = int
    kv_type = KvU64

    @property
    def default_limit(self) -> Limit[int]:
        return LIMIT_KVU64

    @override
    def validate_limit(self, limit: Limit[int]) -> Limit[int]:
        if limit.limit_exceeded is LimitExceededPolicy.ABORT:
            raise with_notes(
                ValueError(f"Number type {self.name!r} does not support abort limits"),
                "Use 'bigint' (JSBigInt) or 'float' (int/float) to wrap on "
                "0, 2^64 - 1 bounds.",
            )

        if limit.limit_exceeded is LimitExceededPolicy.WRAP and limit != LIMIT_KVU64:
            raise with_notes(
                ValueError(
                    f"Number type {self.name!r} wrap limit's min, max "
                    f"bounds cannot be changed"
                ),
                "'u64' (KvU64) can only wrap at 0 and 2^64 - 1. It can use "
                "clamp with custom bounds through.",
            )
        return limit

    @override
    def get_sum_mutations(
        self,
        sum: Sum[Literal["u64"], int, KvU64],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]:
        self.validate_limit(sum.limit)
        assert sum.limit.limit_exceeded is not LimitExceededPolicy.ABORT
        if sum.limit.limit_exceeded is LimitExceededPolicy.WRAP:
            return self._get_sum_wrap_mutations(sum)
        elif sum.limit.limit_exceeded is LimitExceededPolicy.CLAMP:
            return self._get_sum_clamp_mutations(sum)
        else:
            assert_never(sum.limit.limit_exceeded)

    def _get_sum_clamp_mutations(
        self, sum: Sum[Literal["u64"], int, KvU64]
    ) -> Sequence[dp_protobuf.Mutation]:
        assert sum.limit.limit_exceeded is LimitExceededPolicy.CLAMP

        limit_min = 0 if sum.limit.min is None else sum.limit.min
        limit_max = KvU64.RANGE.stop - 1 if sum.limit.max is None else sum.limit.max
        if limit_min not in KvU64.RANGE:
            raise with_notes(
                ValueError("sum.limit.min must be in KvU64.RANGE"),
                f"sum.limit.min: {limit_min}",
            )
        if limit_max not in KvU64.RANGE:
            raise with_notes(
                ValueError("sum.limit.max must be in KvU64.RANGE"),
                f"sum.limit.max: {limit_max}",
            )

        delta = self._normalise_clamp_delta(sum.delta)

        if delta < 0:
            return self._get_negative_sum_clamp_mutations(
                sum, delta, limit_min, limit_max
            )
        else:
            return self._get_positive_sum_clamp_mutations(
                sum, delta, limit_min, limit_max
            )

    def _get_positive_sum_clamp_mutations(
        self,
        sum: Sum[Literal["u64"], int, KvU64],
        delta: int,
        limit_min: int,
        limit_max: int,
    ) -> Sequence[dp_protobuf.Mutation]:
        assert delta in KvU64.RANGE
        assert limit_min in KvU64.RANGE
        assert limit_max in KvU64.RANGE

        # When the upper limit is <= the delta, the result is always clamped at the
        # upper limit. Likewise if the lower limit pushes the result above the upper
        # limit, the upper limit is used (it's applied last).
        min_result = delta
        if limit_max <= min_result or limit_max <= (limit_min or 0):
            return [self._mutate_set(sum, KvU64(limit_max))]  # result is constant

        mutations = list[dp_protobuf.Mutation]()

        if limit_min >= limit_max or limit_min <= delta:
            limit_min = 0  # lower bound can have no effect on the result

        # We clamp the final result to be <= the limit_max by clamping the db
        # value to the highest value that won't exceed the limit_max when the
        # delta is added.

        # delta is always < limit_max, otherwise the result is constant, which
        # is handled above.
        max_start = limit_max - delta
        assert max_start > 0
        mutations.append(self._mutate_min(sum, KvU64(max_start)))

        if delta != 0:
            mutations.append(self._mutate_sum(sum, KvU64(delta)))

        if limit_min > 0:
            mutations.append(self._mutate_max(sum, KvU64(limit_min)))

        return mutations

    def _get_negative_sum_clamp_mutations(
        self,
        sum: Sum[Literal["u64"], int, KvU64],
        delta: int,
        limit_min: int,
        limit_max: int,
    ) -> Sequence[dp_protobuf.Mutation]:
        assert -delta in KvU64.RANGE
        assert limit_min in KvU64.RANGE
        assert limit_max in KvU64.RANGE

        # If value after adding the (negative) delta is always <= the lower
        # limit, the lower limit is always the result. However the upper limit
        # applies last, so if the upper limit is lower than the lower limit, it
        # applies instead.
        if limit_max <= limit_min:
            return [self._mutate_set(sum, KvU64(limit_max))]
        max_result = (KvU64.RANGE.stop - 1) + delta
        if limit_min >= max_result:
            assert limit_max > limit_min
            return [self._mutate_set(sum, KvU64(limit_min))]

        mutations = list[dp_protobuf.Mutation]()

        # Offset the start to prevent it going negative after adding the delta
        min_start = abs(delta) + limit_min
        # min_start cannot exceed the range, because abs(delta) values >= the
        # difference between limit_min and the top of the range trigger the
        # constant result short-circuit above, as the result is always limit_min
        assert min_start in KvU64.RANGE
        mutations.append(self._mutate_max(sum, KvU64(min_start)))

        # Make the negative delta a positive delta that overflows to the result
        # of applying the original negative delta offset.
        if delta != 0:
            delta = KvU64.RANGE.stop + delta
            assert delta in KvU64.RANGE

            # Apply the delta (effectively subtracting)
            mutations.append(self._mutate_sum(sum, KvU64(delta)))

        if limit_max >= max_result:
            # limit_max can have no effect on the result
            assert limit_max > limit_min
        else:
            mutations.append(self._mutate_min(sum, KvU64(limit_max)))

        return mutations

    def _get_sum_wrap_mutations(
        self, sum: Sum[Literal["u64"], int, KvU64]
    ) -> Sequence[dp_protobuf.Mutation]:
        assert sum.limit.limit_exceeded is LimitExceededPolicy.WRAP
        # Only one wrapping limit is available for KvU64
        # (the default 64-bit uint bounds).
        if sum.limit != LIMIT_KVU64:
            raise with_notes(
                ValueError(
                    f"Deno KV does not support {LimitExceededPolicy.WRAP} with "
                    f"non-default min/max for KvU64 values"
                ),
                f"sum.limit: {sum.limit}",
            )

        delta = self._normalise_wrap_delta(sum.delta)
        # M_SUM mutations for KvU64 only support positive delta values, because
        # KvU64 is unsigned. We support negative effective deltas by taking
        # advantage of integer overflow/wrapping — we add a positive value that
        # overflows to the equivalent of subtracting delta.
        #
        # For example to subtract 2 from 10, we are calculating
        # (10 + (2**64 - 2)) % 2**64 = 8
        if delta < 0:
            delta = KvU64.RANGE.stop + delta
        assert delta in KvU64.RANGE

        return [self._mutate_sum(sum, KvU64(delta))]

    @override
    def get_min_mutations(
        self,
        min: Min[Literal["u64"], int, KvU64],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_MIN,
            key=pack_key(min.key),
            value=dp_protobuf.KvValue(
                data=bytes(KvU64(min.value)),
                encoding=dp_protobuf.ValueEncoding.VE_LE64,
            ),
            expire_at_ms=min.expire_at_ms(),
        )
        return [mutation]

    @override
    def get_max_mutations(
        self,
        max: Max[Literal["u64"], int, KvU64],
        *,
        v8_encoder: Encoder | None = None,
    ) -> Sequence[dp_protobuf.Mutation]:
        mutation = dp_protobuf.Mutation(
            mutation_type=dp_protobuf.MutationType.M_MAX,
            key=pack_key(max.key),
            value=dp_protobuf.KvValue(
                data=bytes(KvU64(max.value)),
                encoding=dp_protobuf.ValueEncoding.VE_LE64,
            ),
            expire_at_ms=max.expire_at_ms(),
        )
        return [mutation]

    @staticmethod
    def _normalise_wrap_delta(delta: int) -> int:
        """
        Normalise a sum delta value to be within +/- 2**64 for limit type wrap.

        This method wraps delta values larger than 2**64 - 1, in contrast with
        _normalise_clamp_delta(), which clamps at the max value.

        Examples
        --------
        >>> U64KvNumberInfo._normalise_wrap_delta(-5)
        -5
        >>> U64KvNumberInfo._normalise_wrap_delta(-5 - 2**64)
        -5
        >>> U64KvNumberInfo._normalise_wrap_delta(5)
        5
        >>> U64KvNumberInfo._normalise_wrap_delta(5 + 2**64)
        5
        >>> U64KvNumberInfo._normalise_wrap_delta(2**64)
        0
        >>> U64KvNumberInfo._normalise_wrap_delta(-2**64)
        0
        """
        pos_wrapped_delta = abs(delta) % KvU64.RANGE.stop
        return -pos_wrapped_delta if delta < 0 else pos_wrapped_delta

    @staticmethod
    def _normalise_clamp_delta(delta: int) -> int:
        """
        Normalise a sum delta value to be within +/- 2**64 for limit type clamp.

        This method clamps delta values larger than 2**64 - 1 at the max value,
        in contrast with _normalise_wrap_delta(), which wraps over the max value.

        Examples
        --------
        >>> U64KvNumberInfo._normalise_clamp_delta(-5)
        -5
        >>> U64KvNumberInfo._normalise_clamp_delta(-5 - 2**64)
        -18446744073709551615
        >>> U64KvNumberInfo._normalise_clamp_delta(5)
        5
        >>> U64KvNumberInfo._normalise_clamp_delta(5 + 2**64)
        18446744073709551615
        >>> U64KvNumberInfo._normalise_clamp_delta(2**64)
        18446744073709551615
        >>> U64KvNumberInfo._normalise_clamp_delta(-2**64)
        -18446744073709551615
        """
        pos_clamped_delta = min(abs(delta), KvU64.RANGE.stop - 1)
        return -pos_clamped_delta if delta < 0 else pos_clamped_delta

    def _mutate(
        self,
        sum: Sum[Literal["u64"], int, KvU64],
        mutation_type: dp_protobuf.MutationType,
        value: KvU64,
    ) -> dp_protobuf.Mutation:
        return dp_protobuf.Mutation(
            key=pack_key(sum.key),
            expire_at_ms=sum.expire_at_ms(),
            mutation_type=mutation_type,
            value=dp_protobuf.KvValue(data=bytes(value), encoding=dp_protobuf.VE_LE64),
        )

    def _mutate_set(
        self, sum: Sum[Literal["u64"], int, KvU64], value: KvU64
    ) -> dp_protobuf.Mutation:
        return self._mutate(sum, dp_protobuf.MutationType.M_SET, value)

    def _mutate_max(
        self, sum: Sum[Literal["u64"], int, KvU64], value: KvU64
    ) -> dp_protobuf.Mutation:
        return self._mutate(sum, dp_protobuf.MutationType.M_MAX, value)

    def _mutate_min(
        self, sum: Sum[Literal["u64"], int, KvU64], value: KvU64
    ) -> dp_protobuf.Mutation:
        return self._mutate(sum, dp_protobuf.MutationType.M_MIN, value)

    def _mutate_sum(
        self, sum: Sum[Literal["u64"], int, KvU64], value: KvU64
    ) -> dp_protobuf.Mutation:
        return self._mutate(sum, dp_protobuf.MutationType.M_SUM, value)


@frozen
@total_ordering
class KvNumber(Enum):
    """The types of numbers that the atomic sum/min/max operations can be used with."""

    # _value_: KvNumberInfo

    bigint = BigIntKvNumberInfo()
    """A JavaScript bigint — arbitrary-precision integer."""
    float = FloatKvNumberInfo()
    """A JavaScript number — 64-bit floating-point number."""
    u64 = U64KvNumberInfo()
    """A Deno KV-specific 64-bit unsigned integer."""

    @overload
    @classmethod
    def resolve(
        cls, identifier: BigIntKvNumberIdentifier
    ) -> Literal[KvNumber.bigint]: ...

    @overload
    @classmethod
    def resolve(
        cls, identifier: FloatKvNumberIdentifier
    ) -> Literal[KvNumber.float]: ...

    @overload
    @classmethod
    def resolve(cls, identifier: U64KvNumberIdentifier) -> Literal[KvNumber.u64]: ...

    @overload
    @classmethod
    def resolve(cls, identifier: KvNumber) -> LiteralKvNumber: ...

    @overload
    @classmethod
    def resolve(cls, /, *, number: KvU64) -> Literal[KvNumber.u64]: ...

    @overload
    @classmethod
    def resolve(cls, /, *, number: JSBigInt) -> Literal[KvNumber.bigint]: ...  # pyright: ignore[reportOverlappingOverload]

    @overload
    @classmethod
    def resolve(cls, /, *, number: float_) -> Literal[KvNumber.float]: ...

    @classmethod
    def resolve(
        cls,
        identifier: KvNumberIdentifier | None = None,
        *,
        number: KvU64 | JSBigInt | __builtins__.float | None = None,
    ) -> LiteralKvNumber:
        if identifier is not None:
            return cast(LiteralKvNumber, KvNumber(identifier))

        if number is None:
            raise TypeError("resolve() missing 1 required argument: 'identifier'")
        try:
            return cast(LiteralKvNumber, KvNumber(type(number)))
        except Exception as e:
            raise TypeError(
                f"number is not supported by any KvNumber: {number!r}"
            ) from e

    @classmethod
    def _missing_(cls, value: Any) -> KvNumber | None:
        return cls.__members__.get(value)

    def __lt__(self, other: object) -> bool:
        if type(other) is KvNumber:
            self_value: KvNumberInfo[Any, Any, Any] = self.value
            other_value: KvNumberInfo[Any, Any, Any] = other.value
            return self_value < other_value
        return NotImplemented


LiteralKvNumber: TypeAlias = Literal[KvNumber.bigint, KvNumber.float, KvNumber.u64]
KvNumber._value2member_map_[JSBigInt] = KvNumber.bigint
KvNumber._value2member_map_[float] = KvNumber.float
# int values correspond to KvNumber (float64) because JavaScript integer values
# are float64, and v8serialize by default encodes and decodes int values as
# Number not Bigint (JSBigInt is used for BigInt).
KvNumber._value2member_map_[int] = KvNumber.float
KvNumber._value2member_map_[KvU64] = KvNumber.u64

BigIntKvNumberIdentifier: TypeAlias = Union[
    Literal["bigint", KvNumber.bigint], type[JSBigInt]
]
FloatKvNumberIdentifier: TypeAlias = Union[
    Literal["float", KvNumber.float], type[float]
]
U64KvNumberIdentifier: TypeAlias = Union[Literal["u64", KvNumber.u64], type[KvU64]]
KvNumberIdentifier: TypeAlias = Union[
    BigIntKvNumberIdentifier, FloatKvNumberIdentifier, U64KvNumberIdentifier, KvNumber
]


def encode_v8_number(number: float, /) -> bytes:
    """Encode a Python float as a JavaScript Number in V8 serialization format."""
    if not KvNumber.float.value.is_kv_number(number):
        raise with_notes(
            TypeError("number must be a float or int in the float-safe range"),
            f"number: {number!r} ({type(number)})",
        )
    wts = WritableTagStream()
    wts.write_header()
    # It's OK to pass an int, they'll be encoded as float64
    wts.write_double(number)
    return bytes(wts.data)


def encode_v8_bigint(number: JSBigInt, /) -> bytes:
    """Encode a Python JSBigInt as a JavaScript BigInt in V8 serialization format."""
    if not KvNumber.bigint.value.is_kv_number(number):
        raise TypeError(f"number must be a JSBigInt, not {type(number)}")
    wts = WritableTagStream()
    wts.write_header()
    wts.write_bigint(number)
    return bytes(wts.data)


@overload
def encode_kv_write_value(
    value: KvU64 | bytes | JSBigInt | float, *, v8_encoder: Encoder | None = None
) -> dp_protobuf.KvValue: ...


@overload
def encode_kv_write_value(
    value: object, *, v8_encoder: Encoder
) -> dp_protobuf.KvValue: ...


def encode_kv_write_value(
    value: object, *, v8_encoder: Encoder | None = None
) -> dp_protobuf.KvValue:
    if isinstance(value, KvU64):
        return dp_protobuf.KvValue(
            data=bytes(value),
            encoding=dp_protobuf.ValueEncoding.VE_LE64,
        )
    elif isinstance(value, bytes):
        return dp_protobuf.KvValue(
            data=value, encoding=dp_protobuf.ValueEncoding.VE_BYTES
        )
    elif isinstance(value, JSBigInt):
        return dp_protobuf.KvValue(
            data=encode_v8_bigint(value), encoding=dp_protobuf.ValueEncoding.VE_V8
        )
    elif isinstance(value, float):
        return dp_protobuf.KvValue(
            data=encode_v8_number(value), encoding=dp_protobuf.ValueEncoding.VE_V8
        )
    else:
        if v8_encoder is None:
            raise TypeError(
                "v8_encoder cannot be None when encoding an arbitrary object"
            )
        return dp_protobuf.KvValue(
            data=bytes(v8_encoder.encode(value)),
            encoding=dp_protobuf.ValueEncoding.VE_V8,
        )


class MutationOptions(TypedDict, total=False):
    expire_at: datetime | None


class LimitOptions(Generic[NumberT], TypedDict, total=False):
    clamp_over: NumberT | None
    clamp_under: NumberT | None
    abort_over: NumberT | None
    abort_under: NumberT | None
    limit: Limit[NumberT] | None


class SumOptions(LimitOptions[NumberT_co], MutationOptions):
    """Keyword arguments accepted by `sum()`/`Sum()`."""


class SumArgs(
    SumOptions[NumberT], Generic[KvNumberNameT, NumberT, KvNumberTypeT], total=False
):
    """All arguments accepted by `sum()`/`Sum()`."""

    key: AnyKvKey
    delta: JSBigInt | float | KvU64 | NumberT | KvNumberTypeT
    number_type: (
        KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT] | KvNumberIdentifier | None
    )


class CheckMixin(Generic[CheckResultT]):
    @abstractmethod
    def _check(self, check: CheckRepresentation, /) -> CheckResultT:
        raise NotImplementedError

    @overload
    def check(
        self, key: AnyKvKey, versionstamp: VersionStamp | None = None
    ) -> CheckResultT: ...

    @overload
    def check(self, check: CheckRepresentation, /) -> CheckResultT: ...

    @overload
    def check(self, check: AnyKeyVersion, /) -> CheckResultT: ...

    def check(
        self,
        key: CheckRepresentation | AnyKeyVersion | AnyKvKey,
        versionstamp: VersionStamp | None = None,
    ) -> CheckResultT:
        if isinstance(key, CheckRepresentation):
            if versionstamp is not None:
                raise TypeError(
                    "'versionstamp' argument cannot be set when the first argument "
                    "to check() is an object with an 'as_protobuf' method"
                )
            return self._check(key)
        elif isinstance(key, AnyKeyVersion):
            if versionstamp is not None:
                raise TypeError(
                    "'versionstamp' argument cannot be set when the first argument "
                    "to check() is an object with 'key' and 'versionstamp' attributes"
                )
            return self._check(Check(key.key, key.versionstamp))
        else:
            return self._check(Check(key, versionstamp))

    def check_key_has_version(
        self, key: AnyKvKey, versionstamp: VersionStamp
    ) -> CheckResultT:
        return self._check(Check.for_key_with_version(key, versionstamp))

    def check_key_not_set(self, key: AnyKvKey) -> CheckResultT:
        return self._check(Check.for_key_not_set(key))


class MutatorMixin(Generic[MutateResultT]):
    @abstractmethod
    def mutate(self, mutation: MutationRepresentation) -> MutateResultT:
        raise NotImplementedError


class SetMutatorMixin(MutatorMixin[MutateResultT]):
    def set(
        self, key: AnyKvKey, value: object, *, versioned: bool = False
    ) -> MutateResultT:
        return self.mutate(Set(key, value, versioned=versioned))


class SumMutatorMixin(MutatorMixin[MutateResultT]):
    # The overloads here have two categories: Firstly overloads based on known
    # Known KvNumber enum numbers — bigint, float and u64. Secondly,
    # generic/catch-all for any KvNumberInfo instance.
    @overload
    def sum(
        self,
        key: AnyKvKey,
        delta: JSBigInt,
        number_type: None = None,
        **options: Unpack[SumOptions[int]],
    ) -> MutateResultT: ...

    @overload
    def sum(
        self,
        key: AnyKvKey,
        delta: int | JSBigInt,
        number_type: BigIntKvNumberIdentifier,
        **options: Unpack[SumOptions[int]],
    ) -> MutateResultT: ...

    @overload
    def sum(
        self,
        key: AnyKvKey,
        delta: KvU64,
        number_type: None = None,
        **options: Unpack[SumOptions[int]],
    ) -> MutateResultT: ...

    @overload
    def sum(
        self,
        key: AnyKvKey,
        delta: int | KvU64,
        number_type: U64KvNumberIdentifier,
        **options: Unpack[SumOptions[int]],
    ) -> MutateResultT: ...

    @overload
    def sum(
        self,
        key: AnyKvKey,
        delta: float,
        number_type: FloatKvNumberIdentifier | None = None,
        **options: Unpack[SumOptions[float]],
    ) -> MutateResultT: ...

    @overload
    def sum(
        self,
        key: AnyKvKey,
        delta: NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
        # Can't use float limits unless the float type is explicitly being used,
        # as float is incompatible with the other number types, but int is
        # compatible.
        **options: Unpack[SumOptions[NumberT]],
    ) -> MutateResultT: ...

    def sum(
        self,
        key: AnyKvKey,
        delta: JSBigInt | float | KvU64 | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
        **options: Unpack[SumOptions[NumberT]],
    ) -> MutateResultT:
        delta = cast(Union[NumberT, KvNumberTypeT], delta)
        number_type = cast(
            KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT], number_type
        )
        return self.mutate(Sum(key, delta, number_type, **options))

    def sum_bigint(
        self,
        key: AnyKvKey,
        delta: int | JSBigInt,
        **options: Unpack[SumOptions[int]],
    ) -> MutateResultT:
        return self.sum(key, delta, number_type=KvNumber.bigint, **options)

    def sum_float(
        self,
        key: AnyKvKey,
        delta: float,
        **options: Unpack[SumOptions[float]],
    ) -> MutateResultT:
        return self.sum(key, delta, number_type=KvNumber.float, **options)

    def sum_kvu64(
        self,
        key: AnyKvKey,
        delta: int | KvU64,
        **options: Unpack[SumOptions[int]],
    ) -> MutateResultT:
        return self.sum(key, delta, number_type=KvNumber.u64, **options)


class MinMutatorMixin(MutatorMixin[MutateResultT]):
    @overload
    def min(
        self,
        key: AnyKvKey,
        value: JSBigInt,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def min(
        self,
        key: AnyKvKey,
        value: int | JSBigInt,
        number_type: BigIntKvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def min(
        self,
        key: AnyKvKey,
        value: KvU64,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def min(
        self,
        key: AnyKvKey,
        value: int | KvU64,
        number_type: U64KvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def min(
        self,
        key: AnyKvKey,
        value: float,
        number_type: FloatKvNumberIdentifier | None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def min(
        self,
        key: AnyKvKey,
        value: NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
        # Can't use float limits unless the float type is explicitly being used,
        # as float is incompatible with the other number types, but int is
        # compatible.
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    def min(
        self,
        key: AnyKvKey,
        value: JSBigInt | float | KvU64 | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        value = cast(Union[NumberT, KvNumberTypeT], value)
        number_type = cast(
            KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT], number_type
        )
        return self.mutate(Min(key, value, number_type, **options))

    def min_bigint(
        self,
        key: AnyKvKey,
        value: int | JSBigInt,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        return self.min(key, value, number_type=KvNumber.bigint, **options)

    def min_float(
        self,
        key: AnyKvKey,
        value: float,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        return self.min(key, value, number_type=KvNumber.float, **options)

    def min_kvu64(
        self,
        key: AnyKvKey,
        value: int | KvU64,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        return self.min(key, value, number_type=KvNumber.u64, **options)


class MaxMutatorMixin(MutatorMixin[MutateResultT]):
    @overload
    def max(
        self,
        key: AnyKvKey,
        value: JSBigInt,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def max(
        self,
        key: AnyKvKey,
        value: int | JSBigInt,
        number_type: BigIntKvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def max(
        self,
        key: AnyKvKey,
        value: KvU64,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def max(
        self,
        key: AnyKvKey,
        value: int | KvU64,
        number_type: U64KvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def max(
        self,
        key: AnyKvKey,
        value: float,
        number_type: FloatKvNumberIdentifier | None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    @overload
    def max(
        self,
        key: AnyKvKey,
        value: NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
        # Can't use float limits unless the float type is explicitly being used,
        # as float is incompatible with the other number types, but int is
        # compatible.
        **options: Unpack[MutationOptions],
    ) -> MutateResultT: ...

    def max(
        self,
        key: AnyKvKey,
        value: JSBigInt | float | KvU64 | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        value = cast(Union[NumberT, KvNumberTypeT], value)
        number_type = cast(
            KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT], number_type
        )
        return self.mutate(Max(key, value, number_type, **options))

    def max_bigint(
        self,
        key: AnyKvKey,
        value: int | JSBigInt,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        return self.max(key, value, number_type=KvNumber.bigint, **options)

    def max_float(
        self,
        key: AnyKvKey,
        value: float,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        return self.max(key, value, number_type=KvNumber.float, **options)

    def max_kvu64(
        self,
        key: AnyKvKey,
        value: int | KvU64,
        **options: Unpack[MutationOptions],
    ) -> MutateResultT:
        return self.max(key, value, number_type=KvNumber.u64, **options)


class DeleteMutatorMixin(MutatorMixin[MutateResultT]):
    def delete(self, key: AnyKvKey) -> MutateResultT:
        if isinstance(key, Delete):
            return self.mutate(key)
        return self.mutate(Delete(key))


class EnqueueMixin(Generic[EnqueueResultT]):
    @abstractmethod
    def _enqueue(self, enqueue: Enqueue, /) -> EnqueueResultT:
        raise NotImplementedError

    @overload
    def enqueue(self, enqueue: Enqueue, /) -> EnqueueResultT: ...

    @overload
    def enqueue(
        self,
        message: object,
        *,
        delivery_time: datetime | None = None,
        retry_delays: Backoff | None = None,
        dead_letter_keys: Sequence[AnyKvKey] | None = None,
    ) -> EnqueueResultT: ...

    def enqueue(
        self,
        message: object | Enqueue,
        *,
        delivery_time: datetime | None = None,
        retry_delays: Backoff | None = None,
        dead_letter_keys: Sequence[AnyKvKey] | None = None,
    ) -> EnqueueResultT:
        if isinstance(message, Enqueue):
            enqueue = message
        else:
            enqueue = Enqueue(
                message,
                delivery_time=delivery_time,
                retry_delays=retry_delays,
                dead_letter_keys=dead_letter_keys,
            )
        return self._enqueue(enqueue)


@dataclass(init=False)
class PlannedWrite(
    CheckMixin["PlannedWrite"],
    SetMutatorMixin["PlannedWrite"],
    SumMutatorMixin["PlannedWrite"],
    MinMutatorMixin["PlannedWrite"],
    MaxMutatorMixin["PlannedWrite"],
    DeleteMutatorMixin["PlannedWrite"],
    EnqueueMixin["PlannedWrite"],
    AtomicWriteRepresentationWriter["CompletedWrite"],
):
    kv: KvWriter | None
    checks: MutableSequence[CheckRepresentation]
    mutations: MutableSequence[MutationRepresentation]
    enqueues: MutableSequence[EnqueueRepresentation]
    v8_encoder: Encoder | None

    def __init__(
        self,
        kv: KvWriter | None = None,
        checks: MutableSequence[CheckRepresentation] | None = None,
        mutations: MutableSequence[MutationRepresentation] | None = None,
        enqueues: MutableSequence[EnqueueRepresentation] | None = None,
        *,
        v8_encoder: Encoder | None = None,
    ) -> None:
        self.kv = kv
        self.checks = list(checks or ())
        self.mutations = list(mutations or ())
        self.enqueues = list(enqueues or ())
        self.v8_encoder = v8_encoder

    @override
    async def write(
        self, kv: KvWriter | None = None, *, v8_encoder: Encoder | None = None
    ) -> CompletedWrite:
        _kv = self.kv if kv is None else kv
        if _kv is None:
            raise TypeError(
                f"{type(self).__name__}.write() must get a value for its 'kv' "
                "argument when 'self.kv' isn't set"
            )

        _v8_encoder = self.v8_encoder if v8_encoder is None else v8_encoder
        if _v8_encoder is None:
            _v8_encoder = get_v8_encoder(_kv).value_or(None)
        if _v8_encoder is None:
            raise TypeError(
                f"{type(self).__name__}.write() must get a value for its "
                "'v8_encoder' keyword argument when 'self.v8_encoder' isn't "
                "set and 'kv' does not provide one."
            )

        (pb_atomic_write,) = self.as_protobuf(v8_encoder=_v8_encoder)
        # Copy the write components so that the results are not affected if the
        # PlannedWrite is modified during this write.
        checks = tuple(self.checks)
        mutations = tuple(self.mutations)
        enqueues = tuple(self.enqueues)
        result = await _kv.write(protobuf_atomic_write=pb_atomic_write)

        if is_err(result):
            if isinstance(result.error, CheckFailure):
                check_failure = result.error
                return ConflictedWrite(
                    failed_checks=check_failure.failed_check_indexes,
                    checks=checks,
                    mutations=mutations,
                    enqueues=enqueues,
                    endpoint=check_failure.endpoint,
                    cause=check_failure,
                )
            raise FailedWrite(
                checks=checks,
                mutations=mutations,
                enqueues=enqueues,
                endpoint=result.error.endpoint,
            ) from result.error

        versionstamp, endpoint = result.value
        return CommittedWrite(
            versionstamp=versionstamp,
            checks=checks,
            mutations=mutations,
            enqueues=enqueues,
            endpoint=endpoint,
        )

    def as_protobuf(self, *, v8_encoder: Encoder) -> tuple[AtomicWrite]:
        return (
            AtomicWrite(
                checks=[
                    pb_msg
                    for check in self.checks
                    for pb_msg in check.as_protobuf(v8_encoder=v8_encoder)
                ],
                mutations=[
                    pb_msg
                    for mut in self.mutations
                    for pb_msg in mut.as_protobuf(v8_encoder=v8_encoder)
                ],
                enqueues=[
                    pb_msg
                    for enq in self.enqueues
                    for pb_msg in enq.as_protobuf(v8_encoder=v8_encoder)
                ],
            ),
        )

    @override
    def _check(self, check: CheckRepresentation, /) -> Self:
        self.checks.append(check)
        return self

    @override
    def mutate(self, mutation: MutationRepresentation) -> Self:
        self.mutations.append(mutation)
        return self

    @override
    def _enqueue(self, enqueue: Enqueue, /) -> Self:
        self.enqueues.append(enqueue)
        return self


EMPTY_MAP: Final[Mapping[Any, Any]] = MappingProxyType({})


# TODO: Support capturing retries in the FailedWrite/CommittedWrite?
@dataclass(init=False, unsafe_hash=True)
class FailedWrite(FrozenAfterInitDataclass, AnyFailure, DenoKvError):
    if TYPE_CHECKING:

        def _AnyFailure_marker(self, no_call: Never) -> Never: ...

    checks: Final[Sequence[CheckRepresentation]] = field()
    failed_checks: Final[Sequence[int]] = field()
    has_unknown_conflicts: Final[bool] = field()
    """
    Whether the check(s) that failed are unknown.

    KV servers may or may not report which check(s) failed when a write
    fails due to a check conflict.
    """
    mutations: Final[Sequence[MutationRepresentation]] = field()
    enqueues: Final[Sequence[EnqueueRepresentation]] = field()
    endpoint: Final[EndpointInfo] = field()
    ok: Final[Literal[False]] = False  # noqa: PYI064
    versionstamp: Final[None] = None

    def __init__(
        self,
        checks: Iterable[CheckRepresentation],
        mutations: Iterable[MutationRepresentation],
        enqueues: Iterable[EnqueueRepresentation],
        endpoint: EndpointInfo,
        *,
        cause: BaseException | None = None,
    ) -> None:
        super(FailedWrite, self).__init__()
        self.checks = tuple(checks)  # type: ignore[misc] # Cannot assign to final
        # Allow subclass to initialise failed_checks
        if not hasattr(self, "failed_checks"):
            self.failed_checks = tuple()  # type: ignore[misc] # Cannot assign to final
            self.has_unknown_conflicts = False  # type: ignore[misc] # Cannot assign to final
        self.mutations = tuple(mutations)  # type: ignore[misc] # Cannot assign to final
        self.enqueues = tuple(enqueues)  # type: ignore[misc] # Cannot assign to final
        self.endpoint = endpoint  # type: ignore[misc] # Cannot assign to final
        self.__cause__ = cause

    @property
    def conflicts(self) -> Mapping[AnyKvKey, CheckRepresentation]:
        checks = self.checks
        return {checks[i].key: checks[i] for i in self.failed_checks}

    def _get_cause_description(self) -> str:
        if self.__cause__:
            return type(self.__cause__).__name__
        return "unspecified cause"

    @property
    def message(self) -> str:
        # TODO: after xxx attempts?
        return (
            f"to {str(self.endpoint.url)!r} "
            f"due to {self._get_cause_description()}, "
            f"with {len(self.checks)} checks, "
            f"{len(self.mutations)} mutations, "
            f"{len(self.enqueues)} enqueues"
        )

    def __str__(self) -> str:
        return f"Write failed {self.message}"

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.message}>"


def _normalise_failed_checks(
    failed_checks: Iterable[int], checks: tuple[CheckRepresentation, ...]
) -> tuple[int, ...]:
    failed_checks = tuple(sorted(failed_checks))
    # If the server didn't report failed checks and there was only one check, we
    # know the single check must have failed, so report that.
    if len(failed_checks) == 0 and len(checks) == 1:
        return (0,)
    if failed_checks and (failed_checks[0] < 0 or failed_checks[-1] >= len(checks)):
        raise ValueError("failed_checks contains out-of-bounds index")
    return failed_checks


class ConflictedWrite(FailedWrite):
    def __init__(
        self,
        failed_checks: Iterable[int] | None,
        checks: Iterable[CheckRepresentation],
        mutations: Iterable[MutationRepresentation],
        enqueues: Iterable[EnqueueRepresentation],
        endpoint: EndpointInfo,
        *,
        cause: BaseException | None = None,
    ) -> None:
        _checks = tuple(checks)
        self.failed_checks = _normalise_failed_checks(  # type: ignore[misc] # Cannot assign to final attribute "failed_checks"
            failed_checks or [],
            checks=_checks,
        )
        self.has_unknown_conflicts = len(self.failed_checks) == 0  # type: ignore[misc] # Cannot assign to final attribute
        super(ConflictedWrite, self).__init__(
            _checks, mutations, enqueues, endpoint, cause=cause
        )

    @property
    def message(self) -> str:
        return (
            f"NOT APPLIED to {str(self.endpoint.url)!r} with "
            f"{len(self.conflicts)}/{len(self.checks)} checks CONFLICTING, "
            f"{len(self.mutations)} mutations, "
            f"{len(self.enqueues)} enqueues"
        )

    def __str__(self) -> str:
        return f"Write {self.message}"


@dataclass(init=False, unsafe_hash=True, **slots_if310())
class CommittedWrite(FrozenAfterInitDataclass, AnySuccess):
    if TYPE_CHECKING:

        def _AnySuccess_marker(self, no_call: Never) -> Never: ...

    ok: Final[Literal[True]]  # noqa: PYI064
    conflicts: Final[Mapping[KvKey, CheckRepresentation]]  # empty
    has_unknown_conflicts: Final[Literal[False]]
    versionstamp: Final[VersionStamp]
    checks: Final[Sequence[CheckRepresentation]]
    mutations: Final[Sequence[MutationRepresentation]]
    enqueues: Final[Sequence[EnqueueRepresentation]]
    endpoint: Final[EndpointInfo]

    def __init__(
        self,
        versionstamp: VersionStamp,
        checks: Sequence[CheckRepresentation],
        mutations: Sequence[MutationRepresentation],
        enqueues: Sequence[EnqueueRepresentation],
        endpoint: EndpointInfo,
    ) -> None:
        self.ok = True
        self.conflicts = EMPTY_MAP
        self.has_unknown_conflicts = False
        self.versionstamp = versionstamp
        self.checks = tuple(checks)
        self.mutations = tuple(mutations)
        self.enqueues = tuple(enqueues)
        self.endpoint = endpoint

    @property
    def _message(self) -> str:
        return (
            f"version 0x{self.versionstamp} to {str(self.endpoint.url)!r} with "
            f"{len(self.checks)} checks, "
            f"{len(self.mutations)} mutations, "
            f"{len(self.enqueues)} enqueues"
        )

    def __str__(self) -> str:
        return f"Write committed {self._message}"

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self._message}>"


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


class CheckRepresentation(
    SingleProtobufMessageRepresentation[dp_protobuf.Check], AnyKeyVersion
):
    __slots__ = ()

    # Check never needs an Encoder, so override the signature to make it optional.
    @override
    @abstractmethod
    def as_protobuf(
        self, *, v8_encoder: Encoder | None = None
    ) -> tuple[dp_protobuf.Check]: ...


@dataclass(frozen=True, **slots_if310())
class Check(CheckRepresentation, AnyKeyVersion):
    """
    A condition that must hold for a database write operation to be applied.

    By applying checks to a write operation, writes can ensure that the changes
    they make are changing the existing values they expect. Without appropriate
    checks, write operations could overwrite another writer's changes to the
    database.

    Checks are part of Deno KV's
    [Multi-version concurrency control](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
    support.
    """

    key: AnyKvKey
    """The key that the check applies to."""
    versionstamp: VersionStamp | None
    """
    The version that that the key's value must have for the check to succeed.

    `None` means the key must not have a value set for the check to succeed.
    """

    @classmethod
    def for_key_with_version(cls, key: AnyKvKey, versionstamp: VersionStamp) -> Self:
        return cls(key, versionstamp)

    @classmethod
    def for_key_not_set(cls, key: AnyKvKey) -> Self:
        return cls(key, versionstamp=None)

    @override
    def as_protobuf(
        self, *, v8_encoder: Encoder | None = None
    ) -> tuple[dp_protobuf.Check]:
        return (
            dp_protobuf.Check(key=pack_key(self.key), versionstamp=self.versionstamp),
        )


class MutationRepresentation(ProtobufMessageRepresentation[dp_protobuf.Mutation]):
    __slots__ = ()

    @abstractmethod
    def as_protobuf(self, *, v8_encoder: Encoder) -> Sequence[dp_protobuf.Mutation]: ...


@dataclass(init=False, **slots_if310())
class Mutation(FrozenAfterInitDataclass, MutationRepresentation):
    key: AnyKvKey
    expire_at: datetime | None

    def __init__(self, key: AnyKvKey, **options: Unpack[MutationOptions]) -> None:
        if type(self) is Mutation:
            raise TypeError("cannot create Mutation instances directly")
        self.key = key
        self.expire_at = options.get("expire_at")

    def expire_at_ms(self) -> int:
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

    @override
    def as_protobuf(self, *, v8_encoder: Encoder) -> tuple[dp_protobuf.Mutation]:
        return (
            dp_protobuf.Mutation(
                mutation_type=dp_protobuf.MutationType.M_SET_SUFFIX_VERSIONSTAMPED_KEY
                if self.versioned
                else dp_protobuf.MutationType.M_SET,
                key=pack_key(self.key),
                value=encode_kv_write_value(self.value, v8_encoder=v8_encoder),
                expire_at_ms=self.expire_at_ms(),
            ),
        )


class LimitExceededPolicy(EvalEnumRepr, Enum):
    ABORT = "abort"
    CLAMP = "clamp"
    WRAP = "wrap"


LimitExceededInput = Literal[
    "abort",
    "clamp",
    LimitExceededPolicy.ABORT,
    LimitExceededPolicy.CLAMP,
]


@dataclass(frozen=True, **slots_if310())
class Limit(Container[NumberT_co]):
    """
    A range of numbers used to define the allowed range of `Sum` operations.

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

    min: NumberT_co | None = field(default=None)
    max: NumberT_co | None = field(default=None)
    limit_exceeded: LimitExceededPolicy = field(default=LimitExceededPolicy.ABORT)

    if TYPE_CHECKING:
        # Customise the init signature to:
        # - accept string values to init limit_exceeded
        # - Hide the LimitExceededPolicy.WRAP option from the init signature so
        #   that using it is a type error. There's no way to use a custom wrap
        #   limit, only LIMIT_KVU64 is supported.
        def __init__(
            self,
            min: NumberT_co | None = None,
            max: NumberT_co | None = None,
            limit_exceeded: LimitExceededInput | None = LimitExceededPolicy.ABORT,
        ) -> None:
            pass

    def __post_init__(self) -> None:
        # Support specifying limit_exceeded via the enum's string values.
        if not isinstance(self.limit_exceeded, LimitExceededPolicy):
            object.__setattr__(
                self, "limit_exceeded", LimitExceededPolicy(self.limit_exceeded)
            )

    def __contains__(self, x: object) -> bool:
        if not isinstance(x, (int, float)):
            return False
        return (self.min is None or self.min <= x) and (
            self.max is None or self.max >= x
        )


LIMIT_KVU64 = Limit(
    min=KvU64.RANGE[0],
    max=KvU64.RANGE[-1],
    # Not normally allowed by types because only LIMIT_KVU64 can use WRAP.
    limit_exceeded=cast(LimitExceededInput, LimitExceededPolicy.WRAP),
)
LIMIT_UNLIMITED = Limit[Any]()


class AmbiguousNumberWarning(UserWarning):
    pass


@dataclass(init=False, **slots_if310())
class NumberMutation(Mutation, Generic[KvNumberNameT_co, NumberT_co, KvNumberTypeT_co]):
    number_type: KvNumberInfo[KvNumberNameT_co, NumberT_co, KvNumberTypeT_co]

    def __init__(
        self,
        *,
        key: AnyKvKey,
        expire_at: datetime | None = None,
        number_type: KvNumberInfo[KvNumberNameT_co, NumberT_co, KvNumberTypeT_co],
    ) -> None:
        super(NumberMutation, self).__init__(key, expire_at=expire_at)
        self.number_type = number_type

    @classmethod
    def _resolve_number_value_type(
        cls,
        value: JSBigInt | KvU64 | float | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
    ) -> tuple[NumberT, KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]]:
        resolved_number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        if isinstance(number_type, KvNumberInfo):
            resolved_number_type = number_type
        elif number_type is not None:
            number_identifier: KvNumberIdentifier = number_type
            resolved_number_type = KvNumber.resolve(number_identifier).value  # pyright: ignore[reportAssignmentType]
        else:
            known_number = cast(Union[KvU64, JSBigInt, float], value)
            resolved_number_type = KvNumber.resolve(number=known_number).value  # pyright: ignore[reportAssignmentType]

        resolved_value = cast(Union[KvNumberTypeT, NumberT], value)

        return (
            resolved_number_type.as_py_number(resolved_value),
            resolved_number_type,
        )


@dataclass(init=False, **slots_if310())
class Sum(NumberMutation[KvNumberNameT_co, NumberT_co, KvNumberTypeT_co]):
    _INIT_OPTIONS: ClassVar = frozenset(
        ["clamp_over", "clamp_under", "abort_over", "abort_under", "limit", "expire_at"]
    )
    delta: Final[NumberT_co]  # type: ignore[misc]
    limit: Final[Limit[NumberT_co]]  # type: ignore[misc]

    @override
    def as_protobuf(
        self, *, v8_encoder: Encoder | None = None
    ) -> Sequence[dp_protobuf.Mutation]:
        return self.number_type.get_sum_mutations(self, v8_encoder=v8_encoder)

    @overload
    def __init__(  # pyright: ignore[reportOverlappingOverload]
        self: BigIntSum,
        key: AnyKvKey,
        delta: JSBigInt,
        number_type: None = None,
        **options: Unpack[SumOptions[int]],
    ) -> None: ...

    @overload
    def __init__(
        self: BigIntSum,
        key: AnyKvKey,
        delta: int | JSBigInt,
        number_type: BigIntKvNumberIdentifier,
        **options: Unpack[SumOptions[int]],
    ) -> None: ...

    @overload
    def __init__(
        self: U64Sum,
        key: AnyKvKey,
        delta: KvU64,
        number_type: None = None,
        **options: Unpack[SumOptions[int]],
    ) -> None: ...

    @overload
    def __init__(
        self: U64Sum,
        key: AnyKvKey,
        delta: int | KvU64,
        number_type: U64KvNumberIdentifier,
        **options: Unpack[SumOptions[int]],
    ) -> None: ...

    @overload
    def __init__(
        self: FloatSum,
        key: AnyKvKey,
        delta: float,
        number_type: FloatKvNumberIdentifier | None = None,
        **options: Unpack[SumOptions[float]],
    ) -> None: ...

    @overload
    def __init__(
        self: Sum[KvNumberNameT, NumberT, KvNumberTypeT],
        key: AnyKvKey,
        delta: NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
        # Can't use float limits unless the float type is explicitly being used,
        # as float is incompatible with the other number types, but int is
        # compatible.
        **options: Unpack[SumOptions[NumberT]],
    ) -> None: ...

    def __init__(
        self: Sum[KvNumberNameT, NumberT, KvNumberTypeT],
        key: AnyKvKey,
        delta: JSBigInt | KvU64 | float | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
        **options: Unpack[SumOptions[int | float | NumberT]],
    ) -> None:
        if options.keys() - self._INIT_OPTIONS:
            arg = next(iter(options.keys() - self._INIT_OPTIONS))
            raise TypeError(
                f"Sum.__init__() got an unexpected keyword argument {arg!r}"
            )
        resolved_delta, resolved_number_type = Sum._resolve_number_value_type(
            delta, number_type
        )
        super(Sum, self).__init__(
            key=key,
            expire_at=options.pop("expire_at", None),
            number_type=resolved_number_type,
        )
        self.limit = (
            Sum._create_limit(**cast(LimitOptions[NumberT], options))
            or resolved_number_type.default_limit
        )
        resolved_number_type.validate_limit(self.limit)
        self.delta = resolved_delta

    @classmethod
    def _create_limit(
        cls, **options: Unpack[LimitOptions[NumberT]]
    ) -> Limit[NumberT] | None:
        limits = dict[Literal["limit=", "clamp_*=", "abort_*="], Limit[NumberT]]()

        if limit := options.get("limit"):
            limits["limit="] = limit

        if "clamp_under" in options or "clamp_over" in options:
            limits["clamp_*="] = Limit(
                min=options.get("clamp_under"),
                max=options.get("clamp_over"),
                limit_exceeded=LimitExceededPolicy.CLAMP,
            )

        if "abort_under" in options or "abort_over" in options:
            limits["abort_*="] = Limit(
                min=options.get("abort_under"),
                max=options.get("abort_over"),
                limit_exceeded=LimitExceededPolicy.ABORT,
            )

        if len(limits) > 1:
            options_used = ", ".join(sorted(limits))
            raise with_notes(
                ValueError(
                    f"Limit keyword arguments in conflict: "
                    f"Options {options_used} cannot be used together."
                ),
                "Use limit=Limit(limit_exceeded=..., ...) to create a limit "
                "with a dynamic type.",
            )
        return next(iter(limits.values()), None)


BigIntSum: TypeAlias = Sum[Literal["bigint"], int, JSBigInt]
FloatSum: TypeAlias = Sum[Literal["float"], float, float]
U64Sum: TypeAlias = Sum[Literal["u64"], int, KvU64]


@dataclass(init=False, **slots_if310())
class Min(NumberMutation[KvNumberNameT_co, NumberT_co, KvNumberTypeT_co]):
    value: Final[NumberT_co]  # type: ignore[misc]

    @overload
    def __init__(  # pyright: ignore[reportOverlappingOverload]
        self: BigIntMin,
        key: AnyKvKey,
        value: JSBigInt,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: BigIntMin,
        key: AnyKvKey,
        value: int | JSBigInt,
        number_type: BigIntKvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: U64Min,
        key: AnyKvKey,
        value: KvU64,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: U64Min,
        key: AnyKvKey,
        value: int | KvU64,
        number_type: U64KvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: FloatMin,
        key: AnyKvKey,
        value: float,
        number_type: FloatKvNumberIdentifier | None = None,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: Min[KvNumberNameT, NumberT, KvNumberTypeT],
        key: AnyKvKey,
        value: NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
        # Can't use float limits unless the float type is explicitly being used,
        # as float is incompatible with the other number types, but int is
        # compatible.
        **options: Unpack[MutationOptions],
    ) -> None: ...

    def __init__(
        self: Min[KvNumberNameT, NumberT, KvNumberTypeT],
        key: AnyKvKey,
        value: JSBigInt | KvU64 | float | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
        **options: Unpack[MutationOptions],
    ) -> None:
        resolved_number, resolved_number_type = Min._resolve_number_value_type(
            value, number_type
        )
        super(Min, self).__init__(key=key, number_type=resolved_number_type, **options)
        self.value = resolved_number

    @override
    def as_protobuf(self, *, v8_encoder: Encoder) -> Sequence[dp_protobuf.Mutation]:
        return self.number_type.get_min_mutations(self, v8_encoder=v8_encoder)


BigIntMin: TypeAlias = Min[Literal["bigint"], int, JSBigInt]
FloatMin: TypeAlias = Min[Literal["float"], float, float]
U64Min: TypeAlias = Min[Literal["u64"], int, KvU64]


@dataclass(init=False, **slots_if310())
class Max(NumberMutation[KvNumberNameT_co, NumberT_co, KvNumberTypeT_co]):
    value: Final[NumberT_co]  # type: ignore[misc]

    @overload
    def __init__(  # pyright: ignore[reportOverlappingOverload]
        self: BigIntMax,
        key: AnyKvKey,
        value: JSBigInt,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: BigIntMax,
        key: AnyKvKey,
        value: int | JSBigInt,
        number_type: BigIntKvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: U64Max,
        key: AnyKvKey,
        value: KvU64,
        number_type: None = None,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: U64Max,
        key: AnyKvKey,
        value: int | KvU64,
        number_type: U64KvNumberIdentifier,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: FloatMax,
        key: AnyKvKey,
        value: float,
        number_type: FloatKvNumberIdentifier | None = None,
        **options: Unpack[MutationOptions],
    ) -> None: ...

    @overload
    def __init__(
        self: Max[KvNumberNameT, NumberT, KvNumberTypeT],
        key: AnyKvKey,
        value: NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT],
        # Can't use float limits unless the float type is explicitly being used,
        # as float is incompatible with the other number types, but int is
        # compatible.
        **options: Unpack[MutationOptions],
    ) -> None: ...

    def __init__(
        self: Max[KvNumberNameT, NumberT, KvNumberTypeT],
        key: AnyKvKey,
        value: JSBigInt | KvU64 | float | NumberT | KvNumberTypeT,
        number_type: KvNumberInfo[KvNumberNameT, NumberT, KvNumberTypeT]
        | KvNumberIdentifier
        | None = None,
        **options: Unpack[MutationOptions],
    ) -> None:
        resolved_number, resolved_number_type = Max._resolve_number_value_type(
            value, number_type
        )
        super(Max, self).__init__(key=key, number_type=resolved_number_type, **options)
        self.value = resolved_number

    @override
    def as_protobuf(self, *, v8_encoder: Encoder) -> Sequence[dp_protobuf.Mutation]:
        return self.number_type.get_max_mutations(self, v8_encoder=v8_encoder)


BigIntMax: TypeAlias = Max[Literal["bigint"], int, JSBigInt]
FloatMax: TypeAlias = Max[Literal["float"], float, float]
U64Max: TypeAlias = Max[Literal["u64"], int, KvU64]


@dataclass(**slots_if310())
class Delete(Mutation):
    def __init__(self, key: AnyKvKey) -> None:
        super(Delete, self).__init__(key, expire_at=None)

    @override
    def as_protobuf(
        self, *, v8_encoder: Encoder | None = None
    ) -> tuple[dp_protobuf.Mutation]:
        return (
            dp_protobuf.Mutation(
                mutation_type=dp_protobuf.MutationType.M_DELETE, key=pack_key(self.key)
            ),
        )


DEFAULT_ENQUEUE_RETRY_DELAYS = ExponentialBackoff(
    initial_interval_seconds=1, multiplier=3
)
DEFAULT_ENQUEUE_RETRY_DELAY_COUNT = 10


class EnqueueRepresentation(SingleProtobufMessageRepresentation[dp_protobuf.Enqueue]):
    __slots__ = ()


@dataclass(init=False, **slots_if310())
class Enqueue(FrozenAfterInitDataclass, EnqueueRepresentation):
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

    @override
    def as_protobuf(self, *, v8_encoder: Encoder) -> tuple[dp_protobuf.Enqueue]:
        deadline_ms = None
        if self.delivery_time is not None:
            deadline_ms = int(self.delivery_time.timestamp() * 1000)
        return (
            dp_protobuf.Enqueue(
                payload=bytes(v8_encoder.encode(self.message)),
                keys_if_undelivered=[pack_key(k) for k in self.dead_letter_keys],
                deadline_ms=deadline_ms,
                backoff_schedule=self._evaluate_backoff_schedule(),
            ),
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


WriteOperation: TypeAlias = Union[
    CheckRepresentation, MutationRepresentation, EnqueueRepresentation
]
