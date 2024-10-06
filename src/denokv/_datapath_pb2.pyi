from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SnapshotReadStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    SR_UNSPECIFIED: _ClassVar[SnapshotReadStatus]
    SR_SUCCESS: _ClassVar[SnapshotReadStatus]
    SR_READ_DISABLED: _ClassVar[SnapshotReadStatus]

class MutationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    M_UNSPECIFIED: _ClassVar[MutationType]
    M_SET: _ClassVar[MutationType]
    M_DELETE: _ClassVar[MutationType]
    M_SUM: _ClassVar[MutationType]
    M_MAX: _ClassVar[MutationType]
    M_MIN: _ClassVar[MutationType]
    M_SET_SUFFIX_VERSIONSTAMPED_KEY: _ClassVar[MutationType]

class ValueEncoding(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    VE_UNSPECIFIED: _ClassVar[ValueEncoding]
    VE_V8: _ClassVar[ValueEncoding]
    VE_LE64: _ClassVar[ValueEncoding]
    VE_BYTES: _ClassVar[ValueEncoding]

class AtomicWriteStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    AW_UNSPECIFIED: _ClassVar[AtomicWriteStatus]
    AW_SUCCESS: _ClassVar[AtomicWriteStatus]
    AW_CHECK_FAILURE: _ClassVar[AtomicWriteStatus]
    AW_WRITE_DISABLED: _ClassVar[AtomicWriteStatus]
SR_UNSPECIFIED: SnapshotReadStatus
SR_SUCCESS: SnapshotReadStatus
SR_READ_DISABLED: SnapshotReadStatus
M_UNSPECIFIED: MutationType
M_SET: MutationType
M_DELETE: MutationType
M_SUM: MutationType
M_MAX: MutationType
M_MIN: MutationType
M_SET_SUFFIX_VERSIONSTAMPED_KEY: MutationType
VE_UNSPECIFIED: ValueEncoding
VE_V8: ValueEncoding
VE_LE64: ValueEncoding
VE_BYTES: ValueEncoding
AW_UNSPECIFIED: AtomicWriteStatus
AW_SUCCESS: AtomicWriteStatus
AW_CHECK_FAILURE: AtomicWriteStatus
AW_WRITE_DISABLED: AtomicWriteStatus

class SnapshotRead(_message.Message):
    __slots__ = ["ranges"]
    RANGES_FIELD_NUMBER: _ClassVar[int]
    ranges: _containers.RepeatedCompositeFieldContainer[ReadRange]
    def __init__(self, ranges: _Optional[_Iterable[_Union[ReadRange, _Mapping]]] = ...) -> None: ...

class SnapshotReadOutput(_message.Message):
    __slots__ = ["ranges", "read_disabled", "read_is_strongly_consistent", "status"]
    RANGES_FIELD_NUMBER: _ClassVar[int]
    READ_DISABLED_FIELD_NUMBER: _ClassVar[int]
    READ_IS_STRONGLY_CONSISTENT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ranges: _containers.RepeatedCompositeFieldContainer[ReadRangeOutput]
    read_disabled: bool
    read_is_strongly_consistent: bool
    status: SnapshotReadStatus
    def __init__(self, ranges: _Optional[_Iterable[_Union[ReadRangeOutput, _Mapping]]] = ..., read_disabled: bool = ..., read_is_strongly_consistent: bool = ..., status: _Optional[_Union[SnapshotReadStatus, str]] = ...) -> None: ...

class ReadRange(_message.Message):
    __slots__ = ["start", "end", "limit", "reverse"]
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    REVERSE_FIELD_NUMBER: _ClassVar[int]
    start: bytes
    end: bytes
    limit: int
    reverse: bool
    def __init__(self, start: _Optional[bytes] = ..., end: _Optional[bytes] = ..., limit: _Optional[int] = ..., reverse: bool = ...) -> None: ...

class ReadRangeOutput(_message.Message):
    __slots__ = ["values"]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[KvEntry]
    def __init__(self, values: _Optional[_Iterable[_Union[KvEntry, _Mapping]]] = ...) -> None: ...

class AtomicWrite(_message.Message):
    __slots__ = ["checks", "mutations", "enqueues"]
    CHECKS_FIELD_NUMBER: _ClassVar[int]
    MUTATIONS_FIELD_NUMBER: _ClassVar[int]
    ENQUEUES_FIELD_NUMBER: _ClassVar[int]
    checks: _containers.RepeatedCompositeFieldContainer[Check]
    mutations: _containers.RepeatedCompositeFieldContainer[Mutation]
    enqueues: _containers.RepeatedCompositeFieldContainer[Enqueue]
    def __init__(self, checks: _Optional[_Iterable[_Union[Check, _Mapping]]] = ..., mutations: _Optional[_Iterable[_Union[Mutation, _Mapping]]] = ..., enqueues: _Optional[_Iterable[_Union[Enqueue, _Mapping]]] = ...) -> None: ...

class AtomicWriteOutput(_message.Message):
    __slots__ = ["status", "versionstamp", "failed_checks"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    VERSIONSTAMP_FIELD_NUMBER: _ClassVar[int]
    FAILED_CHECKS_FIELD_NUMBER: _ClassVar[int]
    status: AtomicWriteStatus
    versionstamp: bytes
    failed_checks: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, status: _Optional[_Union[AtomicWriteStatus, str]] = ..., versionstamp: _Optional[bytes] = ..., failed_checks: _Optional[_Iterable[int]] = ...) -> None: ...

class Check(_message.Message):
    __slots__ = ["key", "versionstamp"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VERSIONSTAMP_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    versionstamp: bytes
    def __init__(self, key: _Optional[bytes] = ..., versionstamp: _Optional[bytes] = ...) -> None: ...

class Mutation(_message.Message):
    __slots__ = ["key", "value", "mutation_type", "expire_at_ms", "sum_min", "sum_max", "sum_clamp"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    MUTATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    EXPIRE_AT_MS_FIELD_NUMBER: _ClassVar[int]
    SUM_MIN_FIELD_NUMBER: _ClassVar[int]
    SUM_MAX_FIELD_NUMBER: _ClassVar[int]
    SUM_CLAMP_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: KvValue
    mutation_type: MutationType
    expire_at_ms: int
    sum_min: bytes
    sum_max: bytes
    sum_clamp: bool
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[_Union[KvValue, _Mapping]] = ..., mutation_type: _Optional[_Union[MutationType, str]] = ..., expire_at_ms: _Optional[int] = ..., sum_min: _Optional[bytes] = ..., sum_max: _Optional[bytes] = ..., sum_clamp: bool = ...) -> None: ...

class KvValue(_message.Message):
    __slots__ = ["data", "encoding"]
    DATA_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    data: bytes
    encoding: ValueEncoding
    def __init__(self, data: _Optional[bytes] = ..., encoding: _Optional[_Union[ValueEncoding, str]] = ...) -> None: ...

class KvEntry(_message.Message):
    __slots__ = ["key", "value", "encoding", "versionstamp"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    ENCODING_FIELD_NUMBER: _ClassVar[int]
    VERSIONSTAMP_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    encoding: ValueEncoding
    versionstamp: bytes
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ..., encoding: _Optional[_Union[ValueEncoding, str]] = ..., versionstamp: _Optional[bytes] = ...) -> None: ...

class Enqueue(_message.Message):
    __slots__ = ["payload", "deadline_ms", "keys_if_undelivered", "backoff_schedule"]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    KEYS_IF_UNDELIVERED_FIELD_NUMBER: _ClassVar[int]
    BACKOFF_SCHEDULE_FIELD_NUMBER: _ClassVar[int]
    payload: bytes
    deadline_ms: int
    keys_if_undelivered: _containers.RepeatedScalarFieldContainer[bytes]
    backoff_schedule: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, payload: _Optional[bytes] = ..., deadline_ms: _Optional[int] = ..., keys_if_undelivered: _Optional[_Iterable[bytes]] = ..., backoff_schedule: _Optional[_Iterable[int]] = ...) -> None: ...

class Watch(_message.Message):
    __slots__ = ["keys"]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[WatchKey]
    def __init__(self, keys: _Optional[_Iterable[_Union[WatchKey, _Mapping]]] = ...) -> None: ...

class WatchOutput(_message.Message):
    __slots__ = ["status", "keys"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    KEYS_FIELD_NUMBER: _ClassVar[int]
    status: SnapshotReadStatus
    keys: _containers.RepeatedCompositeFieldContainer[WatchKeyOutput]
    def __init__(self, status: _Optional[_Union[SnapshotReadStatus, str]] = ..., keys: _Optional[_Iterable[_Union[WatchKeyOutput, _Mapping]]] = ...) -> None: ...

class WatchKey(_message.Message):
    __slots__ = ["key"]
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    def __init__(self, key: _Optional[bytes] = ...) -> None: ...

class WatchKeyOutput(_message.Message):
    __slots__ = ["changed", "entry_if_changed"]
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    ENTRY_IF_CHANGED_FIELD_NUMBER: _ClassVar[int]
    changed: bool
    entry_if_changed: KvEntry
    def __init__(self, changed: bool = ..., entry_if_changed: _Optional[_Union[KvEntry, _Mapping]] = ...) -> None: ...
