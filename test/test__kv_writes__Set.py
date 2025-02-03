from v8serialize import Encoder

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_values import KvU64
from denokv._kv_writes import Set
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.kv_keys import KvKey

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()


def test_constructors() -> None:
    value = {"foo": "bar"}
    instance = Set(KvKey("a"), value)
    assert instance.key == KvKey("a")
    assert instance.value is value
    assert instance.versioned is False
    assert instance.expire_at is None

    instance = Set(KvKey("a"), value, expire_at=T1, versioned=True)
    assert instance.key == KvKey("a")
    assert instance.value is value
    assert instance.versioned is True
    assert instance.expire_at == T1


def test_as_protobuf(v8_encoder: Encoder) -> None:
    v8_value = {"foo": "bar"}
    instance = Set(KvKey("a"), v8_value)

    assert instance.as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.Mutation(
            mutation_type=datapath_pb2.M_SET,
            key=bytes(KvKey("a")),
            value=datapath_pb2.KvValue(
                data=bytes(v8_encoder.encode(v8_value)), encoding=datapath_pb2.VE_V8
            ),
        ),
    )

    byte_value = b"\x00\xff"
    instance = Set(KvKey("a"), byte_value)

    assert instance.as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.Mutation(
            mutation_type=datapath_pb2.M_SET,
            key=bytes(KvKey("a")),
            value=datapath_pb2.KvValue(data=byte_value, encoding=datapath_pb2.VE_BYTES),
        ),
    )

    kvu64_value = KvU64(2)
    instance = Set(KvKey("a"), kvu64_value)

    assert instance.as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.Mutation(
            mutation_type=datapath_pb2.M_SET,
            key=bytes(KvKey("a")),
            value=datapath_pb2.KvValue(
                data=bytes(kvu64_value), encoding=datapath_pb2.VE_LE64
            ),
        ),
    )

    instance = Set(KvKey("a"), v8_value, expire_at=T1, versioned=True)

    assert instance.as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.Mutation(
            mutation_type=datapath_pb2.M_SET_SUFFIX_VERSIONSTAMPED_KEY,
            key=bytes(KvKey("a")),
            value=datapath_pb2.KvValue(
                data=bytes(v8_encoder.encode(v8_value)), encoding=datapath_pb2.VE_V8
            ),
            expire_at_ms=int(T1.timestamp() * 1000),
        ),
    )
