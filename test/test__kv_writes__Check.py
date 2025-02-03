from v8serialize import Encoder

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_values import VersionStamp
from denokv._kv_writes import Check
from denokv.kv_keys import KvKey


def test_constructors() -> None:
    assert Check(KvKey("a"), VersionStamp(1)) == Check.for_key_with_version(
        KvKey("a"), VersionStamp(1)
    )
    assert Check(KvKey("a"), None) == Check.for_key_not_set(KvKey("a"))


def test_as_protobuf(v8_encoder: Encoder) -> None:
    protobuf = (
        datapath_pb2.Check(key=bytes(KvKey("a")), versionstamp=bytes(VersionStamp(1))),
    )
    # v8_encoder is optional
    assert Check(KvKey("a"), VersionStamp(1)).as_protobuf() == protobuf
    assert Check(KvKey("a"), VersionStamp(1)).as_protobuf(v8_encoder=None) == protobuf

    assert (
        Check(KvKey("a"), VersionStamp(1)).as_protobuf(v8_encoder=v8_encoder)
        == protobuf
    )


def test_as_protobuf__empty_version_is_different_to_zero_version() -> None:
    assert datapath_pb2.Check(key=bytes(KvKey("a"))) == datapath_pb2.Check(
        key=bytes(KvKey("a")), versionstamp=b""
    )
    assert datapath_pb2.Check(key=bytes(KvKey("a"))) != datapath_pb2.Check(
        versionstamp=VersionStamp(0)
    )
