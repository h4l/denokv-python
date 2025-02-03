from datetime import datetime

from v8serialize import Encoder

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_writes import Delete
from denokv.kv_keys import KvKey

T1 = datetime.fromisoformat("2000-01-02T03:04:05.6Z")


def test_constructors() -> None:
    instance = Delete(KvKey("a"))
    assert instance.key == KvKey("a")


def test_as_protobuf(v8_encoder: Encoder) -> None:
    delete = Delete(KvKey("a"))
    assert delete.as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.Mutation(
            key=bytes(KvKey("a")), mutation_type=datapath_pb2.M_DELETE
        ),
    )
