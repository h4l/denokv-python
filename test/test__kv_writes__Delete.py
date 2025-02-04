import pytest
from v8serialize import Encoder

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_writes import Delete
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.kv_keys import KvKey
from test.denokv_testing import create_dataclass_slots_test

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()


@pytest.fixture
def instance() -> Delete:
    return Delete(KvKey("a"))


test_instances_dont_have_dict_because_of_slots = create_dataclass_slots_test()


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
