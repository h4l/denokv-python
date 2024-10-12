from typing import cast

import pytest

from denokv._datapath_pb2 import AtomicWriteStatus
from denokv._datapath_pb2 import MutationType
from denokv._datapath_pb2 import ValueEncoding
from denokv._pycompat.protobuf import enum_name


def test_enum_name() -> None:
    assert (
        enum_name(AtomicWriteStatus, AtomicWriteStatus.AW_CHECK_FAILURE)
        == "AW_CHECK_FAILURE"
    )
    assert enum_name(ValueEncoding, ValueEncoding.VE_LE64) == "VE_LE64"
    assert enum_name(MutationType, MutationType.M_DELETE) == "M_DELETE"

    with pytest.raises(
        ValueError, match=r"Enum ValueEncoding has no name defined for value 100"
    ):
        assert enum_name(ValueEncoding, cast(ValueEncoding, 100))
