from __future__ import annotations

from typing import overload

from denokv._datapath_pb2 import AtomicWriteStatus
from denokv._datapath_pb2 import MutationType
from denokv._datapath_pb2 import ValueEncoding


@overload
def enum_name(enum_type: type[AtomicWriteStatus], value: AtomicWriteStatus) -> str: ...
@overload
def enum_name(enum_type: type[MutationType], value: MutationType) -> str: ...
@overload
def enum_name(enum_type: type[ValueEncoding], value: ValueEncoding) -> str: ...
def enum_name(
    enum_type: type, value: MutationType | ValueEncoding | AtomicWriteStatus
) -> str:
    return enum_type.Name(value)  # type: ignore[no-any-return,attr-defined]
