from abc import ABC
from abc import abstractmethod

from google.protobuf.message import Message
from v8serialize import Encoder

from denokv._datapath_pb2 import AtomicWrite
from denokv._pycompat.typing import Generic
from denokv._pycompat.typing import Protocol
from denokv._pycompat.typing import TypeVar

WriteResultT = TypeVar("WriteResultT")
MessageT_co = TypeVar("MessageT_co", bound=Message, covariant=True)


class ProtobufMessageRepresentation(Generic[MessageT_co], ABC):
    """An object that can represent itself as a protobuf Message."""

    __slots__ = ()

    @abstractmethod
    def as_protobuf(self, *, v8_encoder: Encoder) -> MessageT_co: ...


class AtomicWriteRepresentation(ProtobufMessageRepresentation[AtomicWrite]):
    __slots__ = ()


class KvWriter(Protocol):
    async def write(
        self, atomic_write: AtomicWriteRepresentation, /
    ) -> WriteResultT: ...
