from __future__ import annotations

from abc import ABC
from abc import abstractmethod

from google.protobuf.message import Message
from v8serialize import Encoder

from denokv._datapath_pb2 import AtomicWrite
from denokv._kv_values import VersionStamp
from denokv._pycompat.typing import Generic
from denokv._pycompat.typing import Protocol
from denokv._pycompat.typing import Sequence
from denokv._pycompat.typing import TypeAlias
from denokv._pycompat.typing import TypeGuard
from denokv._pycompat.typing import TypeVar
from denokv._pycompat.typing import Union
from denokv.auth import EndpointInfo
from denokv.datapath import CheckFailure
from denokv.datapath import DataPathError
from denokv.result import Nothing
from denokv.result import Option
from denokv.result import Result
from denokv.result import Some

WriteResultT = TypeVar("WriteResultT")
WriteResultT_co = TypeVar("WriteResultT_co", covariant=True)
MessageT_co = TypeVar("MessageT_co", bound=Message, covariant=True)


class ProtobufMessageRepresentation(Generic[MessageT_co], ABC):
    """An object that can represent itself as a protobuf Messages."""

    __slots__ = ()

    @abstractmethod
    def as_protobuf(self, *, v8_encoder: Encoder) -> Sequence[MessageT_co]: ...


class SingleProtobufMessageRepresentation(ProtobufMessageRepresentation[MessageT_co]):
    """An object that can represent itself as a single protobuf Message."""

    __slots__ = ()

    @abstractmethod
    def as_protobuf(self, *, v8_encoder: Encoder) -> tuple[MessageT_co]: ...


class AtomicWriteRepresentation(SingleProtobufMessageRepresentation[AtomicWrite]):
    __slots__ = ()


class AtomicWriteRepresentationWriter(
    AtomicWriteRepresentation, Generic[WriteResultT_co]
):
    __slots__ = ()

    @abstractmethod
    async def write(self, kv: KvWriter, *, v8_encoder: Encoder) -> WriteResultT_co: ...


KvWriterWriteResult: TypeAlias = Result[
    tuple[VersionStamp, EndpointInfo], Union[CheckFailure, DataPathError]
]


class KvWriter(ABC):
    """A low-level interface for objects that can perform KV writes."""

    @abstractmethod
    async def write(self, *, protobuf_atomic_write: AtomicWrite) -> KvWriterWriteResult:
        """Write a protobuf AtomicWrite message to the database."""


class V8EncoderProvider(Protocol):
    @property
    def v8_encoder(self) -> Encoder: ...


def is_v8_encoder_provider(obj: object) -> TypeGuard[V8EncoderProvider]:
    return isinstance(getattr(obj, "v8_encoder", None), Encoder)


def get_v8_encoder(maybe_v8_encoder_provider: object) -> Option[Encoder]:
    if is_v8_encoder_provider(maybe_v8_encoder_provider):
        return Some(maybe_v8_encoder_provider.v8_encoder)
    return Nothing()
