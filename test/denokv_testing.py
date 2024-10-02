from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from itertools import groupby
from typing import Iterable
from typing import Mapping
from typing import NamedTuple
from typing import Sequence
from typing import TypeVar
from uuid import UUID

import v8serialize

from denokv._datapath_pb2 import KvEntry as ProtobufKvEntry
from denokv._datapath_pb2 import KvValue
from denokv._datapath_pb2 import ReadRange
from denokv._datapath_pb2 import ReadRangeOutput
from denokv._datapath_pb2 import ValueEncoding
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.datapath import KvKeyTuple
from denokv.datapath import pack_key
from denokv.datapath import parse_protobuf_kv_entry
from denokv.kv import KvEntry
from denokv.kv import KvKey
from denokv.kv import KvU64
from denokv.kv import VersionStamp
from denokv.result import Ok
from denokv.result import Result

T = TypeVar("T")
E = TypeVar("E")

v8_decoder = v8serialize.Decoder()


def assume_ok(result: Result[T, E]) -> T:
    if isinstance(result, Ok):
        return result.value
    raise AssertionError(f"result is not Ok: {result}")


def mk_db_meta(endpoints: Sequence[EndpointInfo]) -> DatabaseMetadata:
    """Create a placeholder DB meta object with the provided endpoints."""
    return DatabaseMetadata(
        version=3,
        database_id=UUID("00000000-0000-0000-0000-000000000000"),
        expires_at=datetime.now() + timedelta(hours=1),
        endpoints=[*endpoints],
        token="secret",
    )


class MockKvDbEntry(NamedTuple):
    key: bytes
    versionstamp: int
    encoding: ValueEncoding
    value: bytes


@dataclass
class MockKvDb:
    entries: list[MockKvDbEntry]
    next_version: int

    def __init__(self, entries: Iterable[tuple[bytes, KvValue]] = ()) -> None:
        self.clear()
        self.extend(entries)

    def clear(self) -> None:
        self.entries = []
        self.next_version = 0

    def extend(self, entries: Iterable[tuple[bytes, KvValue]]) -> None:
        version = self.next_version
        self.next_version += 1

        self.entries.extend(
            MockKvDbEntry(
                key=key,
                versionstamp=version,
                encoding=kv_value.encoding,
                value=kv_value.data,
            )
            for (key, kv_value) in entries
        )
        self.entries.sort(key=lambda e: (e.key, e.versionstamp))

    def _read_range(
        self, start: bytes, end: bytes, limit: int, reverse: bool
    ) -> Sequence[MockKvDbEntry]:
        assert limit >= 0
        matches = [e for e in self.entries if start <= e.key < end]
        latest_matches = [
            list(versions)[-1]
            for (k, versions) in groupby(matches, key=lambda m: m.key)
        ]
        if reverse:
            latest_matches = list(reversed(latest_matches))
        return latest_matches[:limit]

    def snapshot_read_range(self, read: ReadRange) -> ReadRangeOutput:
        entries = self._read_range(
            start=read.start, end=read.end, limit=read.limit, reverse=read.reverse
        )
        return ReadRangeOutput(
            values=[
                ProtobufKvEntry(
                    key=e.key,
                    versionstamp=bytes(VersionStamp(e.versionstamp)),
                    encoding=e.encoding,
                    value=e.value,
                )
                for e in entries
            ]
        )


def encode_protobuf_kv_value(value: object) -> KvValue:
    if isinstance(value, KvU64):
        return KvValue(data=bytes(value), encoding=ValueEncoding.VE_LE64)
    elif isinstance(value, bytes):
        return KvValue(data=value, encoding=ValueEncoding.VE_BYTES)
    else:
        return KvValue(data=v8serialize.dumps(value), encoding=ValueEncoding.VE_V8)


def add_entries(
    db: MockKvDb,
    entries: Mapping[KvKeyTuple, object] | Iterable[tuple[KvKeyTuple, object]],
) -> VersionStamp:
    if isinstance(entries, Mapping):
        entries = entries.items()

    version = VersionStamp(db.next_version)
    encoded_entries = [
        (pack_key(key), encode_protobuf_kv_value(value)) for (key, value) in entries
    ]
    db.extend(encoded_entries)
    return version


def unsafe_parse_protobuf_kv_entry(raw: ProtobufKvEntry) -> KvEntry:
    key, value, versionstamp = assume_ok(
        parse_protobuf_kv_entry(raw, v8_decoder=v8_decoder, le64_type=KvU64)
    )
    return KvEntry(KvKey.wrap_tuple_keys(key), value, VersionStamp(versionstamp))
