"""The Deno KV [Data Path Protocol].

[Data Path Protocol]: https://github.com/denoland/denokv/blob/main/proto/kv-connect.md#data-path-protocol
"""

from __future__ import annotations

import aiohttp
from fdb.tuple import pack

from denokv._datapath_pb2 import ReadRange
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo


async def snapshot_read(
    *,
    session: aiohttp.ClientSession,
    meta: DatabaseMetadata,
    endpoint: EndpointInfo,
    read: SnapshotRead,
) -> SnapshotReadOutput:
    url = endpoint.url.joinpath("snapshot_read")
    async with session.post(
        url,
        data=read.SerializeToString(),
        headers={
            "authorization": f"Bearer {meta.token}",
            "content-type": "application/x-protobuf",
            "x-denokv-database-id": str(meta.database_id),
            "x-denokv-version": str(meta.version),
        },
    ) as response:
        response.raise_for_status()
        assert response.headers.get("content-type") == "application/x-protobuf"
        return SnapshotReadOutput.FromString(await response.read())


def snapshot_read_for_get(key: tuple[int | str, ...]) -> SnapshotRead:
    return SnapshotRead(
        ranges=[ReadRange(start=pack(key), end=b"\xff", limit=1, reverse=False)]
    )


read = SnapshotRead(ranges=[ReadRange()])

result = SnapshotReadOutput()
