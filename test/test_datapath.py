from __future__ import annotations

import functools
import re
import struct
from typing import Literal

import pytest
import pytest_asyncio
import v8serialize
from aiohttp import web
from aiohttp.test_utils import TestClient as _TestClient
from aiohttp.typedefs import Handler
from fdb.tuple import pack
from fdb.tuple import unpack
from google.protobuf.message import Message
from hypothesis import example
from hypothesis import given
from hypothesis import strategies as st
from v8serialize import Decoder
from v8serialize.jstypes import JSBigInt
from yarl import URL

from denokv import datapath
from denokv._datapath_pb2 import AtomicWrite
from denokv._datapath_pb2 import AtomicWriteOutput
from denokv._datapath_pb2 import AtomicWriteStatus
from denokv._datapath_pb2 import Check
from denokv._datapath_pb2 import KvEntry as ProtobufKvEntry
from denokv._datapath_pb2 import KvValue
from denokv._datapath_pb2 import Mutation
from denokv._datapath_pb2 import MutationType
from denokv._datapath_pb2 import ReadRange
from denokv._datapath_pb2 import ReadRangeOutput
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv._datapath_pb2 import SnapshotReadStatus
from denokv._datapath_pb2 import ValueEncoding
from denokv._kv_values import KvEntry
from denokv._kv_values import KvU64
from denokv._kv_values import VersionStamp
from denokv._pycompat.typing import Awaitable
from denokv._pycompat.typing import Callable
from denokv._pycompat.typing import Iterable
from denokv._pycompat.typing import Mapping
from denokv._pycompat.typing import Sequence
from denokv._pycompat.typing import TypeAlias
from denokv._pycompat.typing import TypeVar
from denokv._pycompat.typing import cast
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.datapath import KV_KEY_PIECE_TYPES
from denokv.datapath import AutoRetry
from denokv.datapath import CheckFailure
from denokv.datapath import DataPathDenoKvError
from denokv.datapath import EndpointNotUsable
from denokv.datapath import EndpointNotUsableReason
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyTuple
from denokv.datapath import ProtocolViolation
from denokv.datapath import RequestUnsuccessful
from denokv.datapath import ResponseUnsuccessful
from denokv.datapath import _DataPathRequestKind
from denokv.datapath import atomic_write
from denokv.datapath import increment_packed_key
from denokv.datapath import is_any_kv_key
from denokv.datapath import is_kv_key_tuple
from denokv.datapath import pack_key
from denokv.datapath import pack_key_range
from denokv.datapath import parse_protobuf_kv_entry
from denokv.datapath import read_range_single
from denokv.datapath import snapshot_read
from denokv.kv_keys import KvKey
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result
from denokv.result import is_ok
from test.denokv_testing import MockKvDb
from test.denokv_testing import add_entries
from test.denokv_testing import default_v8_encoder
from test.denokv_testing import make_database_metadata
from test.denokv_testing import meta_endpoint
from test.denokv_testing import mock_db_api
from test.denokv_testing import nextafter
from test.denokv_testing import unsafe_parse_protobuf_kv_entry

TestClient: TypeAlias = _TestClient[web.Request, web.Application]

pytest_mark_asyncio = pytest.mark.asyncio()

MessageT = TypeVar("MessageT", bound=Message)


@pytest.fixture
def mock_db() -> MockKvDb:
    return MockKvDb()


@pytest.fixture
def example_entries() -> Mapping[KvKeyTuple, object]:
    return {
        ("bytes", 0): b"foo",
        ("bytes", 1): b"bar",
        ("bytes", 2): b"baz",
        ("strings", 0): "foo",
        ("strings", 1): "bar",
        ("strings", 2): "baz",
        ("kvu64s", 0): KvU64(10),
        ("kvu64s", 1): KvU64(100),
        ("kvu64s", 2): KvU64(1000),
    }


@pytest.fixture
def db_api(mock_db: MockKvDb) -> web.Application:
    """HTTP endpoints backed by a MockKvDb, plus various misbehaving endpoints."""

    # Generic Data Path errors
    async def violation_2xx_text_body(request: web.Request) -> web.Response:
        """Only 200, not 2xx is the permitted successful response status."""
        return web.Response(
            status=201,
            content_type="text/plain",
            body="Strange behaviour.",
        )

    async def violation_2xx_protobuf_body(request: web.Request) -> web.Response:
        """Only 200, not 2xx is the permitted successful response status."""
        return web.Response(
            status=201,
            content_type="application/x-protobuf",
            body=SnapshotReadOutput(
                ranges=[],
                read_is_strongly_consistent=True,
                status=SnapshotReadStatus.SR_SUCCESS,
            ).SerializeToString(),
        )

    async def violation_307(request: web.Request) -> web.Response:
        """Clients do not follow redirects."""
        return web.Response(
            status=307, headers={"location": "/foo"}, body="testdb: redirecting to /foo"
        )

    async def errors_401(request: web.Request) -> web.Response:
        return web.Response(status=401, body="testdb: Unauthorized")

    async def errors_503(request: web.Request) -> web.Response:
        return web.Response(status=503, body="testdb: Unavailable")

    async def violation_bad_content_type(request: web.Request) -> web.Response:
        return web.Response(status=200, content_type="text/plain", body="Foo")

    async def violation_invalid_protobuf_body(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=b"\x00foo",
        )

    # Invalid snapshot_read handlers
    async def unusable_disabled_via_read_disabled(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=SnapshotReadOutput(
                ranges=[], read_disabled=True, read_is_strongly_consistent=True
            ).SerializeToString(),
        )

    async def unusable_disabled_via_status(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=SnapshotReadOutput(
                ranges=[],
                read_disabled=False,
                read_is_strongly_consistent=True,
                status=SnapshotReadStatus.SR_READ_DISABLED,
            ).SerializeToString(),
        )

    async def unusable_wrong_consistency(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=SnapshotReadOutput(
                read_is_strongly_consistent=False,
                status=SnapshotReadStatus.SR_SUCCESS,
            ).SerializeToString(),
        )

    async def violation_v3_no_status(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=SnapshotReadOutput(
                read_is_strongly_consistent=True,
                # This is the default — not setting is equivalent to setting
                # status=SnapshotReadStatus.SR_UNSPECIFIED,
            ).SerializeToString(),
        )

    async def violation_wrong_ranges(request: web.Request) -> web.Response:
        req_body_bytes = await request.read()
        read = SnapshotRead()
        read.ParseFromString(req_body_bytes)

        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=SnapshotReadOutput(
                # return too many ranges
                ranges=[ReadRangeOutput() for _ in range(len(read.ranges) + 1)],
                read_is_strongly_consistent=True,
                status=SnapshotReadStatus.SR_SUCCESS,
            ).SerializeToString(),
        )

    # Invalid atomic_write handlers
    async def violation_atomic_write_success_with_failed_checks(
        request: web.Request,
    ) -> web.Response:
        write = AtomicWrite()
        write.ParseFromString(await request.read())
        assert len(write.checks) > 0, "write request must have at least one check"

        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=AtomicWriteStatus.AW_SUCCESS,
                failed_checks=[0],
                versionstamp=VersionStamp(0),
            ).SerializeToString(),
        )

    async def violation_atomic_write_success_with_invalid_versionstamp(
        request: web.Request,
    ) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=AtomicWriteStatus.AW_SUCCESS, versionstamp=b"\xff"
            ).SerializeToString(),
        )

    async def violation_atomic_write_check_failure_with_out_of_bounds_index(
        request: web.Request,
    ) -> web.Response:
        write = AtomicWrite()
        write.ParseFromString(await request.read())
        assert len(write.checks) > 0, "write request must have at least one check"

        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=AtomicWriteStatus.AW_CHECK_FAILURE,
                failed_checks=[len(write.checks)],
            ).SerializeToString(),
        )

    # The denokv self-hosted implementation does not return indexes of failed
    # checks.
    # https://github.com/denoland/denokv/issues/110
    async def quirk_atomic_write_check_failure_without_failed_checks(
        request: web.Request,
    ) -> web.Response:
        write = AtomicWrite()
        write.ParseFromString(await request.read())
        assert len(write.checks) > 0, "write request must have at least one check"

        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=AtomicWriteStatus.AW_CHECK_FAILURE
            ).SerializeToString(),
        )

    async def violation_atomic_write_unspecified_status(
        request: web.Request,
    ) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=AtomicWriteStatus.AW_UNSPECIFIED
            ).SerializeToString(),
        )

    async def violation_atomic_write_invalid_status(
        request: web.Request,
    ) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=max(AtomicWriteStatus.values()) + 1  # type: ignore[attr-defined]
            ).SerializeToString(),
        )

    async def unusable_atomic_write(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/x-protobuf",
            body=AtomicWriteOutput(
                status=AtomicWriteStatus.AW_WRITE_DISABLED
            ).SerializeToString(),
        )

    def add_datapath_post(app: web.Application, path: str, handler: Handler) -> None:
        assert path.startswith("/") and not path.endswith("/")
        for req_kind in _DataPathRequestKind:
            app.router.add_post(f"{path}/{req_kind.value}", handler)

    app = mock_db_api(mock_db)

    # Generic error endpoints
    add_datapath_post(app, "/violation_2xx_text_body", violation_2xx_text_body)

    add_datapath_post(app, "/violation_2xx_protobuf_body", violation_2xx_protobuf_body)
    add_datapath_post(app, "/violation_307", violation_307)
    add_datapath_post(app, "/errors_401", errors_401)
    add_datapath_post(app, "/errors_503", errors_503)
    add_datapath_post(app, "/violation_bad_content_type", violation_bad_content_type)
    add_datapath_post(
        app, "/violation_invalid_protobuf_body", violation_invalid_protobuf_body
    )

    # snapshot_read only error endpoints
    app.router.add_post(
        "/unusable_disabled_via_read_disabled/snapshot_read",
        unusable_disabled_via_read_disabled,
    )
    app.router.add_post(
        "/unusable_disabled_via_status/snapshot_read",
        unusable_disabled_via_status,
    )
    app.router.add_post(
        "/unusable_wrong_consistency/snapshot_read",
        unusable_wrong_consistency,
    )
    app.router.add_post(
        "/violation_v3_no_status/snapshot_read",
        violation_v3_no_status,
    )
    app.router.add_post(
        "/violation_wrong_ranges/snapshot_read",
        violation_wrong_ranges,
    )

    # atomic_write only error endpoints
    app.router.add_post(
        "/success_with_failed_checks/atomic_write",
        violation_atomic_write_success_with_failed_checks,
    )
    app.router.add_post(
        "/success_with_invalid_versionstamp/atomic_write",
        violation_atomic_write_success_with_invalid_versionstamp,
    )
    app.router.add_post(
        "/check_failure_with_out_of_bounds_index/atomic_write",
        violation_atomic_write_check_failure_with_out_of_bounds_index,
    )
    app.router.add_post(
        "/check_failure_without_failed_checks/atomic_write",
        quirk_atomic_write_check_failure_without_failed_checks,
    )
    app.router.add_post("/unusable/atomic_write", unusable_atomic_write)
    app.router.add_post(
        "/unspecified_status/atomic_write", violation_atomic_write_unspecified_status
    )
    app.router.add_post(
        "/invalid_status/atomic_write", violation_atomic_write_invalid_status
    )

    return app


@pytest_asyncio.fixture
async def client(
    db_api: web.Application,
    aiohttp_client: Callable[[web.Application], Awaitable[TestClient]],
) -> TestClient:
    return await aiohttp_client(db_api)


@pytest.mark.parametrize(
    "datapath_request_fn",
    [
        pytest.param(
            functools.partial(snapshot_read, read=SnapshotRead()), id="snapshot_read"
        ),
        pytest.param(
            functools.partial(atomic_write, write=AtomicWrite()), id="atomic_write"
        ),
    ],
)
@pytest_mark_asyncio
async def test_datapath_request_function__handles_network_error(
    client: TestClient,
    unused_tcp_port_factory: Callable[[], int],
    datapath_request_fn: functools.partial[Awaitable[Result[object, object]]],
) -> None:
    server_url = client.make_url("/")
    server_url = server_url.with_port(unused_tcp_port_factory())

    meta, endpoint = meta_endpoint(make_database_metadata(endpoints=server_url))

    # will fail to connect to URL with nothing listening on the port
    result = await datapath_request_fn(
        session=client.session,
        meta=meta,
        endpoint=endpoint,
    )
    assert isinstance(result, Err)
    assert result.error == RequestUnsuccessful(
        "Failed to make Data Path HTTP request to KV server",
        endpoint=endpoint,
        auto_retry=AutoRetry.AFTER_BACKOFF,
    )


generic_datapath_unsuccessful_response_params: Sequence[
    tuple[str, Callable[[EndpointInfo], DataPathDenoKvError]]
] = [
    (
        "/violation_2xx_text_body",
        lambda endpoint: ResponseUnsuccessful(
            "Server responded to Data Path request with unexpected HTTP status",
            status=201,
            body_text="Strange behaviour.",
            auto_retry=AutoRetry.NEVER,
            endpoint=endpoint,
        ),
    ),
    (
        "/violation_2xx_protobuf_body",
        lambda endpoint: ResponseUnsuccessful(
            "Server responded to Data Path request with unexpected HTTP status",
            status=201,
            body_text="Response content-type: application/x-protobuf",
            auto_retry=AutoRetry.NEVER,
            endpoint=endpoint,
        ),
    ),
    (
        "/violation_307",
        lambda endpoint: ResponseUnsuccessful(
            "Server responded to Data Path request with unexpected HTTP status",
            status=307,
            body_text="testdb: redirecting to /foo",
            auto_retry=AutoRetry.NEVER,
            endpoint=endpoint,
        ),
    ),
    (
        "/errors_401",
        lambda endpoint: ResponseUnsuccessful(
            "Server rejected Data Path request indicating client error",
            status=401,
            body_text="testdb: Unauthorized",
            auto_retry=AutoRetry.NEVER,
            endpoint=endpoint,
        ),
    ),
    (
        "/errors_503",
        lambda endpoint: ResponseUnsuccessful(
            "Server failed to respond to Data Path request indicating server error",
            status=503,
            body_text="testdb: Unavailable",
            auto_retry=AutoRetry.AFTER_BACKOFF,
            endpoint=endpoint,
        ),
    ),
    (
        "/violation_bad_content_type",
        lambda endpoint: ProtocolViolation(
            "response content-type is not application/x-protobuf: text/plain",
            data="text/plain",
            endpoint=endpoint,
        ),
    ),
]


@pytest.mark.parametrize(
    "path, mk_error",
    [
        *generic_datapath_unsuccessful_response_params,
        (
            "/violation_invalid_protobuf_body",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path request with invalid "
                "SnapshotReadOutput",
                data=b"\x00foo",
                endpoint=endpoint,
            ),
        ),
        (
            "/unusable_disabled_via_read_disabled",
            lambda endpoint: EndpointNotUsable(
                "Server responded to Data Path request indicating it is disabled",
                endpoint=endpoint,
                reason=EndpointNotUsableReason.DISABLED,
            ),
        ),
        (
            "/unusable_disabled_via_status",
            lambda endpoint: EndpointNotUsable(
                "Server responded to Data Path request indicating it is disabled",
                endpoint=endpoint,
                reason=EndpointNotUsableReason.DISABLED,
            ),
        ),
        (
            "/unusable_wrong_consistency",
            lambda endpoint: EndpointNotUsable(
                "Server expected to be strongly-consistent responded to Data "
                "Path request with a non-strongly-consistent read",
                endpoint=endpoint,
                reason=EndpointNotUsableReason.CONSISTENCY_CHANGED,
            ),
        ),
        (
            "/violation_v3_no_status",
            lambda endpoint: ProtocolViolation(
                "v3 server responded to Data Path request with status SR_UNSPECIFIED",
                endpoint=endpoint,
                data=SnapshotReadOutput(read_is_strongly_consistent=True),
            ),
        ),
        (
            "/violation_wrong_ranges",
            lambda endpoint: ProtocolViolation(
                "Server responded to request with 0 ranges with 1 ranges",
                endpoint=endpoint,
                data=SnapshotReadOutput(
                    ranges=[ReadRangeOutput()],
                    read_is_strongly_consistent=True,
                    status=SnapshotReadStatus.SR_SUCCESS,
                ),
            ),
        ),
    ],
)
@pytest_mark_asyncio
async def test_snapshot_read__handles_unsuccessful_responses(
    client: TestClient,
    path: str,
    mk_error: Callable[[EndpointInfo], DataPathDenoKvError],
) -> None:
    server_url = client.make_url(path)
    meta, endpoint = meta_endpoint(make_database_metadata(endpoints=server_url))
    error = mk_error(endpoint)
    assert isinstance(error, DataPathDenoKvError)
    read = SnapshotRead(ranges=[])

    result = await snapshot_read(
        session=client.session,
        meta=meta,
        endpoint=endpoint,
        read=read,
    )
    assert isinstance(result, Err)
    assert result.error == error


@pytest.mark.parametrize(
    "read_ranges, result_ranges",
    [
        (
            [
                ReadRange(
                    start=pack_key(("bytes", 1)), end=b"\xff", limit=1, reverse=False
                ),
            ],
            [[(("bytes", 1), b"bar")]],
        ),
        (
            [
                ReadRange(
                    start=pack_key(("bytes", 0)),
                    end=pack_key(("bytes", 10)),
                    limit=2,
                    reverse=False,
                ),
            ],
            [
                [
                    (("bytes", 0), b"foo"),
                    (("bytes", 1), b"bar"),
                ]
            ],
        ),
        (
            [
                ReadRange(
                    start=pack_key(("strings", 1)), end=b"\xff", limit=1, reverse=False
                ),
            ],
            [[(("strings", 1), "bar")]],
        ),
        (
            [
                ReadRange(
                    start=pack_key(("kvu64s", 1)), end=b"\xff", limit=1, reverse=False
                ),
            ],
            [[(("kvu64s", 1), KvU64(100))]],
        ),
        ([], []),
        (
            [
                ReadRange(
                    start=pack_key(("bytes",)), end=b"\xff", limit=4, reverse=False
                ),
            ],
            [
                [
                    (("bytes", 0), b"foo"),
                    (("bytes", 1), b"bar"),
                    (("bytes", 2), b"baz"),
                    (("kvu64s", 0), KvU64(10)),
                ]
            ],
        ),
        (
            [
                ReadRange(
                    start=pack_key(("bytes",)), end=b"\xff", limit=1, reverse=False
                ),
                ReadRange(
                    start=pack_key(("kvu64s", 1)), end=b"\xff", limit=0, reverse=True
                ),
                ReadRange(
                    start=pack_key(("kvu64s",)),
                    end=pack_key(("kvu64s", 2)),
                    limit=1,
                    reverse=True,
                ),
                ReadRange(
                    start=pack_key(("kvu64s", 0)),
                    end=pack_key(("kvu64s", 3)),
                    limit=3,
                    reverse=True,
                ),
                ReadRange(
                    start=pack_key(("strings", 1)),
                    end=pack_key(("strings",)) + b"\xff",
                    limit=2,
                    reverse=True,
                ),
            ],
            [
                [(("bytes", 0), b"foo")],
                [],
                [(("kvu64s", 1), KvU64(100))],
                [
                    (("kvu64s", 2), KvU64(1000)),
                    (("kvu64s", 1), KvU64(100)),
                    (("kvu64s", 0), KvU64(10)),
                ],
                [
                    (("strings", 2), "baz"),
                    (("strings", 1), "bar"),
                ],
            ],
        ),
    ],
)
@pytest.mark.parametrize("version", [1, 2, 3])
@pytest_mark_asyncio
async def test_snapshot_read__reads_expected_values(
    client: TestClient,
    mock_db: MockKvDb,
    example_entries: Mapping[KvKeyTuple, object],
    read_ranges: list[ReadRange],
    result_ranges: list[list[tuple[KvKeyTuple, object]]],
    version: Literal[1, 2, 3],
) -> None:
    server_url = client.make_url(f"/v{version}/consistency/strong/")
    meta, endpoint = meta_endpoint(
        make_database_metadata(endpoints=server_url, version=version)
    )
    ver = add_entries(mock_db, example_entries)

    read = SnapshotRead(ranges=read_ranges)

    result = await snapshot_read(
        session=client.session,
        meta=meta,
        endpoint=endpoint,
        read=read,
    )

    assert isinstance(result, Ok)
    actual_result_ranges = [
        [unsafe_parse_protobuf_kv_entry(raw_entry) for raw_entry in res_range.values]
        for res_range in result.value.ranges
    ]
    expected_result_ranges = [
        [KvEntry(KvKey(*key), value, versionstamp=ver) for (key, value) in entries]
        for entries in result_ranges
    ]
    assert actual_result_ranges == expected_result_ranges


@pytest_mark_asyncio
async def test_atomic_write__raises_when_given_endpoint_without_strong_consistency(
    client: TestClient,
) -> None:
    # this is considered an avoidable programmer error, so it raises
    meta, eventual_endpoint = meta_endpoint(
        make_database_metadata(
            URL("https://example/"), endpoint_consistency=ConsistencyLevel.EVENTUAL
        )
    )
    with pytest.raises(
        ValueError,
        match=r"endpoints used with atomic_write must be "
        r"<ConsistencyLevel.STRONG: 'strong'>",
    ):
        await atomic_write(
            session=client.session,
            meta=meta,
            endpoint=eventual_endpoint,
            write=AtomicWrite(),
        )


@pytest.mark.parametrize(
    "path, mk_error",
    [
        *generic_datapath_unsuccessful_response_params,
        (
            "/violation_invalid_protobuf_body",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path request with invalid "
                "AtomicWriteOutput",
                data=b"\x00foo",
                endpoint=endpoint,
            ),
        ),
        (
            "/success_with_failed_checks",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path Atomic Write with SUCCESS "
                "containing failed checks",
                data=AtomicWriteOutput(
                    status=AtomicWriteStatus.AW_SUCCESS,
                    failed_checks=[0],
                    versionstamp=VersionStamp(0),
                ),
                endpoint=endpoint,
            ),
        ),
        (
            "/success_with_invalid_versionstamp",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path Atomic Write with SUCCESS "
                "containing an invalid versionstamp",
                data=AtomicWriteOutput(
                    status=AtomicWriteStatus.AW_SUCCESS,
                    versionstamp=b"\xff",
                ),
                endpoint=endpoint,
            ),
        ),
        (
            "/check_failure_with_out_of_bounds_index",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path Atomic Write with CHECK_FAILURE "
                "referencing out-of-bounds check index",
                data=AtomicWriteOutput(
                    status=AtomicWriteStatus.AW_CHECK_FAILURE,
                    failed_checks=[1],
                ),
                endpoint=endpoint,
            ),
        ),
        (
            "/check_failure_without_failed_checks",
            lambda endpoint: CheckFailure(
                "Not all checks required by the Atomic Write passed",
                all_checks=[
                    Check(key=pack_key(("x",)), versionstamp=bytes(VersionStamp(0)))
                ],
                failed_check_indexes=[],
                endpoint=endpoint,
            ),
        ),
        (
            "/unspecified_status",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path Atomic Write request "
                "with status UNSPECIFIED",
                data=AtomicWriteOutput(status=AtomicWriteStatus.AW_UNSPECIFIED),
                endpoint=endpoint,
            ),
        ),
        (
            "/invalid_status",
            lambda endpoint: ProtocolViolation(
                "Server responded to Data Path Atomic Write request "
                "with status unknown: 6",
                data=AtomicWriteOutput(status=6),  # type: ignore[arg-type]
                endpoint=endpoint,
            ),
        ),
        (
            "/unusable",
            lambda endpoint: EndpointNotUsable(
                "Server responded to Data Path request indicating it is cannot "
                "write this database",
                reason=EndpointNotUsableReason.DISABLED,
                endpoint=endpoint,
            ),
        ),
    ],
)
@pytest_mark_asyncio
async def test_atomic_write__handles_unsuccessful_responses(
    client: TestClient,
    path: str,
    mk_error: Callable[[EndpointInfo], DataPathDenoKvError],
) -> None:
    server_url = client.make_url(path)
    meta, endpoint = meta_endpoint(make_database_metadata(endpoints=server_url))
    error = mk_error(endpoint)
    assert isinstance(error, DataPathDenoKvError)

    result = await atomic_write(
        session=client.session,
        meta=meta,
        endpoint=endpoint,
        write=AtomicWrite(
            checks=[Check(key=pack_key(("x",)), versionstamp=bytes(VersionStamp(0)))]
        ),
    )
    assert isinstance(result, Err)
    assert result.error == error


@pytest.fixture
def example_entries_write() -> Mapping[KvKeyTuple, object]:
    return {("bigint", 1): JSBigInt(10)}


# There's not really much point in testing many successful mutations here, as
# our atomic_write() function is just passing along the encoded protobuf data
# without doing anything to it — we're just testing the db implementation if we
# were to test lots of things here. Error cases are where all the work is.
@pytest.mark.parametrize(
    "write, read_ranges, result_ranges",
    [
        pytest.param(AtomicWrite(), [], [], id="empty"),
        pytest.param(
            AtomicWrite(
                mutations=[
                    Mutation(
                        key=pack_key(("bigint", 1)),
                        value=KvValue(
                            data=bytes(default_v8_encoder.encode(JSBigInt(20))),
                            encoding=ValueEncoding.VE_V8,
                        ),
                        mutation_type=MutationType.M_SET,
                    )
                ]
            ),
            [
                ReadRange(
                    start=pack_key(("bigint", 1)), end=pack_key(("bigint", 2)), limit=1
                ),
            ],
            [[(KvKey("bigint", 1), JSBigInt(20))]],
            id="set",
        ),
        pytest.param(
            AtomicWrite(
                mutations=[
                    Mutation(
                        key=pack_key(("bigint", 1)),
                        value=KvValue(
                            data=bytes(default_v8_encoder.encode(JSBigInt(20))),
                            encoding=ValueEncoding.VE_V8,
                        ),
                        mutation_type=MutationType.M_SUM,
                    )
                ]
            ),
            [
                ReadRange(
                    start=pack_key(("bigint", 1)), end=pack_key(("bigint", 2)), limit=1
                ),
            ],
            [[(KvKey("bigint", 1), JSBigInt(30))]],
            id="sum",
        ),
    ],
)
@pytest.mark.parametrize("version", [1, 2, 3])
@pytest_mark_asyncio
async def test_atomic_write__writes_expected_values(
    client: TestClient,
    mock_db: MockKvDb,
    example_entries_write: Mapping[KvKeyTuple, object],
    write: AtomicWrite,
    read_ranges: list[ReadRange],
    result_ranges: list[list[tuple[KvKeyTuple, object]]],
    version: Literal[1, 2, 3],
) -> None:
    server_url = client.make_url(f"/v{version}/consistency/strong/")
    meta, endpoint = meta_endpoint(
        make_database_metadata(endpoints=server_url, version=version)
    )
    add_entries(mock_db, example_entries_write)

    write_result = await atomic_write(
        session=client.session, meta=meta, endpoint=endpoint, write=write
    )

    assert is_ok(write_result)
    write_ver = VersionStamp(write_result.value)

    actual_result_ranges = [
        [unsafe_parse_protobuf_kv_entry(raw_entry) for raw_entry in res_range.values]
        for res_range in (
            mock_db.snapshot_read_range(read=range) for range in read_ranges
        )
    ]
    expected_result_ranges = [
        [KvEntry(key, value, versionstamp=write_ver) for (key, value) in entries]
        for entries in result_ranges
    ]
    assert actual_result_ranges == expected_result_ranges


@pytest.mark.parametrize(
    "raw_entry, decoded",
    [
        (
            ProtobufKvEntry(
                key=pack(("a",)),
                versionstamp=bytes(VersionStamp("00000000000000000001")),
                encoding=ValueEncoding.VE_BYTES,
                value=b"foo",
            ),
            KvEntry(
                key=KvKey("a"),
                value=b"foo",
                versionstamp=VersionStamp("00000000000000000001"),
            ),
        ),
        (
            ProtobufKvEntry(
                key=pack(("a", 42, 1.0, True, b"b")),
                versionstamp=bytes(VersionStamp("000000000000000f0001")),
                encoding=ValueEncoding.VE_LE64,
                value=struct.pack("<Q", 15043183363527981566),
            ),
            KvEntry(
                key=KvKey("a", 42, 1.0, True, b"b"),
                value=KvU64(15043183363527981566),
                versionstamp=VersionStamp("000000000000000f0001"),
            ),
        ),
        (
            ProtobufKvEntry(
                key=pack(("a",)),
                versionstamp=bytes(VersionStamp("000000000000000f0001")),
                encoding=ValueEncoding.VE_V8,
                value=v8serialize.dumps("foo"),
            ),
            KvEntry(
                key=KvKey("a"),
                value="foo",
                versionstamp=VersionStamp("000000000000000f0001"),
            ),
        ),
    ],
)
def test_parse_protobuf_kv_entry__decodes_valid_kv_entry(
    raw_entry: ProtobufKvEntry, decoded: KvEntry
) -> None:
    kv_entry = unsafe_parse_protobuf_kv_entry(raw_entry)
    assert kv_entry == decoded


@pytest.mark.parametrize(
    "msg_pattern, raw_entry",
    [
        (
            "Invalid encoded key tuple",
            ProtobufKvEntry(
                key=b"\xff",
                versionstamp=bytes(VersionStamp("00000000000000000000")),
                encoding=ValueEncoding.VE_BYTES,
                value=b"",
            ),
        ),
        (
            "versionstamp is not an 80-bit value",
            ProtobufKvEntry(
                key=pack(("a",)),
                versionstamp=b"\x00" * 11,
                encoding=ValueEncoding.VE_BYTES,
                value=b"",
            ),
        ),
        (
            "V8-serialized value is not decodable",
            ProtobufKvEntry(
                key=pack(("a",)),
                versionstamp=bytes(VersionStamp("00000000000000000000")),
                encoding=ValueEncoding.VE_V8,
                value=b"\x00",  # invalid serialized value
            ),
        ),
        (
            "Value encoding is UNSPECIFIED",
            ProtobufKvEntry(
                key=pack(("a",)),
                versionstamp=bytes(VersionStamp("00000000000000000000")),
                encoding=ValueEncoding.VE_UNSPECIFIED,  # not allowed in practice
                value=b"\x00",
            ),
        ),
        (
            "Value encoding is unknown",
            ProtobufKvEntry(
                key=pack(("a",)),
                versionstamp=bytes(VersionStamp("00000000000000000000")),
                encoding=cast(ValueEncoding, 9000),  # not valid
                value=b"\x00",
            ),
        ),
    ],
)
def test_parse_protobuf_kv_entry__reports_invalid_kv_entry(
    raw_entry: ProtobufKvEntry, msg_pattern: str
) -> None:
    value = parse_protobuf_kv_entry(raw_entry, v8_decoder=Decoder(), le64_type=KvU64)
    assert isinstance(value, Err)
    assert isinstance(value.error, ValueError)
    assert re.search(msg_pattern, str(value.error))


# object in type as mypy gets confused at unique_by= and key= below
def fdb_packed_representation(piece: KvKeyPiece | object) -> bytes:
    assert isinstance(piece, KV_KEY_PIECE_TYPES)
    return pack((piece,))


kv_key_piece_strategies = (
    st.floats(),
    st.integers(),
    st.text(),
    st.binary(),
    st.booleans(),
)
kv_key_pieces = st.one_of(kv_key_piece_strategies)

ordered_kv_key_pieces_of_same_type = st.one_of(
    [
        st.lists(s, min_size=2, max_size=2, unique_by=fdb_packed_representation).map(
            lambda pair: tuple(sorted(pair, key=fdb_packed_representation))
        )
        for s in kv_key_piece_strategies
    ]
)


def test_pack_key() -> None:
    pieces = ("a", b"b", 1, 2.0, True)
    packed_default = pack_key(pieces)
    assert isinstance(packed_default, bytes)
    assert tuple((type(p), p) for p in unpack(packed_default)) == (
        (str, "a"),
        (bytes, b"b"),
        (int, 1),
        (float, 2.0),
        (bool, True),
    )


def test_pack_key_cache() -> None:
    for i in range(datapath._PACK_KEY_CACHE_LIMIT * 2):
        assert unpack(pack_key(("foo", i))) == ("foo", i)

    assert len(datapath._PACK_KEY_CACHE) == datapath._PACK_KEY_CACHE_LIMIT


all_floats = st.floats(allow_nan=True, allow_infinity=True, allow_subnormal=True)


@given(f1=all_floats, f2=all_floats)
@example(f1=-0.0, f2=0.0)
def test_pack_key_cache__distinguishes_float_values(f1: float, f2: float) -> None:
    print(f"equal={pack((f1,)) == pack((f2,))}", f1, f2)
    for _i in range(2):
        fdb_packed1 = pack((f1,))
        cached_packed1 = pack_key((f1,))
        fdb_packed2 = pack((f2,))
        cached_packed2 = pack_key((f2,))

        if fdb_packed1 == fdb_packed2:
            assert cached_packed1 == cached_packed2
        else:
            assert cached_packed1 != cached_packed2


def test_pack_key__rejects_unsupported_types() -> None:
    # Only supported types are allowed, not all types allowed by FoundationDB
    # key tuples.

    with pytest.raises(
        TypeError,
        match=r"key contains types other than str, bytes, int, float, bool: "
        r"\('nested', \('a', 'b'\)\)",
    ):
        pack_key(("nested", ("a", "b")))  # type: ignore[arg-type]


def test_increment_packed_key__behaviour() -> None:
    assert b"\x00" < increment_packed_key(b"\x00")
    assert increment_packed_key(b"\x00") < b"\x01"

    # When the key ends with a max byte (255/0xff) the incremented key will be
    # equal to the next-higher key rather than less than, still satisfying the
    # <= property.

    assert increment_packed_key(b"\xff") == b"\xff\x00"


@given(pieces=ordered_kv_key_pieces_of_same_type)
@example((-0.0, 0.0))
def test_increment_packed_key(
    pieces: tuple[float, float]
    | tuple[str, str]
    | tuple[bytes, bytes]
    | tuple[bool, bool],
) -> None:
    p1, p2 = pieces
    k1, k2 = (p1,), (p2,)
    pk1, pk2 = pack_key(k1), pack_key(k2)

    gt_pk1 = increment_packed_key(pk1)
    assert pk1 < gt_pk1
    assert pk2 >= gt_pk1


pack_key_range_example_keys = st.one_of(
    st.none(),
    st.just(()),
    st.tuples(st.sampled_from("_abcd")),
    st.tuples(
        st.sampled_from("_abcd"),
        st.integers(min_value=0, max_value=5),
    ),
)


@given(
    prefix=pack_key_range_example_keys,
    start=pack_key_range_example_keys,
    end=pack_key_range_example_keys,
    exclude_start=st.booleans(),
    exclude_end=st.booleans(),
)
def test_pack_key_range(
    prefix: KvKeyTuple | None,
    start: KvKeyTuple | None,
    end: KvKeyTuple | None,
    exclude_start: bool,
    exclude_end: bool,
) -> None:
    """
    Test `pack_key_range()` by comparing it against Python tuple comparison.

    We implement a simplified version of pack_key_range's bytes key range
    matching in is_included_by_tuple_key() by comparing unpacked key tuples
    directly, using the same logic and parameters as pack_key_range() and how
    the KV DB evaluates the ranges (is_included_by_packed_key()).
    """
    keys = [
        *(("a", i) for i in range(4)),
        *(("b", i) for i in range(4)),
        *(("c", i) for i in range(4)),
    ]

    packed_start, packed_end = pack_key_range(
        prefix=prefix,
        start=start,
        end=end,
        exclude_start=exclude_start,
        exclude_end=exclude_end,
    )

    def is_included_by_packed_key(key: KvKeyTuple) -> bool:
        # Data Path protocol always evaluates ranges with inclusive start and
        # exclusive endpoints. Inclusive/exclusive-ness is baked into the packed
        # value by pack_key_range.
        return packed_start <= pack(key) < packed_end

    def is_included_by_tuple_key(key: KvKeyTuple) -> bool:
        start_tuple = start if start is not None else (prefix or ())
        start_satisfied = (key > start_tuple) if exclude_start else (key >= start_tuple)

        end_tuple = (
            end
            if end is not None
            else tuple["KvKeyPiece | Infinity", ...]((*(prefix or ()), Infinity()))
        )
        end_satisfied = (key < end_tuple) if exclude_end else (key <= end_tuple)

        return start_satisfied and end_satisfied

    keys_by_packed_key = [k for k in keys if is_included_by_packed_key(k)]
    keys_by_tuple_key = [k for k in keys if is_included_by_tuple_key(k)]

    assert keys_by_packed_key == keys_by_tuple_key


@functools.total_ordering
class Infinity:
    """
    Greater than everything.

    >>> 3 < Infinity()
    True
    >>> float('inf') < Infinity()
    True
    """

    def __gt__(self, other: object) -> bool:
        return True

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Infinity)


@pytest.fixture
def example_entries_spaced_keys() -> Mapping[KvKeyTuple, object]:
    return {
        ("bigint", 2): b"bigint2",
        ("bigint", 3): b"bigint3",
        ("bigint", 5): b"bigint5",
        ("bytes", b"\x02"): b"bytes2",
        ("bytes", b"\x03"): b"bytes3",
        ("bytes", b"\x05"): b"bytes5",
        ("bytes", b"\xff"): b"bytesFF",
        ("bytes", b"\xff\x00"): b"bytesFF00",
        ("bytes", b"\xff\xff\x00"): b"bytesFFFF00",
        ("string", "2"): b"string2",
        ("string", "3"): b"string3",
        ("string", "5"): b"string5",
        ("string", "\u007f"): b"string_max1b",  # max 1-byte UTF-8 codepoint
        ("string", "\u0080"): b"string_min2b",  # min 2-byte UTF-8 codepoint
        ("string", "\u07ff"): b"string_max2b",  # max 2-byte UTF-8 codepoint
        ("string", "\u0800"): b"string_min3b",  # min 3-byte UTF-8 codepoint
        ("number", nextafter(1.0, 2.0, steps=0)): b"number1.0",
        ("number", nextafter(1.0, 2.0, steps=1)): b"number1.0next1",
        ("number", nextafter(1.0, 2.0, steps=3)): b"number1.0next3",
        ("number", nextafter(1.0, 2.0, steps=5)): b"number1.0next5",
        ("bool", False): b"false",
        ("bool", True): b"true",
    }


@pytest.mark.parametrize(
    "key, expected",
    [
        (("bigint", 1), None),
        (("bigint", 2), b"bigint2"),
        (("bigint", 3), b"bigint3"),
        (("bigint", 4), None),
        (("bigint", 5), b"bigint5"),
        (("bigint", 6), None),
        (("bytes", b"\x01"), None),
        (("bytes", b"\x02"), b"bytes2"),
        (("bytes", b"\x03"), b"bytes3"),
        (("bytes", b"\x04"), None),
        (("bytes", b"\x05"), b"bytes5"),
        (("bytes", b"\x06"), None),
        (("bytes", b"\xff"), b"bytesFF"),
        (("bytes", b"\xff\x00"), b"bytesFF00"),
        (("bytes", b"\xff\x01"), None),
        (("bytes", b"\xff\xff"), None),
        (("bytes", b"\xff\xff\x00"), b"bytesFFFF00"),
        (("bytes", b"\xff\xff\x01"), None),
        (("string", "1"), None),
        (("string", "2"), b"string2"),
        (("string", "3"), b"string3"),
        (("string", "4"), None),
        (("string", "5"), b"string5"),
        (("string", "6"), None),
        (("string", "\u007e"), None),
        (("string", "\u007f"), b"string_max1b"),
        (("string", "\u0080"), b"string_min2b"),
        (("string", "\u0081"), None),
        (("string", "\u07fe"), None),
        (("string", "\u07ff"), b"string_max2b"),
        (("string", "\u0800"), b"string_min3b"),
        (("string", "\u0801"), None),
        (("number", nextafter(1.0, 0.0, steps=1)), None),
        (("number", 1.0), b"number1.0"),
        (("number", nextafter(1.0, 2.0, steps=1)), b"number1.0next1"),
        (("number", nextafter(1.0, 2.0, steps=2)), None),
        (("number", nextafter(1.0, 2.0, steps=3)), b"number1.0next3"),
        (("number", nextafter(1.0, 2.0, steps=4)), None),
        (("number", nextafter(1.0, 2.0, steps=5)), b"number1.0next5"),
        (("number", nextafter(1.0, 2.0, steps=6)), None),
        (("bool", False), b"false"),
        (("bool", True), b"true"),
    ],
)
@pytest.mark.parametrize("extend_limit", [True, False])
def test_read_range_single(
    mock_db: MockKvDb,
    example_entries_spaced_keys: Mapping[KvKeyTuple, object],
    key: KvKeyTuple,
    expected: None | bytes,
    extend_limit: bool,
) -> None:
    ver = add_entries(mock_db, example_entries_spaced_keys)

    rr = read_range_single(key)
    # Because the bounds are set up to only include the single possible matching
    # key, extending the limit from 1 to 2 does not affect the match (as it
    # would if the end bound was too loose).
    if extend_limit:
        assert rr.limit == 1
        rr.limit = 2
    out = mock_db.snapshot_read_range(rr)

    if expected is None:
        assert len(out.values) == 0
    else:
        assert len(out.values) == 1
        (kve,) = out.values
        assert kve.key == pack_key(key)
        assert kve.value == expected
        assert kve.versionstamp == ver


def test_is_kv_key_tuple() -> None:
    assert is_kv_key_tuple(("a", 1, 1.0, True, b"b"))
    assert is_kv_key_tuple(cast(object, ("a", 1, 1.0, True, b"b")))
    assert not is_kv_key_tuple(((),))
    assert not is_kv_key_tuple(KvKey("x"))


def test_is_any_kv_key() -> None:
    assert is_any_kv_key(KvKey("x"))
    assert is_any_kv_key(cast(object, KvKey("x")))
    assert is_any_kv_key(("a", 1, 1.0, True, b"b"))
    assert not is_any_kv_key([])
    assert not is_any_kv_key(((),))


@pytest.fixture
def example_endpoint() -> EndpointInfo:
    _, endpoint = meta_endpoint(
        make_database_metadata(endpoints=URL("https://example.com"))
    )
    return endpoint


def test_CheckFailure(example_endpoint: EndpointInfo) -> None:
    checks = [
        Check(key=bytes(KvKey(f"a{i}")), versionstamp=bytes(VersionStamp(i)))
        for i in range(4)
    ]
    msg = "Not all checks required by the Atomic Write passed"
    e = CheckFailure(
        msg,
        all_checks=iter(checks),
        failed_check_indexes=[3, 0, 2],
        endpoint=example_endpoint,
    )
    assert e.all_checks == tuple(checks)
    assert e.failed_check_indexes == {0, 2, 3}
    # failed_check_indexes are ordered ascending
    assert list(e.failed_check_indexes) == [0, 2, 3]
    assert e.endpoint is example_endpoint
    assert msg in str(e)


@pytest.mark.parametrize("failed_check_indexes", [None, ()])
def test_CheckFailure__failed_check_indexes_is_None_when_no_indexes(
    failed_check_indexes: Iterable[int] | None, example_endpoint: EndpointInfo
) -> None:
    checks = [
        Check(key=bytes(KvKey(f"a{i}")), versionstamp=bytes(VersionStamp(i)))
        for i in range(4)
    ]
    # Failed_check_indexes can be empty (the self-hosted sqlite implementation
    # does not return the indexes of failed checks).
    e = CheckFailure(
        "Foo",
        all_checks=iter(checks),
        failed_check_indexes=failed_check_indexes,
        endpoint=example_endpoint,
    )
    assert e.all_checks == tuple(checks)
    assert e.failed_check_indexes is None


def test_CheckFailure__validates_constructor_args(
    example_endpoint: EndpointInfo,
) -> None:
    checks = [Check(key=bytes(KvKey("a")), versionstamp=bytes(VersionStamp(1)))]

    with pytest.raises(ValueError, match=r"all_checks is empty"):
        CheckFailure(
            "Foo", all_checks=[], failed_check_indexes=[], endpoint=example_endpoint
        )

    with pytest.raises(
        IndexError, match=r"failed_check_indexes contains out-of-bounds index"
    ):
        CheckFailure(
            "Foo",
            all_checks=checks,
            failed_check_indexes=[5],
            endpoint=example_endpoint,
        )


def test_ResponseUnsuccessful(example_endpoint: EndpointInfo) -> None:
    msg = "Server rejected Data Path request indicating client error"
    response_body = "Info about what is wrong."
    status = 400
    e = ResponseUnsuccessful(
        msg,
        status=status,
        body_text=response_body,
        endpoint=example_endpoint,
        auto_retry=AutoRetry.NEVER,
    )
    assert str(msg) in str(e)
    assert str(status) in str(e)
    assert str(response_body) in str(e)
    assert e.endpoint is example_endpoint
