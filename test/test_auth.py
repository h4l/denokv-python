from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Sequence
from typing import cast
from uuid import UUID

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient
from yarl import URL

from denokv.auth import ConsistencyLevel
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.auth import HttpRequestMetadataExchangeDenoKvError
from denokv.auth import HttpResponseMetadataExchangeDenoKvError
from denokv.auth import InvalidMetadataExchangeDenoKvError
from denokv.auth import InvalidMetadataResponseDenoKvError
from denokv.auth import MetadataExchangeDenoKvError
from denokv.auth import get_database_metadata
from denokv.auth import read_metadata_exchange_response
from test.denokv_testing import assume_err
from test.denokv_testing import assume_ok

pytest_mark_asyncio = pytest.mark.asyncio(loop_scope="module")


@pytest.fixture
def valid_metadata_exchange_response() -> dict[str, object]:
    return {
        "version": 3,
        "uuid": "AD50A341-5351-4FC3-82D0-72CFEE369A09",
        "endpoints": [
            {"url": "/v2", "consistency": "strong"},
            {"url": "https://foo.example.com/v2", "consistency": "eventual"},
        ],
        "token": "thisisnotasecret",
        "expiresAt": "2024-08-05T05:13:59.500444679Z",
    }


@pytest.mark.parametrize("version", [1, 2, 3])
# The spec says "databaseId", but real responses use "uuid"
@pytest.mark.parametrize("database_id_prop", ["databaseId", "uuid"])
def test_read_metadata_exchange_response__parses_valid_data(
    version: int, database_id_prop: str
) -> None:
    base_url = URL("https://db.example.com/auth/")
    data = {
        "version": version,
        database_id_prop: "AD50A341-5351-4FC3-82D0-72CFEE369A09",
        "endpoints": [
            # note that the spec implies v1 does not use relative URLs in
            # # endpoints, but we always resolve, which is compliant.
            {"url": "/v2", "consistency": "strong"},
            {"url": "https://foo.example.com/v2", "consistency": "eventual"},
        ],
        "token": "thisisnotasecret",
        "expiresAt": "2024-08-05T05:13:59.500444679Z",
    }

    dbmeta = assume_ok(read_metadata_exchange_response(data, base_url=base_url))

    assert dbmeta == DatabaseMetadata(
        version=version,
        database_id=UUID("AD50A341-5351-4FC3-82D0-72CFEE369A09"),
        token="thisisnotasecret",
        expires_at=datetime.fromisoformat("2024-08-05T05:13:59.500444679Z"),
        endpoints=(
            EndpointInfo(
                url=URL("https://db.example.com/v2"),
                consistency=ConsistencyLevel.STRONG,
            ),
            EndpointInfo(
                url=URL("https://foo.example.com/v2"),
                consistency=ConsistencyLevel.EVENTUAL,
            ),
        ),
    )


def set_path(obj: Any, path: Sequence[str | int], value: object) -> Any:
    if len(path) == 0:
        return value
    target = obj
    for attr in path[:-1]:
        target = target[attr]
    target[path[-1]] = value
    return obj


@pytest.mark.parametrize(
    "message, mutation_path, invalid_value",
    [
        ("JSON value is not an object", [], []),
        ("unsupported version: ''", ["version"], ""),
        ("unsupported version: 4", ["version"], 4),
        ("databaseId/uuid is not a UUID: 'asdf'", ["databaseId"], "asdf"),
        ("token is not a non-empty string", ["token"], ""),
        ("token is not a non-empty string", ["token"], None),
        ("expiresAt is not an ISO date-time: None", ["expiresAt"], None),
        ("expiresAt is not an ISO date-time: 'Yesterday'", ["expiresAt"], "Yesterday"),
        ("endpoints is not an array", ["endpoints"], None),
        ("endpoints[0] is not an object", ["endpoints", 0], None),
        ("endpoints[0].url is invalid", ["endpoints", 0, "url"], 42),
        ("endpoints[0].url is invalid", ["endpoints", 0, "url"], "ftp://lol.what"),
        (
            "endpoints[1].consistency is not one of strong, eventual",
            ["endpoints", 1, "consistency"],
            "foo",
        ),
    ],
)
def test_read_metadata_exchange_response__rejects_invalid_data(
    message: str,
    mutation_path: Sequence[str | int],
    invalid_value: object,
    valid_metadata_exchange_response: dict[str, object],
) -> None:
    invalid_data = set_path(
        valid_metadata_exchange_response, mutation_path, invalid_value
    )

    result = read_metadata_exchange_response(
        invalid_data, base_url=URL("https://example.com/")
    )

    error = assume_err(result, InvalidMetadataResponseDenoKvError)
    assert error.data == invalid_data
    assert error.message == message


@pytest.fixture
def database_metadata_no_strong(
    valid_metadata_exchange_response: dict[str, object],
) -> dict[str, object]:
    no_strong = deepcopy(valid_metadata_exchange_response)
    for e in cast(list[dict[str, object]], no_strong["endpoints"]):
        e["consistency"] = str(ConsistencyLevel.EVENTUAL)
    return no_strong


@pytest.fixture
def db_api(
    valid_metadata_exchange_response: dict[str, object],
    database_metadata_no_strong: dict[str, object],
) -> web.Application:
    async def auth_401(request: web.Request) -> web.Response:
        return web.Response(status=401, body="testdb: Unauthorized")

    async def auth_503(request: web.Request) -> web.Response:
        return web.Response(status=503, body="testdb: Unavailable")

    async def auth_bad_status(request: web.Request) -> web.Response:
        return web.Response(status=600, body="testdb: ???")

    async def auth_future_version(request: web.Request) -> web.Response:
        return web.Response(
            status=200, content_type="application/json", body=json.dumps({"version": 4})
        )

    async def auth_no_strong(request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            content_type="application/json",
            body=json.dumps(database_metadata_no_strong),
        )

    async def redirect(request: web.Request) -> web.Response:
        location = request.query.get("location")
        assert location
        status = int(request.query.get("status", "307"))
        assert 300 <= status <= 400
        return web.Response(
            status=status,
            headers={"location": location},  # insecure :)
        )

    async def auth(request: web.Request) -> web.Response:
        version = int(request.query.get("version", "3"))
        token = request.query.get("token", "thisisnotasecret")

        if request.headers.get("authorization") != f"Bearer {token}":
            raise web.HTTPUnauthorized()

        data = deepcopy(valid_metadata_exchange_response)
        data["version"] = version
        return web.Response(
            status=200,
            content_type="application/json",
            body=json.dumps(data),
        )

    app = web.Application()
    app.router.add_post("/auth_401", auth_401)
    app.router.add_post("/auth_503", auth_503)
    app.router.add_post("/auth_bad_status", auth_bad_status)
    app.router.add_post("/auth_future_version", auth_future_version)
    app.router.add_post("/auth_no_strong", auth_no_strong)
    app.router.add_post("/sub/path/redirect", redirect)
    app.router.add_post("/auth", auth)
    return app


@pytest_asyncio.fixture
async def client(
    db_api: web.Application,
    aiohttp_client: Callable[[web.Application], Awaitable[TestClient]],
) -> TestClient:
    return await aiohttp_client(db_api)


@pytest.mark.parametrize(
    "path, error",
    [
        (
            "/auth_401",
            HttpResponseMetadataExchangeDenoKvError(
                "Server rejected metadata exchange request indicating client error",
                status=401,
                body_text="testdb: Unauthorized",
                retryable=False,
            ),
        ),
        (
            "/auth",  # access token is invalid
            HttpResponseMetadataExchangeDenoKvError(
                "Server rejected metadata exchange request indicating client error",
                status=401,
                body_text="401: Unauthorized",
                retryable=False,
            ),
        ),
        (
            "/auth_503",
            HttpResponseMetadataExchangeDenoKvError(
                "Server failed to respond to metadata exchange request "
                "indicating server error",
                status=503,
                body_text="testdb: Unavailable",
                retryable=True,
            ),
        ),
        (
            "/auth_bad_status",
            HttpResponseMetadataExchangeDenoKvError(
                "Server responded to metadata exchange request with unexpected status",
                status=600,
                body_text="testdb: ???",
                retryable=False,
            ),
        ),
    ],
)
@pytest_mark_asyncio
async def test_get_database_metadata__handles_unsuccessful_responses(
    client: TestClient, path: str, error: MetadataExchangeDenoKvError
) -> None:
    server_url = client.make_url(path)
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="foo"
    )

    assert assume_err(result) == error


@pytest_mark_asyncio
async def test_get_database_metadata__handles_network_error(
    client: TestClient, unused_tcp_port_factory: Callable[[], int]
) -> None:
    server_url = client.make_url("/")
    server_url = server_url.with_port(unused_tcp_port_factory())
    # will fail to connect to URL with nothing listening on the port
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="foo"
    )

    assert assume_err(result) == HttpRequestMetadataExchangeDenoKvError(
        "Failed to make HTTP request to KV server to exchange metadata", retryable=True
    )


@pytest_mark_asyncio
async def test_get_database_metadata__handles_invalid_initial_server_url(
    client: TestClient,
) -> None:
    result = await get_database_metadata(
        session=client.session, server_url="http://example.com:XXX", access_token="foo"
    )

    error = assume_err(result)
    assert error == HttpRequestMetadataExchangeDenoKvError(
        "Failed to make HTTP request to KV server to exchange metadata", retryable=False
    )
    assert isinstance(error.__cause__, aiohttp.InvalidURL)
    assert "http://example.com:XXX" in str(error.__cause__)


@pytest_mark_asyncio
async def test_get_database_metadata__handles_invalid_metadata(
    client: TestClient,
) -> None:
    server_url = client.make_url("/auth_future_version")
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="foo"
    )

    error = assume_err(result)
    assert error == InvalidMetadataExchangeDenoKvError(
        "Server responded to metadata exchange with invalid metadata",
        data={"version": 4},
        retryable=False,
    )
    assert (
        isinstance(error.__cause__, InvalidMetadataResponseDenoKvError)
        and error.__cause__.message == "unsupported version: 4"
    )


@pytest_mark_asyncio
async def test_get_database_metadata__reject_metadata_without_strong_consistency_endpoint(  # noqa: E501
    client: TestClient,
) -> None:
    server_url = client.make_url("/auth_no_strong")
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="foo"
    )

    error = assume_err(result, InvalidMetadataExchangeDenoKvError)
    assert (
        error.message == "Server responded to metadata exchange without any "
        "strong consistency endpoints"
    )


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest_mark_asyncio
async def test_get_database_metadata__returns_metadata_from_valid_response(
    client: TestClient,
    version: int,
) -> None:
    server_url = client.make_url(f"/auth?version={version}&token=hunter2")
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="hunter2"
    )

    assert assume_ok(result).version == version


@pytest.mark.parametrize("status", [307, 308])
@pytest_mark_asyncio
async def test_get_database_metadata__follows_redirect_during_exchange(
    client: TestClient,
    valid_metadata_exchange_response: dict[str, object],
    status: int,
) -> None:
    """We do support following 307 and 308 redirects when getting metadata.

    See the test for 301-303 status redirects, which we don't support.
    """
    redirected_url = client.make_url("/auth?token=hunter2")
    server_url = client.make_url("/sub/path/redirect")
    server_url = server_url.with_query(
        location="/auth?token=hunter2", status=f"{status}"
    )
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="hunter2"
    )

    expected = assume_ok(
        read_metadata_exchange_response(
            valid_metadata_exchange_response, base_url=redirected_url
        )
    )
    assert assume_ok(result) == expected


@pytest.mark.parametrize("status", [301, 302, 303])
@pytest_mark_asyncio
async def test_get_database_metadata__does_not_follow_ambiguous_redirects(
    client: TestClient, status: int
) -> None:
    """HTTP clients seem to switch to GET for redirects other than 307 and 308.

    We could implement explicit support for following these with POST, but if
    servers use 307 and 308 the default client behaviour should be fine.

    TODO: Verify if we need to support these redirects.

    See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Redirections
    """
    server_url = client.make_url("/sub/path/redirect")
    server_url = server_url.with_query(
        location="/auth?token=hunter2", status=f"{status}"
    )
    result = await get_database_metadata(
        session=client.session, server_url=server_url, access_token="hunter2"
    )

    error = assume_err(result, HttpResponseMetadataExchangeDenoKvError)
    assert error.status == 405  # method not allowed because we switched to GET
