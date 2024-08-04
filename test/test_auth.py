from datetime import datetime
from typing import Any, Callable, Sequence
from uuid import UUID
import pytest

from denokv.auth import (
    ConsistencyLevel,
    DatabaseMetadata,
    InvalidMetadataResponseDenoKvError,
    EndpointInfo,
    read_metadata_exchange_response,
)


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
    base_url = "https://db.example.com/auth/"
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

    dbmeta = read_metadata_exchange_response(data, base_url=base_url)

    assert dbmeta == DatabaseMetadata(
        version=version,
        database_id=UUID("AD50A341-5351-4FC3-82D0-72CFEE369A09"),
        token="thisisnotasecret",
        expires_at=datetime.fromisoformat("2024-08-05T05:13:59.500444679Z"),
        endpoints=(
            EndpointInfo(
                url="https://db.example.com/v2", consistency=ConsistencyLevel.STRONG
            ),
            EndpointInfo(
                url="https://foo.example.com/v2", consistency=ConsistencyLevel.EVENTUAL
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
        invalid_data, base_url="https://example.com/"
    )

    assert isinstance(result, InvalidMetadataResponseDenoKvError)
    assert result.data == invalid_data
    assert result.message == message
