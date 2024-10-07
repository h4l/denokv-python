from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping
from typing import Sequence
from typing import cast
from uuid import UUID

import aiohttp
import aiohttp.client_exceptions
from yarl import URL

from denokv._pycompat.dataclasses import slots_if310
from denokv._pycompat.enum import StrEnum
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.errors import DenoKvError
from denokv.errors import DenoKvValidationError
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result


@dataclass(frozen=True, **slots_if310())
class DatabaseMetadata:
    """
    MetadataExchangeResponse.

    ```json
    {
      "version": 2,
      "uuid": "a1b2c3d4-e5f6-7g8h-9i1j-2k3l4m5n6o7p",
      "endpoints": [
        {
          "url": "/v2",
          "consistency": "strong"
        },
        {
          "url": "https://mirror.example.com/v2",
          "consistency": "eventual"
        }
      ],
      "token": "123abc456def789ghi",
      "expiresAt": "2023-10-01T00:00:00Z"
    }
    ```
    """

    version: int
    database_id: UUID
    """Note: actual responses use "uuid"."""
    endpoints: Sequence[EndpointInfo]
    token: str
    expires_at: datetime


class ConsistencyLevel(StrEnum):
    STRONG = "strong"
    EVENTUAL = "eventual"


@dataclass(frozen=True, **slots_if310())
class EndpointInfo:
    url: URL
    consistency: ConsistencyLevel


@dataclass(init=False)
class InvalidMetadataResponseDenoKvError(DenoKvValidationError):
    data: object

    def __init__(self, message: str, data: object, *args: object) -> None:
        super().__init__(message, data, *args)

    @property  # type: ignore[no-redef]
    def data(self) -> object:
        return self.args[1]


def read_metadata_exchange_response(
    data: object, *, base_url: URL
) -> Result[DatabaseMetadata, InvalidMetadataResponseDenoKvError]:
    # This is caused by using the fn incorrectly, not a data error, so throw)
    if not base_url.is_absolute():
        raise TypeError("base_url is not absolute")

    Error = InvalidMetadataResponseDenoKvError
    if not isinstance(data, Mapping):
        return Err(Error("JSON value is not an object", data))
    version = data.get("version")
    if not (isinstance(version, int) and 1 <= version <= 3):
        return Err(Error(f"unsupported version: {version!r}", data))

    raw_database_id = data.get("databaseId") or data.get("uuid")
    try:
        database_id = UUID(raw_database_id)
    except ValueError as e:
        err = Error(f"databaseId/uuid is not a UUID: {raw_database_id!r}", data=data)
        err.__cause__ = e
        return Err(err)

    token = data.get("token")
    if not (isinstance(token, str) and len(token) > 0):
        return Err(Error("token is not a non-empty string", data))

    raw_expires_at = data.get("expiresAt")
    parsed_expires_at = None
    if isinstance(raw_expires_at, str):
        parsed_expires_at = parse_rfc3339_datetime(raw_expires_at)

    if not isinstance(parsed_expires_at, Ok):
        err = Error(
            f"expiresAt is not an RFC3339 date-time: {raw_expires_at!r}", data=data
        )
        if isinstance(parsed_expires_at, Err):
            err.__cause__ = parsed_expires_at.error
        return Err(err)

    raw_endpoints = data.get("endpoints")
    if not isinstance(raw_endpoints, Sequence):
        return Err(Error("endpoints is not an array", data))
    endpoints: list[EndpointInfo] = []
    for i, raw_endpoint in enumerate(raw_endpoints):
        if not isinstance(raw_endpoint, Mapping):
            return Err(Error(f"endpoints[{i}] is not an object", data))
        raw_url = raw_endpoint.get("url")
        try:
            if not isinstance(raw_url, str):
                raise TypeError("url is not a string")
            url = base_url.join(URL(raw_url))
            if url.scheme not in ("http", "https"):
                raise ValueError("scheme must be http or https")
            # TODO: JSON schema comments that URL must not end with /. Should we
            #   validate/enforce this? or later when using URLs?
        except (TypeError, ValueError) as e:
            err = Error(f"endpoints[{i}].url is invalid", data=data)
            err.__cause__ = e
            return Err(err)

        raw_consistency = raw_endpoint.get("consistency")
        try:
            if not isinstance(raw_consistency, str):
                raise TypeError("value must be a string")
            consistency = ConsistencyLevel(raw_consistency)
        except (TypeError, ValueError):
            return Err(
                Error(
                    f"endpoints[{i}].consistency is not one of "
                    f"{', '.join(ConsistencyLevel)}",
                    data=data,
                )
            )

        endpoints.append(EndpointInfo(url=url, consistency=consistency))

    return Ok(
        DatabaseMetadata(
            version=version,
            database_id=database_id,
            endpoints=tuple(endpoints),
            token=token,
            expires_at=parsed_expires_at.value,
        )
    )


@dataclass(init=False)
class MetadataExchangeDenoKvError(DenoKvError):
    retryable: bool

    def __init__(self, message: str, *args: object, retryable: bool) -> None:
        self.retryable = retryable
        super().__init__(message, *args)


@dataclass(init=False)
class InvalidMetadataExchangeDenoKvError(MetadataExchangeDenoKvError):
    """Metadata received from the KV server is not valid."""

    data: object | DatabaseMetadata
    """The invalid JSON data, or a parsed-but-invalid data."""

    def __init__(
        self,
        message: str,
        *args: object,
        data: object | DatabaseMetadata,
        retryable: bool,
    ) -> None:
        self.data = data
        super().__init__(message, *args, retryable=retryable)


@dataclass(init=False)
class HttpResponseMetadataExchangeDenoKvError(MetadataExchangeDenoKvError):
    """The KV server responded to the metadata exchange request unsuccessfully."""

    status: int
    body_text: str

    def __init__(
        self, message: str, status: int, body_text: str, *args: object, retryable: bool
    ) -> None:
        super().__init__(message, status, body_text, *args, retryable=retryable)

    @property  # type: ignore[no-redef]
    def status(self) -> int:
        return cast(int, self.args[1])

    @property  # type: ignore[no-redef]
    def body_text(self) -> str:
        return cast(str, self.args[2])


class HttpRequestMetadataExchangeDenoKvError(MetadataExchangeDenoKvError):
    """Unable to make an HTTP request to exchange metadata with the KV server."""

    pass


async def get_database_metadata(
    *,
    session: aiohttp.ClientSession,
    server_url: str | URL,
    access_token: str,
) -> Result[DatabaseMetadata, MetadataExchangeDenoKvError]:
    err: MetadataExchangeDenoKvError
    try:
        async with session.post(
            url=server_url,
            json={"supportedVersions": [1, 2, 3]},
            allow_redirects=True,
            headers={"Authorization": f"Bearer {access_token}"},
        ) as response:
            if not response.ok:
                body_text = await response.text()
                if 400 <= response.status < 500:
                    return Err(
                        HttpResponseMetadataExchangeDenoKvError(
                            "Server rejected metadata exchange request indicating "
                            "client error",
                            status=response.status,
                            body_text=body_text,
                            retryable=False,
                        )
                    )
                elif 500 <= response.status < 600:
                    return Err(
                        HttpResponseMetadataExchangeDenoKvError(
                            "Server failed to respond to metadata exchange request "
                            "indicating server error",
                            status=response.status,
                            body_text=body_text,
                            retryable=True,
                        )
                    )
                else:
                    return Err(
                        HttpResponseMetadataExchangeDenoKvError(
                            "Server responded to metadata exchange request with "
                            "unexpected status",
                            status=response.status,
                            body_text=body_text,
                            retryable=False,
                        )
                    )
            data = await response.json()
            base_url = response.history[-1].url if response.history else URL(server_url)
    except aiohttp.client_exceptions.ClientError as e:
        # We don't retry invalid URLs. Invalid URL could be the server_url arg,
        # or we could have been redirected to an invalid URL. The kv-connect
        # spec requires that we abort if redirected to an invalid URL.
        retryable = not isinstance(e, aiohttp.InvalidURL)
        err = HttpRequestMetadataExchangeDenoKvError(
            "Failed to make HTTP request to KV server to exchange metadata",
            retryable=retryable,
        )
        err.__cause__ = e
        return Err(err)

    result = read_metadata_exchange_response(data, base_url=base_url)
    if isinstance(result, Err):
        err = InvalidMetadataExchangeDenoKvError(
            "Server responded to metadata exchange with invalid metadata",
            data=data,
            retryable=False,
        )
        err.__cause__ = result.error
        return Err(err)
    meta = result.value

    # Semantic validation
    if not any(e.consistency == ConsistencyLevel.STRONG for e in meta.endpoints):
        return Err(
            InvalidMetadataExchangeDenoKvError(
                f"Server responded to metadata exchange without any "
                f"{ConsistencyLevel.STRONG} consistency endpoints",
                data=meta,
                retryable=False,
            )
        )

    return Ok(meta)
