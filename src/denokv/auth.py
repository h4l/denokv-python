from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Mapping, Sequence
from urllib.parse import urljoin, urlparse
from uuid import UUID

from denokv.errors import DenoKvValidationError


@dataclass(slots=True, frozen=True)
class DatabaseMetadata:
    """
    MetadataExchangeResponse

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


@dataclass(slots=True, frozen=True)
class EndpointInfo:
    url: str
    consistency: ConsistencyLevel


class InvalidMetadataResponseDenoKvError(DenoKvValidationError):
    def __init__(self, message: str, data: object) -> None:
        super().__init__(message, data)

    @property
    def message(self) -> str:
        return self.args[0]

    @property
    def data(self) -> object:
        return self.args[1]


def read_metadata_exchange_response(
    data: object, *, base_url: str
) -> DatabaseMetadata | InvalidMetadataResponseDenoKvError:
    Err = InvalidMetadataResponseDenoKvError
    if not isinstance(data, Mapping):
        return Err("JSON value is not an object", data)
    version = data.get("version")
    if not (isinstance(version, int) and 1 <= version <= 3):
        return Err(f"unsupported version: {version!r}", data)

    raw_database_id = data.get("databaseId") or data.get("uuid")
    try:
        database_id = UUID(raw_database_id)
    except ValueError as e:
        err = Err(f"databaseId/uuid is not a UUID: {raw_database_id!r}", data=data)
        err.__cause__ = e
        return err

    token = data.get("token")
    if not (isinstance(token, str) and len(token) > 0):
        return Err("token is not a non-empty string", data)

    raw_expires_at = data.get("expiresAt")
    try:
        if not isinstance(raw_expires_at, str):
            raise TypeError("value must be a string")
        expires_at = datetime.fromisoformat(raw_expires_at)
    except (TypeError, ValueError) as e:
        err = Err(f"expiresAt is not an ISO date-time: {raw_expires_at!r}", data=data)
        err.__cause__ = e
        return err

    raw_endpoints = data.get("endpoints")
    if not isinstance(raw_endpoints, Sequence):
        return Err("endpoints is not an array", data)
    endpoints: list[EndpointInfo] = []
    for i, raw_endpoint in enumerate(raw_endpoints):
        if not isinstance(raw_endpoint, Mapping):
            return Err(f"endpoints[{i}] is not an object", data)
        raw_url = raw_endpoint.get("url")
        try:
            url = urljoin(base_url, raw_url)
            if urlparse(url).scheme not in ("http", "https"):
                raise ValueError("scheme must be http or https")
            # TODO: JSON schema comments that URL must not end with /. Should we
            #   validate/enforce this? or later when using URLs?
        except (TypeError, ValueError) as e:
            err = Err(f"endpoints[{i}].url is invalid", data=data)
            err.__cause__ = e
            return err

        raw_consistency = raw_endpoint.get("consistency")
        try:
            if not isinstance(raw_consistency, str):
                raise TypeError("value must be a string")
            consistency = ConsistencyLevel(raw_consistency)
        except (TypeError, ValueError):
            return Err(
                f"endpoints[{i}].consistency is not one of "
                f"{', '.join(ConsistencyLevel)}",
                data=data,
            )

        endpoints.append(EndpointInfo(url=url, consistency=consistency))

    return DatabaseMetadata(
        version=version,
        database_id=database_id,
        endpoints=tuple(endpoints),
        token=token,
        expires_at=expires_at,
    )
