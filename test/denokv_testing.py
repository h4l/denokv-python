from datetime import datetime
from datetime import timedelta
from typing import Sequence
from typing import TypeVar
from uuid import UUID

from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.result import Ok
from denokv.result import Result

T = TypeVar("T")
E = TypeVar("E")


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
