from __future__ import annotations

import re
from datetime import datetime
from unittest.mock import create_autospec

import pytest
from v8serialize import Encoder
from v8serialize.jstypes import JSBigInt
from yarl import URL

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_types import KvWriter
from denokv._kv_types import KvWriterWriteResult
from denokv._kv_values import KvEntry
from denokv._kv_values import KvU64
from denokv._kv_values import VersionStamp
from denokv._kv_writes import Check
from denokv._kv_writes import CommittedWrite
from denokv._kv_writes import ConflictedWrite
from denokv._kv_writes import Delete
from denokv._kv_writes import Enqueue
from denokv._kv_writes import Limit
from denokv._kv_writes import LimitExceededPolicy
from denokv._kv_writes import Max
from denokv._kv_writes import Min
from denokv._kv_writes import PlannedWrite
from denokv._kv_writes import Sum
from denokv._pycompat.typing import TypedDict
from denokv.auth import ConsistencyLevel
from denokv.auth import EndpointInfo
from denokv.datapath import AutoRetry
from denokv.datapath import CheckFailure
from denokv.datapath import ResponseUnsuccessful
from denokv.kv_keys import KvKey
from denokv.result import Err
from denokv.result import Ok
from test.denokv_testing import mocked

EP = EndpointInfo(URL("https://example.com/"), consistency=ConsistencyLevel.STRONG)


@pytest.fixture
def planned_write() -> PlannedWrite:
    return (
        PlannedWrite()
        .check(KvKey("check1"), VersionStamp(1))
        .check(KvKey("check2"), VersionStamp(2))
        .check(KvKey("check3"), None)
        .sum(KvKey("sum1"), 1, clamp_under=0, clamp_over=2)
        .sum(KvKey("sum2"), 2, "bigint", clamp_under=0)
        .sum(KvKey("sum3"), 4, "u64")
        .delete(KvKey("delete1"))
        .enqueue(message="Hi", retry_delays=(1, 2, 3))
    )


@pytest.mark.asyncio()
async def test_as_protobuf(
    v8_encoder: Encoder,
) -> None:
    assert PlannedWrite().as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.AtomicWrite(),
    )

    T1 = datetime.fromisoformat("2000-01-01T00:00:00Z")

    planned_write_start = PlannedWrite()
    planned_write = (
        planned_write_start.check(KvEntry(KvKey("check1"), None, VersionStamp(1)))
        .check(Check(KvKey("check2"), VersionStamp(2)))
        .check_key_not_set(KvKey("check3"))
        .check_key_has_version(KvKey("check4"), VersionStamp(4))
        .check(KvKey("check5"))
        .sum(KvKey("sum1"), KvU64(1))
        .sum(KvKey("sum2"), 2.0, abort_under=0)
        .sum(KvKey("sum3"), 0.2, clamp_under=0, clamp_over=1)
        .sum(KvKey("sum4"), 4.0, limit=Limit(1.0, 3.0, LimitExceededPolicy.CLAMP))
        .mutate(Sum(KvKey("sum5"), JSBigInt(42)))
        .min(KvKey("min1"), 1)
        .min(KvKey("min2"), KvU64(2))
        .mutate(Min(KvKey("min3"), 3, expire_at=T1))
        .max(KvKey("max1"), 1)
        .max(KvKey("max2"), KvU64(2))
        .mutate(Max(KvKey("max3"), 3, expire_at=T1))
        .delete(KvKey("delete1"))
        .mutate(Delete(KvKey("delete2")))
        .enqueue({"event": "example1"}, delivery_time=T1, retry_delays=[1, 10, 100])
        .enqueue(
            Enqueue({"event": "example2"}, delivery_time=T1, retry_delays=[1, 10, 100])
        )
    )
    assert planned_write is planned_write_start  # builder methods update in-place

    assert planned_write.as_protobuf(v8_encoder=v8_encoder) == (
        datapath_pb2.AtomicWrite(
            checks=[
                pb_msg
                for check in [
                    Check(KvKey("check1"), VersionStamp(1)),
                    Check(KvKey("check2"), VersionStamp(2)),
                    Check(KvKey("check3"), None),
                    Check(KvKey("check4"), VersionStamp(4)),
                    Check(KvKey("check5"), None),
                ]
                for pb_msg in check.as_protobuf(v8_encoder=v8_encoder)
            ],
            mutations=[
                pb_msg
                for mutation in [
                    Sum(KvKey("sum1"), KvU64(1)),
                    Sum(KvKey("sum2"), 2.0, abort_under=0),
                    Sum(KvKey("sum3"), 0.2, clamp_under=0, clamp_over=1),
                    Sum(
                        KvKey("sum4"),
                        4.0,
                        limit=Limit(1.0, 3.0, LimitExceededPolicy.CLAMP),
                    ),
                    Sum(KvKey("sum5"), JSBigInt(42)),
                    Min(KvKey("min1"), 1),
                    Min(KvKey("min2"), KvU64(2)),
                    Min(KvKey("min3"), 3, expire_at=T1),
                    Max(KvKey("max1"), 1),
                    Max(KvKey("max2"), KvU64(2)),
                    Max(KvKey("max3"), 3, expire_at=T1),
                    Delete(KvKey("delete1")),
                    Delete(KvKey("delete2")),
                ]
                for pb_msg in mutation.as_protobuf(v8_encoder=v8_encoder)
            ],
            enqueues=[
                pb_msg
                for enqueue in [
                    Enqueue(
                        {"event": "example1"},
                        delivery_time=T1,
                        retry_delays=[1, 10, 100],
                    ),
                    Enqueue(
                        {"event": "example2"},
                        delivery_time=T1,
                        retry_delays=[1, 10, 100],
                    ),
                ]
                for pb_msg in enqueue.as_protobuf(v8_encoder=v8_encoder)
            ],
        ),
    )


class AtomicWriteRepresentationWriterWriteOptions(TypedDict, total=False):
    kv: KvWriter
    v8_encoder: Encoder


@pytest.mark.asyncio()
@pytest.mark.parametrize("kv_via_write_arg", [False, True])
@pytest.mark.parametrize("v8_encoder_via_write_arg", [False, True])
async def test_write__handles_successful_write(
    kv_via_write_arg: bool,
    v8_encoder_via_write_arg: bool,
    planned_write: PlannedWrite,
    v8_encoder: Encoder,
) -> None:
    writer: KvWriter = create_autospec(KvWriter)
    successful_write: KvWriterWriteResult = Ok((VersionStamp(1), EP))
    mocked(writer.write).return_value = successful_write

    kwargs = AtomicWriteRepresentationWriterWriteOptions()
    if kv_via_write_arg:
        kwargs["kv"] = writer
    else:
        planned_write.kv = writer
    if v8_encoder_via_write_arg:
        kwargs["v8_encoder"] = v8_encoder
    else:
        planned_write.v8_encoder = v8_encoder

    result = await planned_write.write(**kwargs)

    versionstamp, endpoint = successful_write.value_or_raise()
    assert result == CommittedWrite(
        versionstamp=versionstamp,
        endpoint=endpoint,
        checks=planned_write.checks,
        mutations=planned_write.mutations,
        enqueues=planned_write.enqueues,
    )
    mocked(writer.write).assert_called_once_with(
        protobuf_atomic_write=planned_write.as_protobuf(v8_encoder=v8_encoder)[0]
    )


@pytest.mark.asyncio()
async def test_write__handles_unsuccessful_conflicted_write(
    planned_write: PlannedWrite,
    v8_encoder: Encoder,
) -> None:
    writer: KvWriter = create_autospec(KvWriter)
    failed_write: KvWriterWriteResult = Err(
        error := CheckFailure(
            "Not all checks required by the Atomic Write passed",
            all_checks=[pb for c in planned_write.checks for pb in c.as_protobuf()],
            failed_check_indexes=[0, 2],
            endpoint=EP,
        )
    )
    mocked(writer.write).return_value = failed_write

    result = await planned_write.write(kv=writer, v8_encoder=v8_encoder)

    assert result == ConflictedWrite(
        failed_checks=list(error.failed_check_indexes),
        checks=planned_write.checks,
        mutations=planned_write.mutations,
        enqueues=planned_write.enqueues,
        endpoint=error.endpoint,
    )


@pytest.mark.asyncio()
async def test_write__handles_write_request_failure(
    planned_write: PlannedWrite, v8_encoder: Encoder
) -> None:
    writer: KvWriter = create_autospec(KvWriter)
    failed_write: KvWriterWriteResult = Err(
        error := ResponseUnsuccessful(
            "Server rejected Data Path request indicating client error",
            status=403,
            body_text="Permission denied",
            endpoint=EP,
            auto_retry=AutoRetry.NEVER,
        )
    )
    mocked(writer.write).return_value = failed_write

    with pytest.raises(ResponseUnsuccessful) as exc_info:
        await planned_write.write(kv=writer, v8_encoder=v8_encoder)

    assert exc_info.value == error


@pytest.mark.asyncio()
async def test_write__requires_Kv() -> None:
    with pytest.raises(
        TypeError,
        match=re.escape(
            "PlannedWrite.write() must get a value for its 'kv' argument when "
            "'self.kv' isn't set"
        ),
    ):
        await PlannedWrite().write()


@pytest.mark.asyncio()
async def test_check__raises_on_invalid_use() -> None:
    with pytest.raises(
        TypeError,
        match=r"'versionstamp' argument cannot be set when the first argument to "
        r"check\(\) is an object with 'key' and 'versionstamp' attributes",
    ):
        PlannedWrite().check(
            KvEntry(KvKey("a"), None, versionstamp=VersionStamp(1)),
            versionstamp=VersionStamp(2),
        )  # type: ignore[call-overload]

    with pytest.raises(
        TypeError,
        match=r"'versionstamp' argument cannot be set when the first argument to "
        r"check\(\) is an object with an 'as_protobuf' method",
    ):
        PlannedWrite().check(
            Check(KvKey("a"), versionstamp=VersionStamp(1)),
            versionstamp=VersionStamp(2),
        )  # type: ignore[call-overload]
