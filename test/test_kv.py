from __future__ import annotations

import asyncio
from datetime import datetime
from datetime import timedelta
from enum import StrEnum
from functools import partial
from itertools import repeat
from typing import TYPE_CHECKING
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Mapping
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch
from uuid import UUID

import aiohttp
import pytest
import pytest_asyncio
import v8serialize
from fdb.tuple import unpack
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st
from v8serialize import Decoder
from yarl import URL

from denokv import datapath
from denokv._datapath_pb2 import KvEntry as ProtobufKvEntry
from denokv._datapath_pb2 import ReadRangeOutput
from denokv._datapath_pb2 import SnapshotRead
from denokv._datapath_pb2 import SnapshotReadOutput
from denokv._datapath_pb2 import SnapshotReadStatus
from denokv._datapath_pb2 import ValueEncoding
from denokv.asyncio import loop_time
from denokv.auth import ConsistencyLevel
from denokv.auth import DatabaseMetadata
from denokv.auth import EndpointInfo
from denokv.auth import HttpResponseMetadataExchangeDenoKvError
from denokv.auth import MetadataExchangeDenoKvError
from denokv.backoff import Backoff
from denokv.datapath import AnyKvKey
from denokv.datapath import AutoRetry
from denokv.datapath import DataPathError
from denokv.datapath import KvKeyTuple
from denokv.datapath import RequestUnsuccessful
from denokv.datapath import ResponseUnsuccessful
from denokv.datapath import SnapshotReadResult
from denokv.datapath import increment_packed_key
from denokv.datapath import pack_key
from denokv.errors import DenoKvError
from denokv.errors import InvalidCursor
from denokv.kv import AuthenticatorFn
from denokv.kv import CachedValue
from denokv.kv import DatabaseMetadataCache
from denokv.kv import EndpointSelector
from denokv.kv import Kv
from denokv.kv import KvEntry
from denokv.kv import KVFlags
from denokv.kv import KvListOptions
from denokv.kv import KvU64
from denokv.kv import VersionStamp
from denokv.kv import normalize_key
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result
from test.advance_time import advance_time
from test.denokv_testing import ExampleCursorFormat
from test.denokv_testing import MockKvDb
from test.denokv_testing import add_entries
from test.denokv_testing import assume_ok
from test.denokv_testing import mk_db_meta
from test.denokv_testing import unsafe_parse_protobuf_kv_entry

if TYPE_CHECKING:
    from typing import Generator

    from typing_extensions import TypeAlias

pytest_mark_asyncio = pytest.mark.asyncio(loop_scope="module")


@given(v=st.integers(min_value=0, max_value=2**80 - 1))
def test_VersionStamp_init(v: int) -> None:
    vs_int = VersionStamp(v)
    assert int(vs_int) == v
    assert VersionStamp(str(vs_int)) == vs_int
    assert VersionStamp(bytes(vs_int)) == vs_int
    assert bytes(vs_int) == vs_int
    assert isinstance(vs_int, bytes)


@given(i=st.integers(min_value=0, max_value=2**64 - 1))
def test_KvU64_init(i: int) -> None:
    u64 = KvU64(i)
    assert int(u64) == i
    assert KvU64(bytes(u64)) == u64
    assert u64.to_bytes() == bytes(u64)
    assert u64.to_bytes() == i.to_bytes(8, "little")


@given(
    v1=st.integers(min_value=0, max_value=2**80 - 1),
    v2=st.integers(min_value=0, max_value=2**80 - 1),
)
def test_VersionStamp_ordering(v1: int, v2: int) -> None:
    vs1, vs2 = VersionStamp(v1), VersionStamp(v2)
    if v1 < v2:
        assert vs1 < vs2
    elif v1 > v2:
        assert vs1 > vs2
    else:
        assert vs1 == vs2


def test_KVU64__bytes() -> None:
    assert KvU64(bytes(KvU64(123456789))).value == 123456789
    assert KvU64(KvU64(123456789).to_bytes()).value == 123456789


def test_EndpointSelector__rejects_meta_without_strong_endpoint() -> None:
    meta_no_strong = mk_db_meta(
        [
            EndpointInfo(
                url=URL("https://example.com/eventual/"),
                consistency=ConsistencyLevel.EVENTUAL,
            )
        ]
    )

    with pytest.raises(ValueError, match=r"no endpoint has strong consistency"):
        EndpointSelector(meta_no_strong)


def test_EndpointSelector__single() -> None:
    meta = mk_db_meta(
        [
            endpoint := EndpointInfo(
                url=URL("https://example.com/"), consistency=ConsistencyLevel.STRONG
            )
        ]
    )

    selector = EndpointSelector(meta)
    assert selector.get_endpoint(consistency=ConsistencyLevel.STRONG) == endpoint
    assert selector.get_endpoint(consistency=ConsistencyLevel.EVENTUAL) == endpoint


def test_EndpointSelector__multi() -> None:
    meta = mk_db_meta(
        [
            endpoint_eventual := EndpointInfo(
                url=URL("https://example.com/eventual/"),
                consistency=ConsistencyLevel.EVENTUAL,
            ),
            endpoint_strong := EndpointInfo(
                url=URL("https://example.com/strong/"),
                consistency=ConsistencyLevel.STRONG,
            ),
        ]
    )

    selector = EndpointSelector(meta)
    assert selector.get_endpoint(consistency=ConsistencyLevel.STRONG) == endpoint_strong
    # Currently we just select the first option, and eventual is first here.
    assert (
        selector.get_endpoint(consistency=ConsistencyLevel.EVENTUAL)
        == endpoint_eventual
    )


def test_CachedValue() -> None:
    cv = CachedValue(fresh_until=10, value="foo")
    assert cv.value == "foo"
    assert cv.is_fresh(time=8)
    assert not cv.is_fresh(time=10)
    assert not cv.is_fresh(time=20)


@pytest.fixture
def get_db_metadata() -> AuthenticatorFn:
    next_token_id = 1

    async def get_db_metadata() -> (
        Result[DatabaseMetadata, MetadataExchangeDenoKvError]
    ):
        nonlocal next_token_id
        token_id = next_token_id
        next_token_id += 1

        return Ok(
            DatabaseMetadata(
                version=3,
                database_id=UUID("00000000-0000-0000-0000-000000000000"),
                expires_at=datetime.now() + timedelta(minutes=5),
                endpoints=[
                    EndpointInfo(
                        url=URL("https://example.com/"),
                        consistency=ConsistencyLevel.STRONG,
                    )
                ],
                token=f"token_{token_id}",
            )
        )

    return get_db_metadata


@pytest_mark_asyncio
async def test_DatabaseMetadataCache__reloads_meta_from_fn(
    get_db_metadata: AuthenticatorFn,
) -> None:
    mock_get_db_metadata = Mock(side_effect=get_db_metadata)

    meta_1 = assume_ok(await get_db_metadata())

    cache = DatabaseMetadataCache(
        get_database_metadata=mock_get_db_metadata, initial=meta_1
    )
    assert cache.current is not None
    assert cache.current.is_fresh(time=loop_time())
    assert assume_ok(await cache.get()) is meta_1

    mock_get_db_metadata.assert_not_called()

    cache.purge(meta_1)

    assert cache.current is None
    meta_2 = assume_ok(await cache.get())
    mock_get_db_metadata.assert_called_once()
    assert meta_2.token == "token_2"

    assert cast(CachedValue[DatabaseMetadata], cache.current).is_fresh(loop_time())
    await advance_time(60 * 5)
    assert not cast(CachedValue[DatabaseMetadata], cache.current).is_fresh(loop_time())

    meta_3 = assume_ok(await cache.get())
    assert len(mock_get_db_metadata.mock_calls) == 2
    assert meta_3.token == "token_3"


GetDbMetaFuture: TypeAlias = asyncio.Future[
    Result[DatabaseMetadata, MetadataExchangeDenoKvError]
]


@pytest_mark_asyncio
async def test_DatabaseMetadataCache__combines_concurrent_reloads_into_1_call(
    get_db_metadata: AuthenticatorFn,
) -> None:
    """We don't make multiple auth calls when the metadata cache expires."""
    meta_1 = assume_ok(await get_db_metadata())
    future_meta_1: GetDbMetaFuture = asyncio.Future()
    mock_get_db_metadata = Mock(side_effect=lambda: future_meta_1)

    cache = DatabaseMetadataCache(get_database_metadata=mock_get_db_metadata)

    future_gets = asyncio.gather(cache.get(), cache.get(), cache.get())
    asyncio.get_event_loop().call_soon(lambda: future_meta_1.set_result(Ok(meta_1)))
    gets = await future_gets
    assert tuple(assume_ok(g) for g in gets) == (meta_1, meta_1, meta_1)
    assert len(mock_get_db_metadata.mock_calls) == 1


@pytest_mark_asyncio
async def test_DatabaseMetadataCache__handles_failed_auth_calls(
    get_db_metadata: AuthenticatorFn,
) -> None:
    """Failed auth calls retry on next get() following error."""
    meta = assume_ok(await get_db_metadata())
    error = HttpResponseMetadataExchangeDenoKvError(
        "Unavailable", status=503, body_text="Unavailable", retryable=True
    )
    future_meta_1: GetDbMetaFuture = asyncio.Future()
    future_meta_2: GetDbMetaFuture = asyncio.Future()

    # Mock returns the future 1 then 2 on sequential calls
    mock_get_db_metadata = Mock(side_effect=[future_meta_1, future_meta_2])

    cache = DatabaseMetadataCache(get_database_metadata=mock_get_db_metadata)

    future_gets_1 = asyncio.gather(cache.get(), cache.get(), cache.get())
    asyncio.get_running_loop().call_soon(lambda: future_meta_1.set_result(Err(error)))
    a, b, c = await future_gets_1
    assert a == b and b == c
    assert isinstance(a, Err)
    assert a.error == error
    mock_get_db_metadata.assert_called_once()

    future_gets_2 = asyncio.gather(cache.get(), cache.get(), cache.get())
    asyncio.get_running_loop().call_soon(lambda: future_meta_2.set_result(Ok(meta)))
    d, e, f = await future_gets_2
    assert d == e and e == f
    assert isinstance(d, Ok)
    assert d.value == meta
    assert len(mock_get_db_metadata.mock_calls) == 2


def test_normalize_key() -> None:
    pieces = ("a", b"b", 1, 2.0, True)
    normalized = normalize_key(pieces, bigints=False)
    assert tuple((type(p), p) for p in normalized) == (
        (str, "a"),
        (bytes, b"b"),
        (float, 1.0),
        (float, 2.0),
        (bool, True),
    )
    normalized_bigint = normalize_key(pieces, bigints=True)
    assert tuple((type(p), p) for p in normalized_bigint) == (
        (str, "a"),
        (bytes, b"b"),
        (int, 1),
        (float, 2.0),
        (bool, True),
    )

    # this is why normalizing for JS compatibility is important
    assert pack_key(normalized) != pack_key(normalized_bigint)

    with pytest.raises(
        ValueError,
        match=r"int value is too large to be losslessly normalized to a float: "
        r"9007199254740992",
    ):
        normalize_key((2**53,))

    assert normalize_key((2**53,), bigints=True) == (2**53,)


@pytest.fixture
def mock_snapshot_read() -> Generator[Mock]:
    mock = AsyncMock(side_effect=NotImplementedError)
    with patch("denokv.datapath.snapshot_read", mock) as mock:
        yield mock


@pytest.fixture
def retry_delays() -> Backoff:
    return ()


@pytest_asyncio.fixture
async def client_session() -> AsyncGenerator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as cs:
        yield cs


@pytest.fixture
def meta() -> DatabaseMetadata:
    return mk_db_meta(
        [EndpointInfo(URL("https://example.com/"), ConsistencyLevel.STRONG)]
    )


@pytest.fixture
def auth_fn(meta: DatabaseMetadata) -> AuthenticatorFn:
    async def auth_fn() -> Result[DatabaseMetadata, MetadataExchangeDenoKvError]:
        return Ok(meta)

    return auth_fn


@pytest.fixture(scope="session")
def v8_decoder() -> Decoder:
    return v8serialize.Decoder()


@pytest.fixture
def kv_flags() -> KVFlags:
    return KVFlags.NoFlag  # disable int -> float normalisation by default


@pytest.fixture
def create_db(
    client_session: aiohttp.ClientSession,
    auth_fn: AuthenticatorFn,
    retry_delays: Backoff,
    v8_decoder: Decoder,
    kv_flags: KVFlags,
) -> partial[Kv]:
    return partial(
        Kv,
        session=client_session,
        auth=auth_fn,
        retry=retry_delays,
        v8_decoder=v8_decoder,
        flags=kv_flags,
    )


@pytest.fixture
def db(create_db: partial[Kv]) -> Kv:
    return create_db()


def pack_kv_entry(
    key: AnyKvKey, value: bytes, versionstamp: int = 1
) -> ProtobufKvEntry:
    return ProtobufKvEntry(
        key=pack_key(key),
        value=value,
        encoding=ValueEncoding.VE_BYTES,
        versionstamp=bytes(VersionStamp(versionstamp)),
    )


@pytest_mark_asyncio
async def test_Kv_get__rejects_invalid_arguments(
    db: Kv, mock_snapshot_read: AsyncMock
) -> None:
    with pytest.raises(
        TypeError, match=r"cannot use positional keys and keys keyword argument"
    ):
        await db.get(("a", 1), keys=[("a", 2)])  # type: ignore[call-overload]
    with pytest.raises(TypeError, match=r"at least one key argument must be passed"):
        await db.get()  # type: ignore[call-overload]


@pytest_mark_asyncio
async def test_Kv_get__returns_single_value_for_single_key(
    db: Kv, mock_snapshot_read: AsyncMock
) -> None:
    read_output = SnapshotReadOutput(
        ranges=[ReadRangeOutput(values=[pack_kv_entry(("a", 1), b"x")])],
        read_disabled=False,
        read_is_strongly_consistent=True,
        status=SnapshotReadStatus.SR_SUCCESS,
    )

    mock_snapshot_read.side_effect = None
    mock_snapshot_read.return_value = Ok(read_output)

    k, kval = await db.get(("a", 1))

    assert k == ("a", 1)
    assert kval is not None
    assert kval.key == k
    assert kval.value == b"x"
    assert kval.versionstamp == VersionStamp(1)


class ArgKind(StrEnum):
    KWARGS = "kwargs"
    POSARGS = "posargs"


@pytest.mark.parametrize("n", [2, 3, 10])
@pytest.mark.parametrize("arg_kind", ArgKind)
@pytest_mark_asyncio
async def test_Kv_get__returns_n_values_for_n_keys(
    n: int, arg_kind: ArgKind, db: Kv, mock_snapshot_read: AsyncMock
) -> None:
    read_output = SnapshotReadOutput(
        ranges=[
            ReadRangeOutput(
                values=[
                    pack_kv_entry(("i", i), bytes([ord("a") + i]), versionstamp=10 + i)
                ]
            )
            for i in range(n)
        ],
        read_disabled=False,
        read_is_strongly_consistent=True,
        status=SnapshotReadStatus.SR_SUCCESS,
    )

    mock_snapshot_read.side_effect = None
    mock_snapshot_read.return_value = Ok(read_output)

    if arg_kind is ArgKind.KWARGS:
        values = await db.get(keys=[("i", i) for i in range(n)])
    else:
        values = await db.get(*[("i", i) for i in range(n)])

    assert isinstance(values, tuple)
    assert len(values) == n
    assert values == tuple(
        (
            ("i", i),
            KvEntry(
                ("i", i), value=bytes([ord("a") + i]), versionstamp=VersionStamp(10 + i)
            ),
        )
        for i in range(n)
    )


@pytest.mark.parametrize(
    "kv_flags, int_type", [(KVFlags.IntAsNumber, float), (KVFlags.NoFlag, int)]
)
@pytest_mark_asyncio
async def test_Kv_get__treats_int_as_float_when_IntAsNumber_enabled(
    db: Kv, mock_snapshot_read: AsyncMock, int_type: type
) -> None:
    read_output = SnapshotReadOutput(
        ranges=[ReadRangeOutput(values=[pack_kv_entry(("a", int_type(1)), b"x")])],
        read_disabled=False,
        read_is_strongly_consistent=True,
        status=SnapshotReadStatus.SR_SUCCESS,
    )

    mock_snapshot_read.side_effect = None
    mock_snapshot_read.return_value = Ok(read_output)

    k, kval = await db.get(("a", 1))

    assert k == ("a", 1)  # 1 == 1.0
    assert type(k[1]) is int_type

    assert kval is not None
    assert kval.key == k
    assert type(kval.key[1]) is int_type

    assert kval.value == b"x"
    assert kval.versionstamp == VersionStamp(1)

    read_input: SnapshotRead = mock_snapshot_read.mock_calls[0].kwargs["read"]
    start_key = unpack(read_input.ranges[0].start)
    assert start_key == k
    assert type(start_key[1]) is int_type


def retryable_errors(
    *, auto_retries: st.SearchStrategy[AutoRetry]
) -> st.SearchStrategy[RequestUnsuccessful | ResponseUnsuccessful]:
    """Generate DataPathError exceptions with retryable auto_retry values."""
    return st.one_of(
        st.builds(
            lambda auto_retry: RequestUnsuccessful(
                message="Example",
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=auto_retry,
            ),
            auto_retries,
        ),
        st.builds(
            lambda auto_retry: ResponseUnsuccessful(
                message="Example",
                body_text="Oops",
                status=500,
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=auto_retry,
            ),
            auto_retries,
        ),
    )


meta_backoff_retryable_errors = retryable_errors(
    auto_retries=st.sampled_from(
        [AutoRetry.AFTER_BACKOFF, AutoRetry.AFTER_METADATA_EXCHANGE]
    )
)


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(data=st.data(), retry_delays=st.sampled_from([[], [1.0], [1.0, 2.0, 4.0]]))
@pytest_mark_asyncio
async def test_Kv_get__retries_retryable_snapshot_read_errors(
    create_db: partial[Kv],
    meta: DatabaseMetadata,
    mock_snapshot_read: AsyncMock,
    data: st.DataObject,
    retry_delays: Backoff,
) -> None:
    auth_fn = AsyncMock(name="auth_fn", return_value=Ok(meta))
    db = create_db(retry=retry_delays, auth=auth_fn)
    retry_errors: list[DataPathError] = []

    def fail_with_retryable_error(*args: Any, **kwargs: Any) -> Err[DenoKvError]:
        err = data.draw(meta_backoff_retryable_errors)
        retry_errors.append(err)
        assert err.auto_retry in {
            AutoRetry.AFTER_BACKOFF,
            AutoRetry.AFTER_METADATA_EXCHANGE,
        }
        return Err(err)

    mock_snapshot_read.side_effect = fail_with_retryable_error

    with pytest.raises(DenoKvError) as exc_info:
        future_get = asyncio.create_task(db.get(("a",)))

        # fast-forward each retry's sleep as it occurs to save test time
        for delay in retry_delays:
            await advance_time(delay, pre_ticks=10)
        await future_get

    # The get() fails with the error on the final (non-retried) attempt
    assert exc_info.value == retry_errors[-1]

    # Retries AFTER_METADATA_EXCHANGE invalidate the metadata cache before
    # retrying, auth_fn should be called once at the first attempt plus once for
    # each AFTER_METADATA_EXCHANGE error, other than the last, which isn't
    # retried
    assert len(auth_fn.mock_calls) == 1 + sum(
        1
        for e in retry_errors[:-1]
        if e.auto_retry == AutoRetry.AFTER_METADATA_EXCHANGE
    )


@pytest_mark_asyncio
async def test_Kv_list__rejects_invalid_arguments(db: Kv) -> None:
    with pytest.raises(ValueError, match=r"limit cannot be negative"):
        async for _ in db.list(limit=-1):
            raise AssertionError("should not generate values")

    with pytest.raises(ValueError, match=r"batch_size cannot be < 1"):
        async for _ in db.list(batch_size=0):
            raise AssertionError("should not generate values")

    with pytest.raises(InvalidCursor, match=r"cursor is not valid URL-safe base64"):
        async for _ in db.list(cursor="x"):
            raise AssertionError("should not generate values")


def pack_example_cursor(key: KvKeyTuple) -> str:
    return assume_ok(ExampleCursorFormat.INSTANCE.get_cursor_for_key(key))


@pytest.mark.parametrize(
    "range_options,cursor_key",
    [
        (dict(prefix=("c",)), ("b",)),
        (dict(prefix=("c",)), ("d",)),
        (dict(start=("c",)), ("b",)),
        (dict(end=("c",)), ("c", 1)),
    ],
)
@pytest_mark_asyncio
async def test_Kv_list__rejects_cursor_outside_listed_range(
    db: Kv, range_options: KvListOptions, cursor_key: KvKeyTuple
) -> None:
    with pytest.raises(
        InvalidCursor, match=r"cursor is not within the the start and end key range"
    ):
        async for _ in db.list(
            **KvListOptions(
                **range_options,
                cursor_format_type=ExampleCursorFormat,
                cursor=pack_example_cursor(cursor_key),
            )
        ):
            raise AssertionError("should not generate values")


@pytest.fixture
def mock_db() -> MockKvDb:
    return MockKvDb()


@pytest.fixture
def list_example_entries() -> Mapping[KvKeyTuple, object]:
    return {
        **{("a", i): f"foo_{i}".encode() for i in range(4)},
        **{("b", i): f"bar_{i}".encode() for i in range(4)},
        **{("c", i): f"baz_{i}".encode() for i in range(4)},
    }


@pytest.fixture
def mock_snapshot_read_to_return_mock_db_results(
    mock_snapshot_read: AsyncMock, mock_db: MockKvDb
) -> Callable[[], AsyncMock]:
    async def snapshot_read_effect(
        *,
        session: aiohttp.ClientSession,
        meta: DatabaseMetadata,
        endpoint: EndpointInfo,
        read: SnapshotRead,
    ) -> SnapshotReadResult:
        assert len(read.ranges) == 1
        snapshot_read_output = SnapshotReadOutput(
            ranges=[mock_db.snapshot_read_range(read.ranges[0])],
            read_disabled=False,
            read_is_strongly_consistent=True,
            status=SnapshotReadStatus.SR_SUCCESS,
        )
        return Ok(snapshot_read_output)

    def apply() -> AsyncMock:
        mock_snapshot_read.side_effect = snapshot_read_effect
        return mock_snapshot_read

    return apply


list_example_keys = st.one_of(
    st.none(),
    st.just(()),
    st.tuples(st.sampled_from("_abcd")),
    st.tuples(
        st.sampled_from("_abcd"),
        st.integers(min_value=-1, max_value=5),
    ),
)


def list_example_cursors(
    start: bytes, end: bytes
) -> st.SearchStrategy[tuple[KvKeyTuple, str] | None]:
    """Generate valid cursors for list_example_keys within a range."""
    p0_choices = list("_abcd")
    p1_choices = range(-1, 6)
    possible_cursor_keys = [
        k
        for k in [
            (),
            *((p0,) for p0 in p0_choices),
            *((p0, p1) for p0 in p0_choices for p1 in p1_choices),
        ]
        if start <= pack_key(k) < end
    ]

    if not possible_cursor_keys:
        return st.none()

    return st.one_of(
        st.none(),
        st.sampled_from(possible_cursor_keys).map(
            lambda k: (k, assume_ok(ExampleCursorFormat.INSTANCE.get_cursor_for_key(k)))
        ),
    )


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    data=st.data(),
    prefix=list_example_keys,
    start=list_example_keys,
    end=list_example_keys,
    limit=st.none() | st.integers(min_value=0, max_value=16),
    reverse=st.none() | st.booleans(),
    consistency=st.none() | st.sampled_from(ConsistencyLevel),
    batch_size=st.none() | st.integers(min_value=1, max_value=16),
)
@pytest_mark_asyncio
async def test_Kv_list__generates_values_from_sequential_snapshot_reads(
    data: st.DataObject,
    db: Kv,
    mock_snapshot_read_to_return_mock_db_results: Callable[[], AsyncMock],
    mock_db: MockKvDb,
    list_example_entries: Mapping[KvKeyTuple, object],
    prefix: KvKeyTuple | None,
    start: KvKeyTuple | None,
    end: KvKeyTuple | None,
    limit: int | None,
    reverse: bool | None,
    consistency: ConsistencyLevel | None,
    batch_size: int | None,
) -> None:
    mock_db.clear()
    add_entries(mock_db, list_example_entries)
    mock_snapshot_read_to_return_mock_db_results()

    # Kv.list() should be equivalent to reading the listed range in one go.
    listed_range = datapath.read_range_multi(
        prefix=prefix,
        start=start,
        end=end,
        limit=len(list_example_entries) if limit is None else limit,
        reverse=reverse or False,
    )

    cursor: tuple[KvKeyTuple, str] | None = data.draw(
        list_example_cursors(listed_range.start, listed_range.end)
    )

    cursor_str: None | str = None
    # Specifying a cursor is equivalent to advancing the start/end boundary,
    # according to direction.
    if cursor is not None:
        cursor_key, cursor_str = cursor
        packed_cursor_key = pack_key(cursor_key)
        if reverse:
            listed_range.end = packed_cursor_key
        else:
            listed_range.start = increment_packed_key(packed_cursor_key)

    listed_kv_entries = [
        (kv_entry.key, kv_entry.value, kv_entry.versionstamp)
        for kv_entry in (
            unsafe_parse_protobuf_kv_entry(x)
            for x in mock_db.snapshot_read_range(listed_range).values
        )
    ]

    results: list[tuple[KvKeyTuple, object, VersionStamp]] = []
    async for kv_entry in db.list(
        prefix=prefix,
        start=start,
        end=end,
        limit=limit,
        reverse=reverse,
        consistency=consistency,
        batch_size=batch_size,
        cursor=cursor_str,
        cursor_format_type=ExampleCursorFormat,
    ):
        results.append((kv_entry.key, kv_entry.value, kv_entry.versionstamp))

        # aside — entries from list() have the cursor value for the entry and a
        # ListContext containing the details of the listing operation.
        assert kv_entry.cursor == assume_ok(
            ExampleCursorFormat.INSTANCE.get_cursor_for_key(kv_entry.key)
        )
        assert kv_entry.listing.prefix == prefix
        assert kv_entry.listing.start == start
        assert kv_entry.listing.end == end
        assert kv_entry.listing.end == end
        assert (
            kv_entry.listing.packed_start
            <= pack_key(kv_entry.key)
            < kv_entry.listing.packed_end
        )
        assert kv_entry.listing.limit == limit
        assert kv_entry.listing.cursor == cursor_str
        assert kv_entry.listing.consistency == consistency or ConsistencyLevel.STRONG
        assert kv_entry.listing.batch_size == batch_size or limit or 100
        assert kv_entry.listing.cursor_format_type == ExampleCursorFormat
        assert isinstance(kv_entry.listing.cursor_format, ExampleCursorFormat)

    assert results == listed_kv_entries


@pytest_mark_asyncio
async def test_Kv_list__retries_retryable_snapshot_read_errors(
    create_db: partial[Kv],
    meta: DatabaseMetadata,
    mock_snapshot_read: AsyncMock,
    # mock_snapshot_read_to_return_mock_db_results: Callable[[], AsyncMock],
    # mock_db: MockKvDb,
    # list_example_entries: Mapping[KvKeyTuple, object],
) -> None:
    auth_fn = AsyncMock(name="auth_fn", return_value=Ok(meta))
    db = create_db(retry=repeat(0), auth=auth_fn)

    auth_fn.side_effect = [
        Err(MetadataExchangeDenoKvError("Failed", retryable=True)),
        Err(MetadataExchangeDenoKvError("Failed", retryable=True)),
        Ok(meta),
        Err(MetadataExchangeDenoKvError("Failed", retryable=True)),
        Ok(meta),
        Ok(meta),
        Ok(meta),
    ]

    mock_snapshot_read.side_effect = [
        Err(
            ResponseUnsuccessful(
                message="Example",
                body_text="Oops",
                status=500,
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=AutoRetry.AFTER_BACKOFF,
            )
        ),
        Err(
            ResponseUnsuccessful(
                message="Example",
                body_text="Oops",
                status=500,
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=AutoRetry.AFTER_METADATA_EXCHANGE,
            )
        ),
        Ok(
            SnapshotReadOutput(
                ranges=[
                    ReadRangeOutput(
                        values=[
                            pack_kv_entry(("a", 1), b"x1"),
                            pack_kv_entry(("a", 2), b"x2"),
                        ]
                    )
                ],
                read_is_strongly_consistent=True,
                status=SnapshotReadStatus.SR_SUCCESS,
            )
        ),
        Err(
            ResponseUnsuccessful(
                message="Example",
                body_text="Oops",
                status=500,
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=AutoRetry.AFTER_BACKOFF,
            )
        ),
        Err(
            ResponseUnsuccessful(
                message="Example",
                body_text="Oops",
                status=500,
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=AutoRetry.AFTER_METADATA_EXCHANGE,
            )
        ),
        Ok(
            SnapshotReadOutput(
                ranges=[
                    ReadRangeOutput(
                        values=[
                            pack_kv_entry(("a", 3), b"x3"),
                            pack_kv_entry(("a", 4), b"x4"),
                        ]
                    )
                ],
                read_is_strongly_consistent=True,
                status=SnapshotReadStatus.SR_SUCCESS,
            )
        ),
        Err(
            ResponseUnsuccessful(
                message="Example",
                body_text="Oops",
                status=500,
                endpoint=EndpointInfo(
                    url=URL("https://example"), consistency=ConsistencyLevel.STRONG
                ),
                auto_retry=AutoRetry.AFTER_METADATA_EXCHANGE,
            )
        ),
        # Empty result - end
        Ok(
            SnapshotReadOutput(
                ranges=[ReadRangeOutput(values=[])],
                read_is_strongly_consistent=True,
                status=SnapshotReadStatus.SR_SUCCESS,
            )
        ),
    ]

    results: list[tuple[KvKeyTuple, object, VersionStamp]] = []
    async for kv_entry in db.list(batch_size=2):
        results.append((kv_entry.key, kv_entry.value, kv_entry.versionstamp))

    assert results == [
        (("a", x), f"x{x}".encode(), VersionStamp(1)) for x in range(1, 5)
    ]
    assert len(auth_fn.mock_calls) == 7
