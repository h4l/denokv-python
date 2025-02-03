from itertools import count
from itertools import islice

from v8serialize import Encoder

from denokv import _datapath_pb2 as datapath_pb2
from denokv._kv_writes import DEFAULT_ENQUEUE_RETRY_DELAY_COUNT
from denokv._kv_writes import DEFAULT_ENQUEUE_RETRY_DELAYS
from denokv._kv_writes import Enqueue
from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.kv_keys import KvKey

T1 = parse_rfc3339_datetime("2000-01-02T03:04:05.6Z").value_or_raise()


def test_constructors() -> None:
    message = {"msg": "Hi"}
    instance = Enqueue(message)
    assert instance.message is message
    assert instance.delivery_time is None
    assert instance.retry_delays == DEFAULT_ENQUEUE_RETRY_DELAYS
    assert len(instance.dead_letter_keys) == 0

    retry_delays = [1, 2, 3]
    dead_letter_keys = (KvKey("a"),)
    instance = Enqueue(
        message,
        delivery_time=T1,
        retry_delays=retry_delays,
        dead_letter_keys=dead_letter_keys,
    )
    assert instance.message is message
    assert instance.delivery_time == T1
    assert instance.retry_delays == retry_delays
    assert instance.dead_letter_keys == dead_letter_keys


def test_as_protobuf__default_retry_delays(v8_encoder: Encoder) -> None:
    message = {"msg": "Hi"}
    instance = Enqueue(message)
    (protobuf,) = instance.as_protobuf(v8_encoder=v8_encoder)

    # Default retry delays have random jitter. A fixed number are drawn from the
    # backoff provider.
    assert len(protobuf.backoff_schedule) == DEFAULT_ENQUEUE_RETRY_DELAY_COUNT
    assert all(delay > 0 for delay in protobuf.backoff_schedule)


def test_as_protobuf(v8_encoder: Encoder) -> None:
    message = {"msg": "Hi"}
    instance = Enqueue(message, retry_delays=[])
    (protobuf,) = instance.as_protobuf(v8_encoder=v8_encoder)
    assert protobuf == datapath_pb2.Enqueue(payload=bytes(v8_encoder.encode(message)))

    evaluated_backoff = [
        i * 1000 for i in islice(count(1), DEFAULT_ENQUEUE_RETRY_DELAY_COUNT)
    ]
    instance = Enqueue(
        message,
        retry_delays=count(1),
        delivery_time=T1,
        dead_letter_keys=[KvKey("a"), KvKey("b")],
    )
    (protobuf,) = instance.as_protobuf(v8_encoder=v8_encoder)
    assert protobuf == datapath_pb2.Enqueue(
        payload=bytes(v8_encoder.encode(message)),
        backoff_schedule=evaluated_backoff,
        keys_if_undelivered=[bytes(KvKey("a")), bytes(KvKey("b"))],
        deadline_ms=int(T1.timestamp() * 1000),
    )
