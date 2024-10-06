from __future__ import annotations

import re
from typing import Literal
from typing import Sequence

import pytest
from fdb.tuple import pack
from typing_extensions import Unpack

from denokv.datapath import AnyKvKey
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyTuple

# from denokv.kv import DefaultKvKey
from denokv.kv import DefaultKvKey
from denokv.kv import KvKey


def test_kvkey__generic_tuple_params() -> None:
    key: KvKey[str, int] = KvKey("foo", 32)
    a: str
    b: int
    a, b = key
    assert a == "foo"
    assert b == 32

    def use_key(key: KvKey[Literal["things"], int]) -> None:
        pass

    use_key(KvKey("things", 1))
    use_key(KvKey("bad", 1))  # type: ignore[arg-type]


def test_from_kv_key_bytes() -> None:
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Cannot create KvKey from packed key: b'\\xff': value is not a "
            "valid packed key"
        ),
    ):
        KvKey.from_kv_key_bytes(b"\xff")

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Cannot create KvKey from packed key: "
            "b'\\x02nested:\\x00\\x05\\x00': key contains types other than "
            "str, bytes, int, float, bool: ('nested:', ())"
        ),
    ):
        KvKey.from_kv_key_bytes(pack(("nested:", ())))

    assert KvKey("foo", 42) == KvKey.from_kv_key_bytes(pack(("foo", 42)))


def test_kv_key_bytes() -> None:
    assert KvKey("foo", b"bar", 1, 2.0, True).kv_key_bytes() == pack(
        (("foo", b"bar", 1, 2.0, True))
    )


def test_is_KvKeyEncodable() -> None:
    assert isinstance(KvKey(), KvKeyEncodable)


def test_unknown_pieces() -> None:
    def get_pieces() -> Sequence[KvKeyPiece]:
        return ["foo", b"bar", 1, 2.0, True]

    key: KvKey[Unpack[tuple[KvKeyPiece, ...]]] = KvKey(*get_pieces())
    key2: KvKey = KvKey(*get_pieces())

    def use_key(key: AnyKvKey) -> None:
        pass

    def use_key_encodable(key: KvKeyEncodable) -> None:
        pass

    def use_key_tuple(key: KvKeyTuple) -> None:
        pass

    # KvKey passes type checks as all key types, including the KvKeyTuple plain
    # tuple type.
    use_key(key)
    use_key_encodable(key)
    use_key_tuple(key)

    use_key(key2)
    use_key_encodable(key2)
    use_key_tuple(key2)


def test_wrap_tuple_keys() -> None:
    key: DefaultKvKey = KvKey.wrap_tuple_keys(("a", 2, True))
    assert isinstance(key, KvKey)
    assert key == KvKey("a", 2, True)
    assert key == ("a", 2, True)
