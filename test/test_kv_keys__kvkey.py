from __future__ import annotations

import re
import weakref
from typing import Literal
from typing import Sequence
from typing import cast

import pytest
from fdb.tuple import pack
from typing_extensions import Unpack

from denokv.datapath import AnyKvKey
from denokv.datapath import KvKeyEncodable
from denokv.datapath import KvKeyPiece
from denokv.datapath import KvKeyTuple
from denokv.datapath import pack_key
from denokv.kv_keys import KvKey


def test_instances_do_not_define_dict() -> None:
    k = KvKey()
    with pytest.raises(AttributeError):
        print(k.__dict__)


def test_instances_are_KvKeyEncodable() -> None:
    key = cast(object, KvKey())
    assert isinstance(key, KvKeyEncodable)
    assert key.kv_key_bytes() == b""


def test_instances_are_Sequences() -> None:
    key = cast(object, KvKey(1, 2, 3))
    assert isinstance(key, Sequence)
    assert list(key) == [1, 2, 3]


@pytest.mark.parametrize("pieces", [(), ("a",), ("a", 1, 2)])
def test_KvKey(pieces: tuple[KvKeyPiece, ...]) -> None:
    key = KvKey(*pieces)
    # KvKey is not actually a tuple subclass at runtime
    assert not isinstance(key, tuple)
    assert type(key) is KvKey
    assert tuple(key) == pieces


def test_wrap_tuple_keys() -> None:
    class CustomKey(KvKeyEncodable):
        def kv_key_bytes(self) -> bytes:
            return b""

    assert KvKey.wrap_tuple_keys(("a", 1)) == KvKey("a", 1)
    custom = CustomKey()
    assert KvKey.wrap_tuple_keys(custom) is custom


def test_eq_hash() -> None:
    a1, a2 = KvKey("foo", 1), KvKey("foo", 1)
    b1, b2 = KvKey("foo", 2), KvKey("foo", 2)

    assert a1 == a2
    assert hash(a1) == hash(a2)
    assert b1 == b2
    assert hash(b1) == hash(b2)
    assert a1 != b2
    assert hash(a1) != hash(b2)

    # not equal to plain tuples — Python compares 1 and 1.0 the same, which
    # makes plain tuples not compare consistently with packed representation.
    assert a1 != tuple(a1)
    assert hash(a1) != hash(tuple(a1))


def test_kv_key_bytes() -> None:
    assert KvKey("foo", 1).kv_key_bytes() == pack_key(("foo", 1))


def test_instances_implement_SupportsBytes() -> None:
    assert bytes(KvKey("foo", 1)) == pack_key(("foo", 1))


@pytest.mark.parametrize("l", [-1, 0, 1, -1.0, 0.0, 1.0, "a", "b"])
@pytest.mark.parametrize("r", [-1, 0, 1, -1.0, 0.0, 1.0, "a", "b"])
def test_order_comparisons(l: KvKeyPiece, r: KvKeyPiece) -> None:  # noqa: E741
    lk, rk = (l,), (r,)
    plk, prk = pack_key(lk), pack_key(rk)

    kvkeyl, kvkr = KvKey(l), KvKey(r)

    assert (kvkeyl < kvkr) is (plk < prk)
    assert (kvkeyl <= kvkr) is (plk <= prk)
    assert (kvkeyl >= kvkr) is (plk >= prk)
    assert (kvkeyl > kvkr) is (plk > prk)


def test_tuple_methods() -> None:
    key = KvKey("a", "b", "c")
    assert len(key) == 3
    assert "b" in key and "d" not in key
    assert key[0] == "a"
    assert key[1:] == KvKey("b", "c")
    assert list(iter(key)) == ["a", "b", "c"]
    assert list(reversed(key)) == ["c", "b", "a"]
    assert key + KvKey("d", "e") == KvKey("a", "b", "c", "d", "e")
    assert key + ("d", "e") == KvKey("a", "b", "c", "d", "e")
    assert key * 2 == KvKey("a", "b", "c", "a", "b", "c")
    assert 2 * key == KvKey("a", "b", "c", "a", "b", "c")
    assert (key * 3).count("b") == 3
    assert key.index("b") == 1


def test_weakref() -> None:
    key = KvKey("foo")
    r = weakref.ref(key)
    assert r() is key
    del key
    assert r() is None


def test_types() -> None:
    _k1: KvKey[Literal["foo"], Literal[1]] = KvKey("foo", 1)
    _k2: KvKey[str, int] = KvKey("foo", 1)

    # expected errors
    _k3: KvKey[str, int] = KvKey(1, "a")  # type: ignore[arg-type]

    k: KvKey[str, int] = KvKey("a", 1)
    _k_item1: str = k[0]
    # expected error
    _k_item2: str = k[1]  # type: ignore[assignment]

    _k_slice1: KvKey = KvKey("a", 1, "b", 2)
    # expected error: cannot currently type custom tuple slice method
    _k_slice2: KvKey[str, int] = _k_slice1[2:]  # type: ignore[assignment]
    _k_slice3: KvKey = _k_slice1[2:]

    a: str
    b: int
    a, b = k

    # expected error:
    b, a = k  # type: ignore[assignment]

    # AFAIK it's not possible to type custom tuple concatenation. tuple's own
    # add method seems to be special-cased by type checkers.
    # https://github.com/python/typing/discussions/1439
    _t1: tuple[str, int, str, int] = ("a", 1) + ("a", 1)
    _k4: KvKey[tuple[KvKeyPiece, ...]] = KvKey("a", 1) + KvKey("a", 1)
    _k5: KvKey = KvKey("a", 1) + KvKey("a", 1)

    _t2: tuple[str, int, str, int] = ("a", 1) * 2
    _k6: KvKey[tuple[KvKeyPiece, ...]] = KvKey("a", 1) * 2
    _k7: KvKey = KvKey("a", 1) * 2
    _k8: KvKey[tuple[KvKeyPiece, ...]] = 2 * KvKey("a", 1)
    _k9: KvKey = 2 * KvKey("a", 1)


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
