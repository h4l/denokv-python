"""
Runtime comparability for typing things that come from typing_extensions.

We import all type annotations/types/functions from here, rather than typing or
typing_extensions in order to handle the differences between them in one place,
without needing if TYPE_CHECKING everywhere.
"""

# ruff: noqa: TID251

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field

# Everything that exist in typing >=py39 except:
# - ByteString (deprecated)
# - overload (ruff does not recognise it when re-exported)
# - Literal (ruff does not recognise it when re-exported)
# - Handled below due to runtime differences:
#   - TypeVar (does not support default argument pre py313)
#   - TypedDict (does not support generics pre py311)
from typing import IO as IO
from typing import TYPE_CHECKING as TYPE_CHECKING
from typing import AbstractSet as AbstractSet
from typing import Annotated as Annotated
from typing import Any as Any
from typing import AnyStr as AnyStr
from typing import AsyncContextManager as AsyncContextManager
from typing import AsyncGenerator as AsyncGenerator
from typing import AsyncIterable as AsyncIterable
from typing import AsyncIterator as AsyncIterator
from typing import Awaitable as Awaitable
from typing import BinaryIO as BinaryIO
from typing import Callable as Callable
from typing import ChainMap as ChainMap
from typing import ClassVar as ClassVar
from typing import Collection as Collection
from typing import Container as Container
from typing import ContextManager as ContextManager
from typing import Coroutine as Coroutine
from typing import Counter as Counter
from typing import DefaultDict as DefaultDict
from typing import Deque as Deque
from typing import Dict as Dict
from typing import Final as Final
from typing import ForwardRef as ForwardRef
from typing import FrozenSet as FrozenSet
from typing import Generator as Generator
from typing import Generic as Generic
from typing import Hashable as Hashable
from typing import ItemsView as ItemsView
from typing import Iterable as Iterable
from typing import Iterator as Iterator
from typing import KeysView as KeysView
from typing import List as List
from typing import Mapping as Mapping
from typing import MappingView as MappingView
from typing import Match as Match
from typing import MutableMapping as MutableMapping
from typing import MutableSequence as MutableSequence
from typing import MutableSet as MutableSet
from typing import NamedTuple as NamedTuple
from typing import NewType as NewType
from typing import NoReturn as NoReturn
from typing import Optional as Optional
from typing import OrderedDict as OrderedDict
from typing import Pattern as Pattern
from typing import Protocol as Protocol
from typing import Reversible as Reversible
from typing import Sequence as Sequence
from typing import Set as Set
from typing import Sized as Sized
from typing import SupportsAbs as SupportsAbs
from typing import SupportsBytes as SupportsBytes
from typing import SupportsComplex as SupportsComplex
from typing import SupportsFloat as SupportsFloat
from typing import SupportsIndex as SupportsIndex
from typing import SupportsInt as SupportsInt
from typing import SupportsRound as SupportsRound
from typing import Text as Text
from typing import TextIO as TextIO
from typing import Tuple as Tuple
from typing import Type as Type
from typing import Union as Union
from typing import ValuesView as ValuesView
from typing import cast as cast
from typing import final as final
from typing import get_args as get_args
from typing import get_origin as get_origin
from typing import get_type_hints as get_type_hints
from typing import no_type_check as no_type_check
from typing import no_type_check_decorator as no_type_check_decorator
from typing import runtime_checkable as runtime_checkable

if TYPE_CHECKING:
    from typing_extensions import Never as Never
    from typing_extensions import Self as Self
    from typing_extensions import TypeAlias as TypeAlias
    from typing_extensions import TypeGuard as TypeGuard
    from typing_extensions import TypeIs as TypeIs
else:
    Never = "Never"
    ParamSpec = "ParamSpec"
    Self = "Self"
    TypeAlias = "TypeAlias"
    TypeGuard = "TypeGuard"
    TypeIs = "TypeIs"

if TYPE_CHECKING:
    from typing_extensions import TypeVar as TypeVar
else:
    from typing import TypeVar as _TypeVar

    def TypeVar(
        name: str,
        *constraints: Any,
        bound: Any | None = None,
        covariant: bool = False,
        contravariant: bool = False,
        default: Any = ...,
        infer_variance: bool = False,
    ) -> TypeVar:
        return _TypeVar(
            name, *constraints, covariant=covariant, contravariant=contravariant
        )


_T = TypeVar("_T")

if TYPE_CHECKING:
    from typing_extensions import TypeVarTuple as TypeVarTuple
else:

    @dataclass
    class TypeVarTuple:
        name: str
        default: Any = field(default=None)


if TYPE_CHECKING:
    from typing_extensions import ParamSpec as ParamSpec
else:
    from denokv._pycompat.dataclasses import kw_only_if310

    @dataclass
    class ParamSpec:
        name: str
        bound: type[Any] | str | None = field(default=None, **kw_only_if310())
        contravariant: bool = field(default=False, **kw_only_if310())
        covariant: bool = field(default=False, **kw_only_if310())
        default: Any = field(default=None, **kw_only_if310())


if TYPE_CHECKING:
    from typing_extensions import Unpack as Unpack
else:

    @dataclass
    class Unpack(Generic[_T]):
        pass


if TYPE_CHECKING:
    from typing_extensions import override as override
else:

    def override(method, /):
        return method


if TYPE_CHECKING:
    from typing_extensions import TypedDict as TypedDict
else:

    class TypedDict(dict):
        def __new__(cls, *args, **kwargs):
            return dict(*args, **kwargs)

        @classmethod
        def __init_subclass__(cls, total: bool = True) -> None: ...


def assert_never(value: Never, /) -> Never:
    """Assert to the type checker that a line of code is unreachable."""
    raise AssertionError(f"Expected code to be unreachable, but got: {value!r}")


if TYPE_CHECKING:
    from typing_extensions import assert_type as assert_type
else:

    def assert_type(val: _T, typ: Any, /) -> _T:
        return val
