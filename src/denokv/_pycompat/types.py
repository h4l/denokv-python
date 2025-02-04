from enum import Enum
from typing import Literal  # noqa: TID251

from denokv._pycompat.typing import TypeAlias


class NotSetEnum(Enum):
    NotSet = "NotSet"
    """
    Sentinel value to use as an argument default.

    It's purpose is to differentiate the argument not being set from an explicit
    None value (or similar).
    """


NotSetType: TypeAlias = Literal[NotSetEnum.NotSet]
NotSet = NotSetEnum.NotSet
