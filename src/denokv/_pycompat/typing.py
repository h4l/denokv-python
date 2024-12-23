from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing_extensions import override as override
else:

    def override(method, /):
        return method
