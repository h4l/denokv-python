from __future__ import annotations

import pytest
from google.protobuf.message import Message
from pytest import Config
from v8serialize import Encoder

from denokv._pycompat.typing import Sequence
from test import advance_time
from test.denokv_testing import diff_protobuf_messages

advance_time_time = advance_time.advance_time_time

_pytest_assertion_verbosity: int = 0


def pytest_configure(config: Config) -> None:
    global _pytest_assertion_verbosity
    _pytest_assertion_verbosity = config.get_verbosity(Config.VERBOSITY_ASSERTIONS)


# Provide descriptive diffs for failed protobuf message equality assertions
def pytest_assertrepr_compare(
    op: str, left: object, right: object
) -> Sequence[str] | None:
    if isinstance(left, Message) and isinstance(right, Message):
        repr_left = f"<{left.DESCRIPTOR.full_name} protobuf message at {hex(id(left))}>"
        repr_right = (
            f"<{right.DESCRIPTOR.full_name} protobuf message at {hex(id(right))}>"
        )
        comparison = [f"{repr_left} {op} {repr_right}"]

        if left == right:
            comparison.append("Protobuf messages are equal")
            return comparison
        if type(left) is not type(right):
            comparison.append("Protobuf messages are different types")
            return comparison

        end = (
            " (use -v for diff)"
            if _pytest_assertion_verbosity == 0
            else " (repeat -v for more context):"
        )
        comparison.append(f"Protobuf messages are not equal{end}")
        if _pytest_assertion_verbosity == 0:
            return comparison

        # Scale context lines with verbosity level: 1=3, 2=9, 3=27, 4=81, 5=243
        context = 3**_pytest_assertion_verbosity
        comparison.extend(
            diff_protobuf_messages(left, right, context_line_count=context, lineterm="")
        )
        return comparison
    return None


@pytest.fixture(scope="session")
def v8_encoder() -> Encoder:
    return Encoder()
