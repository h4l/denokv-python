import math

import pytest

from test.denokv_testing import nextafter


def test_nextafter() -> None:
    mna = math.nextafter

    with pytest.raises(ValueError, match=r"steps must be a non-negative integer"):
        nextafter(1, 2, steps=-1)

    assert nextafter(1.0, 2.0, steps=0) == 1.0
    assert nextafter(1.0, 2.0, steps=1) == mna(1.0, 2.0)
    assert nextafter(1.0, 2.0, steps=2) == mna(mna(1.0, 2.0), 2.0)
