from random import Random

import pytest
from hypothesis import HealthCheck
from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from denokv.backoff import ExponentialBackoff


@given(
    initial_interval=st.floats(min_value=1 / 1000, allow_infinity=False),
    multiplier=st.floats(min_value=1.1, max_value=4096),
    max_interval_seconds=st.floats(min_value=0.1, max_value=60 * 60),
    max_elapsed_seconds=st.floats(min_value=0, max_value=60 * 60 * 24),
    random=st.randoms(),
    fake_start_time=st.floats(min_value=0, allow_infinity=False),
)
@settings(suppress_health_check=[HealthCheck.large_base_example])
def test_simulated_use(
    initial_interval: float,
    multiplier: float,
    max_interval_seconds: float,
    max_elapsed_seconds: float,
    random: Random,
    fake_start_time: float,
) -> None:
    clock_error = 10 / 1000
    now = fake_start_time

    def mock_time() -> float:
        return now

    backoff = ExponentialBackoff(
        multiplier=multiplier,
        initial_interval_seconds=initial_interval,
        max_interval_seconds=max_interval_seconds,
        max_elapsed_seconds=max_elapsed_seconds,
        random=random.random,
        time=mock_time,
    )

    iteration_delay_limit = 0.0
    for i, delay in enumerate(backoff):
        if iteration_delay_limit < max_interval_seconds:
            iteration_delay_limit = min(
                max_interval_seconds, initial_interval * multiplier**i
            )
        assert 0 <= delay <= iteration_delay_limit
        now += max(0, delay + (clock_error * random.random() * 2) - clock_error)

        assert i < 1e6

    assert now >= fake_start_time + max_elapsed_seconds


def test_init_validation() -> None:
    with pytest.raises(ValueError, match=r"multiplier must be >= 1"):
        ExponentialBackoff(multiplier=0.9)

    with pytest.raises(ValueError, match=r"initial_interval_seconds must be > 0"):
        ExponentialBackoff(initial_interval_seconds=0)

    with pytest.raises(ValueError, match=r"max_interval_seconds must be > 0"):
        ExponentialBackoff(max_interval_seconds=0)

    with pytest.raises(ValueError, match=r"max_elapsed_seconds must be >= 0"):
        ExponentialBackoff(max_elapsed_seconds=-1)


@pytest.mark.parametrize("value", [-0.1, 1.1])
def test_random_argument_must_return_values_in_0_1_range(value: float) -> None:
    with pytest.raises(AssertionError, match=r"random sample out of range"):
        list(ExponentialBackoff(random=lambda: value))


def test_max_elapsed_seconds_can_be_0() -> None:
    assert list(ExponentialBackoff(max_elapsed_seconds=0)) == []
