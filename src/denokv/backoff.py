import random
import time
from dataclasses import dataclass
from dataclasses import field
from itertools import count
from typing import Callable
from typing import Iterable
from typing import Iterator
from typing import TypeAlias

Backoff: TypeAlias = Iterable[float]
"""
A backoff strategy for retrying failed operations.

The strategy is simply an iterable of delays (in seconds) to wait for before
retrying after each failure. The strategy may stop iteration after a timeout, or
keep producing delays indefinitely.
"""


@dataclass(kw_only=True)
class ExponentialBackoff(Iterable[float]):
    """
    A `Backoff` strategy that generates randomised, exponentially-increasing delays.

    The delays are $$initial_interval_seconds * multiplier ** attempt$$, with
    the actual delay being randomised from 0 to the calculated delay.

    The `max_interval_seconds` limit the max delay per iteration, and
    `max_elapsed_seconds` limit $$now + delay <= start + max_elapsed_seconds$$.
    Iteration stops once the time reaches $$start + max_elapsed_seconds$$.

    Notes
    -----
    1. The jitter ranging from 0 to the interval baseline delay is based on
       [Exponential Backoff And Jitter].
    2. The default intervals, multipliers and interval/elapsed maximums are those
       used by [google-http-java-client] (but jitter here is different per note
       1.).

    [Exponential Backoff And Jitter]: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    [google-http-java-client]: https://github.com/googleapis/google-http-java-client/blob/c39e63c/google-http-client/src/main/java/com/google/api/client/util/ExponentialBackOff.java

    Examples
    --------
    >>> random.seed(43)
    >>> backoff = ExponentialBackoff()
    >>> for i, delay in zip(range(1, 11), backoff):
    ...     print(f'delay {i}: {delay:.2f} seconds')
    delay 1: 0.02 seconds
    delay 2: 0.52 seconds
    delay 3: 0.16 seconds
    delay 4: 0.78 seconds
    delay 5: 1.70 seconds
    delay 6: 3.01 seconds
    delay 7: 2.58 seconds
    delay 8: 4.26 seconds
    delay 9: 0.25 seconds
    delay 10: 8.31 seconds

    Random jitter is not intended to be configurable, but observing behaviour
    without jitter is illustrative:

    >>> i = 1
    >>> now = 0
    >>> backoff = ExponentialBackoff(random=lambda: 1, time=lambda: now,
    ...                              max_elapsed_seconds=20)
    >>> for delay in backoff:
    ...     print(f'delay {i}: {delay:.2f} seconds ({now:.2f} seconds elapsed)')
    ...
    ...     now += delay
    ...     i += 1
    ... else:
    ...     print(f'end — no more delays available ({now:.2f} seconds elapsed)')
    delay 1: 0.50 seconds (0.00 seconds elapsed)
    delay 2: 0.75 seconds (0.50 seconds elapsed)
    delay 3: 1.12 seconds (1.25 seconds elapsed)
    delay 4: 1.69 seconds (2.38 seconds elapsed)
    delay 5: 2.53 seconds (4.06 seconds elapsed)
    delay 6: 3.80 seconds (6.59 seconds elapsed)
    delay 7: 5.70 seconds (10.39 seconds elapsed)
    delay 8: 3.91 seconds (16.09 seconds elapsed)
    end — no more delays available (20.00 seconds elapsed)
    """

    multiplier: float = field(default=1.5)
    initial_interval_seconds: float = field(default=0.5)
    max_interval_seconds: float = field(default=15 * 60)
    max_elapsed_seconds: float = field(default=15 * 60 * 60)
    random: Callable[[], float] = field(default=random.random)
    time: Callable[[], float] = field(default=time.time)

    def __post_init__(self) -> None:
        if self.multiplier < 1:
            raise ValueError("multiplier must be >= 1")
        if self.initial_interval_seconds <= 0:
            raise ValueError("initial_interval_seconds must be > 0")
        if self.max_interval_seconds <= 0:
            raise ValueError("max_interval_seconds must be > 0")
        if self.max_elapsed_seconds < 0:
            raise ValueError("max_elapsed_seconds must be >= 0")

    def _next_jitter(self) -> float:
        sample = self.random()
        assert 0 <= sample <= 1, "random sample out of range"
        return sample

    def __iter__(self) -> Iterator[float]:
        start = now = self.time()
        limit = start + self.max_elapsed_seconds
        base_interval = 0.0

        for i in count():
            if now >= limit:
                break

            # Stop exponentiating once we hit the max, otherwise we'll overflow
            # if we have lots of iterations with a small max_interval_seconds.
            if base_interval < self.max_interval_seconds:
                base_interval = self.initial_interval_seconds * self.multiplier**i
                base_interval = min(self.max_interval_seconds, base_interval)
            randomised_interval = base_interval * self._next_jitter()  # 0..1
            interval = min(limit - now, randomised_interval)
            yield interval

            now = self.time()
