from __future__ import annotations

import calendar
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from denokv._rfc3339 import parse_rfc3339_datetime
from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result

UTC = timezone.utc


@pytest.mark.parametrize(
    "date_string, expected",
    [
        ("", Err(ValueError("Date string is not an RFC3339 Date/Time: ''"))),
        (
            # Values cannot be out of range
            "2024-01-32T00:00:00",
            Err(
                ValueError(
                    "Date string contains an invalid value: '2024-01-32T00:00:00'"
                )
            ),
        ),
        (
            # Hour cannot be 24
            "2000-01-01 24:00:00",
            Err(
                ValueError(
                    "Date string contains an invalid value: '2000-01-01 24:00:00'"
                )
            ),
        ),
        (
            # Fractional seconds are truncated to microsecond precision — 6
            # decimal digits. Without rounding.
            "2000-01-01 00:00:00.123456789Z",
            Ok(datetime(2000, 1, 1, 0, 0, 0, 123456, UTC)),
        ),
        *[
            (f"2024-05-06 07:08:09{z}", Ok(datetime(2024, 5, 6, 7, 8, 9, 0, UTC)))
            for z in "zZ"
        ],
        *[
            (f"2024-05-06{sep}07:08:09Z", Ok(datetime(2024, 5, 6, 7, 8, 9, 0, UTC)))
            for sep in " tT"
        ],
        (
            # local time (no timezone)
            "2024-05-06T07:08:09",
            Ok(datetime(2024, 5, 6, 7, 8, 9, 0)),
        ),
        # Examples from section 5.8 of RFC3339
        (
            "1985-04-12T23:20:50.52Z",
            Ok(datetime(1985, 4, 12, 23, 20, 50, 520_000, UTC)),
        ),
        (
            "1996-12-19T16:39:57-08:00",
            Ok(datetime(1996, 12, 19, 16, 39, 57, 0, timezone(-timedelta(hours=8)))),
        ),
        (
            # leap second
            "1990-12-31T23:59:60Z",
            Ok(datetime(1991, 1, 1, 00, 00, 00, 0, UTC)),
        ),
        (
            # leap second
            "1990-12-31T15:59:60-08:00",
            Ok(datetime(1990, 12, 31, 16, 00, 00, 0, timezone(-timedelta(hours=8)))),
        ),
        (
            "1937-01-01T12:00:27.87+00:20",
            Ok(
                datetime(
                    1937, 1, 1, 12, 00, 27, 870_000, timezone(timedelta(minutes=20))
                )
            ),
        ),
    ],
)
def test_parse_rfc3339_datetime(
    date_string: str, expected: Result[datetime, ValueError]
) -> None:
    actual = parse_rfc3339_datetime(date_string)

    if isinstance(expected, Ok):
        assert actual == expected
    else:
        assert isinstance(actual, Err)
        assert type(actual) is type(expected)
        assert str(actual) == str(expected)


def test_time_leap_second_behaviour() -> None:
    """
    Demonstrate the behaviour of time module's UNIX time handling of leap seconds.

    Positive leap seconds are not accounted for in UNIX time, they are
    equivalent to the first second of the next minute.
    """

    def get_unix_time(ts: str) -> int:
        return calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%S%z"))

    after_leapsecond = get_unix_time("1991-01-01T00:00:00Z")
    on_leapsecond = get_unix_time("1990-12-31T23:59:60Z")
    before_leapsecond = get_unix_time("1990-12-31T23:59:59Z")

    assert before_leapsecond == on_leapsecond - 1
    assert on_leapsecond == after_leapsecond
