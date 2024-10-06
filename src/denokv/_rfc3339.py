from __future__ import annotations

import re
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Final
from typing import TypedDict
from typing import cast

from denokv.result import Err
from denokv.result import Ok
from denokv.result import Result

RFC3339_DATETIME_PATTERN = r"""
(?P<fullyear>\d{4})-(?P<month>\d{2})-(?P<mday>\d{2})
[ tT]
(?P<hour>\d{2}) : (?P<minute>\d{2}) : (?P<second>\d{2})
(?: \. (?P<secfrac>\d+) )?
(?:
    (?P<utcoffset>[zZ])
    | (?:
        (?P<offset_sign>[+-])
        (?P<offset_hour>\d{2}) : (?P<offset_minute>\d{2})
    )
)?
"""

RFC3339_DATETIME_REGEX: Final = re.compile(
    f"^(?:{RFC3339_DATETIME_PATTERN})$", re.VERBOSE
)


class RFC3339DateTimeGroups(TypedDict):
    fullyear: str
    month: str
    mday: str
    hour: str
    minute: str
    second: str
    secfrac: str | None
    utcoffset: str | None
    offset_sign: str | None


class RFC3339DateTimeOffsetGroups(TypedDict):
    offset_sign: str
    offset_hour: str
    offset_minute: str


def parse_rfc3339_datetime(date_string: str) -> Result[datetime, ValueError]:
    """
    Parse a date string in [RFC3339 5.6. Internet Date/Time Format].

    [RFC3339 5.6. Internet Date/Time Format]: \
https://datatracker.ietf.org/doc/html/rfc3339#section-5.6

    Notes
    -----
    The ABNF grammar for datetime strings is:

    ```ABNF
    date-fullyear   = 4DIGIT
    date-month      = 2DIGIT  ; 01-12
    date-mday       = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on
                                ; month/year
    time-hour       = 2DIGIT  ; 00-23
    time-minute     = 2DIGIT  ; 00-59
    time-second     = 2DIGIT  ; 00-58, 00-59, 00-60 based on leap second
                                ; rules
    time-secfrac    = "." 1*DIGIT
    time-numoffset  = ("+" / "-") time-hour ":" time-minute
    time-offset     = "Z" / time-numoffset

    partial-time    = time-hour ":" time-minute ":" time-second
                        [time-secfrac]
    full-date       = date-fullyear "-" date-month "-" date-mday
    full-time       = partial-time time-offset

    date-time       = full-date "T" full-time
    ```
    """
    match = RFC3339_DATETIME_REGEX.match(date_string)
    if not match:
        return Err(
            ValueError(f"Date string is not an RFC3339 Date/Time: {date_string!r}")
        )
    groups = cast(RFC3339DateTimeGroups, match.groupdict())
    tz: timezone | None = None
    if groups["utcoffset"]:
        tz = timezone.utc
    elif offset_sign := groups["offset_sign"]:
        offset_groups = cast(RFC3339DateTimeOffsetGroups, groups)
        offset_hour = int(offset_groups["offset_hour"])
        offset_minute = int(offset_groups["offset_minute"])
        offset = timedelta(hours=offset_hour, minutes=offset_minute)
        if offset > timedelta(hours=24):
            return Err(
                ValueError(f"UTC offset is greater than 24 hours: {date_string!r}")
            )
        if offset_sign == "-":
            offset = -offset
        tz = timezone(offset)

    # RFC3339 allows seconds to be 60 when leap seconds occur. UNIX time does
    # not account for leap seconds (datetime type does not allow seconds to be
    # 60 either), so a leap second is equivalent to the first second of the
    # next minute. (See test__rfc333.test_time_leap_second_behaviour.)
    second = int(groups["second"])
    leap_second_offset = None
    if second == 60:
        second = 59
        leap_second_offset = timedelta(seconds=1)

    microsecond = 0
    if secfrac := groups["secfrac"]:
        secfrac = secfrac[:6]  # most-significant 6 digits
        secfrac = f"{secfrac:<06}"  # pad to microseconds, so. 45 = 450000 = 0.45
        microsecond = int(secfrac)
    try:
        dt = datetime(
            year=int(groups["fullyear"]),
            month=int(groups["month"]),
            day=int(groups["mday"]),
            hour=int(groups["hour"]),
            minute=int(groups["minute"]),
            second=second,
            microsecond=microsecond,
            tzinfo=tz,
        )
        if leap_second_offset:
            dt += leap_second_offset
        return Ok(dt)
    except Exception as e:
        err = ValueError(f"Date string contains an invalid value: {date_string!r}")
        err.__cause__ = e
        return Err(err)
