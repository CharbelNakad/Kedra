"""Inclusive user dates, half-open intervals, and stable calendar partitions."""

import calendar
import re
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

PartitionSize = Literal["month", "day"]
ONE_DAY = timedelta(days=1)


def parse_date(value: str) -> date:
    """Accept only YYYY-MM-DD, not the other forms accepted by fromisoformat."""
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        raise ValueError("Dates must use YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise ValueError("Date is not a valid calendar date") from None


@dataclass(frozen=True)
class DateRange:
    start: date
    end_exclusive: date

    def __post_init__(self) -> None:
        if type(self.start) is not date or type(self.end_exclusive) is not date:
            raise ValueError("Date range requires calendar dates, not timestamps")
        if self.start >= self.end_exclusive:
            raise ValueError("start_date must be on or before end_date")

    @classmethod
    def from_inputs(cls, start_date: str, end_date: str) -> "DateRange":
        start, end = parse_date(start_date), parse_date(end_date)
        if end == date.max:
            raise ValueError("end_date must be before 9999-12-31 to form an exclusive bound")
        return cls(start, end + ONE_DAY)


@dataclass(frozen=True)
class Partition:
    partition_date: date
    start: date
    end_exclusive: date

    @property
    def website_dates(self) -> tuple[str, str]:
        """Return the site's inclusive DD/MM/YYYY filter values."""
        end = self.end_exclusive - ONE_DAY
        return (
            f"{self.start.day:02d}/{self.start.month:02d}/{self.start.year:04d}",
            f"{end.day:02d}/{end.month:02d}/{end.year:04d}",
        )


def iter_partitions(date_range: DateRange, size: PartitionSize = "month") -> Iterator[Partition]:
    """Yield non-overlapping calendar partitions clipped to the requested range."""
    if size not in ("month", "day"):
        raise ValueError("partition_size must be month or day")
    cursor = date_range.start
    while cursor < date_range.end_exclusive:
        if size == "month":
            label = cursor.replace(day=1)
            month_end = cursor.replace(day=calendar.monthrange(cursor.year, cursor.month)[1])
            # Clip before adding a day, so December of year 9999 does not overflow.
            end = min(month_end, date_range.end_exclusive - ONE_DAY) + ONE_DAY
        else:
            label, end = cursor, cursor + ONE_DAY
        yield Partition(label, cursor, end)
        cursor = end
