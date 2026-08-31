from datetime import date, datetime, timedelta
from itertools import pairwise

import pytest

from kedra.dates import DateRange, iter_partitions, parse_date


def test_clipped_months_keep_calendar_labels_and_inclusive_website_dates():
    partitions = list(iter_partitions(DateRange.from_inputs("2024-01-15", "2024-03-02")))
    assert [p.partition_date.isoformat() for p in partitions] == [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
    ]
    assert [p.website_dates for p in partitions] == [
        ("15/01/2024", "31/01/2024"),
        ("01/02/2024", "29/02/2024"),
        ("01/03/2024", "02/03/2024"),
    ]
    assert partitions[-1].end_exclusive == date(2024, 3, 3)


@pytest.mark.parametrize("size", ["month", "day"])
@pytest.mark.parametrize(
    "start,end",
    [
        ("2024-02-29", "2024-02-29"),
        ("2023-12-31", "2024-03-01"),
        ("2023-02-01", "2023-03-02"),
        ("2024-01-01", "2024-01-31"),
        ("2024-01-31", "2024-02-01"),
        ("9999-12-01", "9999-12-30"),
    ],
)
def test_partitions_cover_every_requested_day_exactly_once(start, end, size):
    requested = DateRange.from_inputs(start, end)
    partitions = list(iter_partitions(requested, size))
    assert partitions[0].start == requested.start
    assert partitions[-1].end_exclusive == requested.end_exclusive
    for left, right in pairwise(partitions):
        assert left.end_exclusive == right.start
    days = [
        p.start + timedelta(days=offset)
        for p in partitions
        for offset in range((p.end_exclusive - p.start).days)
    ]
    assert len(days) == len(set(days)) == (requested.end_exclusive - requested.start).days
    assert all(requested.start <= day < requested.end_exclusive for day in days)


def test_overlapping_month_requests_use_the_same_partition_label():
    full = next(iter_partitions(DateRange.from_inputs("2024-01-01", "2024-01-31")))
    clipped = next(iter_partitions(DateRange.from_inputs("2024-01-15", "2024-01-20")))
    assert full.partition_date == clipped.partition_date == date(2024, 1, 1)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "20240101",
        "01-01-2024",
        "2024-1-01",
        "2024-01-1",
        "2024-01-01T00:00:00",
        " 2024-01-01",
        "2023-02-29",
        "2024-13-01",
        "0000-01-01",
    ],
)
def test_reject_invalid_or_non_iso_dates(value):
    with pytest.raises(ValueError):
        parse_date(value)


def test_reversed_range_and_unrepresentable_end_are_rejected():
    with pytest.raises(ValueError, match="on or before"):
        DateRange.from_inputs("2024-02-01", "2024-01-31")
    with pytest.raises(ValueError, match="exclusive bound"):
        DateRange.from_inputs("9999-12-31", "9999-12-31")


def test_reject_timestamps_and_unknown_partition_size():
    with pytest.raises(ValueError, match="calendar dates"):
        DateRange(datetime(2024, 1, 1), datetime(2024, 2, 1))
    with pytest.raises(ValueError, match="month or day"):
        list(iter_partitions(DateRange.from_inputs("2024-01-01", "2024-01-02"), "week"))
