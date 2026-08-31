"""Validate configuration and preview date partitions without network access."""

import argparse
import json
import sys
from pathlib import Path

from kedra.config import load_settings
from kedra.dates import DateRange, iter_partitions


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kedra")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser(
        "check-config", help="validate settings and dates without network I/O"
    )
    check.add_argument("--config", required=True, type=Path)
    check.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    check.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
    args = parser.parse_args(argv)
    try:
        date_range = DateRange.from_inputs(args.start_date, args.end_date)
        settings = load_settings(args.config)
        count = 0
        preview = []
        last_label = None
        for partition in iter_partitions(date_range, settings.scraping.partition_size):
            count += 1
            last_label = partition.partition_date.isoformat()
            if len(preview) < 3:
                start, end = partition.website_dates
                preview.append({"partition_date": last_label, "from": start, "to": end})
    except (OSError, ValueError) as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": "valid",
                "network_access": False,
                "source": settings.source.name,
                "body_ids": settings.source.body_ids,
                "partition_size": settings.scraping.partition_size,
                "partition_count": count,
                "body_partition_count": count * len(settings.source.body_ids),
                "first_partitions": preview,
                "last_partition_date": last_label,
            }
        )
    )
    return 0
