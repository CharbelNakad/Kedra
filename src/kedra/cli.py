"""Command-line configuration checks, discovery, and immutable ingestion."""

import argparse
import json
import sys
from pathlib import Path

from kedra.config import load_settings
from kedra.dates import DateRange, iter_partitions
from kedra.discovery import run_discovery
from kedra.ingestion import run_ingestion


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="kedra")
    commands = parser.add_subparsers(dest="command", required=True)
    check = commands.add_parser(
        "check-config", help="validate settings and dates without network I/O"
    )
    check.add_argument("--config", required=True, type=Path)
    check.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    check.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
    discover = commands.add_parser(
        "discover", help="enumerate filtered result cards without downloading documents"
    )
    discover.add_argument("--config", required=True, type=Path)
    discover.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    discover.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
    discover.add_argument(
        "--body-id",
        action="append",
        dest="body_ids",
        help="configured body ID to include; repeat as needed (default: all configured bodies)",
    )
    ingest = commands.add_parser(
        "ingest", help="discover and store exact decision assets in the immutable Landing Zone"
    )
    ingest.add_argument("--config", required=True, type=Path)
    ingest.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    ingest.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
    ingest.add_argument(
        "--body-id",
        action="append",
        dest="body_ids",
        help="configured body ID to include; repeat as needed (default: all configured bodies)",
    )
    args = parser.parse_args(argv)
    try:
        date_range = DateRange.from_inputs(args.start_date, args.end_date)
        settings = load_settings(args.config)
        if args.command in ("discover", "ingest"):
            if args.body_ids and (
                len(set(args.body_ids)) != len(args.body_ids)
                or any(body not in settings.source.body_ids for body in args.body_ids)
            ):
                raise ValueError("--body-id must select distinct configured source body IDs")
            runner = run_discovery if args.command == "discover" else run_ingestion
            return runner(settings, date_range, args.body_ids, sys.stdout)
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
