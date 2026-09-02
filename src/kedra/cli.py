"""Command-line configuration checks, discovery, and immutable ingestion."""

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from kedra.config import load_settings
from kedra.dates import DateRange, iter_partitions
from kedra.discovery import run_discovery
from kedra.ingestion import run_ingestion
from kedra.orchestration import run_orchestration
from kedra.transformation import run_transformation


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
    transform = commands.add_parser(
        "transform",
        help="read Landing assets and append deterministic outputs to separate storage",
    )
    transform.add_argument("--config", required=True, type=Path)
    transform.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    transform.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
    transform.add_argument(
        "--ingestion-manifest",
        required=True,
        type=Path,
        help="JSONL output from a complete ingestion run over the same scope",
    )
    orchestrate = commands.add_parser(
        "orchestrate",
        help="run ingestion then transformation through their Dagster dependency",
    )
    orchestrate.add_argument("--config", required=True, type=Path)
    orchestrate.add_argument("--start-date", required=True, help="inclusive YYYY-MM-DD")
    orchestrate.add_argument("--end-date", required=True, help="inclusive YYYY-MM-DD")
    orchestrate.add_argument(
        "--body-id",
        action="append",
        dest="body_ids",
        help="configured body ID to include; repeat as needed (default: all configured bodies)",
    )
    orchestrate.add_argument(
        "--ingest-env",
        required=True,
        type=Path,
        help="restricted ingestion NAME=VALUE credential profile",
    )
    orchestrate.add_argument(
        "--transform-env",
        required=True,
        type=Path,
        help="restricted transformation NAME=VALUE credential profile",
    )
    orchestrate.add_argument(
        "--run-directory",
        required=True,
        type=Path,
        help="ignored directory for per-run JSONL manifests and logs",
    )
    args = parser.parse_args(argv)
    try:
        date_range = DateRange.from_inputs(args.start_date, args.end_date)
        if args.command == "orchestrate":
            return run_orchestration(
                args.config,
                date_range,
                args.body_ids,
                args.ingest_env,
                args.transform_env,
                args.run_directory,
                sys.stdout,
            )
        settings = load_settings(args.config)
        if args.command in ("discover", "ingest"):
            if args.body_ids and (
                len(set(args.body_ids)) != len(args.body_ids)
                or any(body not in settings.source.body_ids for body in args.body_ids)
            ):
                raise ValueError("--body-id must select distinct configured source body IDs")
            runner = run_discovery if args.command == "discover" else run_ingestion
            return runner(settings, date_range, args.body_ids, sys.stdout)
        if args.command == "transform":
            return run_transformation(settings, date_range, args.ingestion_manifest, sys.stdout)
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
        if args.command == "orchestrate":
            print(
                json.dumps(
                    {
                        "run_id": uuid4().hex,
                        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                        "event": "orchestration_run_summary",
                        "ingestion_status": "not_started",
                        "transformation_status": "not_started",
                        "ingestion_complete": False,
                        "transformation_complete": False,
                        "complete": False,
                        "reason": "configuration_error",
                        "error": str(error),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 2
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
