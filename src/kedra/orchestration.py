"""Run ingestion and transformation as an explicit Dagster dependency graph."""

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TextIO

from dagster import Definitions, Failure, Out, job, op

from kedra.config import Settings, load_settings
from kedra.dates import DateRange
from kedra.ingestion import run_ingestion
from kedra.transformation import ManifestError, load_ingestion_manifest, run_transformation

PROFILE_KEYS = frozenset(
    {
        "KEDRA_MONGO_URI",
        "KEDRA_S3_ACCESS_KEY_ID",
        "KEDRA_S3_SECRET_ACCESS_KEY",
    }
)


def load_environment_profile(path: Path) -> dict[str, str]:
    """Read one generated credential profile without changing the process environment."""
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
    except OSError:
        raise ValueError(f"Credential profile is unreadable: {path}") from None
    values: dict[str, str] = {}
    for line_number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            raise ValueError(f"Credential profile line {line_number} must use NAME=VALUE syntax")
        name, value = stripped.split("=", 1)
        if name not in PROFILE_KEYS:
            raise ValueError(f"Credential profile contains unsupported key on line {line_number}")
        if name in values:
            raise ValueError(f"Credential profile repeats {name}")
        if not value.strip():
            raise ValueError(f"Credential profile value for {name} is blank")
        values[name] = value
    missing = sorted(PROFILE_KEYS - values.keys())
    if missing:
        raise ValueError(f"Credential profile is missing required keys: {', '.join(missing)}")
    return values


def load_profile_settings(config_path: Path, profile_path: Path) -> Settings:
    return load_settings(config_path, load_environment_profile(profile_path))


def _selected_body_ids(settings: Settings, requested: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(requested) if requested else settings.source.body_ids
    if len(set(selected)) != len(selected) or any(
        body_id not in settings.source.body_ids for body_id in selected
    ):
        raise ValueError("--body-id must select distinct configured source body IDs")
    return selected


def _stage_completed(path: Path, event_name: str) -> bool:
    try:
        lines = path.read_text(encoding="utf-8-sig").splitlines()
        events = [json.loads(line) for line in lines if line.strip()]
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return bool(
        events
        and isinstance(events[-1], dict)
        and events[-1].get("event") == event_name
        and events[-1].get("complete") is True
    )


def _run_path(directory: Path, run_id: str, stage: str) -> Path:
    return directory / f"{run_id}-{stage}.jsonl"


INGEST_CONFIG: Mapping[str, Any] = {
    "config_path": str,
    "credential_profile": str,
    "start_date": str,
    "end_date": str,
    "body_ids": [str],
    "run_directory": str,
}
TRANSFORM_CONFIG: Mapping[str, Any] = {
    "config_path": str,
    "credential_profile": str,
    "start_date": str,
    "end_date": str,
    "body_ids": [str],
    "run_directory": str,
}


@op(config_schema=INGEST_CONFIG, out=Out(str, description="Completed ingestion manifest path"))
def ingest_landing(context) -> str:
    """Run ingestion and publish a manifest path only after a complete result."""
    config = context.op_config
    config_path = Path(config["config_path"])
    settings = load_profile_settings(config_path, Path(config["credential_profile"]))
    date_range = DateRange.from_inputs(config["start_date"], config["end_date"])
    body_ids = _selected_body_ids(settings, config["body_ids"])
    run_directory = Path(config["run_directory"])
    run_directory.mkdir(parents=True, exist_ok=True)
    manifest_path = _run_path(run_directory, context.run_id, "ingestion")
    with manifest_path.open("x", encoding="utf-8", newline="\n") as stream:
        exit_code = run_ingestion(settings, date_range, body_ids, stream)
    validation_reason = None
    if exit_code == 0:
        try:
            load_ingestion_manifest(
                manifest_path,
                date_range.start,
                date_range.end_exclusive - timedelta(days=1),
                settings.source.name,
                settings.source.body_ids,
                body_ids,
            )
        except ManifestError as error:
            validation_reason = error.reason
    if exit_code != 0 or validation_reason is not None:
        raise Failure(
            description="Ingestion was incomplete; transformation was withheld",
            metadata={
                "manifest_path": str(manifest_path),
                "reason": validation_reason or "ingestion_exit_nonzero",
            },
        )
    context.add_output_metadata({"manifest_path": str(manifest_path)})
    return str(manifest_path)


@op(config_schema=TRANSFORM_CONFIG, out=Out(str, description="Transformation log path"))
def transform_landing(context, ingestion_manifest: str) -> str:
    """Transform the exact versions in the completed upstream manifest."""
    config = context.op_config
    config_path = Path(config["config_path"])
    settings = load_profile_settings(config_path, Path(config["credential_profile"]))
    date_range = DateRange.from_inputs(config["start_date"], config["end_date"])
    body_ids = _selected_body_ids(settings, config["body_ids"])
    run_directory = Path(config["run_directory"])
    run_directory.mkdir(parents=True, exist_ok=True)
    log_path = _run_path(run_directory, context.run_id, "transformation")
    with log_path.open("x", encoding="utf-8", newline="\n") as stream:
        exit_code = run_transformation(
            settings,
            date_range,
            Path(ingestion_manifest),
            stream,
            expected_body_ids=body_ids,
        )
    if exit_code != 0 or not _stage_completed(log_path, "transformation_run_summary"):
        raise Failure(
            description="Transformation did not complete",
            metadata={"transformation_log_path": str(log_path)},
        )
    context.add_output_metadata({"transformation_log_path": str(log_path)})
    return str(log_path)


@job(name="kedra_ingestion_transformation")
def kedra_ingestion_transformation():
    """Ingest first; run transformation only after the manifest output exists."""
    transform_landing(ingest_landing())


definitions = Definitions(jobs=[kedra_ingestion_transformation])


def _same_credentials(first: Settings, second: Settings) -> bool:
    return (
        first.storage.mongo_uri == second.storage.mongo_uri
        and first.storage.s3_access_key_id == second.storage.s3_access_key_id
        and first.storage.s3_secret_access_key == second.storage.s3_secret_access_key
    )


def run_orchestration(
    config_path: Path,
    date_range: DateRange,
    body_ids: Sequence[str] | None,
    ingest_profile: Path,
    transform_profile: Path,
    run_directory: Path,
    stream: TextIO,
) -> int:
    """Execute the local two-task job and emit one credential-free run summary."""
    if ingest_profile.resolve() == transform_profile.resolve():
        raise ValueError("Ingestion and transformation require separate credential profiles")
    ingest_settings = load_profile_settings(config_path, ingest_profile)
    transform_settings = load_profile_settings(config_path, transform_profile)
    if _same_credentials(ingest_settings, transform_settings):
        raise ValueError("Ingestion and transformation profiles must use distinct credentials")
    selected_body_ids = _selected_body_ids(ingest_settings, body_ids or ())
    run_directory = run_directory.resolve()
    run_directory.mkdir(parents=True, exist_ok=True)
    inclusive_end = (date_range.end_exclusive - timedelta(days=1)).isoformat()
    common_config = {
        "config_path": str(config_path.resolve()),
        "start_date": date_range.start.isoformat(),
        "end_date": inclusive_end,
        "run_directory": str(run_directory),
    }
    result = kedra_ingestion_transformation.execute_in_process(
        run_config={
            "ops": {
                "ingest_landing": {
                    "config": {
                        **common_config,
                        "credential_profile": str(ingest_profile.resolve()),
                        "body_ids": list(selected_body_ids),
                    }
                },
                "transform_landing": {
                    "config": {
                        **common_config,
                        "credential_profile": str(transform_profile.resolve()),
                        "body_ids": list(selected_body_ids),
                    }
                },
            }
        },
        raise_on_error=False,
    )
    manifest_path = _run_path(run_directory, result.run_id, "ingestion")
    transform_log_path = _run_path(run_directory, result.run_id, "transformation")
    manifest_failure_reason = None
    try:
        load_ingestion_manifest(
            manifest_path,
            date_range.start,
            date_range.end_exclusive - timedelta(days=1),
            ingest_settings.source.name,
            ingest_settings.source.body_ids,
            selected_body_ids,
        )
    except ManifestError as error:
        manifest_failure_reason = error.reason
    ingestion_complete = (
        result.is_node_success("ingest_landing") and manifest_failure_reason is None
    )
    transformation_complete = result.is_node_success("transform_landing") and _stage_completed(
        transform_log_path, "transformation_run_summary"
    )
    transformation_status = (
        "complete"
        if transformation_complete
        else "skipped"
        if result.is_node_skipped("transform_landing")
        or result.is_node_untouched("transform_landing")
        else "failed"
    )
    reason = None
    if not ingestion_complete:
        reason = manifest_failure_reason or "ingestion_incomplete"
    elif not transformation_complete:
        reason = "transformation_incomplete"
    elif not result.success:
        reason = "orchestrator_execution_failure"
    summary = {
        "run_id": result.run_id,
        "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "event": "orchestration_run_summary",
        "source": ingest_settings.source.name,
        "start_date": date_range.start.isoformat(),
        "end_date": inclusive_end,
        "body_ids": list(selected_body_ids),
        "ingestion_manifest_path": str(manifest_path),
        "transformation_log_path": str(transform_log_path) if transform_log_path.exists() else None,
        "ingestion_status": "complete" if ingestion_complete else "failed",
        "transformation_status": transformation_status,
        "ingestion_complete": ingestion_complete,
        "transformation_complete": transformation_complete,
        "complete": bool(result.success and ingestion_complete and transformation_complete),
        "reason": reason,
    }
    stream.write(json.dumps(summary, ensure_ascii=False, sort_keys=True) + "\n")
    stream.flush()
    return 0 if summary["complete"] else 3
