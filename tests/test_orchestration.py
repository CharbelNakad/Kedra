import io
import json
from datetime import timedelta
from pathlib import Path

import pytest

from kedra.dates import DateRange
from kedra.orchestration import load_environment_profile, run_orchestration
from kedra.transformation import load_ingestion_manifest

pytestmark = pytest.mark.local_runtime

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config.example.toml"


def write_profile(path: Path, role: str) -> None:
    path.write_text(
        "\n".join(
            (
                f"KEDRA_MONGO_URI=mongodb://{role}:password@localhost:27017",
                f"KEDRA_S3_ACCESS_KEY_ID={role}-access",
                f"KEDRA_S3_SECRET_ACCESS_KEY={role}-secret",
                "",
            )
        ),
        encoding="utf-8",
    )


def complete_ingestion(stream, settings, date_range, body_ids) -> None:
    stream.write(
        json.dumps(
            {
                "run_id": "controlled-ingestion-run",
                "event": "ingestion_run_summary",
                "source": settings.source.name,
                "start_date": date_range.start.isoformat(),
                "end_date": str(date_range.end_exclusive - timedelta(days=1)),
                "body_ids": list(body_ids),
                "discovery_complete": True,
                "document_stage": "complete",
                "failed_documents": 0,
                "incomplete_asset_operations": 0,
                "partitions_with_unknown_missing_count": 0,
                "stored_files": 0,
                "successfully_available_records": 0,
                "complete": True,
            }
        )
        + "\n"
    )


def profiles(tmp_path: Path) -> tuple[Path, Path]:
    ingest = tmp_path / "ingest.env"
    transform = tmp_path / "transform.env"
    write_profile(ingest, "ingest")
    write_profile(transform, "transform")
    return ingest, transform


def test_dagster_runs_transformation_only_after_completed_ingestion(tmp_path, monkeypatch):
    ingest_profile, transform_profile = profiles(tmp_path)
    calls = []

    def fake_ingestion(settings, date_range, body_ids, stream):
        calls.append("ingestion")
        complete_ingestion(stream, settings, date_range, body_ids)
        return 0

    def fake_transformation(settings, date_range, manifest_path, stream, *, expected_body_ids=None):
        calls.append("transformation")
        assert expected_body_ids == ("15376",)
        manifest = load_ingestion_manifest(
            manifest_path,
            date_range.start,
            date_range.end_exclusive - timedelta(days=1),
            settings.source.name,
            settings.source.body_ids,
            expected_body_ids,
        )
        assert manifest.run_id == "controlled-ingestion-run"
        stream.write(
            json.dumps(
                {
                    "run_id": "controlled-transformation-run",
                    "event": "transformation_run_summary",
                    "complete": True,
                }
            )
            + "\n"
        )
        return 0

    monkeypatch.setattr("kedra.orchestration.run_ingestion", fake_ingestion)
    monkeypatch.setattr("kedra.orchestration.run_transformation", fake_transformation)
    output = io.StringIO()

    result = run_orchestration(
        EXAMPLE,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        ["15376"],
        ingest_profile,
        transform_profile,
        tmp_path / "runs",
        output,
    )

    assert result == 0
    assert calls == ["ingestion", "transformation"]
    summary = json.loads(output.getvalue())
    assert summary["ingestion_complete"] is True
    assert summary["transformation_complete"] is True
    assert summary["ingestion_status"] == "complete"
    assert summary["transformation_status"] == "complete"
    assert summary["complete"] is True
    assert Path(summary["ingestion_manifest_path"]).is_file()
    assert Path(summary["transformation_log_path"]).is_file()


def test_incomplete_ingestion_withholds_transformation(tmp_path, monkeypatch):
    ingest_profile, transform_profile = profiles(tmp_path)

    def fake_ingestion(settings, date_range, body_ids, stream):
        stream.write(
            json.dumps(
                {
                    "run_id": "incomplete-ingestion-run",
                    "event": "ingestion_run_summary",
                    "complete": False,
                    "reason": "controlled_storage_failure",
                }
            )
            + "\n"
        )
        return 3

    monkeypatch.setattr("kedra.orchestration.run_ingestion", fake_ingestion)
    monkeypatch.setattr(
        "kedra.orchestration.run_transformation",
        lambda *args: pytest.fail("Dagster must not start a dependent task after failure"),
    )
    output = io.StringIO()

    result = run_orchestration(
        EXAMPLE,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        ["15376"],
        ingest_profile,
        transform_profile,
        tmp_path / "runs",
        output,
    )

    assert result == 3
    summary = json.loads(output.getvalue())
    assert summary["ingestion_complete"] is False
    assert summary["transformation_complete"] is False
    assert summary["transformation_log_path"] is None
    assert summary["ingestion_status"] == "failed"
    assert summary["transformation_status"] == "skipped"
    assert summary["reason"] == "ingestion_manifest_incomplete"


def test_manifest_body_scope_mismatch_withholds_transformation(tmp_path, monkeypatch):
    ingest_profile, transform_profile = profiles(tmp_path)

    def fake_ingestion(settings, date_range, body_ids, stream):
        complete_ingestion(stream, settings, date_range, ["2"])
        return 0

    monkeypatch.setattr("kedra.orchestration.run_ingestion", fake_ingestion)
    monkeypatch.setattr(
        "kedra.orchestration.run_transformation",
        lambda *args, **kwargs: pytest.fail(
            "A manifest for another body scope must not reach transformation"
        ),
    )
    output = io.StringIO()

    result = run_orchestration(
        EXAMPLE,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        ["15376"],
        ingest_profile,
        transform_profile,
        tmp_path / "runs",
        output,
    )

    assert result == 3
    summary = json.loads(output.getvalue())
    assert summary["body_ids"] == ["15376"]
    assert summary["ingestion_status"] == "failed"
    assert summary["transformation_status"] == "skipped"
    assert summary["reason"] == "ingestion_manifest_scope_mismatch"


def test_each_rerun_gets_distinct_append_only_logs(tmp_path, monkeypatch):
    ingest_profile, transform_profile = profiles(tmp_path)

    def fake_ingestion(settings, date_range, body_ids, stream):
        complete_ingestion(stream, settings, date_range, body_ids)
        return 0

    def fake_transformation(settings, date_range, manifest_path, stream, *, expected_body_ids=None):
        assert expected_body_ids == settings.source.body_ids
        stream.write(
            json.dumps(
                {
                    "run_id": "controlled-transformation-run",
                    "event": "transformation_run_summary",
                    "complete": True,
                }
            )
            + "\n"
        )
        return 0

    monkeypatch.setattr("kedra.orchestration.run_ingestion", fake_ingestion)
    monkeypatch.setattr("kedra.orchestration.run_transformation", fake_transformation)
    summaries = []
    for _ in range(2):
        output = io.StringIO()
        assert (
            run_orchestration(
                EXAMPLE,
                DateRange.from_inputs("2025-07-17", "2025-07-17"),
                None,
                ingest_profile,
                transform_profile,
                tmp_path / "runs",
                output,
            )
            == 0
        )
        summaries.append(json.loads(output.getvalue()))

    assert summaries[0]["run_id"] != summaries[1]["run_id"]
    assert summaries[0]["ingestion_manifest_path"] != summaries[1]["ingestion_manifest_path"]
    assert len(list((tmp_path / "runs").glob("*.jsonl"))) == 4


def test_credential_profile_is_strict_and_does_not_change_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("KEDRA_MONGO_URI", "process-value")
    profile = tmp_path / "profile.env"
    write_profile(profile, "ingest")

    values = load_environment_profile(profile)

    assert values["KEDRA_MONGO_URI"].startswith("mongodb://ingest:")
    assert __import__("os").environ["KEDRA_MONGO_URI"] == "process-value"
    profile.write_text("KEDRA_MONGO_URI=secret\nUNEXPECTED=value\n", encoding="utf-8")
    with pytest.raises(ValueError, match="unsupported key") as error:
        load_environment_profile(profile)
    assert "secret" not in str(error.value)


def test_orchestration_requires_separate_role_profiles(tmp_path):
    ingest_profile, _ = profiles(tmp_path)
    with pytest.raises(ValueError, match="separate credential profiles"):
        run_orchestration(
            EXAMPLE,
            DateRange.from_inputs("2025-07-17", "2025-07-17"),
            ["15376"],
            ingest_profile,
            ingest_profile,
            tmp_path / "runs",
            io.StringIO(),
        )
