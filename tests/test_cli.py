import json
from pathlib import Path

import pytest

from kedra.cli import main

EXAMPLE = str(Path(__file__).resolve().parents[1] / "config.example.toml")


def test_offline_cli_summary_is_bounded_and_does_not_disclose_secrets(
    monkeypatch, capsys, example_env
):
    for name, value in example_env.items():
        monkeypatch.setenv(name, value)
    assert (
        main(
            [
                "check-config",
                "--config",
                EXAMPLE,
                "--start-date",
                "2024-01-15",
                "--end-date",
                "2024-05-02",
            ]
        )
        == 0
    )
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["status"] == "valid"
    assert result["network_access"] is False
    assert result["partition_count"] == 5
    assert result["body_partition_count"] == 20
    assert len(result["first_partitions"]) == 3
    assert result["last_partition_date"] == "2024-05-01"
    assert result["first_partitions"][0]["from"] == "15/01/2024"
    for secret in example_env.values():
        assert secret not in output.out + output.err


def test_dates_fail_before_attempting_to_load_any_config(capsys):
    assert (
        main(
            [
                "check-config",
                "--config",
                "does-not-exist.toml",
                "--start-date",
                "2024-02-01",
                "--end-date",
                "2024-01-01",
            ]
        )
        == 2
    )
    assert "on or before" in capsys.readouterr().err


def test_missing_environment_is_reported_without_traceback(monkeypatch, capsys):
    monkeypatch.delenv("KEDRA_MONGO_URI", raising=False)
    assert (
        main(
            [
                "check-config",
                "--config",
                EXAMPLE,
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-01",
            ]
        )
        == 2
    )
    result = capsys.readouterr()
    assert result.out == ""
    assert "KEDRA_MONGO_URI" in result.err
    assert "Traceback" not in result.err


def test_missing_date_argument_is_rejected():
    with pytest.raises(SystemExit) as error:
        main(["check-config", "--config", EXAMPLE, "--start-date", "2024-01-01"])
    assert error.value.code == 2


def test_discover_cli_passes_validated_scope_to_scrapy(monkeypatch, capsys, example_env):
    for name, value in example_env.items():
        monkeypatch.setenv(name, value)
    captured = {}

    def fake_run(settings, date_range, body_ids, stream):
        captured.update(
            settings=settings,
            date_range=date_range,
            body_ids=body_ids,
            stream=stream,
        )
        return 3

    monkeypatch.setattr("kedra.cli.run_discovery", fake_run)
    result = main(
        [
            "discover",
            "--config",
            EXAMPLE,
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-17",
            "--body-id",
            "15376",
        ]
    )
    assert result == 3
    assert captured["body_ids"] == ["15376"]
    assert captured["date_range"].start.isoformat() == "2025-07-17"
    assert captured["settings"].source.name == "workplace-relations"
    assert captured["stream"] is not None
    assert capsys.readouterr().err == ""


def test_ingest_cli_passes_validated_scope_to_scrapy(monkeypatch, capsys, example_env):
    for name, value in example_env.items():
        monkeypatch.setenv(name, value)
    captured = {}

    def fake_run(settings, date_range, body_ids, stream):
        captured.update(
            settings=settings,
            date_range=date_range,
            body_ids=body_ids,
            stream=stream,
        )
        return 3

    monkeypatch.setattr("kedra.cli.run_ingestion", fake_run)
    result = main(
        [
            "ingest",
            "--config",
            EXAMPLE,
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-17",
            "--body-id",
            "15376",
        ]
    )
    assert result == 3
    assert captured["body_ids"] == ["15376"]
    assert captured["date_range"].end_exclusive.isoformat() == "2025-07-18"
    assert captured["settings"].storage.landing_bucket == "kedra-landing"
    assert captured["stream"] is not None
    assert capsys.readouterr().err == ""


def test_transform_cli_passes_validated_date_range_to_standalone_runner(
    monkeypatch, capsys, example_env, tmp_path
):
    for name, value in example_env.items():
        monkeypatch.setenv(name, value)
    captured = {}

    manifest = tmp_path / "ingestion.jsonl"

    def fake_run(settings, date_range, manifest_path, stream):
        captured.update(
            settings=settings,
            date_range=date_range,
            manifest_path=manifest_path,
            stream=stream,
        )
        return 3

    monkeypatch.setattr("kedra.cli.run_transformation", fake_run)
    result = main(
        [
            "transform",
            "--config",
            EXAMPLE,
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-18",
            "--ingestion-manifest",
            str(manifest),
        ]
    )

    assert result == 3
    assert captured["date_range"].start.isoformat() == "2025-07-17"
    assert captured["date_range"].end_exclusive.isoformat() == "2025-07-19"
    assert captured["settings"].storage.transformed_bucket == "kedra-transformed"
    assert captured["manifest_path"] == manifest
    assert captured["stream"] is not None
    assert capsys.readouterr().err == ""


def test_transform_cli_requires_completed_ingestion_manifest():
    with pytest.raises(SystemExit) as error:
        main(
            [
                "transform",
                "--config",
                EXAMPLE,
                "--start-date",
                "2030-01-01",
                "--end-date",
                "2030-01-01",
            ]
        )
    assert error.value.code == 2


def test_orchestrate_cli_passes_profiles_and_run_directory_without_process_secrets(
    monkeypatch, capsys, tmp_path
):
    captured = {}

    def fake_run(
        config_path,
        date_range,
        body_ids,
        ingest_profile,
        transform_profile,
        run_directory,
        stream,
    ):
        captured.update(
            config_path=config_path,
            date_range=date_range,
            body_ids=body_ids,
            ingest_profile=ingest_profile,
            transform_profile=transform_profile,
            run_directory=run_directory,
            stream=stream,
        )
        return 3

    monkeypatch.delenv("KEDRA_MONGO_URI", raising=False)
    monkeypatch.setattr("kedra.cli.run_orchestration", fake_run)
    result = main(
        [
            "orchestrate",
            "--config",
            EXAMPLE,
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-17",
            "--body-id",
            "15376",
            "--ingest-env",
            str(tmp_path / "ingest.env"),
            "--transform-env",
            str(tmp_path / "transform.env"),
            "--run-directory",
            str(tmp_path / "runs"),
        ]
    )

    assert result == 3
    assert captured["config_path"] == Path(EXAMPLE)
    assert captured["date_range"].end_exclusive.isoformat() == "2025-07-18"
    assert captured["body_ids"] == ["15376"]
    assert captured["ingest_profile"].name == "ingest.env"
    assert captured["transform_profile"].name == "transform.env"
    assert captured["run_directory"].name == "runs"
    assert captured["stream"] is not None
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("command", ["discover", "ingest"])
@pytest.mark.parametrize("body_ids", [["unknown"], ["15376", "15376"]])
def test_discover_cli_rejects_unknown_or_duplicate_body_scope(
    monkeypatch, capsys, example_env, command, body_ids
):
    for name, value in example_env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(
        f"kedra.cli.run_{'discovery' if command == 'discover' else 'ingestion'}",
        lambda *args, **kwargs: pytest.fail("invalid scope must fail before Scrapy starts"),
    )
    arguments = [
        command,
        "--config",
        EXAMPLE,
        "--start-date",
        "2025-07-17",
        "--end-date",
        "2025-07-17",
    ]
    for body_id in body_ids:
        arguments.extend(["--body-id", body_id])
    assert main(arguments) == 2
    assert "distinct configured" in capsys.readouterr().err


@pytest.mark.parametrize("invalid_setting", ["mongo_uri", "mongo_database", "landing_collection"])
def test_invalid_mongo_settings_return_error_without_secrets(
    tmp_path, monkeypatch, capsys, example_env, invalid_setting
):
    config = Path(EXAMPLE).read_text()
    if invalid_setting == "mongo_uri":
        example_env["KEDRA_MONGO_URI"] = "mongodb://fixture-user:fixture-password@localhost:bad"
        expected_field = "KEDRA_MONGO_URI"
    else:
        original, invalid = (
            ("kedra", "invalid/database")
            if invalid_setting == "mongo_database"
            else ("landing_metadata", "invalid..collection")
        )
        config = config.replace(
            f'{invalid_setting} = "{original}"', f'{invalid_setting} = "{invalid}"'
        )
        expected_field = f"storage.{invalid_setting}"
    path = tmp_path / "config.toml"
    path.write_text(config, encoding="utf-8")
    for name, value in example_env.items():
        monkeypatch.setenv(name, value)
    assert (
        main(
            [
                "check-config",
                "--config",
                str(path),
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ]
        )
        == 2
    )
    output = capsys.readouterr()
    assert output.out == ""
    assert expected_field in output.err
    assert "Traceback" not in output.err
    assert "fixture-user" not in output.err
    assert "fixture-password" not in output.err
    for secret in example_env.values():
        assert secret not in output.err
