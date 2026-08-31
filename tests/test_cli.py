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
