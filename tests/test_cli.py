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
