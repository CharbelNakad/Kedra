import json
import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.live_discovery
ROOT = Path(__file__).resolve().parents[2]


def test_known_single_day_discovery_has_two_pages_and_twelve_records():
    environment = {
        **os.environ,
        "KEDRA_MONGO_URI": "mongodb://fixture-user:fixture-password@localhost:27017",
        "KEDRA_S3_ACCESS_KEY_ID": "live-discovery-unused",
        "KEDRA_S3_SECRET_ACCESS_KEY": "live-discovery-unused",
    }
    result = subprocess.run(
        [
            str(ROOT / ".venv" / "Scripts" / "python.exe"),
            "-m",
            "kedra",
            "discover",
            "--config",
            str(ROOT / "config.example.toml"),
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-17",
            "--body-id",
            "15376",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )
    events = [json.loads(line) for line in result.stdout.splitlines()]
    assert result.returncode == 0, events[-1] if events else result.stderr
    summary = next(event for event in events if event["event"] == "discovery_summary")
    assert summary["complete"] is True
    assert summary["pages_seen"] == 2
    assert summary["advertised_total"] == 12
    assert summary["distinct_records"] == 12
    assert not result.stderr
