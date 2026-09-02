import json
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.local_http

ROOT = Path(__file__).resolve().parents[1]


def test_one_thousand_record_reliability_exercise():
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            str(ROOT / "scripts" / "reliability_exercise.py"),
            "--records",
            "1000",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    summary = json.loads(result.stdout)
    assert summary["records"] == 1000
    assert summary["pages"] == 100
    assert summary["formats"] == {"doc": 250, "docx": 250, "html": 250, "pdf": 250}
    assert summary["injected"] == {
        "bad_html_terminal": 1,
        "http_429_then_success": 1,
        "http_503_then_success": 1,
        "storage_interruption_terminal": 1,
        "timeout_then_success": 1,
    }
    assert summary["first_run"] == {
        "complete": False,
        "failed_records": 2,
        "successful_records": 998,
    }
    assert summary["recovery"] == {"created_metadata": 2, "created_objects": 2}
    assert summary["unchanged_rerun"] == {
        "reused_metadata": 1000,
        "reused_objects": 1000,
    }
    assert summary["transformation"] == {
        "first_created_objects": 1000,
        "rerun_reused_metadata": 1000,
        "rerun_reused_objects": 1000,
    }
    assert summary["max_active_http_requests"] <= summary["configured_http_concurrency"]
    assert summary["peak_python_mib"] < 256
    assert summary["complete"] is True
