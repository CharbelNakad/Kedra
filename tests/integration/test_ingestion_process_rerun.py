import json
import os
import subprocess
import sys
import threading
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from kedra.ingestion import validator_state_id
from kedra.models import RecordMetadata
from kedra.storage import mongo_client
from kedra.storage_admin import local_settings

pytestmark = [pytest.mark.storage, pytest.mark.local_http]

ROOT = Path(__file__).resolve().parents[2]
HOST = "127.0.0.1"
PORT = 18765
ENDPOINT = f"http://{HOST}:{PORT}"
SOURCE_NAME = "controlled-process-rerun"
DOCUMENT_URL = f"{ENDPOINT}/decision.html"
DOCUMENT_BODY = b"<html><body><div class='content'>Process rerun fixture.</div></body></html>"
ETAG = '"process-rerun-v1"'
RECORD = RecordMetadata(
    source=SOURCE_NAME,
    body_id="2",
    title="PROCESS-RERUN-0001",
    reference_number="PROCESS-RERUN-0001",
    description="One-record controlled process rerun.",
    published_date=date(2025, 7, 17),
    source_date_raw="17/07/2025",
    source_url=DOCUMENT_URL,
    partition_date=date(2025, 7, 17),
    partition_size="day",
)


class RerunHandler(BaseHTTPRequestHandler):
    search_queries = []
    document_validators = []
    document_body_responses = 0

    @classmethod
    def reset(cls):
        cls.search_queries = []
        cls.document_validators = []
        cls.document_body_responses = 0

    def log_message(self, format, *args):
        return

    def send_response_body(self, status, body=b"", *, content_type=None, etag=None):
        self.send_response(status)
        if content_type is not None:
            self.send_header("Content-Type", content_type)
        if etag is not None:
            self.send_header("ETag", etag)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path == "/robots.txt":
            self.send_response_body(200, b"User-agent: *\nDisallow:\n", content_type="text/plain")
            return
        if parsed.path == "/search/":
            type(self).search_queries.append(parse_qs(parsed.query))
            body = f"""<html><body><p class="results-count">1 result found</p>
            <li class="each-item"><h2 class="title">{RECORD.title}</h2>
            <p class="description">{RECORD.description}</p>
            <span class="date">{RECORD.source_date_raw}</span>
            <span class="refNO">{RECORD.reference_number}</span>
            <div class="link"><a href="{DOCUMENT_URL}">View</a></div></li>
            </body></html>""".encode()
            self.send_response_body(200, body, content_type="text/html")
            return
        if parsed.path == "/decision.html":
            validator = self.headers.get("If-None-Match")
            type(self).document_validators.append(validator)
            if validator == ETAG:
                self.send_response_body(304, etag=ETAG)
            else:
                type(self).document_body_responses += 1
                self.send_response_body(200, DOCUMENT_BODY, content_type="text/html", etag=ETAG)
            return
        self.send_response_body(404)


def write_config(path, settings):
    quote = json.dumps
    path.write_text(
        f"""[source]
name = {quote(SOURCE_NAME)}
search_url = {quote(f"{ENDPOINT}/search/")}
body_ids = ["2"]

[scraping]
partition_size = "day"
download_delay_seconds = 0.01
concurrency_per_domain = 1
timeout_seconds = 5.0
retry_times = 1
rate_limit_backoff_max_seconds = 1.0
max_response_bytes = 1048576
max_pages_per_partition = 2

[storage]
mongo_database = {quote(settings.mongo_database)}
landing_collection = {quote(settings.landing_collection)}
transformed_collection = {quote(settings.transformed_collection)}
state_collection = {quote(settings.state_collection)}
s3_endpoint_url = {quote(settings.s3_endpoint_url)}
s3_region = {quote(settings.s3_region)}
landing_bucket = {quote(settings.landing_bucket)}
transformed_bucket = {quote(settings.transformed_bucket)}
object_prefix = {quote(settings.object_prefix)}
connect_timeout_seconds = {settings.connect_timeout_seconds}
read_timeout_seconds = {settings.read_timeout_seconds}
max_attempts = {settings.max_attempts}
""",
        encoding="utf-8",
    )


def metadata_snapshot(database, settings):
    documents = database[settings.landing_collection].find({"record_key": RECORD.record_key})
    return sorted(documents, key=lambda document: document["_id"])


def object_key_snapshot(s3, settings):
    prefix = f"{settings.object_prefix}/records/{RECORD.record_key}/"
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=settings.landing_bucket,
        Prefix=prefix,
    )
    return sorted(item["Key"] for page in pages for item in page.get("Contents", []))


def transformed_snapshot():
    settings = local_settings(ROOT / "config.example.toml", "transform")
    with mongo_client(settings) as mongo:
        documents = mongo[settings.mongo_database][settings.transformed_collection].find(
            {"record_key": RECORD.record_key}
        )
        return sorted(documents, key=lambda document: document["_id"])


def run_ingestion(config, settings):
    environment = os.environ.copy()
    environment.update(
        {
            "KEDRA_MONGO_URI": settings.mongo_uri,
            "KEDRA_S3_ACCESS_KEY_ID": settings.s3_access_key_id,
            "KEDRA_S3_SECRET_ACCESS_KEY": settings.s3_secret_access_key,
            "PYTHONUTF8": "1",
        }
    )
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "kedra",
            "ingest",
            "--config",
            str(config),
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-17",
            "--body-id",
            "2",
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=60,
        check=False,
    )
    events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    summary = next(event for event in reversed(events) if event["event"] == "ingestion_run_summary")
    return result, summary


def run_orchestration(config, run_directory):
    environment = os.environ.copy()
    for name in (
        "KEDRA_MONGO_URI",
        "KEDRA_S3_ACCESS_KEY_ID",
        "KEDRA_S3_SECRET_ACCESS_KEY",
    ):
        environment.pop(name, None)
    environment["PYTHONUTF8"] = "1"
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-m",
            "kedra",
            "orchestrate",
            "--config",
            str(config),
            "--start-date",
            "2025-07-17",
            "--end-date",
            "2025-07-17",
            "--body-id",
            "2",
            "--ingest-env",
            str(ROOT / ".local" / "ingest.env"),
            "--transform-env",
            str(ROOT / ".local" / "transform.env"),
            "--run-directory",
            str(run_directory),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=90,
        check=False,
    )
    events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    summary = next(
        event for event in reversed(events) if event["event"] == "orchestration_run_summary"
    )
    return result, summary


def test_two_complete_cli_processes_reuse_one_verified_document_body(tmp_path, stores):
    settings, database, s3, _, _ = stores
    config = tmp_path / "rerun.toml"
    write_config(config, settings)
    state_id = validator_state_id(RECORD.record_key, DOCUMENT_URL)
    database[settings.state_collection].delete_one({"_id": state_id})
    RerunHandler.reset()
    server = ThreadingHTTPServer((HOST, PORT), RerunHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        first, first_summary = run_ingestion(config, settings)
        first_documents = metadata_snapshot(database, settings)
        first_object_keys = object_key_snapshot(s3, settings)
        second, second_summary = run_ingestion(config, settings)
        second_documents = metadata_snapshot(database, settings)
        second_object_keys = object_key_snapshot(s3, settings)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert RerunHandler.search_queries == [
        {"decisions": ["1"], "from": ["17/07/2025"], "to": ["17/07/2025"], "body": ["2"]},
        {"decisions": ["1"], "from": ["17/07/2025"], "to": ["17/07/2025"], "body": ["2"]},
    ]
    assert RerunHandler.document_validators == [None, ETAG]
    assert RerunHandler.document_body_responses == 1
    assert first_documents
    assert first_object_keys
    assert second_documents == first_documents
    assert second_object_keys == first_object_keys

    for summary in (first_summary, second_summary):
        assert summary["advertised_total"] == 1
        assert summary["card_occurrences"] == 1
        assert summary["distinct_records"] == 1
        assert summary["successfully_available_records"] == 1
        assert summary["failed_documents"] == 0
        assert summary["failed_asset_urls"] == []
        assert summary["complete"] is True
    assert first_summary["downloaded_files"] == 1
    assert first_summary["records_with_downloads"] == 1
    assert second_summary["downloaded_files"] == 0
    assert second_summary["not_modified_files"] == 1
    assert second_summary["records_reused_without_download"] == 1


def test_dagster_cli_runs_both_stages_and_reruns_without_new_versions(tmp_path, stores):
    settings, database, s3, _, _ = stores
    config = tmp_path / "orchestration.toml"
    run_directory = tmp_path / "runs"
    write_config(config, settings)
    state_id = validator_state_id(RECORD.record_key, DOCUMENT_URL)
    database[settings.state_collection].delete_one({"_id": state_id})
    RerunHandler.reset()
    server = ThreadingHTTPServer((HOST, PORT), RerunHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        first, first_summary = run_orchestration(config, run_directory)
        first_landing = metadata_snapshot(database, settings)
        first_transformed = transformed_snapshot()
        second, second_summary = run_orchestration(config, run_directory)
        second_landing = metadata_snapshot(database, settings)
        second_transformed = transformed_snapshot()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert first_summary["complete"] is True
    assert second_summary["complete"] is True
    assert first_summary["run_id"] != second_summary["run_id"]
    assert RerunHandler.document_validators == [None, ETAG]
    assert RerunHandler.document_body_responses == 1
    assert first_landing
    assert first_transformed
    assert second_landing == first_landing
    assert second_transformed == first_transformed

    manifests = []
    transformation_logs = []
    for summary in (first_summary, second_summary):
        manifest_path = Path(summary["ingestion_manifest_path"])
        transformation_log_path = Path(summary["transformation_log_path"])
        manifests.append(manifest_path)
        transformation_logs.append(transformation_log_path)
        assert manifest_path.is_file()
        assert transformation_log_path.is_file()
        manifest_events = [
            json.loads(line) for line in manifest_path.read_text(encoding="utf-8").splitlines()
        ]
        transformation_events = [
            json.loads(line)
            for line in transformation_log_path.read_text(encoding="utf-8").splitlines()
        ]
        assert manifest_events[-1]["event"] == "ingestion_run_summary"
        assert manifest_events[-1]["complete"] is True
        assert transformation_events[-1]["event"] == "transformation_run_summary"
        assert transformation_events[-1]["complete"] is True
        assert transformation_events[-1]["ingestion_run_id"] == manifest_events[-1]["run_id"]
    assert manifests[0] != manifests[1]
    assert transformation_logs[0] != transformation_logs[1]
