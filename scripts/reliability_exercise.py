"""Exercise 1,000 controlled records without contacting the public source."""

import argparse
import hashlib
import io
import json
import threading
import time
import tracemalloc
import zipfile
from collections import Counter
from contextlib import suppress
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

from scrapy.crawler import CrawlerProcess

from kedra.config import ScrapingSettings, Settings, SourceSettings, StorageSettings
from kedra.dates import DateRange
from kedra.identity import identifier_filename
from kedra.ingestion import (
    DecisionsIngestionSpider,
    DownloadedAsset,
    LandingAssetService,
    ingestion_crawler_settings,
)
from kedra.storage import IntegrityError, StorageError, StoredObject
from kedra.transformation import TransformationService, transform_documents

SOURCE = "synthetic-reliability"
BODY_ID = "15376"
RUN_DATE = date(2025, 7, 17)
PAGE_SIZE = 10
LANDING_BUCKET = "synthetic-landing"
TRANSFORMED_BUCKET = "synthetic-transformed"
PREFIX = "synthetic-reliability"
RATE_LIMITED = 0
SERVICE_UNAVAILABLE = 1


class MemoryObjectStore:
    """Small create-only adapter used only by the controlled exercise."""

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.objects: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def read(self, key: str, expected_hash: str | None = None) -> bytes:
        with self._lock:
            if key not in self.objects:
                raise StorageError("Synthetic object is missing")
            data = self.objects[key]
        if expected_hash is not None and hashlib.sha256(data).hexdigest() != expected_hash:
            raise IntegrityError("Synthetic object hash differs")
        return data

    def verify(self, stored: StoredObject) -> None:
        if stored.bucket != self.bucket:
            raise IntegrityError("Synthetic object belongs to another bucket")
        data = self.read(stored.key, stored.file_hash)
        if len(data) != stored.size_bytes:
            raise IntegrityError("Synthetic object size differs")

    def put_if_absent(self, key: str, data: bytes) -> StoredObject:
        digest = hashlib.sha256(data).hexdigest()
        with self._lock:
            existing = self.objects.get(key)
            if existing is not None and existing != data:
                raise IntegrityError("Synthetic immutable object conflict")
            created = existing is None
            self.objects.setdefault(key, data)
        return StoredObject(self.bucket, key, digest, len(data), created)


class MemoryMetadataStore:
    """Create-only metadata adapter for Landing and transformed versions."""

    def __init__(self):
        self.documents: dict[str, dict] = {}
        self._lock = threading.Lock()

    def insert_if_absent(self, version) -> bool:
        document = version.to_document()
        with self._lock:
            existing = self.documents.get(version.version_id)
            if existing is not None and existing != document:
                raise IntegrityError("Synthetic immutable metadata conflict")
            created = existing is None
            self.documents.setdefault(version.version_id, document)
        return created


class InterruptOnceService:
    """Fail one selected record before delegating all other writes."""

    validator_store = None

    def __init__(self, service: LandingAssetService, reference_number: str):
        self.service = service
        self.reference_number = reference_number
        self.interrupted = False

    def persist(self, asset: DownloadedAsset):
        if asset.record.reference_number == self.reference_number and not self.interrupted:
            self.interrupted = True
            raise StorageError("Controlled one-time storage interruption")
        return self.service.persist(asset)


def document_format(index: int) -> str:
    return ("html", "pdf", "doc", "docx")[index % 4]


def document_body(index: int) -> bytes:
    kind = document_format(index)
    if kind == "html":
        return (
            "<!doctype html><html><body>"
            f'<h1 class="page-title">SYNTH-{index:06d}</h1>'
            f'<div class="content">Synthetic legal decision {index}.</div>'
            "</body></html>"
        ).encode()
    if kind == "pdf":
        return f"%PDF-1.4\n% controlled decision {index}\n".encode()
    if kind == "doc":
        return bytes.fromhex("d0cf11e0a1b11ae1") + f" controlled decision {index}".encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in (
            ("[Content_Types].xml", b"<Types/>"),
            ("word/document.xml", f"<document>{index}</document>".encode()),
        ):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, data)
    return output.getvalue()


def media_type(index: int) -> str:
    return {
        "html": "text/html",
        "pdf": "application/pdf",
        "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }[document_format(index)]


class SyntheticSite:
    def __init__(self, record_count: int):
        self.record_count = record_count
        self.endpoint = ""
        self.attempts: Counter[int] = Counter()
        self.listing_requests = 0
        self.active_requests = 0
        self.max_active_requests = 0
        self._lock = threading.Lock()

    def begin_request(self) -> None:
        with self._lock:
            self.active_requests += 1
            self.max_active_requests = max(self.max_active_requests, self.active_requests)

    def end_request(self) -> None:
        with self._lock:
            self.active_requests -= 1

    def search_page(self, page_number: int) -> bytes:
        start = (page_number - 1) * PAGE_SIZE
        end = min(start + PAGE_SIZE, self.record_count)
        cards = []
        for index in range(start, end):
            extension = document_format(index)
            cards.append(
                '<li class="each-item">'
                f'<h2 class="title">SYNTH-{index:06d}</h2>'
                f'<p class="description">Controlled record {index}</p>'
                '<span class="date">17/07/2025</span>'
                f'<span class="refNO">Ref no: SYNTH-{index:06d}</span>'
                f'<div class="link"><a href="/documents/{index}.{extension}">View</a></div>'
                "</li>"
            )
        next_link = ""
        if end < self.record_count:
            query = urlencode(
                {
                    "decisions": "1",
                    "from": "17/07/2025",
                    "to": "17/07/2025",
                    "body": BODY_ID,
                    "pageNumber": str(page_number + 1),
                }
            )
            next_link = f'<a href="/search/?{query}">Next</a>'
        return (
            "<html><body>"
            f'<p class="results-count">{self.record_count} results found</p>'
            f"{''.join(cards)}{next_link}</body></html>"
        ).encode()


def handler_for(site: SyntheticSite):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return

        def reply(self, status: int, body: bytes = b"", **headers: str) -> None:
            with suppress(OSError):
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name.replace("_", "-"), value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if body:
                    self.wfile.write(body)

        def do_GET(self):
            site.begin_request()
            try:
                parsed = urlsplit(self.path)
                if parsed.path == "/robots.txt":
                    self.reply(200, b"User-agent: *\nDisallow:\n", Content_Type="text/plain")
                    return
                if parsed.path == "/search/":
                    site.listing_requests += 1
                    page = int(parse_qs(parsed.query).get("pageNumber", ["1"])[0])
                    self.reply(200, site.search_page(page), Content_Type="text/html")
                    return
                if parsed.path.startswith("/documents/"):
                    index = int(Path(parsed.path).stem)
                    site.attempts[index] += 1
                    attempt = site.attempts[index]
                    if index == RATE_LIMITED and attempt == 1:
                        self.reply(429, Retry_After="0")
                        return
                    if index == SERVICE_UNAVAILABLE and attempt == 1:
                        self.reply(503)
                        return
                    if index == site.record_count - 1 and attempt == 1:
                        time.sleep(0.15)
                    if index == site.record_count - 3:
                        self.reply(
                            200,
                            b"<html><body>missing decision content</body></html>",
                            Content_Type="text/html",
                        )
                        return
                    self.reply(
                        200,
                        document_body(index),
                        Content_Type=media_type(index),
                        ETag=f'"synthetic-{index}"',
                    )
                    return
                self.reply(404)
            finally:
                site.end_request()

    return Handler


def exercise_settings(endpoint: str) -> Settings:
    return Settings(
        SourceSettings(SOURCE, f"{endpoint}/search/", (BODY_ID,)),
        ScrapingSettings(
            partition_size="day",
            download_delay_seconds=0.001,
            concurrency_per_domain=8,
            timeout_seconds=0.05,
            retry_times=3,
            rate_limit_backoff_max_seconds=0.05,
            max_response_bytes=1024 * 1024,
            max_pages_per_partition=200,
        ),
        StorageSettings(
            mongo_database="synthetic",
            landing_collection="landing_metadata",
            transformed_collection="transformed_metadata",
            state_collection="crawl_state",
            s3_endpoint_url="http://127.0.0.1:1",
            s3_region="us-east-1",
            landing_bucket=LANDING_BUCKET,
            transformed_bucket=TRANSFORMED_BUCKET,
            object_prefix=PREFIX,
            mongo_uri="mongodb://synthetic:synthetic@127.0.0.1:1",
            s3_access_key_id="synthetic-access",
            s3_secret_access_key="synthetic-secret",
        ),
    )


def asset_for(record, index: int) -> DownloadedAsset:
    kind = document_format(index)
    return DownloadedAsset(
        record=record,
        asset_id="primary",
        role="primary",
        source_url=record.source_url,
        final_url=record.source_url,
        document_format=kind,
        media_type=media_type(index),
        body=document_body(index),
        etag=f'"synthetic-{index}"',
    )


def run_exercise(record_count: int) -> dict:
    if record_count < 500 or record_count > 1000 or record_count % PAGE_SIZE:
        raise ValueError("record_count must be a multiple of 10 between 500 and 1000")
    site = SyntheticSite(record_count)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(site))
    site.endpoint = f"http://127.0.0.1:{server.server_port}"
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    settings = exercise_settings(site.endpoint)
    landing_objects = MemoryObjectStore(LANDING_BUCKET)
    landing_metadata = MemoryMetadataStore()
    landing_service = LandingAssetService(landing_objects, landing_metadata, PREFIX)
    interrupted_index = record_count - 2
    first_run_service = InterruptOnceService(landing_service, f"SYNTH-{interrupted_index:06d}")
    events: list[dict] = []
    process = CrawlerProcess(ingestion_crawler_settings(settings), install_root_handler=False)
    crawler = process.create_crawler(DecisionsIngestionSpider)
    process.crawl(
        crawler,
        app_settings=settings,
        date_range=DateRange.from_inputs(RUN_DATE.isoformat(), RUN_DATE.isoformat()),
        body_ids=[BODY_ID],
        event_sink=events.append,
        asset_service=first_run_service,
    )

    tracemalloc.start()
    started = time.perf_counter()
    try:
        process.start(stop_after_crawl=True)
        first_summary = events[-1]
        records = sorted(
            (state.record for state in crawler.spider.record_states.values()),
            key=lambda record: record.reference_number,
        )
        recovery = [
            landing_service.persist(asset_for(record, index))
            for index, record in enumerate(records)
        ]
        landing_snapshot = (
            dict(landing_objects.objects),
            dict(landing_metadata.documents),
        )
        rerun = [
            landing_service.persist(asset_for(record, index))
            for index, record in enumerate(records)
        ]
        assert landing_snapshot == (
            landing_objects.objects,
            landing_metadata.documents,
        )

        transformed_objects = MemoryObjectStore(TRANSFORMED_BUCKET)
        transformed_metadata = MemoryMetadataStore()
        transformation = TransformationService(
            landing_objects,
            transformed_objects,
            transformed_metadata,
            LANDING_BUCKET,
            PREFIX,
        )
        documents = sorted(landing_metadata.documents.values(), key=lambda item: item["_id"])
        first_transform_events: list[dict] = []
        assert (
            transform_documents(
                documents,
                transformation,
                first_transform_events.append,
                RUN_DATE,
                RUN_DATE,
            )
            == 0
        )
        transformed_snapshot = (
            dict(transformed_objects.objects),
            dict(transformed_metadata.documents),
        )
        second_transform_events: list[dict] = []
        assert (
            transform_documents(
                documents,
                transformation,
                second_transform_events.append,
                RUN_DATE,
                RUN_DATE,
            )
            == 0
        )
        assert transformed_snapshot == (
            transformed_objects.objects,
            transformed_metadata.documents,
        )
        binary_documents = [
            document
            for document in transformed_metadata.documents.values()
            if document["document_format"] != "html"
        ]
        assert all(
            document["landing_file_hash"] == document["file_hash"] for document in binary_documents
        )
        assert all(
            document["object_key"].endswith(
                "/" + identifier_filename(document["identifier"], document["document_format"])
            )
            for document in transformed_metadata.documents.values()
        )
    finally:
        elapsed_seconds = time.perf_counter() - started
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=2)

    first_transform = first_transform_events[-1]
    second_transform = second_transform_events[-1]
    rate_limit_events = [event for event in events if event["event"] == "rate_limited"]
    expected_pages = record_count // PAGE_SIZE
    complete = all(
        (
            first_summary["advertised_total"] == record_count,
            first_summary["card_occurrences"] == record_count,
            first_summary["distinct_records"] == record_count,
            first_summary["successfully_available_records"] == record_count - 2,
            first_summary["failed_documents"] == 2,
            first_summary["download_failures"] == 1,
            first_summary["storage_failures"] == 1,
            site.listing_requests == expected_pages,
            site.attempts[RATE_LIMITED] == 2,
            site.attempts[SERVICE_UNAVAILABLE] == 2,
            site.attempts[record_count - 1] == 2,
            len(rate_limit_events) == 1,
            len(landing_objects.objects) == record_count,
            len(landing_metadata.documents) == record_count,
            sum(result.object_created for result in recovery) == 2,
            sum(result.metadata_created for result in recovery) == 2,
            not any(result.object_created or result.metadata_created for result in rerun),
            first_transform["successfully_transformed_assets"] == record_count,
            first_transform["created_objects"] == record_count,
            second_transform["reused_objects"] == record_count,
            second_transform["reused_metadata_versions"] == record_count,
            len(binary_documents) == record_count * 3 // 4,
            site.max_active_requests <= settings.scraping.concurrency_per_domain,
            peak_bytes < 256 * 1024 * 1024,
        )
    )
    return {
        "event": "reliability_exercise_summary",
        "records": record_count,
        "pages": expected_pages,
        "formats": {name: record_count // 4 for name in ("html", "pdf", "doc", "docx")},
        "injected": {
            "http_429_then_success": site.attempts[RATE_LIMITED] - 1,
            "http_503_then_success": site.attempts[SERVICE_UNAVAILABLE] - 1,
            "timeout_then_success": site.attempts[record_count - 1] - 1,
            "bad_html_terminal": first_summary["download_failures"],
            "storage_interruption_terminal": first_summary["storage_failures"],
        },
        "first_run": {
            "successful_records": first_summary["successfully_available_records"],
            "failed_records": first_summary["failed_documents"],
            "complete": first_summary["complete"],
        },
        "recovery": {
            "created_objects": sum(result.object_created for result in recovery),
            "created_metadata": sum(result.metadata_created for result in recovery),
        },
        "unchanged_rerun": {
            "reused_objects": sum(not result.object_created for result in rerun),
            "reused_metadata": sum(not result.metadata_created for result in rerun),
        },
        "transformation": {
            "first_created_objects": first_transform["created_objects"],
            "rerun_reused_objects": second_transform["reused_objects"],
            "rerun_reused_metadata": second_transform["reused_metadata_versions"],
        },
        "max_active_http_requests": site.max_active_requests,
        "configured_http_concurrency": settings.scraping.concurrency_per_domain,
        "peak_python_mib": round(peak_bytes / (1024 * 1024), 2),
        "elapsed_seconds": round(elapsed_seconds, 2),
        "complete": complete,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=1000)
    args = parser.parse_args()
    try:
        summary = run_exercise(args.records)
    except (AssertionError, OSError, ValueError) as error:
        print(
            json.dumps(
                {
                    "event": "reliability_exercise_summary",
                    "complete": False,
                    "reason": type(error).__name__,
                },
                sort_keys=True,
            )
        )
        return 3
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["complete"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
