import hashlib
import io
import json
import zipfile
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from scrapy.http import HtmlResponse, Request, Response

from kedra.config import load_settings
from kedra.dates import DateRange
from kedra.ingestion import (
    CachedAssetReuse,
    ConditionalAssetMiddleware,
    DecisionsIngestionSpider,
    DownloadedAsset,
    IngestionRecordState,
    LandingAssetPipeline,
    LandingAssetService,
    PendingAssetRequest,
    ValidatorStateStore,
    classify_document,
    ingestion_crawler_settings,
    inspect_html,
    landing_object_key,
    validator_state_id,
)
from kedra.models import RecordMetadata
from kedra.storage import ObjectNotFound, StorageError, StoredObject
from kedra.transformation import load_ingestion_manifest

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config.example.toml"
SOURCE_HOST = "www.workplacerelations.ie"
SOURCE_URL = f"https://{SOURCE_HOST}/en/cases/2025/july/adj-00050000.html"


def sample_record():
    return RecordMetadata(
        source="workplace-relations",
        body_id="15376",
        title="ADJ-00050000",
        reference_number="ADJ-00050000",
        description="Controlled fixture decision.",
        published_date=date(2025, 7, 17),
        source_date_raw="17/07/2025",
        source_url=SOURCE_URL,
        partition_date=date(2025, 7, 17),
        partition_size="day",
    )


def http_response(url, body, *, media_type, request_url=None, status=200):
    request = Request(request_url or url)
    response_type = HtmlResponse if b"<html" in body.lower() else Response
    args = {
        "url": url,
        "body": body,
        "status": status,
        "headers": {"Content-Type": media_type},
        "request": request,
    }
    if response_type is HtmlResponse:
        args["encoding"] = "utf-8"
    return response_type(**args)


def docx_bytes():
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("[Content_Types].xml", "<Types/>")
        archive.writestr("word/document.xml", "<document>fixture</document>")
    return stream.getvalue()


@pytest.mark.parametrize(
    "url,media_type,body,expected",
    [
        (
            "https://www.workplacerelations.ie/misleading.pdf",
            "application/pdf",
            b"<!doctype html><html><body>HTML wins over suffix and MIME.</body></html>",
            "html",
        ),
        (
            "https://www.workplacerelations.ie/misleading.html",
            "text/plain",
            b"%PDF-1.4\nfixture",
            "pdf",
        ),
        (
            "https://www.workplacerelations.ie/decision.bin",
            "application/msword",
            bytes.fromhex("d0cf11e0a1b11ae1") + b"word-fixture",
            "doc",
        ),
        (
            "https://www.workplacerelations.ie/decision.bin",
            "application/octet-stream",
            docx_bytes(),
            "docx",
        ),
    ],
)
def test_classification_uses_bytes_mime_and_final_url(url, media_type, body, expected):
    assert classify_document(http_response(url, body, media_type=media_type)) == expected


@pytest.mark.parametrize(
    "url,media_type,body,reason",
    [
        (SOURCE_URL, "text/html", b"", "empty_document_response"),
        (
            "https://www.workplacerelations.ie/fake.pdf",
            "application/pdf",
            b"not really a PDF",
            "unsupported_document_format",
        ),
        (
            "https://www.workplacerelations.ie/fake.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            b"PK\x03\x04not-a-valid-zip",
            "unsupported_document_format",
        ),
    ],
)
def test_empty_or_unverified_document_formats_fail(url, media_type, body, reason):
    with pytest.raises(RuntimeError, match=reason):
        classify_document(http_response(url, body, media_type=media_type))


def test_html_inspection_keeps_only_explicit_assets_and_excludes_preview_images():
    body = b"""<!doctype html><html><body>
    <div class="content"><p>Substantive decision text.</p></div>
    <div class="related-file">
      <a class="preview" href="/asset.pdf?type=pdfPreview&amp;width=200">preview</a>
      <a class="download" href="/asset.pdf">download</a>
    </div>
    <a data-document-asset="continuation" href="/part-2.html">part 2</a>
    <a href="/unrelated-policy.pdf">policy</a>
    </body></html>"""
    response = http_response(SOURCE_URL, body, media_type="text/html")

    has_content, links = inspect_html(response)

    assert has_content is True
    assert [(link.role, link.url) for link in links] == [
        ("attachment", f"https://{SOURCE_HOST}/asset.pdf"),
        ("continuation", f"https://{SOURCE_HOST}/part-2.html"),
    ]


def test_semantic_next_link_inside_decision_content_is_a_required_continuation():
    response = http_response(
        SOURCE_URL,
        b"""<html><body><div class="content">
        <p>Decision page one.</p><a class="next" href="/part-2.html">Next</a>
        </div></body></html>""",
        media_type="text/html",
    )

    has_content, links = inspect_html(response)

    assert has_content is True
    assert [(link.role, link.url) for link in links] == [
        ("continuation", f"https://{SOURCE_HOST}/part-2.html")
    ]


def test_empty_html_is_only_accepted_as_a_wrapper_with_an_explicit_asset():
    wrapper = http_response(
        SOURCE_URL,
        b"""<html><body><div class="content"></div><div class="related-file">
        <a class="download" href="/decision.pdf">PDF</a></div></body></html>""",
        media_type="text/html",
    )
    empty = http_response(
        SOURCE_URL,
        b"<html><body><div class='content'></div></body></html>",
        media_type="text/html",
    )
    assert inspect_html(wrapper)[0] is False
    assert len(inspect_html(wrapper)[1]) == 1
    assert inspect_html(empty) == (False, ())


class MemoryObjectStore:
    def __init__(self, bucket="kedra-landing"):
        self.bucket = bucket
        self.objects = {}
        self.put_calls = 0

    def put_if_absent(self, key, body):
        self.put_calls += 1
        created = key not in self.objects
        if created:
            self.objects[key] = body
        elif self.objects[key] != body:
            raise AssertionError("deterministic object key must never be overwritten")
        return StoredObject(
            self.bucket,
            key,
            hashlib.sha256(body).hexdigest(),
            len(body),
            created,
        )

    def verify(self, stored):
        body = self.objects.get(stored.key)
        if body is None:
            raise ObjectNotFound("missing object")
        assert stored.bucket == self.bucket
        assert len(body) == stored.size_bytes
        assert hashlib.sha256(body).hexdigest() == stored.file_hash


class MemoryMetadataStore:
    def __init__(self, documents=None, fail_once=False):
        self.documents = {} if documents is None else documents
        self.fail_once = fail_once

    def insert_if_absent(self, version):
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("controlled interruption after object creation")
        created = version.version_id not in self.documents
        self.documents.setdefault(version.version_id, version.to_document())
        return created

    def find(self, version_id):
        return self.documents.get(version_id)


class MemoryStateCollection:
    def __init__(self):
        self.documents = {}

    def find_one(self, query):
        return self.documents.get(query["_id"])

    def replace_one(self, query, document, upsert):
        assert upsert is True
        assert query == {"_id": document["_id"]}
        self.documents[document["_id"]] = document


class SequentialRerunSpider(DecisionsIngestionSpider):
    """Run consecutive single-record checks with one reactor and shared storage."""

    name = "controlled_sequential_rerun"

    def __init__(self, probe_records, **kwargs):
        super().__init__(**kwargs)
        self.probe_records = tuple(probe_records)
        self.probe_index = 0

    def _probe_request(self, record):
        self.record_states[record.record_key] = IngestionRecordState(
            record,
            pending_requests={
                "primary": PendingAssetRequest(record.source_url),
            },
            seen_assets={"primary"},
        )
        return self._asset_request(record.record_key, "primary", "primary", record.source_url)

    async def start(self):
        yield self._probe_request(self.probe_records[0])

    def asset_persisted(self, asset, result):
        super().asset_persisted(asset, result)
        self.probe_index += 1
        if self.probe_index == len(self.probe_records):
            return
        self.crawler.engine.crawl(self._probe_request(self.probe_records[self.probe_index]))


def sample_asset(
    body=b"%PDF-1.4\nexact fixture bytes",
    document_format="pdf",
    media_type="application/pdf",
):
    return DownloadedAsset(
        record=sample_record(),
        asset_id="primary",
        role="primary",
        source_url=SOURCE_URL,
        final_url=f"https://{SOURCE_HOST}/final/decision.{document_format}",
        document_format=document_format,
        media_type=media_type,
        body=body,
        attempt_count=2,
    )


def test_object_key_and_metadata_preserve_exact_bytes_hash_and_asset_provenance():
    objects = MemoryObjectStore()
    metadata = MemoryMetadataStore()
    service = LandingAssetService(objects, metadata, "workplace-relations")
    asset = sample_asset()

    result = service.persist(asset)

    expected_hash = hashlib.sha256(asset.body).hexdigest()
    assert result.object_created is True
    assert result.metadata_created is True
    assert objects.objects[result.version.stored_object.key] == asset.body
    assert result.version.stored_object.file_hash == expected_hash
    assert result.version.stored_object.key == (
        f"workplace-relations/records/{asset.record.record_key}/primary/{expected_hash}.pdf"
    )
    document = result.version.to_document()
    assert document["asset_role"] == "primary"
    assert document["asset_source_url"] == SOURCE_URL
    assert document["asset_final_url"].endswith("/final/decision.pdf")
    assert document["media_type"] == "application/pdf"


def test_restart_after_object_creation_reuses_orphan_and_finishes_metadata():
    objects = MemoryObjectStore()
    documents = {}
    asset = sample_asset()
    interrupted = LandingAssetService(
        objects,
        MemoryMetadataStore(documents, fail_once=True),
        "workplace-relations",
    )
    with pytest.raises(RuntimeError, match="controlled interruption"):
        interrupted.persist(asset)
    assert len(objects.objects) == 1
    assert not documents

    restarted = LandingAssetService(
        objects,
        MemoryMetadataStore(documents),
        "workplace-relations",
    )
    result = restarted.persist(asset)

    assert result.object_created is False
    assert result.metadata_created is True
    assert len(objects.objects) == len(documents) == 1


@pytest.mark.parametrize(
    "document_format,media_type,body",
    [
        (
            "html",
            "text/html",
            b"<html><body><div class='content'>Exact HTML.</div></body></html>",
        ),
        ("pdf", "application/pdf", b"%PDF-1.4\nexact PDF"),
        ("doc", "application/msword", bytes.fromhex("d0cf11e0a1b11ae1") + b"exact DOC"),
        (
            "docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            docx_bytes(),
        ),
    ],
)
def test_every_required_format_is_stored_byte_for_byte(document_format, media_type, body):
    objects = MemoryObjectStore()
    service = LandingAssetService(objects, MemoryMetadataStore(), "workplace-relations")
    asset = sample_asset(body, document_format, media_type)

    result = service.persist(asset)

    assert objects.objects[result.version.stored_object.key] == body
    assert result.version.stored_object.file_hash == hashlib.sha256(body).hexdigest()
    assert result.version.document_format == document_format


def ingestion_spider(example_env, events=None):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(
        loaded,
        scraping=replace(loaded.scraping, partition_size="day"),
    )
    return DecisionsIngestionSpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=(events if events is not None else []).append,
    )


def test_html_redirect_capture_preserves_requested_and_final_urls(example_env):
    spider = ingestion_spider(example_env)
    request = next(spider._accepted_record_outputs(sample_record()))
    response = http_response(
        f"https://{SOURCE_HOST}/moved/decision.html",
        b"<html><body><div class='content'>Decision text.</div></body></html>",
        media_type="text/html",
        request_url=request.url,
    )

    output = list(spider.parse_asset(response, **request.cb_kwargs))

    asset = next(item for item in output if isinstance(item, DownloadedAsset))
    assert asset.source_url == SOURCE_URL
    assert asset.final_url.endswith("/moved/decision.html")
    assert asset.document_format == "html"
    assert asset.body == response.body


def test_blank_content_type_is_a_correlated_download_failure(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    request = next(spider._accepted_record_outputs(sample_record()))
    response = http_response(
        SOURCE_URL,
        b"<html><body><div class='content'>Decision text.</div></body></html>",
        media_type="",
        request_url=request.url,
    )

    assert list(spider.parse_asset(response, **request.cb_kwargs)) == []

    assert spider.download_failures == 1
    failure = events[-1]
    assert failure["event"] == "asset_failed"
    assert failure["reason"] == "blank_content_type"
    assert failure["url"] == SOURCE_URL


def test_off_host_redirect_failure_reports_the_rejected_destination(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    request = next(spider._accepted_record_outputs(sample_record()))
    final_url = "https://example.com/moved.html"
    response = http_response(
        final_url,
        b"<html><body><div class='content'>Untrusted destination.</div></body></html>",
        media_type="text/html",
        request_url=request.url,
    )

    assert list(spider.parse_asset(response, **request.cb_kwargs)) == []

    failure = events[-1]
    assert failure["reason"] == "document_redirected_outside_source_host"
    assert failure["url"] == final_url


def test_wrapper_and_multiple_explicit_assets_are_all_required(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    primary = next(spider._accepted_record_outputs(sample_record()))
    wrapper_body = b"""<html><body><div class="content"></div>
    <div class="related-file"><a class="download" href="/decision.pdf">PDF</a></div>
    <a data-document-asset="continuation" href="/part-2.html">part 2</a>
    </body></html>"""
    outputs = list(
        spider.parse_asset(
            http_response(SOURCE_URL, wrapper_body, media_type="text/html"),
            **primary.cb_kwargs,
        )
    )
    requests = [item for item in outputs if isinstance(item, Request)]
    wrapper = next(item for item in outputs if isinstance(item, DownloadedAsset))
    assert wrapper.asset_id == "wrapper"
    assert wrapper.role == "wrapper"
    assert wrapper.body == wrapper_body
    assert {request.cb_kwargs["asset_role"] for request in requests} == {
        "attachment",
        "continuation",
    }
    assert len(spider.record_states[sample_record().record_key].pending_requests) == 2
    objects = MemoryObjectStore()
    service = LandingAssetService(objects, MemoryMetadataStore(), "workplace-relations")
    spider.asset_persisted(wrapper, service.persist(wrapper))
    for request in requests:
        if request.cb_kwargs["asset_role"] == "attachment":
            response = http_response(
                request.url, b"%PDF-1.4\nattachment", media_type="application/pdf"
            )
        else:
            response = http_response(
                request.url,
                b"<html><body><div class='content'>Part two.</div></body></html>",
                media_type="text/html",
            )
        asset = next(
            item
            for item in spider.parse_asset(response, **request.cb_kwargs)
            if isinstance(item, DownloadedAsset)
        )
        spider.asset_persisted(asset, service.persist(asset))
    state = spider.record_states[sample_record().record_key]
    assert state.complete is True
    assert state.stored_assets == 3
    assert any(value == wrapper_body for value in objects.objects.values())


def test_failed_attachment_is_correlated_and_prevents_record_success(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    next(spider._accepted_record_outputs(sample_record()))
    state = spider.record_states[sample_record().record_key]
    state.pending_requests.pop("primary")
    state.pending_requests["attachment-test"] = f"https://{SOURCE_HOST}/missing.pdf"
    request = spider._asset_request(
        sample_record().record_key,
        "attachment-test",
        "attachment",
        f"https://{SOURCE_HOST}/missing.pdf",
    )
    request.meta["retry_times"] = 2
    failure = SimpleNamespace(
        request=request,
        value=SimpleNamespace(response=Response(request.url, status=503, request=request)),
    )

    spider.asset_request_failed(failure)

    assert state.complete is False
    assert state.failures[0].url.endswith("/missing.pdf")
    assert state.failures[0].http_status == 503
    assert state.failures[0].attempt_count == 3
    assert events[-1]["failure_stage"] == "download"


def test_complete_fixture_ingestion_reconciles_one_record_and_one_asset(example_env, tmp_path):
    events = []
    spider = ingestion_spider(example_env, events)
    listing_request = next(spider.initial_requests())
    listing_body = f"""<html><body><p class="results-count">1 result found</p>
    <li class="each-item"><h2 class="title">ADJ-00050000</h2>
    <p class="description">Controlled fixture decision.</p>
    <span class="date">17/07/2025</span><span class="refNO">ADJ-00050000</span>
    <div class="link"><a href="{SOURCE_URL}">View</a></div></li></body></html>""".encode()
    document_request = next(
        item
        for item in spider.parse_listing(
            HtmlResponse(
                listing_request.url,
                body=listing_body,
                encoding="utf-8",
                request=listing_request,
            ),
            listing_request.cb_kwargs["unit_key"],
        )
        if isinstance(item, Request)
    )
    asset = next(
        item
        for item in spider.parse_asset(
            http_response(
                SOURCE_URL,
                b"<html><body><div class='content'>Decision text.</div></body></html>",
                media_type="text/html",
            ),
            **document_request.cb_kwargs,
        )
        if isinstance(item, DownloadedAsset)
    )
    service = LandingAssetService(MemoryObjectStore(), MemoryMetadataStore(), "workplace-relations")
    spider.asset_persisted(asset, service.persist(asset))

    spider.closed("finished")

    run = events[-1]
    stored_event = next(event for event in events if event["event"] == "asset_stored")
    assert stored_event["landing_version_id"] == next(iter(service.metadata_store.documents))
    assert run["event"] == "ingestion_run_summary"
    assert run["start_date"] == run["end_date"] == "2025-07-17"
    assert run["body_ids"] == ["15376"]
    assert run["discovery_complete"] is True
    assert run["successfully_available_records"] == 1
    assert run["records_with_downloads"] == 1
    assert run["records_reused_without_download"] == 0
    assert run["failed_documents"] == 0
    assert run["downloaded_files"] == run["stored_files"] == 1
    assert run["created_objects"] == run["inserted_metadata_versions"] == 1
    assert run["complete"] is True
    manifest_path = tmp_path / "ingestion.jsonl"
    manifest_path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )
    manifest = load_ingestion_manifest(
        manifest_path,
        date(2025, 7, 17),
        date(2025, 7, 17),
        "workplace-relations",
        ("15376",),
    )
    assert manifest.run_id == spider.run_id
    assert manifest.landing_version_ids == (stored_event["landing_version_id"],)


def test_malformed_listing_card_is_included_in_failed_document_count(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    listing_request = next(spider.initial_requests())
    body = (ROOT / "tests/fixtures/search/malformed-card.html").read_bytes()

    assert (
        list(
            spider.parse_listing(
                HtmlResponse(
                    listing_request.url,
                    body=body,
                    encoding="utf-8",
                    request=listing_request,
                ),
                listing_request.cb_kwargs["unit_key"],
            )
        )
        == []
    )
    spider.closed("finished")

    run = events[-1]
    assert run["failed_card_parses"] == 1
    assert run["ingestion_records"] == 1
    assert run["successfully_available_records"] == 0
    assert run["failed_documents"] == 1
    assert run["complete"] is False


def test_known_missing_pagination_records_are_failed_documents(example_env):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(
        loaded,
        scraping=replace(
            loaded.scraping,
            partition_size="day",
            max_pages_per_partition=1,
        ),
    )
    events = []
    spider = DecisionsIngestionSpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )
    listing_request = next(spider.initial_requests())
    outputs = list(
        spider.parse_listing(
            HtmlResponse(
                listing_request.url,
                body=(ROOT / "tests/fixtures/search/body-15376-page-1.html").read_bytes(),
                encoding="utf-8",
                request=listing_request,
            ),
            listing_request.cb_kwargs["unit_key"],
        )
    )
    document_requests = [item for item in outputs if isinstance(item, Request)]
    service = LandingAssetService(MemoryObjectStore(), MemoryMetadataStore(), "workplace-relations")
    for request in document_requests:
        asset = next(
            item
            for item in spider.parse_asset(
                http_response(
                    request.url,
                    b"<html><body><div class='content'>Decision text.</div></body></html>",
                    media_type="text/html",
                ),
                **request.cb_kwargs,
            )
            if isinstance(item, DownloadedAsset)
        )
        spider.asset_persisted(asset, service.persist(asset))

    spider.closed("finished")

    run = events[-1]
    assert len(document_requests) == 10
    assert run["advertised_total"] == 12
    assert run["card_occurrences"] == 10
    assert run["known_missing_records"] == 2
    assert run["ingestion_records"] == 12
    assert run["successfully_available_records"] == 10
    assert run["failed_documents"] == 2
    assert run["complete"] is False


def test_identity_collision_is_included_in_failed_document_count(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    listing_request = next(spider.initial_requests())
    listing_body = f"""<html><body><p class="results-count">2 results found</p>
    <li class="each-item"><h2 class="title">ADJ-00050000</h2>
    <p class="description">Original card.</p>
    <span class="date">17/07/2025</span><span class="refNO">ADJ-00050000</span>
    <div class="link"><a href="{SOURCE_URL}">View</a></div></li>
    <li class="each-item"><h2 class="title">Conflicting title</h2>
    <p class="description">Conflicting card.</p>
    <span class="date">17/07/2025</span><span class="refNO">ADJ-00050000</span>
    <div class="link"><a href="https://{SOURCE_HOST}/en/cases/conflict.html">View</a></div></li>
    </body></html>""".encode()
    outputs = list(
        spider.parse_listing(
            HtmlResponse(
                listing_request.url,
                body=listing_body,
                encoding="utf-8",
                request=listing_request,
            ),
            listing_request.cb_kwargs["unit_key"],
        )
    )
    document_request = next(item for item in outputs if isinstance(item, Request))
    asset = next(
        item
        for item in spider.parse_asset(
            http_response(
                document_request.url,
                b"%PDF-1.4\ncontrolled collision fixture",
                media_type="application/pdf",
            ),
            **document_request.cb_kwargs,
        )
        if isinstance(item, DownloadedAsset)
    )
    service = LandingAssetService(MemoryObjectStore(), MemoryMetadataStore(), "workplace-relations")
    spider.asset_persisted(asset, service.persist(asset))

    spider.closed("finished")

    run = events[-1]
    assert run["identity_collisions"] == 1
    assert run["ingestion_records"] == 2
    assert run["successfully_available_records"] == 1
    assert run["failed_documents"] == 1
    assert run["complete"] is False


def test_early_close_logs_every_unfinished_asset_url(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    next(spider._accepted_record_outputs(sample_record()))

    spider.closed("shutdown")

    failure = next(event for event in events if event["event"] == "asset_failed")
    run = events[-1]
    assert failure["url"] == SOURCE_URL
    assert failure["reason"] == "asset_download_not_completed"
    assert run["failed_asset_urls"] == [SOURCE_URL]
    assert run["failed_documents"] == 1
    assert run["complete"] is False


def test_early_close_preserves_the_latest_request_attempt_count(example_env):
    events = []
    spider = ingestion_spider(example_env, events)
    request = next(spider._accepted_record_outputs(sample_record()))
    request.meta["retry_times"] = 2
    spider.note_asset_request_attempt(request)

    spider.closed("shutdown")

    failure = next(event for event in events if event["event"] == "asset_failed")
    assert failure["reason"] == "asset_download_not_completed"
    assert failure["attempt_count"] == 3


def test_validator_backed_304_reuses_verified_landing_bytes_without_download(
    monkeypatch, example_env
):
    from twisted.internet import defer, threads

    objects = MemoryObjectStore()
    metadata = MemoryMetadataStore()
    state_collection = MemoryStateCollection()
    service = LandingAssetService(
        objects,
        metadata,
        "workplace-relations",
        ValidatorStateStore(state_collection),
    )
    first_asset = replace(
        sample_asset(),
        attempt_count=1,
        etag='"fixture-etag"',
        last_modified="Wed, 03 Jul 2013 04:35:13 GMT",
    )
    first = service.persist(first_asset)
    assert first.object_created is True
    assert objects.put_calls == 1

    events = []
    spider = ingestion_spider(example_env, events)
    spider.asset_service = service
    request = next(spider._accepted_record_outputs(sample_record()))

    def immediate(function, *args, **kwargs):
        return defer.succeed(function(*args, **kwargs))

    monkeypatch.setattr(threads, "deferToThread", immediate)
    deferred = ConditionalAssetMiddleware().process_request(request, spider)
    assert deferred.called is True
    assert request.headers["If-None-Match"] == b'"fixture-etag"'
    assert request.headers["If-Modified-Since"] == b"Wed, 03 Jul 2013 04:35:13 GMT"

    response = Response(
        request.url,
        status=304,
        body=b"",
        headers={"ETag": b'"fixture-etag"'},
        request=request,
    )
    outputs = list(spider.parse_asset(response, **request.cb_kwargs))
    reuse = next(item for item in outputs if isinstance(item, CachedAssetReuse))
    assert not any(isinstance(item, DownloadedAsset) for item in outputs)
    assert spider.downloaded_files == 0
    assert spider.not_modified_files == 1

    result = service.reuse(reuse)
    spider.asset_persisted(reuse, result)

    assert result.object_created is False
    assert result.metadata_created is False
    assert objects.put_calls == 1
    assert spider.record_states[sample_record().record_key].complete is True
    assert events[-1]["response_not_modified"] is True
    summary = spider._stage_summary_fields(discovery_complete=True)
    assert summary["records_with_downloads"] == 0
    assert summary["records_reused_without_download"] == 1


def test_validator_state_is_not_a_cache_hit_when_the_source_has_no_validator(example_env):
    objects = MemoryObjectStore()
    metadata = MemoryMetadataStore()
    state_collection = MemoryStateCollection()
    service = LandingAssetService(
        objects,
        metadata,
        "workplace-relations",
        ValidatorStateStore(state_collection),
    )
    asset = sample_asset()
    service.persist(asset)

    assert service.find_reusable(asset.record.record_key, asset.source_url) is None
    assert validator_state_id(asset.record.record_key, asset.source_url) in (
        state_collection.documents
    )


def test_validator_state_read_failure_is_reported_as_preflight_storage_failure(
    monkeypatch, example_env
):
    from twisted.internet import defer, threads

    class FailingValidatorStore:
        def find(self, record_key, source_url):
            raise StorageError("controlled validator state failure")

    events = []
    spider = ingestion_spider(example_env, events)
    spider.asset_service = LandingAssetService(
        MemoryObjectStore(),
        MemoryMetadataStore(),
        "workplace-relations",
        FailingValidatorStore(),
    )
    request = next(spider._accepted_record_outputs(sample_record()))

    def immediate(function, *args, **kwargs):
        try:
            return defer.succeed(function(*args, **kwargs))
        except Exception:
            return defer.fail()

    monkeypatch.setattr(threads, "deferToThread", immediate)
    deferred = ConditionalAssetMiddleware().process_request(request, spider)
    failures = []
    deferred.addErrback(lambda failure: failures.append(failure))
    spider.asset_request_failed(SimpleNamespace(request=request, value=failures[0].value))

    state = spider.record_states[sample_record().record_key]
    assert state.failures[0].reason == "validator_state_read_failed"
    assert spider.storage_failures == 1
    assert spider.download_failures == 0
    assert events[-1]["failure_stage"] == "storage"


def test_validator_state_is_not_used_when_its_landing_object_is_missing():
    objects = MemoryObjectStore()
    metadata = MemoryMetadataStore()
    service = LandingAssetService(
        objects,
        metadata,
        "workplace-relations",
        ValidatorStateStore(MemoryStateCollection()),
    )
    asset = replace(sample_asset(), etag='"fixture-etag"')
    result = service.persist(asset)
    del objects.objects[result.version.stored_object.key]

    assert service.find_reusable(asset.record.record_key, asset.source_url) is None


def test_validator_miss_with_changed_bytes_appends_a_new_immutable_version(
    monkeypatch, example_env
):
    from twisted.internet import defer, threads

    objects = MemoryObjectStore()
    metadata = MemoryMetadataStore()
    service = LandingAssetService(
        objects,
        metadata,
        "workplace-relations",
        ValidatorStateStore(MemoryStateCollection()),
    )
    service.persist(replace(sample_asset(), etag='"old-etag"'))
    spider = ingestion_spider(example_env)
    spider.asset_service = service
    request = next(spider._accepted_record_outputs(sample_record()))

    monkeypatch.setattr(
        threads,
        "deferToThread",
        lambda function, *args, **kwargs: defer.succeed(function(*args, **kwargs)),
    )
    ConditionalAssetMiddleware().process_request(request, spider)
    changed_body = b"%PDF-1.4\nchanged fixture bytes"
    response = Response(
        request.url,
        body=changed_body,
        headers={"Content-Type": b"application/pdf", "ETag": b'"new-etag"'},
        request=request,
    )
    asset = next(
        item
        for item in spider.parse_asset(response, **request.cb_kwargs)
        if isinstance(item, DownloadedAsset)
    )

    result = service.persist(asset)

    assert request.headers["If-None-Match"] == b'"old-etag"'
    assert result.object_created is True
    assert result.metadata_created is True
    assert len(objects.objects) == len(metadata.documents) == 2
    assert objects.objects[result.version.stored_object.key] == changed_body


@pytest.mark.local_http
def test_controlled_http_rerun_matrix_preserves_versions_and_transfer_evidence(example_env):
    import threading
    from datetime import timedelta
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    from scrapy.crawler import CrawlerProcess

    validated_body = b"%PDF-1.4\nvalidator-backed response"
    html_first = (
        b"<html><body><div class='content'>Stable legal text.</div>"
        b"<!-- Elapsed time: 001 --></body></html>"
    )
    html_changed = html_first.replace(b"001", b"002")
    binary_first = b"%PDF-1.4\nsame-length-version-A"
    binary_changed = b"%PDF-1.4\nsame-length-version-B"
    assert len(html_first) == len(html_changed)
    assert len(binary_first) == len(binary_changed)

    class Handler(BaseHTTPRequestHandler):
        asset_requests = {
            "/validated.pdf": 0,
            "/unvalidated.html": 0,
            "/changing.pdf": 0,
        }
        body_bytes_sent = 0
        conditional_headers = {
            "/validated.pdf": [],
            "/unvalidated.html": [],
            "/changing.pdf": [],
        }

        def send_body(self, body, media_type, etag=None):
            self.send_response(200)
            self.send_header("Content-Type", media_type)
            self.send_header("Content-Length", str(len(body)))
            if etag is not None:
                self.send_header("ETag", etag)
            self.end_headers()
            self.wfile.write(body)
            type(self).body_bytes_sent += len(body)

        def do_GET(self):
            if self.path == "/robots.txt":
                robots = b"User-agent: *\nAllow: /\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(robots)))
                self.end_headers()
                self.wfile.write(robots)
                return
            if self.path not in type(self).asset_requests:
                self.send_error(404)
                return
            handler = type(self)
            handler.asset_requests[self.path] += 1
            validator = self.headers.get("If-None-Match")
            handler.conditional_headers[self.path].append(validator)
            if self.path == "/validated.pdf":
                if validator == '"validated-v1"':
                    self.send_response(304)
                    self.send_header("ETag", '"validated-v1"')
                    self.end_headers()
                    return
                self.send_body(validated_body, "application/pdf", '"validated-v1"')
                return
            if self.path == "/unvalidated.html":
                body = html_first if handler.asset_requests[self.path] < 3 else html_changed
                self.send_body(body, "text/html")
                return
            body = binary_first if handler.asset_requests[self.path] == 1 else binary_changed
            etag = '"binary-v1"' if handler.asset_requests[self.path] == 1 else '"binary-v2"'
            self.send_body(body, "application/pdf", etag)

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        endpoint = f"http://127.0.0.1:{server.server_port}"
        loaded = load_settings(EXAMPLE, example_env)
        settings = replace(
            loaded,
            source=replace(
                loaded.source,
                name="controlled-http",
                search_url=f"{endpoint}/search",
                body_ids=("15376",),
            ),
            scraping=replace(
                loaded.scraping,
                partition_size="day",
                download_delay_seconds=0.01,
                rate_limit_backoff_max_seconds=0.1,
            ),
        )
        validated_monthly = replace(
            sample_record(),
            source=settings.source.name,
            title="VALIDATED-0001",
            reference_number="VALIDATED-0001",
            description="Original listing metadata.",
            source_url=f"{endpoint}/validated.pdf",
            partition_date=date(2025, 7, 1),
            partition_size="month",
        )
        validated_daily = replace(
            validated_monthly,
            partition_date=date(2025, 7, 17),
            partition_size="day",
        )
        validated_metadata_change = replace(
            validated_daily,
            description="Corrected listing metadata.",
        )
        unvalidated_html = replace(
            sample_record(),
            source=settings.source.name,
            title="HTML-0001",
            reference_number="HTML-0001",
            source_url=f"{endpoint}/unvalidated.html",
        )
        changing_binary = replace(
            sample_record(),
            source=settings.source.name,
            title="BINARY-0001",
            reference_number="BINARY-0001",
            source_url=f"{endpoint}/changing.pdf",
        )
        probe_records = (
            validated_monthly,
            validated_daily,
            validated_metadata_change,
            unvalidated_html,
            unvalidated_html,
            unvalidated_html,
            changing_binary,
            changing_binary,
        )
        objects = MemoryObjectStore()
        metadata = MemoryMetadataStore()
        state_collection = MemoryStateCollection()
        service = LandingAssetService(
            objects,
            metadata,
            "workplace-relations",
            ValidatorStateStore(state_collection),
        )
        process = CrawlerProcess(ingestion_crawler_settings(settings), install_root_handler=False)
        crawler = process.create_crawler(SequentialRerunSpider)
        process.crawl(
            crawler,
            app_settings=settings,
            date_range=DateRange(
                validated_monthly.published_date,
                validated_monthly.published_date + timedelta(days=1),
            ),
            body_ids=[validated_monthly.body_id],
            asset_service=service,
            probe_records=probe_records,
        )
        process.start(stop_after_crawl=True)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    spider = crawler.spider
    assert spider is not None
    assert Handler.asset_requests == {
        "/validated.pdf": 3,
        "/unvalidated.html": 3,
        "/changing.pdf": 2,
    }
    assert Handler.conditional_headers["/validated.pdf"] == [
        None,
        '"validated-v1"',
        '"validated-v1"',
    ]
    assert Handler.conditional_headers["/unvalidated.html"] == [None, None, None]
    assert Handler.conditional_headers["/changing.pdf"] == [None, '"binary-v1"']
    assert Handler.body_bytes_sent == (
        len(validated_body)
        + len(html_first) * 2
        + len(html_changed)
        + len(binary_first)
        + len(binary_changed)
    )
    assert spider.downloaded_files == 6
    assert spider.not_modified_files == 2
    assert spider.stored_files == 8
    assert spider.created_objects == 5
    assert spider.reused_objects == 3
    assert spider.inserted_metadata_versions == 6
    assert spider.reused_metadata_versions == 2
    assert objects.put_calls == 6
    assert len(objects.objects) == 5
    assert len(metadata.documents) == 6
    assert set(objects.objects.values()) == {
        validated_body,
        html_first,
        html_changed,
        binary_first,
        binary_changed,
    }
    validated_documents = [
        document
        for document in metadata.documents.values()
        if document["record_key"] == validated_monthly.record_key
    ]
    assert len(validated_documents) == 2
    assert {document["description"] for document in validated_documents} == {
        "Original listing metadata.",
        "Corrected listing metadata.",
    }
    original_document = next(
        document
        for document in validated_documents
        if document["description"] == "Original listing metadata."
    )
    assert original_document["partition_size"] == "month"
    assert validated_monthly.metadata_hash == validated_daily.metadata_hash
    validated_state = state_collection.documents[
        validator_state_id(validated_monthly.record_key, validated_monthly.source_url)
    ]
    assert validated_state["landing_version_id"] == next(
        version_id
        for version_id, document in metadata.documents.items()
        if document["description"] == "Corrected listing metadata."
    )
    summary = spider._stage_summary_fields(discovery_complete=True)
    assert summary["successfully_available_records"] == 3
    assert summary["records_reused_without_download"] == 1
    assert summary["records_with_downloads"] == 2
    assert spider.exit_code == 0


def test_ingestion_settings_reuse_discovery_limits_and_serialize_persistence(example_env):
    settings = load_settings(EXAMPLE, example_env)
    values = ingestion_crawler_settings(settings)
    assert values["CONCURRENT_ITEMS"] == 1
    assert values["ITEM_PIPELINES"] == {"kedra.ingestion.LandingAssetPipeline": 300}
    assert values["AUTOTHROTTLE_ENABLED"] is True
    assert values["DOWNLOAD_MAXSIZE"] == settings.scraping.max_response_bytes


def test_pipeline_persists_off_reactor_and_reports_success(monkeypatch, example_env):
    from twisted.internet import defer, threads

    events = []
    objects = MemoryObjectStore()
    service = LandingAssetService(objects, MemoryMetadataStore(), "workplace-relations")
    spider = ingestion_spider(example_env, events)
    spider.asset_service = service
    next(spider._accepted_record_outputs(sample_record()))
    state = spider.record_states[sample_record().record_key]
    state.pending_requests.clear()
    asset = sample_asset()
    state.downloaded_assets = 1
    state.pending_persistence[asset.asset_id] = asset
    pipeline = LandingAssetPipeline()
    pipeline.open_spider(spider)
    called = []

    def immediate(function, *args, **kwargs):
        called.append(function)
        return defer.succeed(function(*args, **kwargs))

    monkeypatch.setattr(threads, "deferToThread", immediate)

    result = pipeline.process_item(asset, spider)

    assert called == [service.persist]
    assert result.called is True
    assert state.complete is True
    assert events[-1]["event"] == "asset_stored"
    assert events[-1]["file_hash"] == hashlib.sha256(asset.body).hexdigest()


def test_pipeline_storage_failure_is_terminal_and_correlated(monkeypatch, example_env):
    from twisted.internet import defer, threads

    events = []
    spider = ingestion_spider(example_env, events)
    next(spider._accepted_record_outputs(sample_record()))
    state = spider.record_states[sample_record().record_key]
    state.pending_requests.clear()
    asset = sample_asset()
    state.downloaded_assets = 1
    state.pending_persistence[asset.asset_id] = asset
    pipeline = LandingAssetPipeline()
    pipeline.service = SimpleNamespace(persist=lambda _asset: None)

    def failed_thread(_function, *_args, **_kwargs):
        return defer.fail(RuntimeError("sensitive storage detail"))

    monkeypatch.setattr(threads, "deferToThread", failed_thread)

    result = pipeline.process_item(asset, spider)

    assert result.called is True
    assert state.complete is False
    assert state.failures[0].reason == "asset_storage_failure"
    assert state.failures[0].url == SOURCE_URL
    assert events[-1]["failure_stage"] == "storage"
    assert "sensitive" not in str(events[-1])


@pytest.mark.parametrize("prefix", ["", "/absolute", "trailing/", "../escape"])
def test_landing_object_keys_reject_invalid_prefixes(prefix):
    with pytest.raises(ValueError):
        landing_object_key(prefix, sample_asset())
