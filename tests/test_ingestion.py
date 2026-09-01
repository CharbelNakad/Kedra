import hashlib
import io
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
    DecisionsIngestionSpider,
    DownloadedAsset,
    LandingAssetPipeline,
    LandingAssetService,
    classify_document,
    ingestion_crawler_settings,
    inspect_html,
    landing_object_key,
)
from kedra.models import RecordMetadata
from kedra.storage import StoredObject

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

    def put_if_absent(self, key, body):
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


def test_complete_fixture_ingestion_reconciles_one_record_and_one_asset(example_env):
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
    assert run["event"] == "ingestion_run_summary"
    assert run["discovery_complete"] is True
    assert run["successfully_available_records"] == 1
    assert run["failed_documents"] == 0
    assert run["downloaded_files"] == run["stored_files"] == 1
    assert run["created_objects"] == run["inserted_metadata_versions"] == 1
    assert run["complete"] is True


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
