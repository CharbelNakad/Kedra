import hashlib
import io
import json
from copy import deepcopy
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from bs4 import BeautifulSoup
from pymongo.errors import DuplicateKeyError

from kedra.dates import DateRange
from kedra.models import RecordMetadata
from kedra.storage import (
    IntegrityError,
    LandingVersion,
    ObjectNotFound,
    StorageError,
    StoredObject,
)
from kedra.transformation import (
    IngestionManifest,
    LandingAsset,
    ManifestError,
    TransformationError,
    TransformationService,
    TransformedMetadataStore,
    load_ingestion_manifest,
    run_transformation,
    select_manifest_documents,
    transform_documents,
    transform_html,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "tests" / "fixtures" / "documents"
LANDING_BUCKET = "kedra-landing"
TRANSFORMED_BUCKET = "kedra-transformed"
PREFIX = "workplace-relations"


class MemoryObjectStore:
    def __init__(self, bucket, objects=None):
        self.bucket = bucket
        self.objects = {} if objects is None else objects

    def read(self, key, expected_hash=None):
        if key not in self.objects:
            raise ObjectNotFound("missing")
        data = self.objects[key]
        if expected_hash is not None and hashlib.sha256(data).hexdigest() != expected_hash:
            raise IntegrityError("wrong hash")
        return data

    def verify(self, stored):
        if stored.bucket != self.bucket:
            raise IntegrityError("wrong bucket")
        data = self.read(stored.key, stored.file_hash)
        if len(data) != stored.size_bytes:
            raise IntegrityError("wrong size")

    def put_if_absent(self, key, data):
        digest = hashlib.sha256(data).hexdigest()
        created = key not in self.objects
        if not created and self.objects[key] != data:
            raise IntegrityError("immutable conflict")
        self.objects.setdefault(key, data)
        return StoredObject(self.bucket, key, digest, len(data), created)


class MemoryMetadataStore:
    def __init__(self):
        self.documents = {}

    def insert_if_absent(self, version):
        document = version.to_document()
        created = version.version_id not in self.documents
        if not created and self.documents[version.version_id] != document:
            raise IntegrityError("immutable conflict")
        self.documents.setdefault(version.version_id, document)
        return created


class FailOnceMetadataStore(MemoryMetadataStore):
    def __init__(self):
        super().__init__()
        self.failed = False

    def insert_if_absent(self, version):
        if not self.failed:
            self.failed = True
            raise StorageError("controlled interruption")
        return super().insert_if_absent(version)


def landing_fixture(
    body,
    document_format="html",
    *,
    title="ADJ-00054321",
    reference="ADJ-00054321",
    asset_id="primary",
    asset_role="primary",
    published=date(2025, 7, 17),
):
    record = RecordMetadata(
        source="workplace-relations",
        body_id="15376",
        title=title,
        reference_number=reference,
        description="Controlled transformation fixture.",
        published_date=published,
        source_date_raw=published.strftime("%d/%m/%Y"),
        source_url=f"https://www.workplacerelations.ie/en/cases/{reference}.html",
        partition_date=published,
        partition_size="day",
    )
    digest = hashlib.sha256(body).hexdigest()
    key = f"{PREFIX}/records/{record.record_key}/{asset_id}/{digest}.{document_format}"
    stored = StoredObject(LANDING_BUCKET, key, digest, len(body), False)
    version = LandingVersion(
        record=record,
        asset_id=asset_id,
        document_format=document_format,
        stored_object=stored,
        asset_role=asset_role,
        asset_source_url=record.source_url,
        asset_final_url=record.source_url,
        media_type="text/html" if document_format == "html" else "application/octet-stream",
    )
    return version.to_document(), body


def service_for(*fixtures):
    landing_values = {document["object_key"]: body for document, body in fixtures}
    landing = MemoryObjectStore(LANDING_BUCKET, landing_values)
    transformed = MemoryObjectStore(TRANSFORMED_BUCKET)
    metadata = MemoryMetadataStore()
    service = TransformationService(
        landing,
        transformed,
        metadata,
        LANDING_BUCKET,
        PREFIX,
    )
    return service, landing, transformed, metadata


def write_manifest(path, version_ids, *, complete=True):
    run_id = "complete-ingestion-run"
    events = [
        {
            "event": "asset_stored",
            "run_id": run_id,
            "source": "workplace-relations",
            "body_id": "15376",
            "landing_version_id": version_id,
        }
        for version_id in version_ids
    ]
    events.append(
        {
            "event": "ingestion_run_summary",
            "run_id": run_id,
            "source": "workplace-relations",
            "start_date": "2025-07-17",
            "end_date": "2025-07-17",
            "body_ids": ["15376"],
            "complete": complete,
            "discovery_complete": complete,
            "document_stage": "complete" if complete else "incomplete",
            "failed_documents": 0 if complete else 1,
            "incomplete_asset_operations": 0,
            "partitions_with_unknown_missing_count": 0,
            "stored_files": len(version_ids),
            "successfully_available_records": len(version_ids),
        }
    )
    path.write_text(
        "".join(json.dumps(event) + "\n" for event in events),
        encoding="utf-8",
    )


def test_complete_ingestion_manifest_selects_exact_versions_and_binds_scope(tmp_path):
    first = landing_fixture(b"%PDF-1.4\nfirst manifest fixture", "pdf")
    second = landing_fixture(
        b"%PDF-1.4\nsecond manifest fixture",
        "pdf",
        reference="ADJ-00054322",
        title="ADJ-00054322",
    )
    manifest_path = tmp_path / "ingestion.jsonl"
    write_manifest(manifest_path, [first[0]["_id"]])

    manifest = load_ingestion_manifest(
        manifest_path,
        date(2025, 7, 17),
        date(2025, 7, 17),
        "workplace-relations",
        ("15376",),
    )

    assert manifest.run_id == "complete-ingestion-run"
    assert manifest.landing_version_ids == (first[0]["_id"],)
    assert select_manifest_documents([first[0], second[0]], manifest) == [first[0]]


def test_complete_zero_result_manifest_is_valid_but_an_incomplete_run_is_not(tmp_path):
    empty_path = tmp_path / "empty.jsonl"
    write_manifest(empty_path, [])
    empty = load_ingestion_manifest(
        empty_path,
        date(2025, 7, 17),
        date(2025, 7, 17),
        "workplace-relations",
        ("15376",),
    )
    assert empty.landing_version_ids == ()

    incomplete_path = tmp_path / "incomplete.jsonl"
    write_manifest(incomplete_path, [], complete=False)
    with pytest.raises(ManifestError, match="ingestion_manifest_incomplete"):
        load_ingestion_manifest(
            incomplete_path,
            date(2025, 7, 17),
            date(2025, 7, 17),
            "workplace-relations",
            ("15376",),
        )


def test_standalone_transform_rejects_incomplete_manifest_before_storage_access(tmp_path):
    manifest_path = tmp_path / "incomplete.jsonl"
    write_manifest(manifest_path, [], complete=False)
    settings = SimpleNamespace(
        source=SimpleNamespace(name="workplace-relations", body_ids=("15376",))
    )
    stream = io.StringIO()

    exit_code = run_transformation(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        manifest_path,
        stream,
    )

    summary = json.loads(stream.getvalue())
    assert exit_code == 3
    assert summary["event"] == "transformation_run_summary"
    assert summary["reason"] == "ingestion_manifest_incomplete"
    assert summary["complete"] is False


def test_manifest_rejects_date_mismatch_and_missing_landing_version(tmp_path):
    version_id = "a" * 64
    manifest_path = tmp_path / "ingestion.jsonl"
    write_manifest(manifest_path, [version_id])
    with pytest.raises(ManifestError, match="ingestion_manifest_scope_mismatch"):
        load_ingestion_manifest(
            manifest_path,
            date(2025, 7, 16),
            date(2025, 7, 17),
            "workplace-relations",
            ("15376",),
        )
    manifest = IngestionManifest(
        "run",
        "workplace-relations",
        date(2025, 7, 17),
        date(2025, 7, 17),
        ("15376",),
        (version_id,),
        1,
    )
    with pytest.raises(ManifestError, match="ingestion_manifest_landing_version_missing"):
        select_manifest_documents([], manifest)


def test_html_transform_keeps_complete_legal_region_and_removes_site_chrome():
    document, body = landing_fixture((FIXTURES / "decision.html").read_bytes())
    landing = LandingAsset.from_document(document, LANDING_BUCKET)

    first = transform_html(body, landing)
    second = transform_html(body, landing)
    output = first.decode("utf-8")
    soup = BeautifulSoup(first, "html.parser")

    assert first == second
    assert output.startswith("<!doctype html>")
    assert soup.select_one("h1.page-title").get_text(strip=True) == "ADJ-00054321"
    assert "Opening legal text." in output
    assert "Closing legal text." in output
    assert [cell.get_text(strip=True) for cell in soup.select("table td")] == [
        "A. Worker",
        "Example Limited",
    ]
    for excluded in (
        "Website notice",
        "Website footer",
        "Return to Search",
        "Binder",
        "siteTracking",
        "insideDecisionTracking",
        "elapsed:",
    ):
        assert excluded not in output


def test_wrapper_transform_keeps_only_explicit_attachment_provenance():
    document, body = landing_fixture(
        (FIXTURES / "wrapper.html").read_bytes(),
        title="WRAPPER/0001",
        reference="WRAPPER-0001",
        asset_id="wrapper",
        asset_role="wrapper",
    )
    landing = LandingAsset.from_document(document, LANDING_BUCKET)

    output = transform_html(body, landing).decode("utf-8")

    assert "WRAPPER/0001" in output
    assert "Download signed decision" in output
    assert 'data-asset-role="attachment"' in output
    assert "https://www.workplacerelations.ie/documents/decision.pdf" in output
    assert "pdfPreview" not in output
    assert "Website header" not in output
    assert "Website footer" not in output


@pytest.mark.parametrize(
    "body,reason",
    [
        (b"<html><body><p>no decision region</p></body></html>", "missing_decision_content"),
        (b"<html><body><div class='content'></div></body></html>", "empty_decision_content"),
        (
            b"<html><body><div class='content'>one</div>"
            b"<div class='content'>two</div></body></html>",
            "ambiguous_decision_content",
        ),
    ],
)
def test_missing_empty_or_ambiguous_html_content_fails_explicitly(body, reason):
    document, _ = landing_fixture(body)
    landing = LandingAsset.from_document(document, LANDING_BUCKET)

    with pytest.raises(TransformationError, match=reason):
        transform_html(body, landing)


@pytest.mark.parametrize(
    "document_format,body",
    [
        ("pdf", b"%PDF-1.4\nexact binary fixture"),
        ("doc", bytes.fromhex("d0cf11e0a1b11ae1") + b"exact DOC fixture"),
        ("docx", b"PK\x03\x04exact DOCX fixture"),
    ],
)
def test_binary_outputs_are_exact_and_use_reversible_identifier_filenames(document_format, body):
    fixture = landing_fixture(
        body,
        document_format,
        title="TE257/2012",
        reference=f"binary-{document_format}",
    )
    service, landing, transformed, metadata = service_for(fixture)
    landing_before = deepcopy(landing.objects)

    first = service.transform(fixture[0])
    second = service.transform(fixture[0])

    output = first.version.stored_object
    assert transformed.objects[output.key] == body
    assert output.file_hash == fixture[0]["file_hash"]
    assert output.key.endswith(f"/TE257%2F2012.{document_format}")
    assert first.version.content_transformed is False
    assert first.object_created is first.metadata_created is True
    assert second.object_created is second.metadata_created is False
    assert landing.objects == landing_before
    assert metadata.documents[first.version.version_id]["landing_file_hash"] == output.file_hash


def test_html_service_writes_new_hash_and_separate_provenance_without_changing_landing():
    fixture = landing_fixture((FIXTURES / "decision.html").read_bytes())
    service, landing, transformed, metadata = service_for(fixture)
    landing_before = deepcopy(landing.objects)

    result = service.transform(fixture[0])
    document = metadata.documents[result.version.version_id]
    output = transformed.objects[result.version.stored_object.key]

    assert landing.objects == landing_before
    assert result.version.content_transformed is True
    assert document["landing_version_id"] == fixture[0]["_id"]
    assert document["landing_object_bucket"] == LANDING_BUCKET
    assert document["object_bucket"] == TRANSFORMED_BUCKET
    assert document["landing_file_hash"] == fixture[0]["file_hash"]
    assert document["file_hash"] == hashlib.sha256(output).hexdigest()
    assert document["file_hash"] != document["landing_file_hash"]


def test_metadata_interruption_reports_created_object_and_rerun_completes_it():
    fixture = landing_fixture(b"%PDF-1.4\ninterruption fixture", "pdf")
    landing = MemoryObjectStore(LANDING_BUCKET, {fixture[0]["object_key"]: fixture[1]})
    transformed = MemoryObjectStore(TRANSFORMED_BUCKET)
    metadata = FailOnceMetadataStore()
    service = TransformationService(
        landing,
        transformed,
        metadata,
        LANDING_BUCKET,
        PREFIX,
    )
    landing_before = deepcopy(landing.objects)
    events = []

    first_exit = transform_documents(
        [fixture[0]],
        service,
        events.append,
        date(2025, 7, 17),
        date(2025, 7, 17),
        run_id="interrupted-run",
    )
    recovered = service.transform(fixture[0])

    failure = events[-2]
    summary = events[-1]
    assert first_exit == 3
    assert failure["reason"] == "transformed_metadata_write_failure"
    assert failure["object_created"] is True
    assert failure["object_key"] in transformed.objects
    assert summary["created_objects"] == 1
    assert summary["inserted_metadata_versions"] == 0
    assert recovered.object_created is False
    assert recovered.metadata_created is True
    assert landing.objects == landing_before


def test_transformed_metadata_duplicate_must_match_and_never_updates():
    fixture = landing_fixture(b"%PDF-1.4\nmetadata fixture", "pdf")
    service, _, _, metadata = service_for(fixture)
    version = service.transform(fixture[0]).version
    collection = Mock()
    objects = Mock()
    collection.insert_one.side_effect = DuplicateKeyError("duplicate")
    collection.find_one.return_value = version.to_document()
    store = TransformedMetadataStore(collection, objects)

    assert store.insert_if_absent(version) is False

    collection.find_one.return_value = {**version.to_document(), "title": "different"}
    with pytest.raises(IntegrityError, match="cannot be replaced"):
        store.insert_if_absent(version)
    collection.update_one.assert_not_called()
    collection.delete_one.assert_not_called()


def test_run_summary_accounts_for_invalid_landing_metadata_without_hiding_success():
    good = landing_fixture(b"%PDF-1.4\nrun fixture", "pdf")
    invalid = {**good[0], "_id": "not-a-version-id"}
    service, _, _, _ = service_for(good)
    events = []

    exit_code = transform_documents(
        [good[0], invalid],
        service,
        events.append,
        date(2025, 7, 17),
        date(2025, 7, 17),
        run_id="controlled-run",
    )

    summary = events[-1]
    assert exit_code == 3
    assert summary["event"] == "transformation_run_summary"
    assert summary["selected_assets"] == 2
    assert summary["successfully_transformed_assets"] == 1
    assert summary["failed_assets"] == 1
    assert summary["failure_reasons"] == ["invalid_landing_metadata"]
    assert summary["complete"] is False
    assert all(event["run_id"] == "controlled-run" for event in events)


@pytest.mark.parametrize(
    "body,remove_object,reason",
    [
        (b"<html><body><div class='content'></div></body></html>", False, "empty_decision_content"),
        (b"%PDF-1.4\nmissing fixture", True, "landing_object_missing"),
    ],
)
def test_run_logs_malformed_content_and_missing_landing_objects(body, remove_object, reason):
    document_format = "pdf" if body.startswith(b"%PDF") else "html"
    fixture = landing_fixture(body, document_format)
    service, landing, _, _ = service_for(fixture)
    if remove_object:
        landing.objects.clear()
    events = []

    exit_code = transform_documents(
        [fixture[0]],
        service,
        events.append,
        date(2025, 7, 17),
        date(2025, 7, 17),
        run_id="failed-transform",
    )

    failure = events[-2]
    summary = events[-1]
    assert exit_code == 3
    assert failure["event"] == "asset_transform_failed"
    assert failure["landing_version_id"] == fixture[0]["_id"]
    assert failure["reason"] == reason
    assert summary["failed_assets"] == 1
    assert summary["failure_reasons"] == [reason]
    assert summary["complete"] is False
