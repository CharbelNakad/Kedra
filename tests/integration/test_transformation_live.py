import json
import os
import subprocess
import sys
from contextlib import closing
from copy import deepcopy
from datetime import date
from pathlib import Path, PurePosixPath

import pytest

from kedra.ingestion import DownloadedAsset, LandingAssetService
from kedra.models import RecordMetadata
from kedra.storage import ObjectStore, mongo_client, s3_client
from kedra.storage_admin import local_settings

pytestmark = pytest.mark.storage

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "config.example.toml"
SOURCE = "synthetic-transform-check"
INSIDE_START = date(2099, 1, 2)
INSIDE_END = date(2099, 1, 4)
OUTSIDE = date(2099, 1, 5)


def record(title, published):
    return RecordMetadata(
        source=SOURCE,
        body_id="2",
        title=title,
        reference_number=title.replace("/", "-"),
        description="Fixed Docker-backed transformation fixture.",
        published_date=published,
        source_date_raw=published.strftime("%d/%m/%Y"),
        source_url=f"https://example.invalid/{title.replace('/', '-')}.html",
        partition_date=published,
        partition_size="day",
    )


def fixed_assets():
    html_record = record("TRANSFORM/HTML-0001", INSIDE_START)
    wrapper_record = record("TRANSFORM-WRAPPER-0002", date(2099, 1, 3))
    doc_record = record("TRANSFORM-DOC-0003", INSIDE_END)
    outside_record = record("TRANSFORM-DOCX-OUTSIDE", OUTSIDE)
    return [
        DownloadedAsset(
            record=html_record,
            asset_id="primary",
            role="primary",
            source_url=html_record.source_url,
            final_url=html_record.source_url,
            document_format="html",
            media_type="text/html",
            body=b"""<!doctype html><html><body><header>site header</header>
            <h1 class="page-title">TRANSFORM/HTML-0001</h1><div class="content">
            <p>Opening integration text.</p><table><tr><td>Kept table cell</td></tr></table>
            <p>Closing integration text.</p></div><footer>site footer</footer></body></html>""",
        ),
        DownloadedAsset(
            record=wrapper_record,
            asset_id="wrapper",
            role="wrapper",
            source_url=wrapper_record.source_url,
            final_url=wrapper_record.source_url,
            document_format="html",
            media_type="text/html",
            body=b"""<html><body><h1 class="page-title">TRANSFORM-WRAPPER-0002</h1>
            <div class="content"></div><div class="related-file">
            <a class="download" href="/fixed-decision.pdf">Decision PDF</a>
            </div></body></html>""",
        ),
        DownloadedAsset(
            record=wrapper_record,
            asset_id="attachment-fixed",
            role="attachment",
            source_url="https://example.invalid/fixed-decision.pdf",
            final_url="https://example.invalid/fixed-decision.pdf",
            document_format="pdf",
            media_type="application/pdf",
            body=b"%PDF-1.4\nfixed transformed binary fixture",
        ),
        DownloadedAsset(
            record=doc_record,
            asset_id="primary",
            role="primary",
            source_url=doc_record.source_url,
            final_url=doc_record.source_url,
            document_format="doc",
            media_type="application/msword",
            body=bytes.fromhex("d0cf11e0a1b11ae1") + b"fixed transformed DOC fixture",
        ),
        DownloadedAsset(
            record=outside_record,
            asset_id="primary",
            role="primary",
            source_url=outside_record.source_url,
            final_url=outside_record.source_url,
            document_format="docx",
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            body=b"PK\x03\x04fixed outside-range DOCX fixture",
        ),
    ]


def run_cli(settings):
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
            "transform",
            "--config",
            str(EXAMPLE),
            "--start-date",
            INSIDE_START.isoformat(),
            "--end-date",
            INSIDE_END.isoformat(),
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
    summary = next(
        event for event in reversed(events) if event["event"] == "transformation_run_summary"
    )
    return result, summary


def transformed_snapshot(database, collection_name, landing_ids):
    documents = database[collection_name].find({"landing_version_id": {"$in": list(landing_ids)}})
    return {document["landing_version_id"]: document for document in documents}


def test_transform_cli_is_date_bounded_idempotent_and_leaves_landing_unchanged(stores):
    ingest_settings, database, _, landing_metadata, landing_objects = stores
    assets = fixed_assets()
    service = LandingAssetService(
        landing_objects,
        landing_metadata,
        ingest_settings.object_prefix,
    )
    versions = [service.persist(asset).version for asset in assets]
    inside_versions = versions[:-1]
    inside_ids = {version.version_id for version in inside_versions}
    outside_id = versions[-1].version_id
    landing_documents_before = {
        version.version_id: deepcopy(landing_metadata.find(version.version_id))
        for version in versions
    }
    landing_bytes_before = {
        version.stored_object.key: landing_objects.read(
            version.stored_object.key, version.stored_object.file_hash
        )
        for version in versions
    }
    transform_settings = local_settings(EXAMPLE, "transform")

    first, first_summary = run_cli(transform_settings)
    with mongo_client(transform_settings) as mongo, closing(s3_client(transform_settings)) as s3:
        transform_database = mongo[transform_settings.mongo_database]
        first_outputs = transformed_snapshot(
            transform_database,
            transform_settings.transformed_collection,
            inside_ids,
        )
        transformed_objects = ObjectStore(
            s3,
            transform_settings.transformed_bucket,
            transform_settings.object_prefix,
        )
        first_output_bytes = {
            landing_id: transformed_objects.read(document["object_key"], document["file_hash"])
            for landing_id, document in first_outputs.items()
        }
        second, second_summary = run_cli(transform_settings)
        second_outputs = transformed_snapshot(
            transform_database,
            transform_settings.transformed_collection,
            inside_ids,
        )
        second_output_bytes = {
            landing_id: transformed_objects.read(document["object_key"], document["file_hash"])
            for landing_id, document in second_outputs.items()
        }
        assert (
            transform_database[transform_settings.transformed_collection].count_documents(
                {"landing_version_id": outside_id}
            )
            == 0
        )

    landing_documents_after = {
        version.version_id: landing_metadata.find(version.version_id) for version in versions
    }
    landing_bytes_after = {
        version.stored_object.key: landing_objects.read(
            version.stored_object.key, version.stored_object.file_hash
        )
        for version in versions
    }

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert first_summary["selected_assets"] == len(inside_versions) == 4
    assert first_summary["successfully_transformed_assets"] == 4
    assert first_summary["html_transformed"] == 2
    assert first_summary["binary_copied"] == 2
    assert first_summary["failed_assets"] == 0
    assert len(first_outputs) == 4
    assert second_summary["selected_assets"] == 4
    assert second_summary["reused_objects"] == 4
    assert second_summary["reused_metadata_versions"] == 4
    assert second_summary["created_objects"] == 0
    assert second_summary["inserted_metadata_versions"] == 0
    assert first_outputs == second_outputs
    assert first_output_bytes == second_output_bytes
    assert landing_documents_after == landing_documents_before
    assert landing_bytes_after == landing_bytes_before

    by_format = {document["document_format"]: document for document in first_outputs.values()}
    source_by_id = {
        version.version_id: asset.body for version, asset in zip(versions, assets, strict=True)
    }
    assert (
        first_output_bytes[by_format["pdf"]["landing_version_id"]]
        == source_by_id[by_format["pdf"]["landing_version_id"]]
    )
    assert (
        first_output_bytes[by_format["doc"]["landing_version_id"]]
        == source_by_id[by_format["doc"]["landing_version_id"]]
    )
    html_outputs = [
        first_output_bytes[landing_id]
        for landing_id, document in first_outputs.items()
        if document["document_format"] == "html"
    ]
    assert any(b"Opening integration text." in output for output in html_outputs)
    assert any(b"fixed-decision.pdf" in output for output in html_outputs)
    assert all(
        b"site header" not in output and b"site footer" not in output for output in html_outputs
    )
    escaped_name = next(
        PurePosixPath(document["object_key"]).name
        for document in first_outputs.values()
        if document["identifier"] == "TRANSFORM/HTML-0001"
    )
    assert escaped_name == "TRANSFORM%2FHTML-0001.html"
