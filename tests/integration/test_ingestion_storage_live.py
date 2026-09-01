import hashlib
from datetime import date

import pytest

from kedra.ingestion import DownloadedAsset, LandingAssetService
from kedra.models import RecordMetadata

pytestmark = pytest.mark.storage
BODY = b"<html><body><div class='content'>Fixed M4 storage check.</div></body></html>"


def test_fixed_ingestion_asset_reuses_exact_object_and_metadata(stores):
    settings, _, _, metadata, objects = stores
    record = RecordMetadata(
        source="fixture-m4-ingestion",
        body_id="15376",
        title="FIXTURE-M4-0001",
        reference_number="FIXTURE-M4-0001",
        description="Controlled local storage integration fixture.",
        published_date=date(2025, 7, 17),
        source_date_raw="17/07/2025",
        source_url="https://example.invalid/fixture-m4-0001.html",
        partition_date=date(2025, 7, 1),
        partition_size="month",
    )
    asset = DownloadedAsset(
        record=record,
        asset_id="primary",
        role="primary",
        source_url=record.source_url,
        final_url="https://example.invalid/final/fixture-m4-0001.html",
        document_format="html",
        media_type="text/html",
        body=BODY,
    )
    service = LandingAssetService(objects, metadata, settings.object_prefix)

    first = service.persist(asset)
    second = service.persist(asset)

    assert second.object_created is False
    assert second.metadata_created is False
    assert first.version.version_id == second.version.version_id
    assert objects.read(first.version.stored_object.key) == BODY
    assert first.version.stored_object.file_hash == hashlib.sha256(BODY).hexdigest()
    assert metadata.find(first.version.version_id) == first.version.to_document()
