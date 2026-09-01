import hashlib
from contextlib import closing
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from kedra.models import RecordMetadata
from kedra.storage import LandingMetadataStore, ObjectStore, mongo_client, s3_client
from kedra.storage_admin import local_settings

EXAMPLE = Path(__file__).resolve().parents[2] / "config.example.toml"
BODY = b"Synthetic persistent storage sample, not a downloaded decision.\x00\xff\n"


@pytest.fixture
def stores():
    settings = local_settings(EXAMPLE, "ingest")
    assert urlsplit(settings.s3_endpoint_url).hostname in ("localhost", "127.0.0.1")
    assert urlsplit(settings.mongo_uri).hostname in ("localhost", "127.0.0.1")
    with mongo_client(settings) as mongo, closing(s3_client(settings)) as s3:
        db = mongo[settings.mongo_database]
        objects = ObjectStore(s3, settings.landing_bucket, settings.object_prefix)
        yield (
            settings,
            db,
            s3,
            LandingMetadataStore(db[settings.landing_collection], objects),
            objects,
        )


@pytest.fixture
def sample(stores):
    settings = stores[0]
    digest = hashlib.sha256(BODY).hexdigest()
    key = f"{settings.object_prefix}/_checks/{digest}.bin"
    record = RecordMetadata(
        source="synthetic-storage-check",
        body_id="2",
        title="SYNTHETIC-0001",
        reference_number="probe-0001",
        description=None,
        published_date=date(2024, 2, 29),
        source_date_raw="29/02/2024",
        source_url="https://example.invalid/decision/1",
        partition_date=date(2024, 2, 1),
        partition_size="month",
    )
    return BODY, record, key, "primary", "html"
