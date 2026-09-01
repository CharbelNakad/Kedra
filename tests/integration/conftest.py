import hashlib
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit

import pytest

from kedra.storage import MetadataStore, ObjectStore, mongo_client, s3_client
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
        yield (
            settings,
            db,
            s3,
            MetadataStore(db[settings.landing_collection]),
            ObjectStore(s3, settings.landing_bucket, settings.object_prefix),
        )


@pytest.fixture
def sample(stores):
    settings = stores[0]
    digest = hashlib.sha256(BODY).hexdigest()
    key = f"{settings.object_prefix}/_checks/{digest}.bin"
    return BODY, {
        "_id": "synthetic-storage-probe-v1",
        "source": "synthetic-storage-check",
        "published_date": "2024-02-29",
        "partition_date": "2024-02-01",
        "partition_size": "month",
        "object_key": key,
        "file_hash": digest,
        "size_bytes": len(BODY),
    }
