import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError
from pymongo.errors import OperationFailure

from kedra.storage import (
    IntegrityError,
    MetadataStore,
    ObjectNotFound,
    ObjectStore,
    StorageError,
    mongo_client,
    s3_client,
)
from kedra.storage_admin import local_settings

pytestmark = pytest.mark.storage
EXAMPLE = Path(__file__).resolve().parents[2] / "config.example.toml"


def _sample_snapshot(stores):
    settings, db, client, _, objects = stores
    documents = list(
        db[settings.landing_collection]
        .find({"source": {"$regex": "^synthetic-"}})
        .sort("_id")
        .limit(101)
    )
    result = client.list_objects_v2(
        Bucket=settings.landing_bucket, Prefix=settings.object_prefix + "/_checks/", MaxKeys=100
    )
    assert len(documents) <= 100 and not result["IsTruncated"], (
        "Snapshot is limited to 100 synthetic samples"
    )
    entries = {}
    for entry in result.get("Contents", []):
        key = entry["Key"]
        head = client.head_object(Bucket=settings.landing_bucket, Key=key)
        entries[key] = {
            "hash": hashlib.sha256(objects.read(key)).hexdigest(),
            "size": head["ContentLength"],
            "etag": head["ETag"],
            "last_modified": head["LastModified"].isoformat(),
            "metadata": head["Metadata"],
        }
    return {"documents": documents, "objects": entries}


def test_exact_bytes_and_duplicate_versions(stores, sample):
    settings, db, _, metadata, objects = stores
    body, document = sample
    stored = objects.put_if_absent(document["object_key"], body)
    assert stored.file_hash == document["file_hash"]
    metadata.insert_if_absent(document)
    assert objects.put_if_absent(document["object_key"], body).created is False
    assert metadata.insert_if_absent(document) is False
    assert db[settings.landing_collection].count_documents({"_id": document["_id"]}) == 1
    with pytest.raises(IntegrityError):
        objects.put_if_absent(document["object_key"], b"different")
    with pytest.raises(IntegrityError):
        metadata.insert_if_absent({**document, "source": "different"})
    assert objects.read(document["object_key"], document["file_hash"]) == body
    assert metadata.find(document["_id"]) == document


def test_mongo_permissions_prevent_landing_update_and_delete(stores):
    settings, db, _, _, _ = stores
    collection = db[settings.landing_collection]
    # Nonmatching filters prove authorization rejection without risking deletion of a stored record.
    for operation in (
        lambda: collection.update_one({"_id": "never-created"}, {"$set": {"source": "denied"}}),
        lambda: collection.delete_one({"_id": "never-created"}),
    ):
        with pytest.raises(OperationFailure) as error:
            operation()
        assert error.value.code == 13
    state = db[settings.state_collection]
    state.replace_one({"_id": "synthetic-checkpoint"}, {"value": 1}, upsert=True)
    state.update_one({"_id": "synthetic-checkpoint"}, {"$set": {"value": 2}})
    assert state.find_one({"_id": "synthetic-checkpoint"})["value"] == 2


@pytest.mark.parametrize(
    "operation", ["unconditional", "delete", "tagging", "copy", "competing-condition", "multipart"]
)
def test_s3_gateway_rejects_mutating_or_unconditional_requests(stores, operation):
    settings, _, client, _, _ = stores
    key = f"{settings.object_prefix}/_checks/never-created-{operation}"
    args = {"Bucket": settings.landing_bucket, "Key": key}
    with pytest.raises(ClientError) as error:
        if operation == "unconditional":
            client.put_object(**args, Body=b"denied")
        elif operation == "delete":
            client.delete_object(**args)
        elif operation == "tagging":
            client.put_object_tagging(**args, Tagging={"TagSet": []})
        elif operation == "copy":
            client.copy_object(**args, CopySource={"Bucket": settings.landing_bucket, "Key": key})
        elif operation == "competing-condition":
            client.put_object(**args, Body=b"denied", IfNoneMatch="*", IfMatch="*")
        else:
            client.create_multipart_upload(**args)
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_transform_credentials_can_read_landing_and_write_only_separate_outputs(stores, sample):
    _, _, _, metadata, objects = stores
    body, document = sample
    objects.put_if_absent(document["object_key"], body)
    metadata.insert_if_absent(document)
    settings = local_settings(EXAMPLE, "transform")
    with mongo_client(settings) as mongo, closing(s3_client(settings)) as client:
        db = mongo[settings.mongo_database]
        landing = ObjectStore(client, settings.landing_bucket, settings.object_prefix)
        assert landing.read(document["object_key"], document["file_hash"]) == body
        assert MetadataStore(db[settings.landing_collection]).find(document["_id"]) == document
        with pytest.raises(StorageError):
            landing.put_if_absent(f"{settings.object_prefix}/_checks/transform-denied", b"denied")
        with pytest.raises(OperationFailure) as error:
            db[settings.landing_collection].insert_one({"_id": "transform-denied"})
        assert error.value.code == 13
        output = ObjectStore(client, settings.transformed_bucket, settings.object_prefix)
        output.put_if_absent(document["object_key"], body)
        assert output.read(document["object_key"], document["file_hash"]) == body
        MetadataStore(db[settings.transformed_collection]).insert_if_absent(document)
        assert settings.transformed_collection != settings.landing_collection
        assert settings.transformed_bucket != settings.landing_bucket


def test_missing_object_and_unavailable_storage_are_explicit(stores):
    settings, _, _, _, objects = stores
    with pytest.raises(ObjectNotFound):
        objects.read(f"{settings.object_prefix}/_checks/never-created")
    unavailable = replace(
        settings, s3_endpoint_url="http://127.0.0.1:1", connect_timeout_seconds=1, max_attempts=1
    )
    with closing(s3_client(unavailable)) as client:
        with pytest.raises(StorageError, match="read failed"):
            ObjectStore(client, settings.landing_bucket, settings.object_prefix).read(
                f"{settings.object_prefix}/_checks/never-created"
            )


def test_concurrent_creators_leave_one_immutable_object_and_metadata_version(stores):
    settings, _, _, metadata, objects = stores
    version = "synthetic-race-" + uuid4().hex
    key = f"{settings.object_prefix}/_checks/{version}"
    document = {
        "_id": version,
        "source": "synthetic-concurrency-check",
        "object_key": key,
        "file_hash": hashlib.sha256(b"race sample").hexdigest(),
    }
    with ThreadPoolExecutor(max_workers=2) as pool:
        created = list(
            pool.map(lambda _: objects.put_if_absent(key, b"race sample").created, range(2))
        )
        inserted = list(pool.map(lambda _: metadata.insert_if_absent(document), range(2)))
    assert sorted(created) == [False, True]
    assert sorted(inserted) == [False, True]


def test_application_credentials_cannot_reconfigure_storage_or_cross_write_zones(stores):
    settings, db, client, _, _ = stores
    with pytest.raises(ClientError) as error:
        # Invalid policy body avoids changing policy even if a permission regression occurs.
        client.put_bucket_policy(Bucket=settings.landing_bucket, Policy="{}")
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    with pytest.raises(ClientError) as error:
        client.put_object(
            Bucket=settings.transformed_bucket,
            Key=settings.object_prefix + "/_checks/ingest-denied",
            Body=b"denied",
            IfNoneMatch="*",
        )
    assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    with pytest.raises(OperationFailure) as error:
        db[settings.transformed_collection].insert_one({"_id": "ingest-denied"})
    assert error.value.code == 13


def test_invalid_s3_credentials_are_rejected(stores):
    settings = replace(stores[0], s3_access_key_id="unrecognized-access-key")
    with closing(s3_client(settings)) as client:
        with pytest.raises(ClientError) as error:
            client.list_objects_v2(Bucket=settings.landing_bucket, MaxKeys=1)
        assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_capture_immutable_samples_for_read_only_restart_verification(stores):
    snapshot = _sample_snapshot(stores)
    assert snapshot["documents"] and snapshot["objects"]
    Path(".local/storage-snapshot.json").write_text(
        json.dumps(snapshot, sort_keys=True), encoding="utf-8"
    )


@pytest.mark.persistence
def test_existing_sample_is_readable_without_recreating_any_data(stores, sample):
    _, _, _, metadata, objects = stores
    body, document = sample
    assert metadata.find(document["_id"]) == document
    assert objects.read(document["object_key"], document["file_hash"]) == body
    before = json.loads(Path(".local/storage-snapshot.json").read_text(encoding="utf-8"))
    assert _sample_snapshot(stores) == before
