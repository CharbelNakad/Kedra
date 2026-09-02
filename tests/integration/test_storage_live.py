import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
from pymongo.errors import DuplicateKeyError, OperationFailure, WriteError

from kedra.models import RecordMetadata
from kedra.storage import (
    IntegrityError,
    LandingVersion,
    ObjectNotFound,
    ObjectStore,
    StorageError,
    StoredObject,
    mongo_client,
    s3_client,
)
from kedra.storage_admin import local_settings
from kedra.transformation import LandingAsset, TransformedVersion

pytestmark = pytest.mark.storage
EXAMPLE = Path(__file__).resolve().parents[2] / "config.example.toml"


def _sample_snapshot(stores):
    settings, db, client, _, objects = stores
    documents = list(
        db[settings.landing_collection].find({"source": {"$regex": "^synthetic-"}}).sort("_id")
    )
    # Extended JSON makes BSON datetimes stable across snapshot write/read.
    documents = json.loads(dumps(documents, json_options=CANONICAL_JSON_OPTIONS, sort_keys=True))
    entries = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=settings.landing_bucket,
        Prefix=settings.object_prefix + "/_checks/",
        PaginationConfig={"PageSize": 2},
    ):
        for entry in page.get("Contents", []):
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


def _stored_version(objects, sample):
    body, record, key, asset_id, document_format = sample
    stored = objects.put_if_absent(key, body)
    return LandingVersion(record, asset_id, document_format, stored)


def test_exact_bytes_typed_metadata_and_duplicate_versions(stores, sample):
    settings, db, _, metadata, objects = stores
    body, record, key, asset_id, document_format = sample
    version = _stored_version(objects, sample)
    assert version.stored_object.file_hash == hashlib.sha256(body).hexdigest()
    metadata.insert_if_absent(version)

    duplicate = LandingVersion(
        record,
        asset_id,
        document_format,
        objects.put_if_absent(key, body),
    )
    assert duplicate.stored_object.created is False
    assert metadata.insert_if_absent(duplicate) is False
    assert db[settings.landing_collection].count_documents({"_id": version.version_id}) == 1
    with pytest.raises(IntegrityError):
        objects.put_if_absent(key, b"different")
    assert objects.read(key, version.stored_object.file_hash) == body
    assert metadata.find(version.version_id) == version.to_document()


def test_metadata_rejects_missing_or_mismatched_object_receipts(stores, sample):
    settings, _, _, metadata, objects = stores
    _, record, _, asset_id, document_format = sample
    missing = StoredObject(
        settings.landing_bucket,
        f"{settings.object_prefix}/_checks/never-created-metadata",
        "0" * 64,
        1,
        False,
    )
    with pytest.raises(ObjectNotFound):
        metadata.insert_if_absent(LandingVersion(record, asset_id, document_format, missing))

    stored = _stored_version(objects, sample).stored_object
    wrong_hash = replace(stored, file_hash="0" * 64)
    with pytest.raises(IntegrityError):
        metadata.insert_if_absent(LandingVersion(record, asset_id, document_format, wrong_hash))
    wrong_size = replace(stored, size_bytes=stored.size_bytes + 1)
    with pytest.raises(IntegrityError):
        metadata.insert_if_absent(LandingVersion(record, asset_id, document_format, wrong_size))


def test_database_schema_and_logical_identity_reject_invalid_direct_inserts(stores, sample):
    settings, db, _, metadata, objects = stores
    collection = db[settings.landing_collection]
    version = _stored_version(objects, sample)
    metadata.insert_if_absent(version)

    with pytest.raises(WriteError) as invalid:
        collection.insert_one({"_id": "f" * 64, "object_key": "missing"})
    assert invalid.value.code == 121

    duplicate = {**version.to_document(), "_id": "e" * 64}
    with pytest.raises(DuplicateKeyError):
        collection.insert_one(duplicate)
    assert collection.count_documents({"_id": {"$in": ["e" * 64, "f" * 64]}}) == 0


def test_published_date_queries_use_inclusive_calendar_boundaries(stores, sample):
    _, _, _, metadata, objects = stores
    version = _stored_version(objects, sample)
    metadata.insert_if_absent(version)
    same_day = metadata.find_published_between(date(2024, 2, 29), date(2024, 2, 29))
    assert version.version_id in {item["_id"] for item in same_day}
    assert all(item["published_date"].date() == date(2024, 2, 29) for item in same_day)
    assert version.version_id not in {
        item["_id"] for item in metadata.find_published_between(date(2024, 2, 1), date(2024, 2, 28))
    }
    assert version.version_id not in {
        item["_id"] for item in metadata.find_published_between(date(2024, 3, 1), date(2024, 3, 31))
    }


def test_mongo_permissions_prevent_landing_update_and_delete(stores):
    settings, db, _, _, _ = stores
    collection = db[settings.landing_collection]
    # Nonmatching filters prove authorization rejection without risking a stored record.
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
    body, record, key, _, document_format = sample
    version = LandingVersion(
        record,
        "transform-check",
        document_format,
        objects.put_if_absent(key, body),
        asset_role="primary",
        asset_source_url=record.source_url,
        asset_final_url=record.source_url,
        media_type="text/html",
    )
    metadata.insert_if_absent(version)
    settings = local_settings(EXAMPLE, "transform")
    with mongo_client(settings) as mongo, closing(s3_client(settings)) as client:
        db = mongo[settings.mongo_database]
        landing = ObjectStore(client, settings.landing_bucket, settings.object_prefix)
        assert landing.read(key, version.stored_object.file_hash) == body
        assert db[settings.landing_collection].find_one({"_id": version.version_id}) == (
            version.to_document()
        )
        with pytest.raises(StorageError):
            landing.put_if_absent(f"{settings.object_prefix}/_checks/transform-denied", b"denied")
        with pytest.raises(OperationFailure) as error:
            db[settings.landing_collection].insert_one({"_id": "transform-denied"})
        assert error.value.code == 13
        output = ObjectStore(client, settings.transformed_bucket, settings.object_prefix)
        output_receipt = output.put_if_absent(key, body)
        assert output.read(key, version.stored_object.file_hash) == body
        output_version = TransformedVersion(
            LandingAsset.from_document(version.to_document(), settings.landing_bucket),
            output_receipt,
            content_transformed=True,
        )
        output_document = output_version.to_document()
        try:
            db[settings.transformed_collection].insert_one(output_document)
        except DuplicateKeyError:
            assert (
                db[settings.transformed_collection].find_one({"_id": output_version.version_id})
                == output_document
            )
        with pytest.raises(WriteError) as invalid_output:
            db[settings.transformed_collection].insert_one({"_id": "0" * 64})
        assert invalid_output.value.code == 121
        with pytest.raises(DuplicateKeyError):
            db[settings.transformed_collection].insert_one({**output_document, "_id": "f" * 64})
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


def test_concurrent_creators_reuse_one_immutable_object_and_metadata_version(stores):
    settings, _, _, metadata, objects = stores
    body = b"repeatable concurrency sample"
    key = f"{settings.object_prefix}/_checks/concurrency-v2.bin"
    record = RecordMetadata(
        source="synthetic-concurrency-check",
        body_id="2",
        title="SYNTHETIC-CONCURRENCY",
        reference_number="concurrency-v2",
        description=None,
        published_date=date(2024, 3, 1),
        source_date_raw="01/03/2024",
        source_url="https://example.invalid/decision/concurrency-v2",
        partition_date=date(2024, 3, 1),
        partition_size="month",
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = list(pool.map(lambda _: objects.put_if_absent(key, body), range(2)))
        versions = [LandingVersion(record, "primary", "html", item) for item in receipts]
        inserted = list(pool.map(metadata.insert_if_absent, versions))
    assert sorted(item.created for item in receipts) in ([False, False], [False, True])
    assert sorted(inserted) in ([False, False], [False, True])
    assert len({item.version_id for item in versions}) == 1


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
    body, _, key, _, _ = sample
    receipt = StoredObject(
        stores[0].landing_bucket,
        key,
        hashlib.sha256(body).hexdigest(),
        len(body),
        False,
    )
    version = LandingVersion(sample[1], sample[3], sample[4], receipt)
    assert metadata.find(version.version_id) == version.to_document()
    assert objects.read(key, receipt.file_hash) == body
    before = json.loads(Path(".local/storage-snapshot.json").read_text(encoding="utf-8"))
    assert _sample_snapshot(stores) == before
