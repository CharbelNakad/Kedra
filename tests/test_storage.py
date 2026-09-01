import hashlib
from contextlib import closing
from dataclasses import replace
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from bson import BSON
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from kedra.config import load_settings
from kedra.models import RecordMetadata
from kedra.storage import (
    IntegrityError,
    LandingMetadataStore,
    LandingVersion,
    ObjectNotFound,
    ObjectStore,
    StorageError,
    StoredObject,
    mongo_date,
    published_date_filter,
    s3_client,
)
from kedra.storage_admin import local_settings, prepare

EXAMPLE = Path(__file__).resolve().parents[1] / "config.example.toml"
DATA = b"Synthetic storage fixture\x00\xff\n"
KEY = "workplace-relations/probe.bin"


def sample_record(partition_size="month"):
    published = date(2024, 2, 29)
    return RecordMetadata(
        source="synthetic-storage-check",
        body_id="2",
        title="SYNTHETIC-0001",
        reference_number="probe-0001",
        description=None,
        published_date=published,
        source_date_raw="29/02/2024",
        source_url="https://example.invalid/decision/1",
        partition_date=published.replace(day=1) if partition_size == "month" else published,
        partition_size=partition_size,
    )


def sample_version(record=None, **stored_changes):
    stored = StoredObject(
        bucket="kedra-landing",
        key=KEY,
        file_hash=hashlib.sha256(DATA).hexdigest(),
        size_bytes=len(DATA),
        created=True,
    )
    return LandingVersion(
        record or sample_record(),
        "primary",
        "html",
        replace(stored, **stored_changes),
    )


@pytest.fixture
def client(example_env):
    with closing(s3_client(load_settings(EXAMPLE, example_env).storage)) as client:
        yield client


def response(data=DATA):
    return {"Body": StreamingBody(BytesIO(data), len(data)), "ContentLength": len(data)}


def test_object_creation_requires_precondition_and_verifies_stored_bytes(client):
    with Stubber(client) as stub:
        stub.add_response(
            "put_object",
            {},
            {
                "Bucket": "kedra-landing",
                "Key": KEY,
                "Body": DATA,
                "IfNoneMatch": "*",
                "ChecksumSHA256": ANY,
            },
        )
        stub.add_response("get_object", response(), {"Bucket": "kedra-landing", "Key": KEY})
        stored = ObjectStore(client, "kedra-landing", "workplace-relations").put_if_absent(
            KEY, DATA
        )
        assert stored.created is True
        assert stored.file_hash == hashlib.sha256(DATA).hexdigest()
        assert stored.size_bytes == len(DATA)
        stub.assert_no_pending_responses()


@pytest.mark.parametrize("existing", [DATA, b"different bytes"])
def test_existing_object_is_reused_only_after_exact_hash_verification(client, existing):
    with Stubber(client) as stub:
        stub.add_client_error("put_object", "PreconditionFailed", http_status_code=412)
        stub.add_response("get_object", response(existing))
        store = ObjectStore(client, "kedra-landing", "workplace-relations")
        if existing == DATA:
            assert store.put_if_absent(KEY, DATA).created is False
        else:
            with pytest.raises(IntegrityError):
                store.put_if_absent(KEY, DATA)
        stub.assert_no_pending_responses()


def test_corrupt_readback_does_not_report_a_successful_upload(client):
    with Stubber(client) as stub:
        stub.add_response("put_object", {})
        stub.add_response("get_object", response(b"truncated"))
        with pytest.raises(IntegrityError):
            ObjectStore(client, "kedra-landing", "workplace-relations").put_if_absent(KEY, DATA)


@pytest.mark.parametrize(
    "code,error_type", [("NoSuchKey", ObjectNotFound), ("AccessDenied", StorageError)]
)
def test_failed_reads_are_explicit_and_do_not_echo_service_errors(client, code, error_type):
    with Stubber(client) as stub:
        stub.add_client_error("get_object", code, service_message="sensitive-value")
        with pytest.raises(error_type) as error:
            ObjectStore(client, "kedra-landing", "workplace-relations").read(KEY)
        assert "sensitive-value" not in str(error.value)


def test_failed_upload_is_not_treated_as_an_existing_version(client):
    with Stubber(client) as stub:
        stub.add_client_error("put_object", "ServiceUnavailable", http_status_code=503)
        with pytest.raises(StorageError, match="creation failed"):
            ObjectStore(client, "kedra-landing", "workplace-relations").put_if_absent(KEY, DATA)
        stub.assert_no_pending_responses()


@pytest.mark.parametrize(
    "key",
    [
        "other/file",
        "/workplace-relations/file",
        "workplace-relations/../file",
        "workplace-relations//file",
    ],
)
def test_object_keys_cannot_escape_the_configured_prefix(client, key):
    with pytest.raises(ValueError):
        ObjectStore(client, "kedra-landing", "workplace-relations").put_if_absent(key, DATA)


def test_landing_version_serializes_dates_to_bson_utc_midnight():
    version = sample_version()
    document = version.to_document()
    BSON.encode(document)
    assert document["published_date"] == datetime(2024, 2, 29, tzinfo=UTC)
    assert document["partition_date"] == datetime(2024, 2, 1, tzinfo=UTC)
    assert type(document["published_date"]) is datetime
    assert type(version.record.published_date) is date


def test_metadata_insert_verifies_the_object_and_uses_canonical_document():
    collection = Mock()
    objects = Mock(spec=ObjectStore)
    version = sample_version()
    assert LandingMetadataStore(collection, objects).insert_if_absent(version) is True
    objects.verify.assert_called_once_with(version.stored_object)
    collection.insert_one.assert_called_once_with(version.to_document())
    collection.update_one.assert_not_called()
    collection.replace_one.assert_not_called()


def test_duplicate_metadata_must_match_the_stored_source_version():
    collection = Mock()
    objects = Mock(spec=ObjectStore)
    version = sample_version()
    collection.insert_one.side_effect = DuplicateKeyError("duplicate")
    collection.find_one.return_value = version.to_document()
    assert LandingMetadataStore(collection, objects).insert_if_absent(version) is False

    collection.find_one.return_value = {**version.to_document(), "title": "different"}
    with pytest.raises(IntegrityError):
        LandingMetadataStore(collection, objects).insert_if_absent(version)
    collection.update_one.assert_not_called()
    collection.delete_one.assert_not_called()


def test_repartitioning_reuses_the_same_immutable_source_version():
    monthly = sample_version()
    daily = sample_version(sample_record("day"))
    assert monthly.version_id == daily.version_id
    collection = Mock()
    collection.insert_one.side_effect = DuplicateKeyError("duplicate")
    collection.find_one.return_value = monthly.to_document()
    objects = Mock(spec=ObjectStore)
    assert LandingMetadataStore(collection, objects).insert_if_absent(daily) is False


@pytest.mark.parametrize(
    "failure",
    [ObjectNotFound("missing"), IntegrityError("wrong hash"), IntegrityError("wrong size")],
)
def test_metadata_is_not_inserted_until_the_object_receipt_is_verified(failure):
    collection = Mock()
    objects = Mock(spec=ObjectStore)
    objects.verify.side_effect = failure
    with pytest.raises(type(failure)):
        LandingMetadataStore(collection, objects).insert_if_absent(sample_version())
    collection.insert_one.assert_not_called()


def test_mongo_failure_does_not_become_a_duplicate_or_disclose_connection_details():
    collection = Mock()
    objects = Mock(spec=ObjectStore)
    collection.insert_one.side_effect = ConnectionFailure("sensitive-value")
    with pytest.raises(StorageError, match="insert failed") as error:
        LandingMetadataStore(collection, objects).insert_if_absent(sample_version())
    assert "sensitive-value" not in str(error.value)
    collection.find_one.assert_not_called()


def test_published_date_filter_is_inclusive_and_uses_a_half_open_bson_range():
    assert published_date_filter(date(2024, 2, 29), date(2024, 3, 1)) == {
        "published_date": {
            "$gte": datetime(2024, 2, 29, tzinfo=UTC),
            "$lt": datetime(2024, 3, 2, tzinfo=UTC),
        }
    }
    assert mongo_date(date(2024, 2, 29)) == datetime(2024, 2, 29, tzinfo=UTC)


@pytest.mark.parametrize(
    "start,end",
    [
        (date(2024, 3, 1), date(2024, 2, 29)),
        (datetime(2024, 2, 29), date(2024, 2, 29)),
        (date(2024, 2, 29), date.max),
    ],
)
def test_invalid_mongo_date_ranges_are_rejected(start, end):
    with pytest.raises(ValueError):
        published_date_filter(start, end)


@pytest.mark.parametrize(
    "field", ["connect_timeout_seconds", "read_timeout_seconds", "max_attempts"]
)
@pytest.mark.parametrize("value", [0, -1, True, 1.5])
def test_storage_limits_must_be_positive_integers(example_env, field, value):
    settings = load_settings(EXAMPLE, example_env).storage
    with pytest.raises(ValueError, match=field):
        replace(settings, **{field: value})


def test_local_preparation_preserves_credentials_on_rerun_and_separates_roles(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    first = {p.name: p.read_bytes() for p in directory.iterdir()}
    prepare(EXAMPLE, 27017, directory)
    assert first == {p.name: p.read_bytes() for p in directory.iterdir()}
    roles = [local_settings(EXAMPLE, role, directory) for role in ("admin", "ingest", "transform")]
    assert len({role.mongo_uri for role in roles}) == 3
    assert len({role.s3_access_key_id for role in roles}) == 3


def test_local_preparation_reconstructs_missing_derived_files_without_rotating_secrets(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    before = {path.name: path.read_bytes() for path in directory.iterdir()}
    for name in ("mongo-root-password", "s3.json", "compose.env", "ingest.env", "transform.env"):
        (directory / name).unlink()
    prepare(EXAMPLE, 27017, directory)
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == before


def test_local_preparation_refuses_to_replace_a_conflicting_derived_file(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    credentials = (directory / "credentials.json").read_bytes()
    (directory / "ingest.env").write_text("conflicting", encoding="utf-8")
    with pytest.raises(ValueError, match="ingest.env differs"):
        prepare(EXAMPLE, 27017, directory)
    assert (directory / "credentials.json").read_bytes() == credentials
    assert (directory / "ingest.env").read_text(encoding="utf-8") == "conflicting"


def test_local_preparation_resumes_after_only_the_directory_was_created(tmp_path):
    directory = tmp_path / "local"
    directory.mkdir()
    prepare(EXAMPLE, 27017, directory)
    assert local_settings(EXAMPLE, "ingest", directory).landing_bucket == "kedra-landing"


def test_local_preparation_promotes_a_complete_pending_manifest(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    credentials = (directory / "credentials.json").read_bytes()
    (directory / "credentials.json").rename(directory / "credentials.json.pending")
    prepare(EXAMPLE, 27017, directory)
    assert (directory / "credentials.json").read_bytes() == credentials
    assert not (directory / "credentials.json.pending").exists()


def test_local_preparation_replaces_only_an_incomplete_derived_temporary_file(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    expected = (directory / "compose.env").read_bytes()
    (directory / "compose.env").unlink()
    (directory / "compose.env.pending").write_text("partial", encoding="utf-8")
    prepare(EXAMPLE, 27017, directory)
    assert (directory / "compose.env").read_bytes() == expected
    assert not (directory / "compose.env.pending").exists()


def test_local_preparation_does_not_rotate_credentials_to_apply_changed_config(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    before = (directory / "credentials.json").read_bytes()
    changed = tmp_path / "changed.toml"
    changed.write_text(
        EXAMPLE.read_text().replace('mongo_database = "kedra"', 'mongo_database = "other"')
    )
    with pytest.raises(ValueError, match="differs"):
        prepare(changed, 27017, directory)
    assert (directory / "credentials.json").read_bytes() == before


def test_local_runtime_limits_and_prefix_can_change_without_reprovisioning(tmp_path):
    directory = tmp_path / "local"
    prepare(EXAMPLE, 27017, directory)
    before = (directory / "credentials.json").read_bytes()
    changed = tmp_path / "changed.toml"
    changed.write_text(
        EXAMPLE.read_text()
        .replace("read_timeout_seconds = 30", "read_timeout_seconds = 15")
        .replace('object_prefix = "workplace-relations"', 'object_prefix = "another-prefix"')
    )
    settings = local_settings(changed, "ingest", directory)
    assert settings.read_timeout_seconds == 15
    assert settings.object_prefix == "another-prefix"
    assert (directory / "credentials.json").read_bytes() == before


@pytest.mark.parametrize("contents", ['{"sensitive-value":', "{}", "[]"])
def test_corrupt_local_profiles_fail_without_disclosing_content(tmp_path, contents):
    (tmp_path / "credentials.json").write_text(contents)
    with pytest.raises(ValueError, match="Local credential file is invalid") as error:
        local_settings(EXAMPLE, "ingest", tmp_path)
    assert "sensitive-value" not in str(error.value)
