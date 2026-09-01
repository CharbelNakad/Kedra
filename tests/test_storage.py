import hashlib
from contextlib import closing
from dataclasses import replace
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

import pytest
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from kedra.config import load_settings
from kedra.storage import (
    IntegrityError,
    MetadataStore,
    ObjectNotFound,
    ObjectStore,
    StorageError,
    s3_client,
)
from kedra.storage_admin import local_settings, prepare

EXAMPLE = Path(__file__).resolve().parents[1] / "config.example.toml"
DATA = b"Synthetic storage fixture\x00\xff\n"
KEY = "workplace-relations/probe.bin"


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


def test_metadata_insert_does_not_mutate_the_callers_document():
    collection = Mock()
    document = {"_id": "version-1", "title": "synthetic"}
    assert MetadataStore(collection).insert_if_absent(document) is True
    collection.insert_one.assert_called_once_with(document)
    assert collection.insert_one.call_args.args[0] is not document
    collection.update_one.assert_not_called()
    collection.replace_one.assert_not_called()


@pytest.mark.parametrize(
    "existing",
    [{"_id": "version-1", "title": "same"}, {"_id": "version-1", "title": "different"}, None],
)
def test_duplicate_metadata_must_match_the_complete_stored_document(existing):
    collection = Mock()
    collection.insert_one.side_effect = DuplicateKeyError("duplicate")
    collection.find_one.return_value = existing
    document = {"_id": "version-1", "title": "same"}
    if existing == document:
        assert MetadataStore(collection).insert_if_absent(document) is False
    else:
        with pytest.raises(IntegrityError):
            MetadataStore(collection).insert_if_absent(document)
    collection.update_one.assert_not_called()
    collection.delete_one.assert_not_called()


def test_mongo_failure_does_not_become_a_duplicate_or_disclose_connection_details():
    collection = Mock()
    collection.insert_one.side_effect = ConnectionFailure("sensitive-value")
    with pytest.raises(StorageError, match="insert failed") as error:
        MetadataStore(collection).insert_if_absent({"_id": "version-1"})
    assert "sensitive-value" not in str(error.value)
    collection.find_one.assert_not_called()


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
