"""Create-only storage adapters for immutable source objects and metadata."""

import base64
import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

from kedra.config import StorageSettings
from kedra.identity import canonical_url, stable_hash
from kedra.models import RecordMetadata

DOCUMENT_FORMATS = frozenset({"html", "pdf", "doc", "docx"})


class StorageError(RuntimeError):
    """A storage operation failed; messages do not contain connection secrets."""


class ObjectNotFound(StorageError):
    """The referenced object is missing; callers must not treat this as a cache hit."""


class IntegrityError(StorageError):
    """Stored bytes or metadata disagree with the immutable version requested."""


def mongo_client(settings: StorageSettings) -> MongoClient:
    return MongoClient(
        settings.mongo_uri,
        serverSelectionTimeoutMS=settings.connect_timeout_seconds * 1000,
        connectTimeoutMS=settings.connect_timeout_seconds * 1000,
        socketTimeoutMS=settings.read_timeout_seconds * 1000,
        retryWrites=False,
        tz_aware=True,
        tzinfo=UTC,
    )


def s3_client(settings: StorageSettings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=settings.connect_timeout_seconds,
            read_timeout=settings.read_timeout_seconds,
            retries={"mode": "standard", "total_max_attempts": settings.max_attempts},
        ),
    )


def mongo_date(value: date) -> datetime:
    """Represent a source calendar date as UTC midnight in BSON."""
    if type(value) is not date:
        raise ValueError("Mongo date values must be calendar dates")
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def published_date_filter(start_date: date, end_date: date) -> dict[str, dict[str, datetime]]:
    """Build an inclusive calendar-date filter using a half-open BSON range."""
    if type(start_date) is not date or type(end_date) is not date:
        raise ValueError("Date bounds must be calendar dates")
    if start_date > end_date:
        raise ValueError("start_date must not be after end_date")
    if end_date == date.max:
        raise ValueError("end_date is too large for an exclusive upper bound")
    return {
        "published_date": {
            "$gte": mongo_date(start_date),
            "$lt": mongo_date(end_date + timedelta(days=1)),
        }
    }


@dataclass(frozen=True)
class StoredObject:
    bucket: str
    key: str
    file_hash: str
    size_bytes: int
    created: bool


@dataclass(frozen=True)
class LandingVersion:
    """Canonical metadata for one source record, asset and exact byte version."""

    record: RecordMetadata
    asset_id: str
    document_format: str
    stored_object: StoredObject

    def __post_init__(self) -> None:
        if not isinstance(self.record, RecordMetadata):
            raise ValueError("record must be RecordMetadata")
        if not isinstance(self.asset_id, str) or not self.asset_id.strip():
            raise ValueError("asset_id must be a nonblank string")
        if self.document_format not in DOCUMENT_FORMATS:
            raise ValueError("document_format must be html, pdf, doc or docx")
        stored = self.stored_object
        if not isinstance(stored, StoredObject):
            raise ValueError("stored_object must be StoredObject")
        if not isinstance(stored.bucket, str) or not stored.bucket.strip():
            raise ValueError("Stored object bucket must be a nonblank string")
        if not isinstance(stored.key, str) or not stored.key.strip():
            raise ValueError("Stored object key must be a nonblank string")
        if (
            not isinstance(stored.file_hash, str)
            or len(stored.file_hash) != 64
            or any(character not in "0123456789abcdef" for character in stored.file_hash)
        ):
            raise ValueError("Stored object hash must be a lowercase SHA-256")
        if type(stored.size_bytes) is not int or stored.size_bytes < 0:
            raise ValueError("Stored object size must be a nonnegative integer")

    @property
    def version_id(self) -> str:
        return stable_hash(
            {
                "record_key": self.record.record_key,
                "asset_id": self.asset_id.strip(),
                "metadata_hash": self.record.metadata_hash,
                "file_hash": self.stored_object.file_hash,
                "document_format": self.document_format,
            }
        )

    def to_document(self) -> dict[str, Any]:
        """Serialize domain dates to BSON-compatible UTC datetimes."""
        record = self.record
        stored = self.stored_object
        return {
            "_id": self.version_id,
            "schema_version": 1,
            "record_key": record.record_key,
            "asset_id": self.asset_id.strip(),
            "source": record.source,
            "body_id": record.body_id,
            "title": record.title,
            "identifier": record.identifier,
            "reference_number": record.reference_number,
            "description": record.description,
            "published_date": mongo_date(record.published_date),
            "source_date_raw": record.source_date_raw,
            "date_semantics": record.date_semantics,
            "source_url": canonical_url(record.source_url),
            "partition_date": mongo_date(record.partition_date),
            "partition_size": record.partition_size,
            "metadata_hash": record.metadata_hash,
            "object_bucket": stored.bucket,
            "object_key": stored.key,
            "file_hash": stored.file_hash,
            "size_bytes": stored.size_bytes,
            "document_format": self.document_format,
        }


class ObjectStore:
    """Append objects or reuse identical bytes; never overwrite or delete a key."""

    def __init__(self, client, bucket: str, prefix: str):
        self._client = client
        self._bucket = bucket
        self._prefix = prefix + "/"

    def _check_key(self, key: str) -> None:
        if (
            not isinstance(key, str)
            or not key.startswith(self._prefix)
            or "\\" in key
            or any(part in ("", ".", "..") for part in key.split("/"))
        ):
            raise ValueError(
                "Object key must stay within the configured prefix without dot segments"
            )

    def read(self, key: str, expected_hash: str | None = None) -> bytes:
        self._check_key(key)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            with response["Body"] as body:
                data = body.read()
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404", "NotFound"):
                raise ObjectNotFound("Referenced storage object is missing") from None
            raise StorageError("Object read failed") from None
        except BotoCoreError:
            raise StorageError("Object read failed") from None
        if expected_hash is not None and hashlib.sha256(data).hexdigest() != expected_hash:
            raise IntegrityError("Stored bytes do not match the expected SHA-256")
        return data

    def verify(self, stored: StoredObject) -> None:
        """Prove that an immutable-object receipt still describes the stored bytes."""
        if not isinstance(stored, StoredObject):
            raise ValueError("stored must be StoredObject")
        if stored.bucket != self._bucket:
            raise IntegrityError("Stored object belongs to a different bucket")
        self._check_key(stored.key)
        if (
            not isinstance(stored.file_hash, str)
            or len(stored.file_hash) != 64
            or any(character not in "0123456789abcdef" for character in stored.file_hash)
        ):
            raise ValueError("Stored object hash must be a lowercase SHA-256")
        if type(stored.size_bytes) is not int or stored.size_bytes < 0:
            raise ValueError("Stored object size must be a nonnegative integer")
        data = self.read(stored.key, stored.file_hash)
        if len(data) != stored.size_bytes:
            raise IntegrityError("Stored bytes do not match the expected size")

    def put_if_absent(self, key: str, data: bytes) -> StoredObject:
        self._check_key(key)
        if not isinstance(data, bytes):
            raise ValueError("Object data must be bytes")
        digest = hashlib.sha256(data)
        created = True
        try:
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=data,
                IfNoneMatch="*",
                ChecksumSHA256=base64.b64encode(digest.digest()).decode("ascii"),
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ("PreconditionFailed", "412"):
                raise StorageError("Conditional object creation failed") from None
            created = False
        except BotoCoreError:
            raise StorageError("Conditional object creation failed") from None
        # Read back exact bytes even after a duplicate: an ETag is not a SHA-256 integrity check.
        self.read(key, digest.hexdigest())
        return StoredObject(self._bucket, key, digest.hexdigest(), len(data), created)


class LandingMetadataStore:
    """Append typed Landing versions only after their exact object has been verified."""

    def __init__(self, collection, object_store: ObjectStore):
        self._collection = collection
        self._object_store = object_store

    def find(self, version_id: str) -> dict[str, Any] | None:
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("version_id must be a nonblank string")
        try:
            return self._collection.find_one({"_id": version_id})
        except PyMongoError:
            raise StorageError("Metadata read failed") from None

    def find_published_between(self, start_date: date, end_date: date) -> list[dict[str, Any]]:
        query = published_date_filter(start_date, end_date)
        try:
            return list(self._collection.find(query).sort([("published_date", 1), ("_id", 1)]))
        except PyMongoError:
            raise StorageError("Metadata date query failed") from None

    def insert_if_absent(self, version: LandingVersion) -> bool:
        if not isinstance(version, LandingVersion):
            raise ValueError("Landing metadata requires a LandingVersion")
        self._object_store.verify(version.stored_object)
        document = version.to_document()
        try:
            self._collection.insert_one(document)
            return True
        except DuplicateKeyError:
            existing = self.find(version.version_id)
            # A valid daily and monthly run describe the same source version. Keep the
            # first immutable partition label rather than creating a processing-context copy.
            ignored = {"partition_date", "partition_size"}
            comparable_existing = (
                {key: value for key, value in existing.items() if key not in ignored}
                if existing is not None
                else None
            )
            comparable_document = {
                key: value for key, value in document.items() if key not in ignored
            }
            if comparable_existing != comparable_document:
                raise IntegrityError(
                    "Existing metadata version differs; it cannot be replaced"
                ) from None
            return False
        except PyMongoError:
            raise StorageError("Metadata insert failed") from None
