"""Small create-only storage adapters; no source requests or document parsing."""

import base64
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

from kedra.config import StorageSettings


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


@dataclass(frozen=True)
class StoredObject:
    bucket: str
    key: str
    file_hash: str
    size_bytes: int
    created: bool


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


class MetadataStore:
    """Insert immutable version documents under Mongo's unique _id; expose no mutation API."""

    def __init__(self, collection):
        self._collection = collection

    def find(self, version_id: str) -> dict[str, Any] | None:
        try:
            return self._collection.find_one({"_id": version_id})
        except PyMongoError:
            raise StorageError("Metadata read failed") from None

    def insert_if_absent(self, document: Mapping[str, Any]) -> bool:
        version_id = document.get("_id")
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("Metadata requires a nonblank string _id identifying its version")
        try:
            self._collection.insert_one(dict(document))
            return True
        except DuplicateKeyError:
            if self.find(version_id) != document:
                raise IntegrityError(
                    "Existing metadata version differs; it cannot be replaced"
                ) from None
            return False
        except PyMongoError:
            raise StorageError("Metadata insert failed") from None
