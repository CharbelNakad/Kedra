"""Transform immutable Landing assets into a separate output bucket and collection."""

import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from html import escape
from typing import Any, TextIO
from urllib.parse import urljoin
from uuid import uuid4

from bs4 import BeautifulSoup, Comment
from pymongo.errors import DuplicateKeyError, PyMongoError

from kedra.config import Settings
from kedra.dates import DateRange
from kedra.identity import canonical_url, identifier_filename, stable_hash
from kedra.models import RecordMetadata
from kedra.storage import (
    IntegrityError,
    LandingMetadataStore,
    LandingVersion,
    ObjectNotFound,
    ObjectStore,
    StorageError,
    StoredObject,
    mongo_client,
    mongo_date,
    s3_client,
)

TRANSFORM_VERSION = "wrc-content-v1"
DOCUMENT_FORMATS = frozenset({"html", "pdf", "doc", "docx"})
ASSET_ROLES = frozenset({"primary", "wrapper", "attachment", "continuation"})
SAFE_SEGMENT = re.compile(r"[a-z0-9][a-z0-9-]{0,79}")
SHA256 = re.compile(r"[0-9a-f]{64}")
EventSink = Callable[[dict[str, Any]], None]


class TransformationError(RuntimeError):
    """One Landing asset cannot safely produce a transformed output."""

    def __init__(self, reason: str, output_object: StoredObject | None = None):
        super().__init__(reason)
        self.reason = reason
        self.output_object = output_object


def _required_text(document: Mapping[str, Any], name: str) -> str:
    value = document.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")
    return value


def _optional_text(document: Mapping[str, Any], name: str) -> str | None:
    value = document.get(name)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{name} must be a string or null")
    return value


def _calendar_date(document: Mapping[str, Any], name: str) -> date:
    value = document.get(name)
    if not isinstance(value, datetime) or any(
        (value.hour, value.minute, value.second, value.microsecond)
    ):
        raise ValueError(f"{name} must be a BSON date at midnight")
    if value.tzinfo is not None and value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must represent UTC")
    return value.date()


@dataclass(frozen=True)
class LandingAsset:
    """Validated metadata and object receipt for one immutable Landing version."""

    landing_version_id: str
    record: RecordMetadata
    asset_id: str
    asset_role: str
    asset_source_url: str
    asset_final_url: str
    media_type: str
    document_format: str
    stored_object: StoredObject

    @classmethod
    def from_document(cls, document: Mapping[str, Any], landing_bucket: str) -> "LandingAsset":
        landing_version_id = _required_text(document, "_id")
        record_key = _required_text(document, "record_key")
        metadata_hash = _required_text(document, "metadata_hash")
        file_hash = _required_text(document, "file_hash")
        if not all(
            SHA256.fullmatch(value)
            for value in (landing_version_id, record_key, metadata_hash, file_hash)
        ):
            raise ValueError("Landing identities and hashes must be lowercase SHA-256 values")
        partition_size = _required_text(document, "partition_size")
        record = RecordMetadata(
            source=_required_text(document, "source"),
            body_id=_required_text(document, "body_id"),
            title=_required_text(document, "title"),
            reference_number=_optional_text(document, "reference_number"),
            description=_optional_text(document, "description"),
            published_date=_calendar_date(document, "published_date"),
            source_date_raw=_required_text(document, "source_date_raw"),
            source_url=_required_text(document, "source_url"),
            partition_date=_calendar_date(document, "partition_date"),
            partition_size=partition_size,
        )
        if (
            document.get("identifier") != record.identifier
            or document.get("date_semantics") != record.date_semantics
            or record_key != record.record_key
            or metadata_hash != record.metadata_hash
        ):
            raise ValueError("Landing record identity does not match its source metadata")
        asset_id = _required_text(document, "asset_id")
        asset_role = _required_text(document, "asset_role")
        document_format = _required_text(document, "document_format")
        if not SAFE_SEGMENT.fullmatch(asset_id):
            raise ValueError("asset_id is not a safe path segment")
        if asset_role not in ASSET_ROLES:
            raise ValueError("asset_role is invalid")
        if document_format not in DOCUMENT_FORMATS:
            raise ValueError("document_format is invalid")
        if asset_role == "wrapper" and document_format != "html":
            raise ValueError("Only HTML assets can be wrappers")
        object_bucket = _required_text(document, "object_bucket")
        if object_bucket != landing_bucket:
            raise ValueError("Landing object points outside the configured Landing bucket")
        size_bytes = document.get("size_bytes")
        if type(size_bytes) is not int or size_bytes < 1:
            raise ValueError("size_bytes must be a positive integer")
        stored = StoredObject(
            bucket=object_bucket,
            key=_required_text(document, "object_key"),
            file_hash=file_hash,
            size_bytes=size_bytes,
            created=False,
        )
        asset_source_url = canonical_url(_required_text(document, "asset_source_url"))
        asset_final_url = canonical_url(_required_text(document, "asset_final_url"))
        media_type = _required_text(document, "media_type").strip().lower()
        source_version = LandingVersion(
            record=record,
            asset_id=asset_id,
            document_format=document_format,
            stored_object=stored,
            asset_role=asset_role,
            asset_source_url=asset_source_url,
            asset_final_url=asset_final_url,
            media_type=media_type,
        )
        if source_version.version_id != landing_version_id:
            raise ValueError("Landing version ID does not match its canonical metadata")
        return cls(
            landing_version_id,
            record,
            asset_id,
            asset_role,
            asset_source_url,
            asset_final_url,
            media_type,
            document_format,
            stored,
        )


@dataclass(frozen=True)
class TransformedVersion:
    """One deterministic output linked to an immutable Landing version."""

    landing: LandingAsset
    stored_object: StoredObject
    content_transformed: bool
    transform_version: str = TRANSFORM_VERSION

    @property
    def version_id(self) -> str:
        return stable_hash(
            {
                "landing_version_id": self.landing.landing_version_id,
                "transform_version": self.transform_version,
                "file_hash": self.stored_object.file_hash,
            }
        )

    def to_document(self) -> dict[str, Any]:
        landing = self.landing
        record = landing.record
        output = self.stored_object
        return {
            "_id": self.version_id,
            "schema_version": 1,
            "landing_version_id": landing.landing_version_id,
            "transform_version": self.transform_version,
            "record_key": record.record_key,
            "asset_id": landing.asset_id,
            "asset_role": landing.asset_role,
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
            "asset_source_url": landing.asset_source_url,
            "asset_final_url": landing.asset_final_url,
            "media_type": landing.media_type,
            "landing_object_bucket": landing.stored_object.bucket,
            "landing_object_key": landing.stored_object.key,
            "landing_file_hash": landing.stored_object.file_hash,
            "object_bucket": output.bucket,
            "object_key": output.key,
            "file_hash": output.file_hash,
            "size_bytes": output.size_bytes,
            "document_format": landing.document_format,
            "content_transformed": self.content_transformed,
        }


@dataclass(frozen=True)
class TransformationResult:
    version: TransformedVersion
    object_created: bool
    metadata_created: bool


class TransformedMetadataStore:
    """Append transformed metadata only after verifying its output object."""

    def __init__(self, collection, object_store: ObjectStore):
        self._collection = collection
        self._object_store = object_store

    def find(self, version_id: str) -> dict[str, Any] | None:
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("version_id must be a nonblank string")
        try:
            return self._collection.find_one({"_id": version_id})
        except PyMongoError:
            raise StorageError("Transformed metadata read failed") from None

    def insert_if_absent(self, version: TransformedVersion) -> bool:
        if not isinstance(version, TransformedVersion):
            raise ValueError("Transformed metadata requires a TransformedVersion")
        self._object_store.verify(version.stored_object)
        document = version.to_document()
        try:
            self._collection.insert_one(document)
            return True
        except DuplicateKeyError:
            if self.find(version.version_id) != document:
                raise IntegrityError(
                    "Existing transformed metadata differs; it cannot be replaced"
                ) from None
            return False
        except PyMongoError:
            raise StorageError("Transformed metadata insert failed") from None


def transformed_object_key(prefix: str, landing: LandingAsset) -> str:
    filename = identifier_filename(landing.record.identifier, landing.document_format)
    return (
        f"{prefix}/transformed/{landing.record.record_key}/{landing.landing_version_id}/"
        f"{TRANSFORM_VERSION}/{landing.asset_id}/{filename}"
    )


def _page_heading(soup: BeautifulSoup, fallback: str) -> str:
    headings = soup.select("h1.page-title")
    if len(headings) > 1:
        raise TransformationError("ambiguous_page_title")
    if headings:
        text = headings[0].get_text(" ", strip=True)
        if text:
            return text
    return fallback


def _document_html(heading: str, body: str, body_class: str) -> bytes:
    output = (
        "<!doctype html>\n"
        '<html><head><meta charset="utf-8">'
        f"<title>{escape(heading)}</title></head><body>\n"
        '<main class="decision-document">\n'
        f'<h1 class="page-title">{escape(heading)}</h1>\n'
        f'<section class="{body_class}">{body}</section>\n'
        "</main>\n</body></html>\n"
    )
    return output.encode("utf-8")


def _wrapper_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str, str]]:
    selectors = (
        ".related-file a.download[href], .attachments a.download[href], "
        "a[data-document-asset][href], .content a.next[href], "
        '.content a[rel~="next"][href]'
    )
    links: dict[str, tuple[str, str, str]] = {}
    for node in soup.select(selectors):
        url = canonical_url(urljoin(base_url, node["href"]))
        declared = (node.get("data-document-asset") or "").lower()
        classes = {value.lower() for value in node.get("class", [])}
        relations = {value.lower() for value in node.get("rel", [])}
        role = (
            declared
            if declared in ("attachment", "continuation")
            else "continuation"
            if "next" in classes or "next" in relations
            else "attachment"
        )
        label = node.get_text(" ", strip=True) or "Decision document"
        links.setdefault(url, (role, label, url))
    return list(links.values())


def transform_html(data: bytes, landing: LandingAsset) -> bytes:
    """Keep the identifier heading and legal-content region in deterministic UTF-8 HTML."""
    soup = BeautifulSoup(data, "html.parser")
    heading = _page_heading(soup, landing.record.identifier)
    if landing.asset_role == "wrapper":
        links = _wrapper_links(soup, landing.asset_final_url)
        if not links:
            raise TransformationError("wrapper_missing_document_links")
        items = "".join(
            f'<li data-asset-role="{role}"><a href="{escape(url, quote=True)}">'
            f"{escape(label)}</a></li>"
            for role, label, url in links
        )
        return _document_html(heading, f"<ul>{items}</ul>", "document-assets")

    contents = soup.select(".content")
    if not contents:
        raise TransformationError("missing_decision_content")
    if len(contents) > 1:
        raise TransformationError("ambiguous_decision_content")
    content = contents[0]
    for node in content.select(
        "script, style, nav, header, footer, button, .return-to-search, .binder, [data-site-chrome]"
    ):
        node.decompose()
    for comment in content.find_all(string=lambda value: isinstance(value, Comment)):
        comment.extract()
    text = content.get_text(" ", strip=True)
    structured = content.find(("table", "img", "object", "iframe")) is not None
    if not text and not structured:
        raise TransformationError("empty_decision_content")
    body = content.decode_contents(formatter="minimal").strip()
    return _document_html(heading, body, "content")


class TransformationService:
    """Read one verified Landing object and append its deterministic transformed output."""

    def __init__(
        self,
        landing_objects: ObjectStore,
        transformed_objects: ObjectStore,
        transformed_metadata: TransformedMetadataStore,
        landing_bucket: str,
        object_prefix: str,
    ):
        self.landing_objects = landing_objects
        self.transformed_objects = transformed_objects
        self.transformed_metadata = transformed_metadata
        self.landing_bucket = landing_bucket
        self.object_prefix = object_prefix

    def transform(self, document: Mapping[str, Any]) -> TransformationResult:
        landing = LandingAsset.from_document(document, self.landing_bucket)
        try:
            source = self.landing_objects.read(
                landing.stored_object.key, landing.stored_object.file_hash
            )
        except ObjectNotFound:
            raise TransformationError("landing_object_missing") from None
        except IntegrityError:
            raise TransformationError("landing_object_integrity_failure") from None
        except StorageError:
            raise TransformationError("landing_object_read_failure") from None
        if len(source) != landing.stored_object.size_bytes:
            raise TransformationError("landing_object_size_mismatch")
        output = transform_html(source, landing) if landing.document_format == "html" else source
        key = transformed_object_key(self.object_prefix, landing)
        try:
            stored = self.transformed_objects.put_if_absent(key, output)
        except IntegrityError:
            raise TransformationError("transformed_object_conflict") from None
        except StorageError:
            raise TransformationError("transformed_object_write_failure") from None
        version = TransformedVersion(
            landing=landing,
            stored_object=stored,
            content_transformed=landing.document_format == "html",
        )
        try:
            metadata_created = self.transformed_metadata.insert_if_absent(version)
        except IntegrityError:
            raise TransformationError("transformed_metadata_conflict", stored) from None
        except StorageError:
            raise TransformationError("transformed_metadata_write_failure", stored) from None
        return TransformationResult(version, stored.created, metadata_created)


def transform_documents(
    documents: Sequence[Mapping[str, Any]],
    service: TransformationService,
    event_sink: EventSink,
    start_date: date,
    end_date: date,
    *,
    run_id: str | None = None,
) -> int:
    active_run_id = run_id or uuid4().hex

    def emit(event: dict[str, Any]) -> None:
        event_sink(
            {
                "run_id": active_run_id,
                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                **event,
            }
        )

    emit(
        {
            "event": "transformation_started",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "selected_assets": len(documents),
            "transform_version": TRANSFORM_VERSION,
        }
    )
    results: list[TransformationResult] = []
    failures: list[tuple[str | None, str, StoredObject | None]] = []
    for document in documents:
        landing_version_id = document.get("_id")
        safe_id = landing_version_id if isinstance(landing_version_id, str) else None
        try:
            result = service.transform(document)
        except TransformationError as error:
            failures.append((safe_id, error.reason, error.output_object))
            emit(
                {
                    "event": "asset_transform_failed",
                    "landing_version_id": safe_id,
                    "reason": error.reason,
                    "object_key": (
                        error.output_object.key if error.output_object is not None else None
                    ),
                    "object_created": (
                        error.output_object.created if error.output_object is not None else None
                    ),
                }
            )
            continue
        except ValueError:
            failures.append((safe_id, "invalid_landing_metadata", None))
            emit(
                {
                    "event": "asset_transform_failed",
                    "landing_version_id": safe_id,
                    "reason": "invalid_landing_metadata",
                }
            )
            continue
        results.append(result)
        version = result.version
        landing = version.landing
        emit(
            {
                "event": "asset_transformed",
                "source": landing.record.source,
                "body_id": landing.record.body_id,
                "published_date": landing.record.published_date.isoformat(),
                "partition_date": landing.record.partition_date.isoformat(),
                "record_key": landing.record.record_key,
                "landing_version_id": landing.landing_version_id,
                "transformed_version_id": version.version_id,
                "asset_id": landing.asset_id,
                "document_format": landing.document_format,
                "content_transformed": version.content_transformed,
                "landing_file_hash": landing.stored_object.file_hash,
                "file_hash": version.stored_object.file_hash,
                "object_bucket": version.stored_object.bucket,
                "object_key": version.stored_object.key,
                "object_created": result.object_created,
                "metadata_created": result.metadata_created,
            }
        )
    complete = not failures
    emit(
        {
            "event": "transformation_run_summary",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "transform_version": TRANSFORM_VERSION,
            "selected_assets": len(documents),
            "successfully_transformed_assets": len(results),
            "failed_assets": len(failures),
            "html_transformed": sum(
                result.version.landing.document_format == "html" for result in results
            ),
            "binary_copied": sum(
                result.version.landing.document_format != "html" for result in results
            ),
            "created_objects": sum(result.object_created for result in results)
            + sum(output.created for _, _, output in failures if output is not None),
            "reused_objects": sum(not result.object_created for result in results)
            + sum(not output.created for _, _, output in failures if output is not None),
            "inserted_metadata_versions": sum(result.metadata_created for result in results),
            "reused_metadata_versions": sum(not result.metadata_created for result in results),
            "failed_landing_version_ids": [version_id for version_id, _, _ in failures],
            "failure_reasons": [reason for _, reason, _ in failures],
            "complete": complete,
        }
    )
    return 0 if complete else 3


def run_transformation(
    settings: Settings,
    date_range: DateRange,
    stream: TextIO,
) -> int:
    """Run the standalone storage-to-storage transformation without source HTTP access."""
    run_id = uuid4().hex

    def write(event: dict[str, Any]) -> None:
        stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        stream.flush()

    end_date = date_range.end_exclusive - timedelta(days=1)
    try:
        with mongo_client(settings.storage) as mongo, closing(s3_client(settings.storage)) as s3:
            database = mongo[settings.storage.mongo_database]
            landing_objects = ObjectStore(
                s3,
                settings.storage.landing_bucket,
                settings.storage.object_prefix,
            )
            transformed_objects = ObjectStore(
                s3,
                settings.storage.transformed_bucket,
                settings.storage.object_prefix,
            )
            landing_metadata = LandingMetadataStore(
                database[settings.storage.landing_collection], landing_objects
            )
            transformed_metadata = TransformedMetadataStore(
                database[settings.storage.transformed_collection], transformed_objects
            )
            documents = landing_metadata.find_published_between(date_range.start, end_date)
            service = TransformationService(
                landing_objects,
                transformed_objects,
                transformed_metadata,
                settings.storage.landing_bucket,
                settings.storage.object_prefix,
            )
            return transform_documents(
                documents,
                service,
                write,
                date_range.start,
                end_date,
                run_id=run_id,
            )
    except StorageError:
        write(
            {
                "run_id": run_id,
                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "event": "transformation_run_summary",
                "start_date": date_range.start.isoformat(),
                "end_date": end_date.isoformat(),
                "transform_version": TRANSFORM_VERSION,
                "selected_assets": None,
                "successfully_transformed_assets": 0,
                "failed_assets": None,
                "html_transformed": 0,
                "binary_copied": 0,
                "created_objects": 0,
                "reused_objects": 0,
                "inserted_metadata_versions": 0,
                "reused_metadata_versions": 0,
                "failed_landing_version_ids": [],
                "failure_reasons": ["landing_metadata_query_or_storage_failure"],
                "complete": False,
                "reason": "landing_metadata_query_or_storage_failure",
            }
        )
        return 3
