"""Download explicit decision assets and append exact bytes to the Landing Zone."""

import hashlib
import io
import re
import zipfile
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from pymongo.errors import PyMongoError
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse, Request, Response

from kedra.config import Settings
from kedra.dates import DateRange
from kedra.discovery import (
    DecisionsDiscoverySpider,
    EventSink,
    JsonLineWriter,
    crawler_settings,
)
from kedra.identity import canonical_url
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
    s3_client,
)

DocumentFormat = Literal["html", "pdf", "doc", "docx"]
AssetRole = Literal["primary", "wrapper", "attachment", "continuation"]
ASSET_ID_PATTERN = re.compile(r"[a-z0-9][a-z0-9-]{0,79}")
OLE_HEADER = bytes.fromhex("d0cf11e0a1b11ae1")


class AssetError(RuntimeError):
    """A response cannot safely become a required decision asset."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class AssetLink:
    asset_id: str
    role: Literal["attachment", "continuation"]
    url: str

    def __post_init__(self) -> None:
        if not ASSET_ID_PATTERN.fullmatch(self.asset_id):
            raise ValueError("Related asset_id is invalid")
        if self.role not in ("attachment", "continuation"):
            raise ValueError("Related asset role is invalid")
        canonical_url(self.url)


@dataclass
class PendingAssetRequest:
    url: str
    attempt_count: int = 1


@dataclass(frozen=True)
class CachedAsset:
    """A verified Landing object plus server validators kept outside Landing."""

    state_id: str
    source_url: str
    asset_id: str
    role: AssetRole
    final_url: str
    document_format: DocumentFormat
    media_type: str
    stored_object: StoredObject
    version_id: str
    etag: str | None = None
    last_modified: str | None = None
    related_assets: tuple[AssetLink, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.state_id, str) or not self.state_id:
            raise ValueError("state_id must be a nonblank string")
        canonical_url(self.source_url)
        canonical_url(self.final_url)
        if not ASSET_ID_PATTERN.fullmatch(self.asset_id):
            raise ValueError("Cached asset_id is invalid")
        if self.role not in ("primary", "wrapper", "attachment", "continuation"):
            raise ValueError("Cached asset role is invalid")
        if self.document_format not in ("html", "pdf", "doc", "docx"):
            raise ValueError("Cached document format is invalid")
        if not isinstance(self.media_type, str) or not self.media_type.strip():
            raise ValueError("Cached media_type must be nonblank")
        if not isinstance(self.stored_object, StoredObject):
            raise ValueError("Cached stored_object is invalid")
        if not isinstance(self.version_id, str) or not self.version_id:
            raise ValueError("Cached version_id must be nonblank")
        for name, value in (("etag", self.etag), ("last_modified", self.last_modified)):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"Cached {name} must be nonblank when present")
        if not isinstance(self.related_assets, tuple) or any(
            not isinstance(link, AssetLink) for link in self.related_assets
        ):
            raise ValueError("Cached related assets are invalid")

    @property
    def has_validator(self) -> bool:
        return self.etag is not None or self.last_modified is not None


@dataclass(frozen=True)
class CachedAssetReuse:
    """A zero-body 304 response that reuses a previously verified Landing object."""

    record: RecordMetadata
    cached: CachedAsset
    attempt_count: int

    def __post_init__(self) -> None:
        if not isinstance(self.record, RecordMetadata):
            raise ValueError("record must be RecordMetadata")
        if not isinstance(self.cached, CachedAsset):
            raise ValueError("cached must be CachedAsset")
        if type(self.attempt_count) is not int or self.attempt_count < 1:
            raise ValueError("attempt_count must be a positive integer")

    @property
    def asset_id(self) -> str:
        return self.cached.asset_id

    @property
    def role(self) -> AssetRole:
        return self.cached.role

    @property
    def source_url(self) -> str:
        return self.cached.source_url

    @property
    def final_url(self) -> str:
        return self.cached.final_url

    @property
    def document_format(self) -> DocumentFormat:
        return self.cached.document_format

    @property
    def media_type(self) -> str:
        return self.cached.media_type


@dataclass(frozen=True)
class DownloadedAsset:
    record: RecordMetadata
    asset_id: str
    role: AssetRole
    source_url: str
    final_url: str
    document_format: DocumentFormat
    media_type: str
    body: bytes = field(repr=False)
    attempt_count: int = 1
    etag: str | None = None
    last_modified: str | None = None
    related_assets: tuple[AssetLink, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.record, RecordMetadata):
            raise ValueError("record must be RecordMetadata")
        if not ASSET_ID_PATTERN.fullmatch(self.asset_id):
            raise ValueError("asset_id must be a short lowercase path-safe label")
        if self.role not in ("primary", "wrapper", "attachment", "continuation"):
            raise ValueError("Unsupported asset role")
        canonical_url(self.source_url)
        canonical_url(self.final_url)
        if self.document_format not in ("html", "pdf", "doc", "docx"):
            raise ValueError("Unsupported document format")
        if not isinstance(self.media_type, str) or not self.media_type.strip():
            raise ValueError("media_type must be a nonblank string")
        if not isinstance(self.body, bytes) or not self.body:
            raise ValueError("Downloaded asset bytes must not be empty")
        if type(self.attempt_count) is not int or self.attempt_count < 1:
            raise ValueError("attempt_count must be a positive integer")
        for name, value in (("etag", self.etag), ("last_modified", self.last_modified)):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"{name} must be nonblank when present")
        if not isinstance(self.related_assets, tuple) or any(
            not isinstance(link, AssetLink) for link in self.related_assets
        ):
            raise ValueError("related_assets must contain AssetLink values")


@dataclass(frozen=True)
class PersistedAsset:
    version: LandingVersion
    object_created: bool
    metadata_created: bool


@dataclass(frozen=True)
class AssetFailure:
    asset_id: str
    url: str
    reason: str
    http_status: int | None
    attempt_count: int


@dataclass
class IngestionRecordState:
    record: RecordMetadata
    pending_requests: dict[str, PendingAssetRequest] = field(default_factory=dict)
    seen_assets: set[str] = field(default_factory=set)
    pending_persistence: dict[str, DownloadedAsset | CachedAssetReuse] = field(default_factory=dict)
    downloaded_assets: int = 0
    stored_assets: int = 0
    stored_roles: set[str] = field(default_factory=set)
    wrapper_seen: bool = False
    failures: list[AssetFailure] = field(default_factory=list)

    @property
    def complete(self) -> bool:
        if (
            self.pending_requests
            or self.pending_persistence
            or self.failures
            or not self.stored_assets
        ):
            return False
        if self.wrapper_seen and not self.stored_roles.intersection({"attachment", "continuation"}):
            return False
        return True


def response_media_type(response: Response) -> str:
    value = response.headers.get("Content-Type")
    if value is None:
        return "application/octet-stream"
    media_type = value.decode("latin-1", errors="replace").split(";", 1)[0].strip().lower()
    if not media_type:
        raise AssetError("blank_content_type")
    return media_type


def response_validator(response: Response, header: str) -> str | None:
    value = response.headers.get(header)
    if value is None:
        return None
    decoded = value.decode("latin-1", errors="replace").strip()
    return decoded or None


def validator_state_id(record_key: str, source_url: str) -> str:
    if not isinstance(record_key, str) or not record_key:
        raise ValueError("record_key must be a nonblank string")
    return hashlib.sha256(
        f"source-validator\0{record_key}\0{canonical_url(source_url)}".encode()
    ).hexdigest()


def _is_docx(data: bytes) -> bool:
    if not data.startswith(b"PK\x03\x04"):
        return False
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            names = set(archive.namelist())
    except (OSError, zipfile.BadZipFile):
        return False
    return "[Content_Types].xml" in names and "word/document.xml" in names


def classify_document(response: Response) -> DocumentFormat:
    """Combine strong byte signatures with MIME and final-URL evidence."""
    data = response.body
    if not data:
        raise AssetError("empty_document_response")
    media_type = response_media_type(response)
    path = urlsplit(response.url).path.lower()
    head = data[:2048].lstrip().lower()
    if data.startswith(b"%PDF-"):
        return "pdf"
    if _is_docx(data):
        return "docx"
    if data.startswith(OLE_HEADER) and (
        media_type in ("application/msword", "application/vnd.ms-word") or path.endswith(".doc")
    ):
        return "doc"
    if data.startswith(b"{\\rtf") and (
        media_type in ("application/msword", "application/rtf") or path.endswith(".doc")
    ):
        return "doc"
    looks_html = head.startswith(b"<!doctype html") or head.startswith(b"<html") or b"<html" in head
    if looks_html:
        return "html"
    raise AssetError("unsupported_document_format")


def _preview_or_image(url: str) -> bool:
    parts = urlsplit(url)
    query = parse_qs(parts.query)
    if any(value.lower() == "pdfpreview" for value in query.get("type", [])):
        return True
    return parts.path.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))


def _asset_id(role: str, url: str) -> str:
    digest = hashlib.sha256(canonical_url(url).encode("utf-8")).hexdigest()
    return f"{role}-{digest}"


def inspect_html(response: Response) -> tuple[bool, tuple[AssetLink, ...]]:
    """Identify substantive content and only explicit decision asset links."""
    html = (
        response
        if isinstance(response, HtmlResponse)
        else HtmlResponse(
            response.url,
            body=response.body,
            headers=response.headers,
            request=response.request,
            encoding="utf-8",
        )
    )
    content = html.css(".content")
    text = " ".join(content.xpath("normalize-space(string(.))").getall()).strip()
    structured_content = bool(content.css("table, img[src], object, iframe"))
    has_substantive_content = bool(content and (text or structured_content))
    found: dict[str, AssetLink] = {}

    def add(nodes, default_role: Literal["attachment", "continuation"]) -> None:
        for node in nodes:
            href = node.attrib.get("href")
            if not href:
                continue
            url = canonical_url(html.urljoin(href))
            if _preview_or_image(url):
                continue
            declared = (node.attrib.get("data-document-asset") or "").lower()
            role = declared if declared in ("attachment", "continuation") else default_role
            found.setdefault(url, AssetLink(_asset_id(role, url), role, url))

    add(html.css(".related-file a.download, .attachments a.download"), "attachment")
    add(html.css("a[data-document-asset]"), "continuation")
    return has_substantive_content, tuple(found.values())


def landing_object_key(prefix: str, asset: DownloadedAsset) -> str:
    if (
        not isinstance(prefix, str)
        or not prefix.strip("/")
        or prefix != prefix.strip("/")
        or "\\" in prefix
        or any(part in ("", ".", "..") for part in prefix.split("/"))
    ):
        raise ValueError("Object prefix must be a nonblank relative path")
    digest = hashlib.sha256(asset.body).hexdigest()
    return (
        f"{prefix}/records/{asset.record.record_key}/{asset.asset_id}/"
        f"{digest}.{asset.document_format}"
    )


class ValidatorStateStore:
    """Keep mutable HTTP validators separate from immutable Landing metadata."""

    def __init__(self, collection):
        self._collection = collection

    def find(self, record_key: str, source_url: str) -> dict[str, Any] | None:
        state_id = validator_state_id(record_key, source_url)
        try:
            return self._collection.find_one({"_id": state_id})
        except PyMongoError:
            raise StorageError("Validator state read failed") from None

    def save(
        self,
        version: LandingVersion,
        source_url: str,
        etag: str | None,
        last_modified: str | None,
        related_assets: tuple[AssetLink, ...],
    ) -> None:
        state_id = validator_state_id(version.record.record_key, source_url)
        document = {
            "_id": state_id,
            "schema_version": 1,
            "record_key": version.record.record_key,
            "source_url": canonical_url(source_url),
            "landing_version_id": version.version_id,
            "related_assets": [asdict(link) for link in related_assets],
            "updated_at": datetime.now(UTC),
        }
        if etag is not None:
            document["etag"] = etag
        if last_modified is not None:
            document["last_modified"] = last_modified
        try:
            self._collection.replace_one({"_id": state_id}, document, upsert=True)
        except PyMongoError:
            raise StorageError("Validator state write failed") from None


class LandingAssetService:
    """Persist one response atomically enough for safe object-first recovery."""

    def __init__(
        self,
        object_store: ObjectStore,
        metadata_store: LandingMetadataStore,
        object_prefix: str,
        validator_store: ValidatorStateStore | None = None,
    ):
        self.object_store = object_store
        self.metadata_store = metadata_store
        self.object_prefix = object_prefix
        self.validator_store = validator_store

    def find_reusable(self, record_key: str, source_url: str) -> CachedAsset | None:
        if self.validator_store is None:
            return None
        state = self.validator_store.find(record_key, source_url)
        if state is None or not (state.get("etag") or state.get("last_modified")):
            return None
        try:
            version_id = state["landing_version_id"]
            document = self.metadata_store.find(version_id)
            if document is None:
                return None
            related = tuple(
                AssetLink(item["asset_id"], item["role"], item["url"])
                for item in state.get("related_assets", [])
            )
            stored = StoredObject(
                document["object_bucket"],
                document["object_key"],
                document["file_hash"],
                document["size_bytes"],
                False,
            )
            cached = CachedAsset(
                state_id=state["_id"],
                source_url=state["source_url"],
                asset_id=document["asset_id"],
                role=document["asset_role"],
                final_url=document["asset_final_url"],
                document_format=document["document_format"],
                media_type=document["media_type"],
                stored_object=stored,
                version_id=version_id,
                etag=state.get("etag"),
                last_modified=state.get("last_modified"),
                related_assets=related,
            )
            if (
                state["record_key"] != record_key
                or document["record_key"] != record_key
                or cached.source_url != canonical_url(source_url)
                or cached.state_id != validator_state_id(record_key, source_url)
            ):
                return None
            self.object_store.verify(stored)
            return cached
        except (KeyError, TypeError, ValueError, ObjectNotFound, IntegrityError):
            # Stale or malformed mutable state is a cache miss. A full response can repair it.
            return None

    def persist(self, asset: DownloadedAsset) -> PersistedAsset:
        key = landing_object_key(self.object_prefix, asset)
        stored = self.object_store.put_if_absent(key, asset.body)
        version = LandingVersion(
            record=asset.record,
            asset_id=asset.asset_id,
            document_format=asset.document_format,
            stored_object=stored,
            asset_role=asset.role,
            asset_source_url=asset.source_url,
            asset_final_url=asset.final_url,
            media_type=asset.media_type,
        )
        metadata_created = self.metadata_store.insert_if_absent(version)
        if self.validator_store is not None:
            self.validator_store.save(
                version,
                asset.source_url,
                asset.etag,
                asset.last_modified,
                asset.related_assets,
            )
        return PersistedAsset(version, stored.created, metadata_created)

    def reuse(self, item: CachedAssetReuse) -> PersistedAsset:
        cached = item.cached
        version = LandingVersion(
            record=item.record,
            asset_id=cached.asset_id,
            document_format=cached.document_format,
            stored_object=cached.stored_object,
            asset_role=cached.role,
            asset_source_url=cached.source_url,
            asset_final_url=cached.final_url,
            media_type=cached.media_type,
        )
        metadata_created = self.metadata_store.insert_if_absent(version)
        if self.validator_store is not None:
            self.validator_store.save(
                version,
                cached.source_url,
                cached.etag,
                cached.last_modified,
                cached.related_assets,
            )
        return PersistedAsset(version, False, metadata_created)


class DecisionsIngestionSpider(DecisionsDiscoverySpider):
    """Discover decisions, download explicit assets, and account for every record."""

    name = "wrc_decision_ingestion"

    def __init__(
        self,
        app_settings: Settings,
        date_range: DateRange,
        body_ids: Sequence[str] | None = None,
        event_sink: EventSink | None = None,
        asset_service: LandingAssetService | None = None,
        **kwargs,
    ):
        super().__init__(app_settings, date_range, body_ids, event_sink, **kwargs)
        self.asset_service = asset_service
        self.record_states: dict[str, IngestionRecordState] = {}
        self.downloaded_files = 0
        self.not_modified_files = 0
        self.download_failures = 0
        self.stored_files = 0
        self.storage_failures = 0
        self.created_objects = 0
        self.reused_objects = 0
        self.inserted_metadata_versions = 0
        self.reused_metadata_versions = 0

    def _accepted_record_outputs(self, record: RecordMetadata):
        existing = self.record_states.get(record.record_key)
        if existing is not None:
            if existing.record != record:
                self._asset_failed(
                    existing,
                    "primary",
                    record.source_url,
                    "run_identity_collision",
                    None,
                    1,
                    storage=False,
                )
            return
        state = IngestionRecordState(
            record,
            pending_requests={"primary": PendingAssetRequest(canonical_url(record.source_url))},
            seen_assets={"primary"},
        )
        self.record_states[record.record_key] = state
        yield self._asset_request(record.record_key, "primary", "primary", record.source_url)

    def _asset_request(
        self, record_key: str, asset_id: str, role: AssetRole, source_url: str
    ) -> Request:
        return Request(
            source_url,
            callback=self.parse_asset,
            errback=self.asset_request_failed,
            cb_kwargs={
                "record_key": record_key,
                "asset_id": asset_id,
                "asset_role": role,
                "asset_source_url": canonical_url(source_url),
            },
            meta={
                "handle_httpstatus_list": [304],
                "kedra_asset_request": True,
            },
            dont_filter=True,
        )

    def note_asset_request_attempt(self, request: Request) -> None:
        values = request.cb_kwargs
        state = self.record_states.get(values.get("record_key"))
        if state is None:
            return
        pending = state.pending_requests.get(values.get("asset_id"))
        if pending is not None:
            pending.attempt_count = max(
                pending.attempt_count,
                request.meta.get("retry_times", 0) + 1,
            )

    def _source_host(self) -> str:
        return urlsplit(self.app_settings.source.search_url).hostname.lower()

    def _asset_failed(
        self,
        state: IngestionRecordState,
        asset_id: str,
        url: str,
        reason: str,
        http_status: int | None,
        attempt_count: int,
        *,
        storage: bool,
    ) -> None:
        try:
            failure_url = canonical_url(url)
        except ValueError:
            failure_url = str(url)
        failure = AssetFailure(asset_id, failure_url, reason, http_status, attempt_count)
        state.failures.append(failure)
        if storage:
            self.storage_failures += 1
        else:
            self.download_failures += 1
        self._emit(
            {
                "event": "asset_failed",
                "source": state.record.source,
                "body_id": state.record.body_id,
                "partition_date": state.record.partition_date.isoformat(),
                "record_key": state.record.record_key,
                **asdict(failure),
                "failure_stage": "storage" if storage else "download",
            }
        )

    def parse_asset(
        self,
        response: Response,
        record_key: str,
        asset_id: str,
        asset_role: AssetRole,
        asset_source_url: str,
    ) -> Iterator[DownloadedAsset | CachedAssetReuse | Request]:
        state = self.record_states[record_key]
        attempt_count = response.request.meta.get("retry_times", 0) + 1
        pending = state.pending_requests.get(asset_id)
        if pending is not None:
            pending.attempt_count = max(pending.attempt_count, attempt_count)
        state.pending_requests.pop(asset_id, None)
        failure_url = asset_source_url
        try:
            final_host = urlsplit(response.url).hostname
            if final_host is None or final_host.lower() != self._source_host():
                failure_url = response.url
                raise AssetError("document_redirected_outside_source_host")
            if response.status == 304:
                cached = response.request.meta.get("kedra_cached_asset")
                if not isinstance(cached, CachedAsset) or not cached.has_validator:
                    raise AssetError("not_modified_without_valid_cache")
                if cached.source_url != canonical_url(asset_source_url):
                    raise AssetError("not_modified_cache_mismatch")
                cached = replace(
                    cached,
                    etag=response_validator(response, "ETag") or cached.etag,
                    last_modified=(
                        response_validator(response, "Last-Modified") or cached.last_modified
                    ),
                )
                yield from self._schedule_related(record_key, state, cached.related_assets)
                item = CachedAssetReuse(state.record, cached, attempt_count)
                state.wrapper_seen = state.wrapper_seen or cached.role == "wrapper"
                state.pending_persistence[item.asset_id] = item
                self.not_modified_files += 1
                yield item
                return
            if response.status != 200:
                raise AssetError("unexpected_document_status")
            document_format = classify_document(response)
            role = asset_role
            stored_asset_id = asset_id
            links: tuple[AssetLink, ...] = ()
            if document_format == "html":
                has_content, links = inspect_html(response)
                if not has_content and not links:
                    raise AssetError("empty_html_without_document_asset")
                if not has_content:
                    role = "wrapper"
                    if asset_id == "primary":
                        stored_asset_id = "wrapper"
            yield from self._schedule_related(record_key, state, links)
            asset = DownloadedAsset(
                record=state.record,
                asset_id=stored_asset_id,
                role=role,
                source_url=asset_source_url,
                final_url=response.url,
                document_format=document_format,
                media_type=response_media_type(response),
                body=response.body,
                attempt_count=attempt_count,
                etag=response_validator(response, "ETag"),
                last_modified=response_validator(response, "Last-Modified"),
                related_assets=links,
            )
        except (AssetError, ValueError) as error:
            reason = error.reason if isinstance(error, AssetError) else "invalid_asset_response"
            self._asset_failed(
                state,
                asset_id,
                failure_url,
                reason,
                response.status,
                attempt_count,
                storage=False,
            )
            return
        state.wrapper_seen = state.wrapper_seen or role == "wrapper"
        state.downloaded_assets += 1
        state.pending_persistence[asset.asset_id] = asset
        self.downloaded_files += 1
        yield asset

    def _schedule_related(
        self,
        record_key: str,
        state: IngestionRecordState,
        links: tuple[AssetLink, ...],
    ) -> Iterator[Request]:
        for link in links:
            if urlsplit(link.url).hostname.lower() != self._source_host():
                self._asset_failed(
                    state,
                    link.asset_id,
                    link.url,
                    "asset_link_outside_source_host",
                    None,
                    1,
                    storage=False,
                )
                continue
            if link.asset_id in state.seen_assets:
                continue
            state.seen_assets.add(link.asset_id)
            state.pending_requests[link.asset_id] = PendingAssetRequest(link.url)
            yield self._asset_request(record_key, link.asset_id, link.role, link.url)

    def asset_request_failed(self, failure) -> None:
        request = failure.request
        values = request.cb_kwargs
        state = self.record_states[values["record_key"]]
        state.pending_requests.pop(values["asset_id"], None)
        response = getattr(failure.value, "response", None)
        status = response.status if response is not None else None
        reason = "document_http_failure" if response is not None else "document_request_failure"
        self._asset_failed(
            state,
            values["asset_id"],
            request.url,
            reason,
            status,
            request.meta.get("retry_times", 0) + 1,
            storage=False,
        )

    def asset_persisted(
        self, asset: DownloadedAsset | CachedAssetReuse, result: PersistedAsset
    ) -> None:
        state = self.record_states[asset.record.record_key]
        state.pending_persistence.pop(asset.asset_id, None)
        state.stored_assets += 1
        state.stored_roles.add(asset.role)
        self.stored_files += 1
        self.created_objects += result.object_created
        self.reused_objects += not result.object_created
        self.inserted_metadata_versions += result.metadata_created
        self.reused_metadata_versions += not result.metadata_created
        stored = result.version.stored_object
        self._emit(
            {
                "event": "asset_stored",
                "source": asset.record.source,
                "body_id": asset.record.body_id,
                "partition_date": asset.record.partition_date.isoformat(),
                "record_key": asset.record.record_key,
                "asset_id": asset.asset_id,
                "asset_role": asset.role,
                "source_url": asset.source_url,
                "final_url": asset.final_url,
                "document_format": asset.document_format,
                "media_type": asset.media_type,
                "file_hash": stored.file_hash,
                "size_bytes": stored.size_bytes,
                "object_bucket": stored.bucket,
                "object_key": stored.key,
                "object_created": result.object_created,
                "metadata_created": result.metadata_created,
                "response_not_modified": isinstance(asset, CachedAssetReuse),
            }
        )

    def asset_persist_failed(self, asset: DownloadedAsset | CachedAssetReuse) -> None:
        state = self.record_states[asset.record.record_key]
        state.pending_persistence.pop(asset.asset_id, None)
        self._asset_failed(
            state,
            asset.asset_id,
            asset.source_url,
            "asset_storage_failure",
            None,
            asset.attempt_count,
            storage=True,
        )

    def _stage_summary_fields(self, discovery_complete: bool) -> dict[str, Any]:
        states = list(self.record_states.values())
        successful = sum(state.complete for state in states)
        malformed_records = sum(summary.malformed_cards for summary in self.summaries.values())
        document_candidates = len(states) + malformed_records
        failures = [failure for state in states for failure in state.failures]
        pending = sum(
            len(state.pending_requests) + len(state.pending_persistence) for state in states
        )
        return {
            "document_stage": (
                "complete"
                if discovery_complete and successful == len(states) and not pending
                else "incomplete"
            ),
            "ingestion_records": document_candidates,
            "successfully_available_records": successful,
            "failed_documents": document_candidates - successful,
            "downloaded_files": self.downloaded_files,
            "not_modified_files": self.not_modified_files,
            "download_failures": self.download_failures,
            "stored_files": self.stored_files,
            "storage_failures": self.storage_failures,
            "created_objects": self.created_objects,
            "reused_objects": self.reused_objects,
            "inserted_metadata_versions": self.inserted_metadata_versions,
            "reused_metadata_versions": self.reused_metadata_versions,
            "incomplete_asset_operations": pending,
            "failed_asset_urls": [failure.url for failure in failures],
        }

    def _run_is_complete(self, discovery_complete: bool) -> bool:
        return discovery_complete and all(state.complete for state in self.record_states.values())

    def _run_summary_event(self) -> str:
        return "ingestion_run_summary"

    def closed(self, reason: str) -> None:
        for state in self.record_states.values():
            for asset_id, pending in tuple(state.pending_requests.items()):
                self._asset_failed(
                    state,
                    asset_id,
                    pending.url,
                    "asset_download_not_completed",
                    None,
                    pending.attempt_count,
                    storage=False,
                )
            state.pending_requests.clear()
            for asset in tuple(state.pending_persistence.values()):
                self._asset_failed(
                    state,
                    asset.asset_id,
                    asset.source_url,
                    "asset_storage_not_completed",
                    None,
                    asset.attempt_count,
                    storage=True,
                )
            state.pending_persistence.clear()
        super().closed(reason)


class ConditionalAssetMiddleware:
    """Attach trusted validators without performing blocking storage reads on the reactor."""

    def process_request(self, request: Request, spider):
        if not isinstance(spider, DecisionsIngestionSpider) or not request.meta.get(
            "kedra_asset_request"
        ):
            return None
        spider.note_asset_request_attempt(request)
        if request.meta.get("kedra_cache_checked"):
            return None
        request.meta["kedra_cache_checked"] = True
        service = spider.asset_service
        if service is None or service.validator_store is None:
            return None

        from twisted.internet import threads

        values = request.cb_kwargs
        deferred = threads.deferToThread(
            service.find_reusable,
            values["record_key"],
            values["asset_source_url"],
        )

        def apply_validator(cached: CachedAsset | None):
            if cached is None:
                return None
            request.meta["kedra_cached_asset"] = cached
            if cached.etag is not None:
                request.headers["If-None-Match"] = cached.etag
            if cached.last_modified is not None:
                request.headers["If-Modified-Since"] = cached.last_modified
            return None

        deferred.addCallback(apply_validator)
        return deferred


class LandingAssetPipeline:
    """Serialize blocking S3/Mongo operations outside Scrapy's reactor thread."""

    def __init__(self):
        self.service: LandingAssetService | None = None
        self._mongo = None
        self._s3 = None
        self._owns_service = False

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider) -> None:
        if not isinstance(spider, DecisionsIngestionSpider):
            return
        if spider.asset_service is not None:
            self.service = spider.asset_service
            return
        settings = spider.app_settings.storage
        self._mongo = mongo_client(settings)
        self._s3 = s3_client(settings)
        objects = ObjectStore(self._s3, settings.landing_bucket, settings.object_prefix)
        database = self._mongo[settings.mongo_database]
        metadata = LandingMetadataStore(database[settings.landing_collection], objects)
        validators = ValidatorStateStore(database[settings.state_collection])
        self.service = LandingAssetService(
            objects,
            metadata,
            settings.object_prefix,
            validators,
        )
        spider.asset_service = self.service
        self._owns_service = True

    def process_item(self, item, spider):
        if not isinstance(item, (DownloadedAsset, CachedAssetReuse)) or not isinstance(
            spider, DecisionsIngestionSpider
        ):
            return item
        if self.service is None:
            spider.asset_persist_failed(item)
            return item
        from twisted.internet import threads

        operation = (
            self.service.reuse if isinstance(item, CachedAssetReuse) else self.service.persist
        )
        deferred = threads.deferToThread(operation, item)

        def succeeded(result):
            spider.asset_persisted(item, result)
            return item

        def failed(_failure):
            spider.asset_persist_failed(item)
            return item

        deferred.addCallbacks(succeeded, failed)
        return deferred

    def close_spider(self, spider) -> None:
        if not self._owns_service:
            return
        if self._mongo is not None:
            self._mongo.close()
        if self._s3 is not None:
            self._s3.close()


def ingestion_crawler_settings(settings: Settings) -> dict[str, Any]:
    values = crawler_settings(settings)
    values["DOWNLOADER_MIDDLEWARES"]["kedra.ingestion.ConditionalAssetMiddleware"] = 540
    values.update(
        {
            "ITEM_PIPELINES": {"kedra.ingestion.LandingAssetPipeline": 300},
            # One blocking persistence operation at a time is sufficient for this source
            # and bounds memory while the network downloader remains asynchronous.
            "CONCURRENT_ITEMS": 1,
        }
    )
    return values


def run_ingestion(
    settings: Settings,
    date_range: DateRange,
    body_ids: Sequence[str] | None,
    stream,
) -> int:
    writer = JsonLineWriter(stream)
    process = CrawlerProcess(ingestion_crawler_settings(settings), install_root_handler=False)
    crawler = process.create_crawler(DecisionsIngestionSpider)
    process.crawl(
        crawler,
        app_settings=settings,
        date_range=date_range,
        body_ids=body_ids,
        event_sink=writer,
    )
    process.start(stop_after_crawl=True)
    spider = crawler.spider
    if spider is None:
        writer(
            {
                "run_id": uuid4().hex,
                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "event": "ingestion_run_summary",
                "document_stage": "incomplete",
                "complete": False,
                "reason": "startup_failed",
            }
        )
        return 3
    return spider.exit_code
