"""Scrapy discovery for filtered WRC result pages; document downloads are out of scope."""

import hashlib
import json
import math
import random
import re
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, TextIO
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.http import HtmlResponse, Request

from kedra.config import Settings
from kedra.dates import DateRange, Partition, PartitionSize, iter_partitions
from kedra.models import RecordMetadata

EventSink = Callable[[dict[str, Any]], None]


class DiscoveryError(RuntimeError):
    """A listing cannot be completely and safely enumerated."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class DiscoveryUnit:
    source: str
    search_url: str
    body_id: str
    partition: Partition
    partition_size: PartitionSize

    @property
    def key(self) -> str:
        start = self.partition.start.isoformat()
        end = self.partition.end_exclusive.isoformat()
        return f"{self.body_id}:{start}:{end}"

    @property
    def query_values(self) -> dict[str, str]:
        start, end = self.partition.website_dates
        return {"decisions": "1", "from": start, "to": end, "body": self.body_id}

    @property
    def first_url(self) -> str:
        parts = urlsplit(self.search_url)
        return urlunsplit(
            (parts.scheme, parts.netloc, parts.path, urlencode(self.query_values), "")
        )


@dataclass(frozen=True)
class CardFailure:
    page_number: int
    card_number: int
    source_url: str | None
    title: str | None
    reference_number: str | None
    reason: str


@dataclass(frozen=True)
class IdentityCollision:
    record_key: str
    reference_number: str | None
    first_title: str
    conflicting_title: str
    first_source_url: str
    conflicting_source_url: str
    first_metadata_hash: str
    conflicting_metadata_hash: str


@dataclass(frozen=True)
class SearchPage:
    page_number: int
    advertised_total: int | None
    card_occurrences: int
    records: tuple[RecordMetadata, ...]
    failures: tuple[CardFailure, ...]
    next_url: str | None
    fingerprint: str


@dataclass(frozen=True)
class DiscoverySummary:
    source: str
    body_id: str
    partition_date: str
    request_from: str
    request_to: str
    advertised_total: int | None
    pages_seen: int
    card_occurrences: int
    successfully_parsed_cards: int
    distinct_records: int
    duplicate_cards: int
    identity_collisions: int
    malformed_cards: int
    failed_listing_pages: int
    failed_listing_urls: tuple[str, ...]
    known_missing_records: int | None
    complete: bool
    reasons: tuple[str, ...]

    def event(self) -> dict[str, Any]:
        return {"event": "discovery_summary", **asdict(self)}


def _element_text(selector, css: str) -> str | None:
    node = selector.css(css)
    if not node:
        return None
    value = node.xpath("string(.)").get()
    value = " ".join(value.split()) if value else ""
    return value or None


def _published_date(value: str) -> date:
    match = re.search(r"\b[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\b", value)
    if match:
        try:
            return datetime.strptime(match.group(), "%d/%m/%Y").date()
        except ValueError:
            pass
    cleaned = re.sub(r"\b([0-9]{1,2})(?:st|nd|rd|th)\b", r"\1", value, flags=re.I)
    for date_format in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(cleaned, date_format).date()
        except ValueError:
            continue
    raise ValueError("unsupported source date")


def _reference_number(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(
        r"^(?:reference(?:\s*(?:no\.?|number))?|ref\.?\s*no\.?)\s*:?\s*",
        "",
        value,
        flags=re.I,
    ).strip()
    return cleaned or None


TOTAL_SELECTORS = (
    "[data-total-results]::attr(data-total-results)",
    ".results-count",
    ".result-count",
    ".total-results",
    ".search-results-count",
    ".pagination-results",
)
NO_RESULTS_SELECTORS = (".no-results",)


def _advertised_total(response: HtmlResponse, *, allow_body_fallback: bool = True) -> int | None:
    candidates: set[int] = set()
    for css in TOTAL_SELECTORS:
        for node in response.css(css):
            value = (
                node.get()
                if css.endswith("::attr(data-total-results)")
                else node.xpath("string(.)").get()
            )
            if not value:
                continue
            numbers = re.findall(r"[0-9][0-9,]*", value)
            if numbers:
                candidates.add(int(numbers[-1].replace(",", "")))
    if not candidates and allow_body_fallback:
        text = " ".join(response.xpath("//body//text()").getall())
        patterns = (
            r"\b([0-9][0-9,]*)\s+(?:search\s+)?results?\s+(?:found|returned)\b",
            r"\b(?:found|returned)\s+([0-9][0-9,]*)\s+(?:search\s+)?results?\b",
            r"\bof\s+([0-9][0-9,]*)\s+(?:search\s+)?results?\b",
            r"\b(?:search\s+)?results?\s*[:(]\s*([0-9][0-9,]*)\s*\)?",
            r"\b([0-9][0-9,]*)\s+(?:search\s+)?results?\b",
        )
        for pattern in patterns:
            candidates.update(
                int(value.replace(",", "")) for value in re.findall(pattern, text, flags=re.I)
            )
    if len(candidates) > 1:
        raise DiscoveryError("ambiguous_advertised_total")
    if candidates:
        return candidates.pop()
    return None


def _has_explicit_zero(response: HtmlResponse) -> bool:
    selected = " ".join(
        node.xpath("string(.)").get() or ""
        for css in NO_RESULTS_SELECTORS
        for node in response.css(css)
    )
    pattern = r"\b(?:no (?:search )?results?|search returned no results?)\b"
    return bool(re.search(pattern, selected, flags=re.I))


def _page_number(url: str) -> int:
    values = parse_qs(urlsplit(url).query).get("pageNumber", ["1"])
    if len(values) != 1 or not re.fullmatch(r"[1-9][0-9]*", values[0]):
        raise DiscoveryError("invalid_page_number")
    return int(values[0])


def _validate_listing_url(url: str, unit: DiscoveryUnit) -> None:
    expected_url = urlsplit(unit.search_url)
    actual_url = urlsplit(url)
    if (
        actual_url.scheme.lower() != expected_url.scheme.lower()
        or actual_url.netloc.lower() != expected_url.netloc.lower()
        or actual_url.path != expected_url.path
        or actual_url.fragment
    ):
        raise DiscoveryError("listing_location_changed")
    query = parse_qs(actual_url.query)
    if any(query.get(name) != [value] for name, value in unit.query_values.items()):
        raise DiscoveryError("listing_filters_changed")
    if set(query) - {*unit.query_values, "pageNumber"}:
        raise DiscoveryError("unexpected_listing_query")


def _next_page(response: HtmlResponse, unit: DiscoveryUnit, current_page: int) -> str | None:
    candidates: dict[int, str] = {}
    for href in response.css("a[href*='pageNumber=']::attr(href)").getall():
        url = response.urljoin(href)
        try:
            _validate_listing_url(url, unit)
        except DiscoveryError:
            raise DiscoveryError("pagination_location_or_filters_changed") from None
        page = _page_number(url)
        if page > current_page:
            candidates.setdefault(page, url)
    if not candidates:
        return None
    expected_page = current_page + 1
    if expected_page not in candidates:
        raise DiscoveryError("pagination_page_skipped")
    return candidates[expected_page]


def _parse_card(
    card, unit: DiscoveryUnit, response: HtmlResponse, page_number: int, card_number: int
):
    title = _element_text(card, "h2.title")
    raw_date = _element_text(card, "span.date")
    reference_number = _reference_number(_element_text(card, ".refNO"))
    link = card.css(".link a::attr(href)").get()
    source_url = response.urljoin(link) if link else None
    missing = [
        name
        for name, value in (("title", title), ("published_date", raw_date), ("document_link", link))
        if not value
    ]
    if missing:
        return None, CardFailure(
            page_number,
            card_number,
            source_url,
            title,
            reference_number,
            "missing_" + "_and_".join(missing),
        )
    try:
        published = _published_date(raw_date)
    except ValueError:
        return None, CardFailure(
            page_number,
            card_number,
            source_url,
            title,
            reference_number,
            "invalid_published_date",
        )
    if not unit.partition.start <= published < unit.partition.end_exclusive:
        return None, CardFailure(
            page_number,
            card_number,
            source_url,
            title,
            reference_number,
            "published_date_outside_partition",
        )
    try:
        record = RecordMetadata(
            source=unit.source,
            body_id=unit.body_id,
            title=title,
            reference_number=reference_number,
            description=_element_text(card, "p.description"),
            published_date=published,
            source_date_raw=raw_date,
            source_url=source_url,
            partition_date=unit.partition.partition_date,
            partition_size=unit.partition_size,
        )
    except ValueError:
        return None, CardFailure(
            page_number,
            card_number,
            source_url,
            title,
            reference_number,
            "invalid_card_metadata",
        )
    return record, None


def parse_search_page(response: HtmlResponse, unit: DiscoveryUnit) -> SearchPage:
    """Parse one listing response without scheduling or downloading a decision."""
    _validate_listing_url(response.url, unit)
    if not response.body:
        raise DiscoveryError("empty_listing_response")
    cards = response.css("li.each-item")
    explicit_zero = _has_explicit_zero(response)
    if not cards and not explicit_zero:
        raise DiscoveryError("missing_results_region")
    total = _advertised_total(response, allow_body_fallback=not explicit_zero)
    if explicit_zero and (cards or total not in (None, 0)):
        raise DiscoveryError("contradictory_zero_results")
    page_number = _page_number(response.url)
    records: list[RecordMetadata] = []
    failures: list[CardFailure] = []
    for card_number, card in enumerate(cards, start=1):
        record, failure = _parse_card(card, unit, response, page_number, card_number)
        if record is not None:
            records.append(record)
        if failure is not None:
            failures.append(failure)
    fingerprint_values = [
        f"{record.record_key}:{record.metadata_hash}:{record.source_url}" for record in records
    ]
    fingerprint_values.extend(
        f"failure:{failure.page_number}:{failure.card_number}:{failure.source_url}:{failure.reason}"
        for failure in failures
    )
    fingerprint = hashlib.sha256("\n".join(fingerprint_values).encode()).hexdigest()
    return SearchPage(
        page_number=page_number,
        advertised_total=total,
        card_occurrences=len(cards),
        records=tuple(records),
        failures=tuple(failures),
        next_url=_next_page(response, unit, page_number),
        fingerprint=fingerprint,
    )


class DiscoveryTracker:
    """Reconcile pages and cards for one body/date partition."""

    def __init__(self, unit: DiscoveryUnit):
        self.unit = unit
        self.advertised_total: int | None = None
        self.pages: set[int] = set()
        self.fingerprints: set[str] = set()
        self.records: dict[str, RecordMetadata] = {}
        self.card_occurrences = 0
        self.duplicate_cards = 0
        self.identity_collisions: list[IdentityCollision] = []
        self.failures: list[CardFailure] = []
        self.failed_listing_pages = 0
        self.failed_listing_urls: list[str] = []
        self.pending_url: str | None = unit.first_url
        self.summary: DiscoverySummary | None = None

    def observe(self, page: SearchPage) -> DiscoverySummary | None:
        if self.summary is not None:
            raise DiscoveryError("page_after_partition_finished")
        if page.page_number != len(self.pages) + 1 or page.page_number in self.pages:
            raise DiscoveryError("pagination_out_of_order")
        if page.fingerprint in self.fingerprints:
            raise DiscoveryError("repeated_page_content")
        if page.advertised_total is not None:
            if self.advertised_total is not None and page.advertised_total != self.advertised_total:
                raise DiscoveryError("advertised_total_changed")
            self.advertised_total = page.advertised_total
        self.pages.add(page.page_number)
        self.fingerprints.add(page.fingerprint)
        self.card_occurrences += page.card_occurrences
        self.failures.extend(page.failures)
        for record in page.records:
            existing = self.records.get(record.record_key)
            if existing == record:
                self.duplicate_cards += 1
            elif existing is not None:
                self.identity_collisions.append(
                    IdentityCollision(
                        record_key=record.record_key,
                        reference_number=record.reference_number,
                        first_title=existing.title,
                        conflicting_title=record.title,
                        first_source_url=existing.source_url,
                        conflicting_source_url=record.source_url,
                        first_metadata_hash=existing.metadata_hash,
                        conflicting_metadata_hash=record.metadata_hash,
                    )
                )
            else:
                self.records[record.record_key] = record
        self.pending_url = page.next_url
        return self.finish() if page.next_url is None else None

    def abort(
        self,
        reason: str,
        *,
        failed_listing_page: bool = True,
        failed_url: str | None = None,
    ) -> DiscoverySummary:
        if self.summary is not None:
            return self.summary
        if failed_listing_page:
            self.failed_listing_pages += 1
            url = failed_url or self.pending_url
            if url is not None:
                self.failed_listing_urls.append(url)
        reasons = [reason]
        if self.advertised_total is not None and self.card_occurrences != self.advertised_total:
            reasons.append("advertised_total_mismatch")
        if self.duplicate_cards:
            reasons.append("duplicate_cards")
        if self.identity_collisions:
            reasons.append("identity_collisions")
        if self.failures:
            reasons.append("malformed_cards")
        self.summary = self._summary(tuple(reasons))
        return self.summary

    def finish(self) -> DiscoverySummary:
        if self.summary is not None:
            return self.summary
        reasons: list[str] = []
        if self.advertised_total is None:
            reasons.append("missing_advertised_total")
        elif self.card_occurrences != self.advertised_total:
            reasons.append("advertised_total_mismatch")
        if self.duplicate_cards:
            reasons.append("duplicate_cards")
        if self.identity_collisions:
            reasons.append("identity_collisions")
        if self.failures:
            reasons.append("malformed_cards")
        self.summary = self._summary(tuple(reasons))
        return self.summary

    def _summary(self, reasons: tuple[str, ...]) -> DiscoverySummary:
        start, end = self.unit.partition.website_dates
        return DiscoverySummary(
            source=self.unit.source,
            body_id=self.unit.body_id,
            partition_date=self.unit.partition.partition_date.isoformat(),
            request_from=start,
            request_to=end,
            advertised_total=self.advertised_total,
            pages_seen=len(self.pages),
            card_occurrences=self.card_occurrences,
            successfully_parsed_cards=self.card_occurrences - len(self.failures),
            distinct_records=len(self.records),
            duplicate_cards=self.duplicate_cards,
            identity_collisions=len(self.identity_collisions),
            malformed_cards=len(self.failures),
            failed_listing_pages=self.failed_listing_pages,
            failed_listing_urls=tuple(self.failed_listing_urls),
            known_missing_records=(
                None
                if self.advertised_total is None
                else max(self.advertised_total - self.card_occurrences, 0)
            ),
            complete=not reasons,
            reasons=reasons,
        )


class JsonLineWriter:
    def __init__(self, stream: TextIO):
        self.stream = stream

    def __call__(self, event: dict[str, Any]) -> None:
        self.stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        self.stream.flush()


def _wait_without_blocking(delay_seconds: float):
    # Import after Scrapy installs its reactor; importing it at module load can select
    # the wrong reactor for the process.
    from twisted.internet import reactor
    from twisted.internet.task import deferLater

    return deferLater(reactor, delay_seconds, lambda: None)


class RateLimitRetryMiddleware(RetryMiddleware):
    """Retry 429 responses after a shared, nonblocking origin cooldown."""

    def __init__(
        self,
        settings,
        *,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] | None = None,
        waiter: Callable[[float], Any] = _wait_without_blocking,
        jitter: Callable[[float, float], float] = random.uniform,
    ):
        super().__init__(settings)
        self.clock = clock
        self.wall_clock = wall_clock or (lambda: datetime.now(UTC))
        self.waiter = waiter
        self.jitter = jitter
        self.backoff_base = settings.getfloat("RATE_LIMIT_BACKOFF_BASE_SECONDS")
        self.backoff_max = settings.getfloat("RATE_LIMIT_BACKOFF_MAX_SECONDS")
        self.cooldown_until: dict[tuple[str, str, int | None], float] = {}

    @staticmethod
    def _origin(url: str) -> tuple[str, str, int | None]:
        parts = urlsplit(url)
        return parts.scheme.lower(), (parts.hostname or "").lower(), parts.port

    def _retry_after(self, response: HtmlResponse) -> float | None:
        value = response.headers.get("Retry-After")
        if value is None:
            return None
        text = value.decode("ascii", errors="ignore").strip()
        if re.fullmatch(r"[0-9]+", text):
            seconds = float(text)
            return seconds if math.isfinite(seconds) else None
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (retry_at - self.wall_clock()).total_seconds())

    def _fallback_delay(self, request: Request) -> float:
        retry_count = request.meta.get("retry_times", 0)
        ceiling = min(self.backoff_max, self.backoff_base * (2**retry_count))
        return self.jitter(ceiling / 2, ceiling)

    def process_request(self, request: Request, spider=None):
        origin = self._origin(request.url)
        remaining = self.cooldown_until.get(origin, 0.0) - self.clock()
        if remaining > 0:
            waiting = self.waiter(remaining)
            if hasattr(waiting, "addCallback"):
                waiting.addCallback(lambda _result: self.process_request(request, spider))
            return waiting
        return None

    def process_response(self, request: Request, response: HtmlResponse, spider=None):
        if response.status == 429:
            retry_after = self._retry_after(response)
            source = "retry_after" if retry_after is not None else "exponential_backoff"
            delay = retry_after if retry_after is not None else self._fallback_delay(request)
            origin = self._origin(request.url)
            self.cooldown_until[origin] = max(
                self.cooldown_until.get(origin, 0.0), self.clock() + delay
            )
            active_spider = spider or self.crawler.spider
            if active_spider is not None and hasattr(active_spider, "_emit"):
                active_spider._emit(
                    {
                        "event": "rate_limited",
                        "url": request.url,
                        "http_status": 429,
                        "attempt_count": request.meta.get("retry_times", 0) + 1,
                        "backoff_seconds": delay,
                        "backoff_source": source,
                    }
                )
        return super().process_response(request, response)


class DecisionsDiscoverySpider(scrapy.Spider):
    """Enumerate filtered result cards and pager links, but never fetch card links."""

    name = "wrc_decision_discovery"

    def __init__(
        self,
        app_settings: Settings,
        date_range: DateRange,
        body_ids: Sequence[str] | None = None,
        event_sink: EventSink | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        selected = tuple(body_ids or app_settings.source.body_ids)
        if not selected or any(body not in app_settings.source.body_ids for body in selected):
            raise ValueError("Selected body IDs must be configured source body IDs")
        if len(set(selected)) != len(selected):
            raise ValueError("Selected body IDs must not contain duplicates")
        self.app_settings = app_settings
        self.date_range = date_range
        self.body_ids = selected
        self.event_sink = event_sink or (lambda event: None)
        self.run_id = uuid4().hex
        self.allowed_domains = [urlsplit(app_settings.source.search_url).hostname]
        self.trackers: dict[str, DiscoveryTracker] = {}
        self.summaries: dict[str, DiscoverySummary] = {}
        self.exit_code = 3

    def _emit(self, event: dict[str, Any]) -> None:
        self.event_sink(
            {
                "run_id": self.run_id,
                "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                **event,
            }
        )

    def _units(self) -> Iterator[DiscoveryUnit]:
        for partition in iter_partitions(
            self.date_range, self.app_settings.scraping.partition_size
        ):
            for body_id in self.body_ids:
                yield DiscoveryUnit(
                    self.app_settings.source.name,
                    self.app_settings.source.search_url,
                    body_id,
                    partition,
                    self.app_settings.scraping.partition_size,
                )

    def initial_requests(self) -> Iterator[Request]:
        units = list(self._units())
        self._emit(
            {
                "event": "discovery_started",
                "source": self.app_settings.source.name,
                "body_ids": list(self.body_ids),
                "body_partition_count": len(units),
            }
        )
        for unit in units:
            self.trackers[unit.key] = DiscoveryTracker(unit)
            self._emit(
                {
                    "event": "partition_started",
                    "source": unit.source,
                    "body_id": unit.body_id,
                    "partition_date": unit.partition.partition_date.isoformat(),
                    "url": unit.first_url,
                }
            )
            yield Request(
                unit.first_url,
                callback=self.parse_listing,
                errback=self.listing_failed,
                cb_kwargs={"unit_key": unit.key},
            )

    async def start(self):
        for request in self.initial_requests():
            yield request

    def _complete(self, unit_key: str, summary: DiscoverySummary) -> None:
        self.summaries[unit_key] = summary
        self._emit(summary.event())

    def _accepted_record_outputs(self, record: RecordMetadata):
        """Let discovery yield metadata while ingestion overrides the next step."""
        yield record

    def _stage_summary_fields(self, discovery_complete: bool) -> dict[str, Any]:
        return {
            "document_stage": "not_run",
            "successfully_available_records": None,
            "failed_documents": None,
            "downloaded_files": None,
            "download_failures": None,
            "stored_files": None,
            "storage_failures": None,
        }

    def _run_is_complete(self, discovery_complete: bool) -> bool:
        return discovery_complete

    def _run_summary_event(self) -> str:
        return "discovery_run_summary"

    def parse_listing(self, response: HtmlResponse, unit_key: str):
        tracker = self.trackers[unit_key]
        existing_record_keys = set(tracker.records)
        collision_count = len(tracker.identity_collisions)
        try:
            page = parse_search_page(response, tracker.unit)
            summary = tracker.observe(page)
        except DiscoveryError as error:
            self._emit(
                {
                    "event": "listing_failed",
                    "source": tracker.unit.source,
                    "body_id": tracker.unit.body_id,
                    "partition_date": tracker.unit.partition.partition_date.isoformat(),
                    "url": response.url,
                    "http_status": response.status,
                    "attempt_count": response.request.meta.get("retry_times", 0) + 1,
                    "reason": error.reason,
                }
            )
            self._complete(
                unit_key,
                tracker.abort(error.reason, failed_url=response.url),
            )
            return
        self._emit(
            {
                "event": "listing_page",
                "source": tracker.unit.source,
                "body_id": tracker.unit.body_id,
                "partition_date": tracker.unit.partition.partition_date.isoformat(),
                "page_number": page.page_number,
                "card_occurrences": page.card_occurrences,
                "advertised_total": page.advertised_total,
            }
        )
        for failure in page.failures:
            self._emit(
                {
                    "event": "card_failed",
                    "source": tracker.unit.source,
                    "body_id": tracker.unit.body_id,
                    "partition_date": tracker.unit.partition.partition_date.isoformat(),
                    **asdict(failure),
                }
            )
        emitted_record_keys = set(existing_record_keys)
        for record in page.records:
            if (
                record.record_key in emitted_record_keys
                or tracker.records.get(record.record_key) != record
            ):
                continue
            emitted_record_keys.add(record.record_key)
            self._emit(
                {
                    "event": "record_discovered",
                    **asdict(record),
                    "published_date": record.published_date.isoformat(),
                    "partition_date": record.partition_date.isoformat(),
                    "record_key": record.record_key,
                    "metadata_hash": record.metadata_hash,
                }
            )
            yield from self._accepted_record_outputs(record)
        for collision in tracker.identity_collisions[collision_count:]:
            self._emit(
                {
                    "event": "identity_collision",
                    "source": tracker.unit.source,
                    "body_id": tracker.unit.body_id,
                    "partition_date": tracker.unit.partition.partition_date.isoformat(),
                    **asdict(collision),
                }
            )
        if summary is not None:
            self._complete(unit_key, summary)
            return
        if page.page_number >= self.app_settings.scraping.max_pages_per_partition:
            summary = tracker.abort("page_safety_limit_reached", failed_listing_page=False)
            self._complete(unit_key, summary)
            return
        yield response.follow(
            page.next_url,
            callback=self.parse_listing,
            errback=self.listing_failed,
            cb_kwargs={"unit_key": unit_key},
        )

    def listing_failed(self, failure):
        request = failure.request
        unit_key = request.cb_kwargs["unit_key"]
        tracker = self.trackers[unit_key]
        response = getattr(failure.value, "response", None)
        status = response.status if response is not None else None
        reason = "listing_http_failure" if status is not None else "listing_request_failure"
        self._emit(
            {
                "event": "listing_failed",
                "source": tracker.unit.source,
                "body_id": tracker.unit.body_id,
                "partition_date": tracker.unit.partition.partition_date.isoformat(),
                "url": request.url,
                "http_status": status,
                "attempt_count": request.meta.get("retry_times", 0) + 1,
                "reason": reason,
            }
        )
        self._complete(unit_key, tracker.abort(reason, failed_url=request.url))

    def closed(self, reason: str) -> None:
        for unit_key, tracker in self.trackers.items():
            if unit_key not in self.summaries:
                self._complete(
                    unit_key,
                    tracker.abort(
                        "listing_not_completed",
                        failed_url=tracker.pending_url,
                    ),
                )
        summaries = list(self.summaries.values())
        discovery_complete = len(self.summaries) == len(self.trackers) and all(
            summary.complete for summary in summaries
        )
        complete = self._run_is_complete(discovery_complete)
        known_totals = [
            summary.advertised_total
            for summary in summaries
            if summary.advertised_total is not None
        ]
        known_missing = [
            summary.known_missing_records
            for summary in summaries
            if summary.known_missing_records is not None
        ]
        self.exit_code = 0 if complete else 3
        self._emit(
            {
                "event": self._run_summary_event(),
                "source": self.app_settings.source.name,
                "start_date": self.date_range.start.isoformat(),
                "end_date": (self.date_range.end_exclusive - timedelta(days=1)).isoformat(),
                "body_ids": list(self.body_ids),
                "body_partition_count": len(self.trackers),
                "complete_partitions": sum(summary.complete for summary in summaries),
                "incomplete_partitions": sum(not summary.complete for summary in summaries),
                "advertised_total": sum(known_totals) if known_totals else None,
                "partitions_without_advertised_total": len(summaries) - len(known_totals),
                "card_occurrences": sum(summary.card_occurrences for summary in summaries),
                "successfully_parsed_cards": sum(
                    summary.successfully_parsed_cards for summary in summaries
                ),
                "failed_card_parses": sum(summary.malformed_cards for summary in summaries),
                "malformed_cards": sum(summary.malformed_cards for summary in summaries),
                "distinct_records": sum(summary.distinct_records for summary in summaries),
                "duplicate_cards": sum(summary.duplicate_cards for summary in summaries),
                "identity_collisions": sum(summary.identity_collisions for summary in summaries),
                "failed_listing_pages": sum(summary.failed_listing_pages for summary in summaries),
                "failed_listing_urls": [
                    url for summary in summaries for url in summary.failed_listing_urls
                ],
                "known_missing_records": sum(known_missing) if known_missing else None,
                "partitions_with_unknown_missing_count": len(summaries) - len(known_missing),
                "discovery_complete": discovery_complete,
                **self._stage_summary_fields(discovery_complete),
                "complete": complete,
                "close_reason": reason,
            }
        )


def crawler_settings(settings: Settings) -> dict[str, Any]:
    scraping = settings.scraping
    return {
        "LOG_ENABLED": False,
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": "Kedra coding-test discovery/0.1",
        "COOKIES_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "DOWNLOAD_DELAY": scraping.download_delay_seconds,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": scraping.concurrency_per_domain,
        "DOWNLOAD_TIMEOUT": scraping.timeout_seconds,
        "RETRY_TIMES": scraping.retry_times,
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504, 522, 524],
        "DOWNLOADER_MIDDLEWARES": {
            "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
            "kedra.discovery.RateLimitRetryMiddleware": 550,
        },
        "RATE_LIMIT_BACKOFF_BASE_SECONDS": scraping.download_delay_seconds,
        "RATE_LIMIT_BACKOFF_MAX_SECONDS": scraping.rate_limit_backoff_max_seconds,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": scraping.download_delay_seconds,
        "AUTOTHROTTLE_MAX_DELAY": scraping.rate_limit_backoff_max_seconds,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": scraping.concurrency_per_domain,
        "DOWNLOAD_MAXSIZE": scraping.max_response_bytes,
        "DOWNLOAD_WARNSIZE": scraping.max_response_bytes,
    }


def run_discovery(
    settings: Settings,
    date_range: DateRange,
    body_ids: Sequence[str] | None,
    stream: TextIO,
) -> int:
    """Run one Scrapy reactor for the CLI and return 3 when discovery is incomplete."""
    writer = JsonLineWriter(stream)
    process = CrawlerProcess(crawler_settings(settings), install_root_handler=False)
    crawler = process.create_crawler(DecisionsDiscoverySpider)
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
                "event": "discovery_run_summary",
                "complete": False,
                "reason": "startup_failed",
            }
        )
        return 3
    return spider.exit_code
