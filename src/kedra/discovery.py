"""Scrapy discovery for filtered WRC result pages; document downloads are out of scope."""

import hashlib
import json
import re
from collections.abc import Callable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any, TextIO
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import scrapy
from scrapy.crawler import CrawlerProcess
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
    card_number: int
    source_url: str | None
    reason: str


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
    distinct_records: int
    duplicate_cards: int
    malformed_cards: int
    failed_listing_pages: int
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
NO_RESULTS_SELECTORS = (
    ".no-results",
    ".no-result",
    ".noResults",
    ".search-no-results",
    ".no-results-found",
)


def _advertised_total(response: HtmlResponse, explicit_zero: bool) -> int | None:
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
    if not candidates:
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
    return 0 if explicit_zero else None


def _has_explicit_zero(response: HtmlResponse) -> bool:
    selected = " ".join(
        node.xpath("string(.)").get() or ""
        for css in NO_RESULTS_SELECTORS
        for node in response.css(css)
    )
    body = " ".join(response.xpath("//body//text()").getall())
    pattern = r"\b(?:no (?:search )?results?|search returned no results?)\b"
    return bool(re.search(pattern, selected or body, flags=re.I))


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


def _parse_card(card, unit: DiscoveryUnit, response: HtmlResponse, number: int):
    title = _element_text(card, "h2.title")
    raw_date = _element_text(card, "span.date")
    link = card.css(".link a::attr(href)").get()
    source_url = response.urljoin(link) if link else None
    missing = [
        name
        for name, value in (("title", title), ("published_date", raw_date), ("document_link", link))
        if not value
    ]
    if missing:
        return None, CardFailure(number, source_url, "missing_" + "_and_".join(missing))
    try:
        published = _published_date(raw_date)
    except ValueError:
        return None, CardFailure(number, source_url, "invalid_published_date")
    if not unit.partition.start <= published < unit.partition.end_exclusive:
        return None, CardFailure(number, source_url, "published_date_outside_partition")
    try:
        record = RecordMetadata(
            source=unit.source,
            body_id=unit.body_id,
            title=title,
            reference_number=_reference_number(_element_text(card, ".refNO")),
            description=_element_text(card, "p.description"),
            published_date=published,
            source_date_raw=raw_date,
            source_url=source_url,
            partition_date=unit.partition.partition_date,
            partition_size=unit.partition_size,
        )
    except ValueError:
        return None, CardFailure(number, source_url, "invalid_card_metadata")
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
    total = _advertised_total(response, explicit_zero)
    if explicit_zero and (cards or total != 0):
        raise DiscoveryError("contradictory_zero_results")
    page_number = _page_number(response.url)
    records: list[RecordMetadata] = []
    failures: list[CardFailure] = []
    for number, card in enumerate(cards, start=1):
        record, failure = _parse_card(card, unit, response, number)
        if record is not None:
            records.append(record)
        if failure is not None:
            failures.append(failure)
    fingerprint_values = [record.record_key for record in records]
    fingerprint_values.extend(
        f"failure:{failure.card_number}:{failure.source_url}:{failure.reason}"
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
        self.failures: list[CardFailure] = []
        self.failed_listing_pages = 0
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
            if record.record_key in self.records:
                self.duplicate_cards += 1
            else:
                self.records[record.record_key] = record
        return self.finish() if page.next_url is None else None

    def abort(self, reason: str, *, failed_listing_page: bool = True) -> DiscoverySummary:
        if self.summary is not None:
            return self.summary
        if failed_listing_page:
            self.failed_listing_pages += 1
        reasons = [reason]
        if self.advertised_total is not None and self.card_occurrences != self.advertised_total:
            reasons.append("advertised_total_mismatch")
        if self.duplicate_cards:
            reasons.append("duplicate_cards")
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
            distinct_records=len(self.records),
            duplicate_cards=self.duplicate_cards,
            malformed_cards=len(self.failures),
            failed_listing_pages=self.failed_listing_pages,
            complete=not reasons,
            reasons=reasons,
        )


class JsonLineWriter:
    def __init__(self, stream: TextIO):
        self.stream = stream

    def __call__(self, event: dict[str, Any]) -> None:
        self.stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        self.stream.flush()


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

    def parse_listing(self, response: HtmlResponse, unit_key: str):
        tracker = self.trackers[unit_key]
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
            self._complete(unit_key, tracker.abort(error.reason))
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
        for record in page.records:
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
            yield record
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
        self._complete(unit_key, tracker.abort(reason))

    def closed(self, reason: str) -> None:
        for unit_key, tracker in self.trackers.items():
            if unit_key not in self.summaries:
                self._complete(unit_key, tracker.abort("listing_not_completed"))
        complete = len(self.summaries) == len(self.trackers) and all(
            summary.complete for summary in self.summaries.values()
        )
        self.exit_code = 0 if complete else 3
        self._emit(
            {
                "event": "discovery_run_summary",
                "source": self.app_settings.source.name,
                "body_partition_count": len(self.trackers),
                "complete_partitions": sum(summary.complete for summary in self.summaries.values()),
                "incomplete_partitions": sum(
                    not summary.complete for summary in self.summaries.values()
                ),
                "distinct_records": sum(
                    summary.distinct_records for summary in self.summaries.values()
                ),
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
