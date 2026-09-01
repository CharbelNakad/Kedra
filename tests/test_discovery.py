import io
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from scrapy.http import HtmlResponse, Request
from scrapy.settings import Settings as ScrapySettings

from kedra.config import load_settings
from kedra.dates import DateRange, Partition
from kedra.discovery import (
    DecisionsDiscoverySpider,
    DiscoveryError,
    DiscoveryTracker,
    DiscoveryUnit,
    JsonLineWriter,
    RateLimitRetryMiddleware,
    crawler_settings,
    parse_search_page,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config.example.toml"
FIXTURES = Path(__file__).parent / "fixtures" / "search"
SEARCH_URL = "https://www.workplacerelations.ie/en/search/"


def response(name, url, request=None):
    return HtmlResponse(
        url,
        body=(FIXTURES / name).read_bytes(),
        encoding="utf-8",
        request=request,
    )


def unit(body_id, start, end_exclusive, partition_date, partition_size):
    return DiscoveryUnit(
        "workplace-relations",
        SEARCH_URL,
        body_id,
        Partition(partition_date, start, end_exclusive),
        partition_size,
    )


WRC_UNIT = unit("15376", date(2025, 7, 17), date(2025, 7, 18), date(2025, 7, 17), "day")
WRC_PAGE_1 = WRC_UNIT.first_url
WRC_PAGE_2 = WRC_PAGE_1 + "&pageNumber=2"


def test_all_body_layouts_and_optional_description_are_parsed():
    eat = unit("2", date(2014, 1, 1), date(2014, 2, 1), date(2014, 1, 1), "month")
    equality = unit("1", date(2014, 1, 1), date(2014, 2, 1), date(2014, 1, 1), "month")
    labour = unit("3", date(2025, 7, 17), date(2025, 7, 18), date(2025, 7, 17), "day")

    eat_page = parse_search_page(response("body-2-missing-description.html", eat.first_url), eat)
    equality_page = parse_search_page(response("body-1.html", equality.first_url), equality)
    zero_page = parse_search_page(response("body-3-zero.html", labour.first_url), labour)
    wrc_page = parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT)

    eat_record = eat_page.records[0]
    assert eat_record.title == "TE257/2012"
    assert eat_record.reference_number == "55139"
    assert eat_record.description is None
    assert eat_record.published_date == date(2014, 1, 31)
    assert eat_record.source_url.endswith("/en/cases/2014/february/te257_2012.html")
    assert equality_page.records[0].description == "Café worker equality decision."
    assert equality_page.records[0].reference_number == "00123"
    assert zero_page.advertised_total == 0
    assert zero_page.card_occurrences == 0
    assert zero_page.next_url is None
    assert len(wrc_page.records) == 10


def test_ten_plus_two_pages_reconcile_the_advertised_total():
    tracker = DiscoveryTracker(WRC_UNIT)
    first = parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT)
    second = parse_search_page(response("body-15376-page-2.html", WRC_PAGE_2), WRC_UNIT)
    assert tracker.observe(first) is None
    summary = tracker.observe(second)
    assert summary.complete is True
    assert summary.pages_seen == 2
    assert summary.advertised_total == 12
    assert summary.card_occurrences == 12
    assert summary.distinct_records == 12
    assert summary.duplicate_cards == 0


def test_repeated_page_content_is_not_accepted_as_progress():
    tracker = DiscoveryTracker(WRC_UNIT)
    tracker.observe(parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT))
    repeated = parse_search_page(response("body-15376-page-1.html", WRC_PAGE_2), WRC_UNIT)
    with pytest.raises(DiscoveryError, match="repeated_page_content"):
        tracker.observe(repeated)


def test_changing_advertised_total_makes_the_partition_incomplete():
    tracker = DiscoveryTracker(WRC_UNIT)
    tracker.observe(parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT))
    changed = response("body-15376-page-2.html", WRC_PAGE_2).replace(
        body=(FIXTURES / "body-15376-page-2.html")
        .read_bytes()
        .replace(b"12 results", b"13 results")
    )
    with pytest.raises(DiscoveryError, match="advertised_total_changed"):
        tracker.observe(parse_search_page(changed, WRC_UNIT))


def test_duplicate_card_is_counted_and_prevents_false_reconciliation():
    tracker = DiscoveryTracker(WRC_UNIT)
    first = parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT)
    tracker.observe(first)
    second = parse_search_page(response("body-15376-page-2.html", WRC_PAGE_2), WRC_UNIT)
    duplicate = replace(
        second,
        records=(first.records[0], second.records[1]),
        fingerprint="exact-duplicate-plus-one-new-record",
    )
    summary = tracker.observe(duplicate)
    assert summary.complete is False
    assert summary.card_occurrences == 12
    assert summary.distinct_records == 11
    assert summary.duplicate_cards == 1
    assert summary.reasons == ("duplicate_cards",)


def test_conflicting_metadata_for_one_record_key_is_an_identity_collision():
    tracker = DiscoveryTracker(WRC_UNIT)
    first = parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT)
    tracker.observe(first)
    changed_body = (
        (FIXTURES / "body-15376-page-2.html")
        .read_bytes()
        .replace(
            b'<span class="refNO">ADJ-00054668</span>',
            b'<span class="refNO">ADJ-00054658</span>',
        )
    )
    changed = parse_search_page(
        response("body-15376-page-2.html", WRC_PAGE_2).replace(body=changed_body),
        WRC_UNIT,
    )

    summary = tracker.observe(changed)

    assert summary.complete is False
    assert summary.duplicate_cards == 0
    assert summary.identity_collisions == 1
    assert summary.distinct_records == 11
    assert summary.reasons == ("identity_collisions",)
    collision = tracker.identity_collisions[0]
    assert collision.first_title == "ADJ-00054658"
    assert collision.conflicting_title == "ADJ-00054668"
    assert collision.first_source_url != collision.conflicting_source_url
    assert collision.first_metadata_hash != collision.conflicting_metadata_hash


def test_spider_emits_conflicting_identity_evidence_without_yielding_it(example_env):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(loaded, scraping=replace(loaded.scraping, partition_size="day"))
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )
    first_request = next(spider.initial_requests())
    first_output = list(
        spider.parse_listing(
            response("body-15376-page-1.html", first_request.url, first_request),
            first_request.cb_kwargs["unit_key"],
        )
    )
    next_request = next(item for item in first_output if isinstance(item, Request))
    changed_body = (
        (FIXTURES / "body-15376-page-2.html")
        .read_bytes()
        .replace(
            b'<span class="refNO">ADJ-00054668</span>',
            b'<span class="refNO">ADJ-00054658</span>',
        )
    )

    second_output = list(
        spider.parse_listing(
            response("body-15376-page-2.html", next_request.url, next_request).replace(
                body=changed_body
            ),
            next_request.cb_kwargs["unit_key"],
        )
    )

    collision = next(event for event in events if event["event"] == "identity_collision")
    assert collision["first_source_url"] != collision["conflicting_source_url"]
    assert collision["first_metadata_hash"] != collision["conflicting_metadata_hash"]
    assert [record.title for record in second_output] == ["ADJ-00054669"]
    assert sum(event["event"] == "record_discovered" for event in events) == 11
    assert events[-1]["identity_collisions"] == 1


def test_malformed_card_is_counted_and_prevents_a_complete_summary():
    page = parse_search_page(response("malformed-card.html", WRC_PAGE_1), WRC_UNIT)
    assert page.card_occurrences == 1
    assert not page.records
    assert page.failures[0].reason == "missing_title"
    summary = DiscoveryTracker(WRC_UNIT).observe(page)
    assert summary.complete is False
    assert summary.malformed_cards == 1
    assert summary.reasons == ("malformed_cards",)


def test_card_date_outside_requested_partition_is_a_card_failure():
    changed = response("malformed-card.html", WRC_PAGE_1).replace(
        body=(FIXTURES / "malformed-card.html")
        .read_bytes()
        .replace(b"17/07/2025", b"18/07/2025")
        .replace(b'<span class="date">', b'<h2 class="title">OUTSIDE</h2><span class="date">')
    )
    page = parse_search_page(changed, WRC_UNIT)
    assert page.failures[0].reason == "published_date_outside_partition"


@pytest.mark.parametrize("body", [b"", None])
def test_empty_or_missing_results_region_is_never_interpreted_as_zero(body):
    page = (
        HtmlResponse(WRC_PAGE_1, body=b"", encoding="utf-8")
        if body == b""
        else response("missing-results-region.html", WRC_PAGE_1)
    )
    expected = "empty_listing_response" if body == b"" else "missing_results_region"
    with pytest.raises(DiscoveryError, match=expected):
        parse_search_page(page, WRC_UNIT)


def test_untrusted_no_results_phrase_is_not_interpreted_as_zero():
    page = HtmlResponse(
        WRC_PAGE_1,
        body=b"<html><body><h1>No results</h1></body></html>",
        encoding="utf-8",
    )
    with pytest.raises(DiscoveryError, match="missing_results_region"):
        parse_search_page(page, WRC_UNIT)


def test_trusted_zero_region_without_a_count_remains_incomplete():
    page = HtmlResponse(
        WRC_PAGE_1,
        body=b'<html><body><div class="no-results">No results</div></body></html>',
        encoding="utf-8",
    )
    parsed = parse_search_page(page, WRC_UNIT)
    summary = DiscoveryTracker(WRC_UNIT).observe(parsed)
    assert parsed.advertised_total is None
    assert summary.complete is False
    assert summary.reasons == ("missing_advertised_total",)


def test_pagination_must_preserve_body_and_date_filters():
    original = (FIXTURES / "body-15376-page-1.html").read_bytes()
    changed = response("body-15376-page-1.html", WRC_PAGE_1).replace(
        body=original.replace(b"body=15376", b"body=3")
    )
    with pytest.raises(DiscoveryError, match="pagination_location_or_filters_changed"):
        parse_search_page(changed, WRC_UNIT)


def test_pagination_cannot_skip_the_next_one_based_page():
    original = (FIXTURES / "body-15376-page-1.html").read_bytes()
    skipped = response("body-15376-page-1.html", WRC_PAGE_1).replace(
        body=original.replace(b"pageNumber=2", b"pageNumber=3")
    )
    with pytest.raises(DiscoveryError, match="pagination_page_skipped"):
        parse_search_page(skipped, WRC_UNIT)


def test_cards_without_an_advertised_total_cannot_reconcile_successfully():
    original = (FIXTURES / "body-2-missing-description.html").read_bytes()
    without_total = original.replace(b'<p class="results-count">1 result found</p>', b"")
    eat = unit("2", date(2014, 1, 1), date(2014, 2, 1), date(2014, 1, 1), "month")
    page = parse_search_page(
        response("body-2-missing-description.html", eat.first_url).replace(body=without_total),
        eat,
    )
    summary = DiscoveryTracker(eat).observe(page)
    assert summary.complete is False
    assert summary.reasons == ("missing_advertised_total",)


def test_failed_listing_page_has_explicit_incomplete_accounting():
    summary = DiscoveryTracker(WRC_UNIT).abort("listing_http_failure")
    assert summary.complete is False
    assert summary.failed_listing_pages == 1
    assert summary.card_occurrences == 0
    assert summary.advertised_total is None
    assert summary.failed_listing_urls == (WRC_PAGE_1,)
    assert summary.known_missing_records is None
    assert summary.reasons == ("listing_http_failure",)


def test_card_failure_identifies_its_page_title_reference_and_url():
    body = b"""<html><body>
    <p class="results-count">1 result found</p>
    <li class="each-item"><h2 class="title">BROKEN-1</h2>
    <span class="date">17/07/2025</span><span class="refNO">Reference No: REF-1</span>
    </li></body></html>"""
    page = parse_search_page(HtmlResponse(WRC_PAGE_2, body=body, encoding="utf-8"), WRC_UNIT)
    failure = page.failures[0]
    assert failure.page_number == 2
    assert failure.card_number == 1
    assert failure.title == "BROKEN-1"
    assert failure.reference_number == "REF-1"
    assert failure.source_url is None
    assert failure.reason == "missing_document_link"


def test_spider_schedules_every_configured_body_with_exact_filters(example_env):
    settings = load_settings(EXAMPLE, example_env)
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        event_sink=events.append,
    )

    scheduled = list(spider.initial_requests())
    assert len(scheduled) == 4
    assert {parse_qs(urlsplit(item.url).query)["body"][0] for item in scheduled} == {
        "1",
        "2",
        "3",
        "15376",
    }
    for request in scheduled:
        assert parse_qs(urlsplit(request.url).query) == {
            "decisions": ["1"],
            "from": ["17/07/2025"],
            "to": ["17/07/2025"],
            "body": [request.cb_kwargs["unit_key"].split(":", 1)[0]],
        }
    assert events[0]["body_partition_count"] == 4


def test_scrapy_limits_and_source_policy_are_driven_by_config(example_env):
    settings = load_settings(EXAMPLE, example_env)
    scrapy_settings = crawler_settings(settings)
    assert scrapy_settings["ROBOTSTXT_OBEY"] is True
    assert scrapy_settings["DOWNLOAD_DELAY"] == settings.scraping.download_delay_seconds
    assert scrapy_settings["CONCURRENT_REQUESTS_PER_DOMAIN"] == 1
    assert scrapy_settings["DOWNLOAD_TIMEOUT"] == settings.scraping.timeout_seconds
    assert scrapy_settings["RETRY_TIMES"] == settings.scraping.retry_times
    assert 429 in scrapy_settings["RETRY_HTTP_CODES"]
    assert scrapy_settings["AUTOTHROTTLE_ENABLED"] is True
    assert scrapy_settings["AUTOTHROTTLE_MAX_DELAY"] == 300.0
    assert scrapy_settings["DOWNLOADER_MIDDLEWARES"] == {
        "scrapy.downloadermiddlewares.retry.RetryMiddleware": None,
        "kedra.discovery.RateLimitRetryMiddleware": 550,
    }
    assert scrapy_settings["DOWNLOAD_MAXSIZE"] == settings.scraping.max_response_bytes
    assert scrapy_settings["COOKIES_ENABLED"] is False


def test_spider_emits_json_serializable_records_pages_and_summaries(example_env):
    settings = replace(
        load_settings(EXAMPLE, example_env),
        scraping=replace(load_settings(EXAMPLE, example_env).scraping, partition_size="day"),
    )
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )

    first_request = next(spider.initial_requests())
    first_response = response("body-15376-page-1.html", first_request.url, first_request)
    first_output = list(spider.parse_listing(first_response, first_request.cb_kwargs["unit_key"]))
    next_request = next(item for item in first_output if isinstance(item, Request))
    second_response = response("body-15376-page-2.html", next_request.url, next_request)
    list(spider.parse_listing(second_response, next_request.cb_kwargs["unit_key"]))
    spider.closed("finished")

    assert sum(event["event"] == "record_discovered" for event in events) == 12
    assert events[-2]["event"] == "discovery_summary"
    assert events[-2]["complete"] is True
    assert events[-1]["event"] == "discovery_run_summary"
    assert events[-1]["complete"] is True
    assert events[-1]["advertised_total"] == 12
    assert events[-1]["card_occurrences"] == 12
    assert events[-1]["successfully_parsed_cards"] == 12
    assert events[-1]["failed_card_parses"] == 0
    assert events[-1]["duplicate_cards"] == 0
    assert events[-1]["identity_collisions"] == 0
    assert events[-1]["known_missing_records"] == 0
    assert events[-1]["document_stage"] == "not_run"
    assert events[-1]["downloaded_files"] is None
    assert events[-1]["stored_files"] is None
    for event in events:
        json.dumps(event)
        assert event["run_id"] == spider.run_id
        assert event["timestamp"].endswith("Z")


def test_page_safety_limit_fails_instead_of_truncating(example_env):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(
        loaded,
        scraping=replace(loaded.scraping, partition_size="day", max_pages_per_partition=1),
    )
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )

    request = next(spider.initial_requests())
    output = list(
        spider.parse_listing(
            response("body-15376-page-1.html", request.url, request),
            request.cb_kwargs["unit_key"],
        )
    )
    assert not any(isinstance(item, Request) for item in output)
    assert events[-1]["event"] == "discovery_summary"
    assert events[-1]["complete"] is False
    assert events[-1]["reasons"] == (
        "page_safety_limit_reached",
        "advertised_total_mismatch",
    )


def test_listing_errback_records_status_attempt_and_failure(example_env):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(loaded, scraping=replace(loaded.scraping, partition_size="day"))
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )

    request = next(spider.initial_requests())
    request.meta["retry_times"] = 2
    failed_response = HtmlResponse(request.url, status=503, request=request)
    failure = SimpleNamespace(
        request=request,
        value=SimpleNamespace(response=failed_response),
    )
    spider.listing_failed(failure)
    failure_event = events[-2]
    assert {
        key: value for key, value in failure_event.items() if key not in {"run_id", "timestamp"}
    } == {
        "event": "listing_failed",
        "source": "workplace-relations",
        "body_id": "15376",
        "partition_date": "2025-07-17",
        "url": request.url,
        "http_status": 503,
        "attempt_count": 3,
        "reason": "listing_http_failure",
    }
    assert len(failure_event["run_id"]) == 32
    assert failure_event["timestamp"].endswith("Z")
    assert events[-1]["complete"] is False


def test_listing_transport_failure_keeps_null_http_status(example_env):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(loaded, scraping=replace(loaded.scraping, partition_size="day"))
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )
    request = next(spider.initial_requests())
    spider.listing_failed(SimpleNamespace(request=request, value=TimeoutError()))
    assert events[-2]["http_status"] is None
    assert events[-2]["reason"] == "listing_request_failure"
    assert events[-2]["attempt_count"] == 1


def test_early_close_reports_the_unfinished_listing_url_and_unknown_count(example_env):
    loaded = load_settings(EXAMPLE, example_env)
    settings = replace(loaded, scraping=replace(loaded.scraping, partition_size="day"))
    events = []
    spider = DecisionsDiscoverySpider(
        settings,
        DateRange.from_inputs("2025-07-17", "2025-07-17"),
        body_ids=["15376"],
        event_sink=events.append,
    )
    request = next(spider.initial_requests())

    spider.closed("shutdown")

    partition = next(event for event in events if event["event"] == "discovery_summary")
    run = events[-1]
    assert partition["reasons"] == ("listing_not_completed",)
    assert partition["failed_listing_urls"] == (request.url,)
    assert run["failed_listing_pages"] == 1
    assert run["failed_listing_urls"] == [request.url]
    assert run["advertised_total"] is None
    assert run["known_missing_records"] is None
    assert run["partitions_with_unknown_missing_count"] == 1


class _RetryStats:
    def __init__(self):
        self.values = {}

    def inc_value(self, key):
        self.values[key] = self.values.get(key, 0) + 1


def _rate_limit_middleware(app_settings, waits, events):
    settings = ScrapySettings(crawler_settings(app_settings))
    stats = _RetryStats()
    crawler = SimpleNamespace(settings=settings, stats=stats, spider=None)
    spider = SimpleNamespace(crawler=crawler, _emit=events.append)
    crawler.spider = spider
    middleware = RateLimitRetryMiddleware(
        settings,
        clock=lambda: 100.0,
        waiter=lambda delay: waits.append(delay) or "nonblocking-wait",
        jitter=lambda _low, high: high,
    )
    middleware.crawler = crawler
    return middleware, spider


def test_429_retry_after_creates_a_shared_nonblocking_origin_cooldown(example_env):
    app_settings = load_settings(EXAMPLE, example_env)
    waits = []
    events = []
    middleware, spider = _rate_limit_middleware(app_settings, waits, events)
    request = Request(WRC_PAGE_1)
    limited = HtmlResponse(
        request.url,
        status=429,
        headers={"Retry-After": "60"},
        request=request,
    )

    retry = middleware.process_response(request, limited, spider)

    assert isinstance(retry, Request)
    assert retry.meta["retry_times"] == 1
    assert middleware.process_request(Request(WRC_PAGE_2), spider) == "nonblocking-wait"
    assert waits == [60.0]
    assert middleware.process_request(Request("https://example.test/search"), spider) is None
    assert events == [
        {
            "event": "rate_limited",
            "url": WRC_PAGE_1,
            "http_status": 429,
            "attempt_count": 1,
            "backoff_seconds": 60.0,
            "backoff_source": "retry_after",
        }
    ]


def test_429_without_valid_retry_after_uses_bounded_exponential_backoff(example_env):
    app_settings = load_settings(EXAMPLE, example_env)
    waits = []
    middleware, spider = _rate_limit_middleware(app_settings, waits, [])
    request = Request(WRC_PAGE_1, meta={"retry_times": 2})
    limited = HtmlResponse(
        request.url,
        status=429,
        headers={"Retry-After": "invalid"},
        request=request,
    )

    retry = middleware.process_response(request, limited, spider)

    assert isinstance(retry, Request)
    assert retry.meta["retry_times"] == 3
    assert middleware.process_request(Request(WRC_PAGE_2), spider) == "nonblocking-wait"
    assert waits == [8.0]


def test_exponential_rate_limit_fallback_does_not_exceed_configured_maximum(example_env):
    app_settings = load_settings(EXAMPLE, example_env)
    waits = []
    middleware, spider = _rate_limit_middleware(app_settings, waits, [])
    request = Request(WRC_PAGE_1, meta={"retry_times": 20})
    limited = HtmlResponse(request.url, status=429, request=request)

    response = middleware.process_response(request, limited, spider)

    assert response is limited
    assert middleware.process_request(Request(WRC_PAGE_2), spider) == "nonblocking-wait"
    assert waits == [300.0]


def test_json_line_writer_produces_one_parseable_object_per_event():
    stream = io.StringIO()
    writer = JsonLineWriter(stream)
    writer({"event": "one", "value": "Café"})
    writer({"event": "two", "value": 2})
    assert [json.loads(line)["event"] for line in stream.getvalue().splitlines()] == [
        "one",
        "two",
    ]
