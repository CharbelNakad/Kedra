import io
import json
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
from scrapy.http import HtmlResponse, Request

from kedra.config import load_settings
from kedra.dates import DateRange, Partition
from kedra.discovery import (
    DecisionsDiscoverySpider,
    DiscoveryError,
    DiscoveryTracker,
    DiscoveryUnit,
    JsonLineWriter,
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
    tracker.observe(parse_search_page(response("body-15376-page-1.html", WRC_PAGE_1), WRC_UNIT))
    duplicate_body = (FIXTURES / "body-15376-page-2.html").read_bytes()
    duplicate_body = duplicate_body.replace(b"ADJ-00054668", b"ADJ-00054658")
    duplicate = response("body-15376-page-2.html", WRC_PAGE_2).replace(body=duplicate_body)
    summary = tracker.observe(parse_search_page(duplicate, WRC_UNIT))
    assert summary.complete is False
    assert summary.card_occurrences == 12
    assert summary.distinct_records == 11
    assert summary.duplicate_cards == 1
    assert summary.reasons == ("duplicate_cards",)


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
    assert summary.reasons == ("listing_http_failure",)


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


def test_json_line_writer_produces_one_parseable_object_per_event():
    stream = io.StringIO()
    writer = JsonLineWriter(stream)
    writer({"event": "one", "value": "Café"})
    writer({"event": "two", "value": 2})
    assert [json.loads(line)["event"] for line in stream.getvalue().splitlines()] == [
        "one",
        "two",
    ]
