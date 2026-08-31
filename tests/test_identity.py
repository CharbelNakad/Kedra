from dataclasses import FrozenInstanceError, replace
from datetime import date
from urllib.parse import unquote

import pytest

from kedra.identity import canonical_url, identifier_filename, record_key
from kedra.models import RecordMetadata


@pytest.fixture
def record():
    return RecordMetadata(
        source="workplace-relations",
        body_id="2",
        title="TE257/2012",
        reference_number="55139",
        description=None,
        published_date=date(2014, 1, 31),
        source_date_raw="31/01/2014",
        source_url="https://www.workplacerelations.ie/en/cases/2014/february/te257_2012.html",
        partition_date=date(2014, 1, 1),
        partition_size="month",
    )


def test_title_identifier_reference_and_source_date_are_distinct(record):
    assert record.title == record.identifier == "TE257/2012"
    assert record.reference_number == "55139"
    assert record.description is None
    assert record.published_date == date(2014, 1, 31)  # Not the URL's February folder.
    assert record.date_semantics == "decision_or_determination_date"


def test_identity_does_not_depend_on_partition_or_source_metadata_changes(record):
    repartitioned = replace(record, partition_date=date(2014, 1, 31), partition_size="day")
    changed = replace(record, title="Corrected heading", description="Corrected description")
    assert record.record_key == repartitioned.record_key == changed.record_key
    assert record.metadata_hash == repartitioned.metadata_hash
    assert record.metadata_hash != changed.metadata_hash
    assert record.record_key != replace(record, body_id="1").record_key
    assert record.record_key != replace(record, source="another-source").record_key
    assert record.record_key != replace(record, reference_number="55140").record_key


def test_reference_identity_survives_url_move_but_metadata_fingerprint_changes(record):
    moved = replace(record, source_url="https://www.workplacerelations.ie/en/moved.html")
    assert moved.record_key == record.record_key
    assert moved.metadata_hash != record.metadata_hash


def test_fallback_url_identity_preserves_path_case_and_query(record):
    missing = replace(record, reference_number=None)
    equivalent = replace(
        missing, source_url=missing.source_url.replace("https://www.", "HTTPS://WWW.") + "#decision"
    )
    assert missing.record_key == equivalent.record_key
    assert (
        missing.record_key != replace(missing, source_url=missing.source_url + "?part=2").record_key
    )
    assert canonical_url("https://EXAMPLE.com:443/Cases/A.html#main") == (
        "https://example.com/Cases/A.html"
    )
    assert canonical_url("http://[::1]:80/") == "http://[::1]/"


def test_reference_kind_cannot_collide_with_url_fallback():
    url = "https://example.com/decision"
    assert record_key("wrc", "2", url, url) != record_key("wrc", "2", None, url)
    assert record_key("wrc", "2", " 00123 ", url) == record_key("wrc", "2", "00123", url)
    assert record_key("wrc", "2", "00123", url) != record_key("wrc", "2", "123", url)


def test_metadata_is_immutable(record):
    with pytest.raises(FrozenInstanceError):
        record.title = "changed"


@pytest.mark.parametrize("partition_size", ["month", "day"])
@pytest.mark.parametrize("partition_date", [date(2013, 12, 1), date(2014, 1, 15), date(2014, 2, 1)])
def test_metadata_rejects_incorrect_partition_labels(record, partition_size, partition_date):
    with pytest.raises(ValueError, match="partition_date"):
        replace(record, partition_size=partition_size, partition_date=partition_date)


@pytest.mark.parametrize(
    "partition_size,partition_date", [("month", date(2014, 1, 31)), ("day", date(2014, 1, 1))]
)
def test_metadata_rejects_labels_from_the_other_partition_mode(
    record, partition_size, partition_date
):
    with pytest.raises(ValueError, match="partition_date"):
        replace(record, partition_size=partition_size, partition_date=partition_date)


@pytest.mark.parametrize(
    "partition_size,published_date,partition_date",
    [
        ("month", date(2014, 1, 31), date(2014, 1, 1)),
        ("day", date(2014, 1, 31), date(2014, 1, 31)),
        ("month", date(2024, 2, 29), date(2024, 2, 1)),
        ("day", date(2024, 2, 29), date(2024, 2, 29)),
        ("month", date(2025, 1, 1), date(2025, 1, 1)),
        ("day", date(2025, 1, 1), date(2025, 1, 1)),
    ],
)
def test_metadata_accepts_canonical_calendar_labels(
    record, partition_size, published_date, partition_date
):
    updated = replace(
        record,
        published_date=published_date,
        source_date_raw=published_date.strftime("%d/%m/%Y"),
        partition_size=partition_size,
        partition_date=partition_date,
    )
    assert updated.partition_date == partition_date


def test_metadata_rejects_unsupported_partition_mode(record):
    with pytest.raises(ValueError, match="partition_size"):
        replace(record, partition_size="week")


@pytest.mark.parametrize(
    "identifier",
    [
        "ADJ-00001234",
        "TE257/2012",
        "RP2101/2011, MN1638/2011",
        "IR - SC - 00004134",
        "../decision",
        r"..\decision",
        "a%2Fb",
        "Café قرار",
        "CON",
        "con.txt",
        "LPT1",
        "AUX",
        "NUL",
        "ends.",
        "..",
        " name ",
        'a:b?c*d<e>f|g"',
    ],
)
def test_filenames_are_reversible_and_portable(identifier):
    filename = identifier_filename(identifier, "html")
    assert unquote(filename.removesuffix(".html")) == identifier
    assert not set('/\\:*?"<>|').intersection(filename)
    assert filename.isascii()
    assert len(filename) <= 255
    assert filename.split(".")[0].upper() not in {"CON", "PRN", "AUX", "NUL", "LPT1"}


@pytest.mark.parametrize("extension", ["html", "pdf", "doc", "docx"])
def test_filename_contract_applies_to_every_required_format(extension):
    assert identifier_filename("TE257/2012", extension) == f"TE257%2F2012.{extension}"


def test_encoding_does_not_merge_slash_with_literal_percent_sequence():
    assert identifier_filename("a/b", "pdf") != identifier_filename("a%2Fb", "pdf")


@pytest.mark.parametrize("identifier,extension", [(" ", "html"), ("a", "exe"), ("a" * 300, "pdf")])
def test_invalid_filenames_fail_without_truncation(identifier, extension):
    with pytest.raises(ValueError):
        identifier_filename(identifier, extension)


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "https://",
        "https://u:secret@example.com/a",
        "https://host:bad/a",
        "https://example.com/a b",
        "https://example.com/\nsecret",
        "",
        None,
    ],
)
def test_invalid_source_urls_are_rejected_without_echoing_input(url):
    with pytest.raises(ValueError) as error:
        canonical_url(url)
    assert "secret" not in str(error.value)
