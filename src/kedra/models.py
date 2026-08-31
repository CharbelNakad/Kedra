"""Immutable source metadata, before any document download or storage operation."""

from dataclasses import dataclass
from datetime import date

from kedra.identity import canonical_url, record_key, stable_hash


@dataclass(frozen=True)
class RecordMetadata:
    source: str
    body_id: str
    title: str
    reference_number: str | None
    description: str | None
    published_date: date
    source_date_raw: str
    source_url: str
    partition_date: date

    def __post_init__(self) -> None:
        for name in ("source", "body_id", "title", "source_date_raw"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonblank string")
        for name in ("reference_number", "description"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{name} must be a string or None")
        if type(self.published_date) is not date or type(self.partition_date) is not date:
            raise ValueError("Metadata dates must be calendar dates, not timestamps")
        if self.partition_date > self.published_date:
            raise ValueError("partition_date cannot follow published_date")
        canonical_url(self.source_url)

    @property
    def identifier(self) -> str:
        """Follow the PDF's annotation of the result-card heading."""
        return self.title

    @property
    def date_semantics(self) -> str:
        return "decision_or_determination_date"

    @property
    def record_key(self) -> str:
        return record_key(self.source, self.body_id, self.reference_number, self.source_url)

    @property
    def metadata_hash(self) -> str:
        """Fingerprint source metadata, excluding partition labels and run context."""
        return stable_hash(
            {
                "source": self.source,
                "body_id": self.body_id,
                "title": self.title,
                "identifier": self.identifier,
                "reference_number": self.reference_number,
                "description": self.description,
                "published_date": self.published_date.isoformat(),
                "source_date_raw": self.source_date_raw,
                "date_semantics": self.date_semantics,
                "source_url": canonical_url(self.source_url),
            }
        )
