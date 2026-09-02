"""Read explicit TOML settings and required secrets without opening connections."""

import math
import os
import re
import tomllib
import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import ConfigurationError, InvalidName

from kedra.dates import PartitionSize
from kedra.identity import canonical_url


def _text(value: str, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string")


@dataclass(frozen=True)
class SourceSettings:
    name: str
    search_url: str
    body_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        _text(self.name, "source.name")
        _text(self.search_url, "source.search_url")
        canonical_url(self.search_url)
        if not isinstance(self.body_ids, (list, tuple)) or not self.body_ids:
            raise ValueError("source.body_ids must be a nonempty array of body ID strings")
        if any(not isinstance(b, str) or not re.fullmatch(r"[0-9]+", b) for b in self.body_ids):
            raise ValueError("source.body_ids must contain numeric strings")
        if len(set(self.body_ids)) != len(self.body_ids):
            raise ValueError("source.body_ids must not contain duplicates")
        object.__setattr__(self, "body_ids", tuple(self.body_ids))


@dataclass(frozen=True)
class ScrapingSettings:
    partition_size: PartitionSize
    download_delay_seconds: float
    concurrency_per_domain: int
    timeout_seconds: float
    retry_times: int
    rate_limit_backoff_max_seconds: float
    max_response_bytes: int
    max_pages_per_partition: int
    user_agent: str = "Kedra/0.1 (+rate-limited coding-test client)"

    def __post_init__(self) -> None:
        if self.partition_size not in ("month", "day"):
            raise ValueError("scraping.partition_size must be month or day")
        _text(self.user_agent, "scraping.user_agent")
        if any(ord(character) < 32 or ord(character) == 127 for character in self.user_agent):
            raise ValueError("scraping.user_agent must not contain control characters")
        for name in (
            "download_delay_seconds",
            "timeout_seconds",
            "rate_limit_backoff_max_seconds",
        ):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"scraping.{name} must be a finite positive number")
        if self.rate_limit_backoff_max_seconds < self.download_delay_seconds:
            raise ValueError(
                "scraping.rate_limit_backoff_max_seconds must be at least download_delay_seconds"
            )
        for name in (
            "concurrency_per_domain",
            "retry_times",
            "max_response_bytes",
            "max_pages_per_partition",
        ):
            value = getattr(self, name)
            minimum = 0 if name == "retry_times" else 1
            if type(value) is not int or value < minimum:
                raise ValueError(f"scraping.{name} must be an integer >= {minimum}")


@dataclass(frozen=True)
class StorageSettings:
    mongo_database: str
    landing_collection: str
    transformed_collection: str
    state_collection: str
    s3_endpoint_url: str
    s3_region: str
    landing_bucket: str
    transformed_bucket: str
    object_prefix: str
    mongo_uri: str = field(repr=False)
    s3_access_key_id: str = field(repr=False)
    s3_secret_access_key: str = field(repr=False)
    connect_timeout_seconds: int = 5
    read_timeout_seconds: int = 30
    max_attempts: int = 3

    def __post_init__(self) -> None:
        numeric_fields = ("connect_timeout_seconds", "read_timeout_seconds", "max_attempts")
        for name in self.__dataclass_fields__.keys() - set(numeric_fields):
            _text(getattr(self, name), f"storage.{name}")
        for name in numeric_fields:
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"storage.{name} must be a positive integer")
        try:
            if not self.mongo_uri.startswith(("mongodb://", "mongodb+srv://")):
                raise ValueError
            with warnings.catch_warnings():
                # PyMongo otherwise warns and ignores invalid URI options, possibly echoing secrets.
                warnings.simplefilter("error")
                client = MongoClient(self.mongo_uri, connect=False)
        except (ConfigurationError, ValueError, OSError, Warning):
            raise ValueError(
                "KEDRA_MONGO_URI must be a valid MongoDB connection URI with supported options"
            ) from None
        # Disconnected handles validate names locally; no queries, DNS lookups or writes occur.
        with client:
            try:
                database = client.get_database(self.mongo_database)
            except InvalidName:
                raise ValueError(
                    "storage.mongo_database is not a valid MongoDB database name"
                ) from None
            for name in ("landing_collection", "transformed_collection", "state_collection"):
                try:
                    database.get_collection(getattr(self, name))
                except InvalidName:
                    raise ValueError(
                        f"storage.{name} is not a valid MongoDB collection name"
                    ) from None
        canonical_url(self.s3_endpoint_url)
        if len({self.landing_collection, self.transformed_collection, self.state_collection}) != 3:
            raise ValueError("Landing, transformed and operational collections must be distinct")
        if self.landing_bucket == self.transformed_bucket:
            raise ValueError("Landing and transformed buckets must be distinct")
        for bucket in (self.landing_bucket, self.transformed_bucket):
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]{1,61}[a-z0-9]", bucket):
                raise ValueError("Bucket names must use 3-63 lowercase letters, digits or hyphens")
        if "\\" in self.object_prefix or any(
            part in ("", ".", "..") for part in self.object_prefix.split("/")
        ):
            raise ValueError("storage.object_prefix must be a relative path without dot segments")


@dataclass(frozen=True)
class Settings:
    source: SourceSettings
    scraping: ScrapingSettings
    storage: StorageSettings


def load_settings(path: Path, environ: Mapping[str, str] | None = None) -> Settings:
    """No implicit .env loading or network checks; errors never include secret values."""
    env = os.environ if environ is None else environ
    try:
        with path.open("rb") as file:
            config = tomllib.load(file)
    except tomllib.TOMLDecodeError:
        raise ValueError("Invalid TOML configuration") from None
    if set(config) != {"source", "scraping", "storage"} or any(
        not isinstance(section, dict) for section in config.values()
    ):
        raise ValueError("Config must contain exactly the source, scraping and storage tables")
    secrets = {}
    for field_name, env_name in (
        ("mongo_uri", "KEDRA_MONGO_URI"),
        ("s3_access_key_id", "KEDRA_S3_ACCESS_KEY_ID"),
        ("s3_secret_access_key", "KEDRA_S3_SECRET_ACCESS_KEY"),
    ):
        value = env.get(env_name)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"Required environment variable {env_name} is missing or blank")
        if field_name in config["storage"]:
            raise ValueError(f"Set {env_name} in the environment, not in TOML")
        secrets[field_name] = value
    try:
        return Settings(
            SourceSettings(**config["source"]),
            ScrapingSettings(**config["scraping"]),
            StorageSettings(**config["storage"], **secrets),
        )
    except TypeError:
        raise ValueError(
            "Config has missing or unknown fields; compare config.example.toml"
        ) from None
