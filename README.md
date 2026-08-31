# Kedra

Python utilities for the Workplace Relations coding test: offline configuration
validation, calendar date partitioning, stable document identity and metadata models.

The available command validates configuration and previews date partitions without
network access. Crawling, persistent storage, document transformation and orchestration
are not implemented.

## Setup (PowerShell)

Install Python 3.12 and [uv](https://docs.astral.sh/uv/). From the repository root:

```powershell
uv sync --locked --python 3.12 --cache-dir .uv-cache
```

This creates `.venv` and installs the exact resolved dependencies from `uv.lock`.
No activation is required. If Windows' Python aliases do not work, pass the working
interpreter's absolute path to `--python` instead. Do not use bare `pip`, which may
belong to another Python installation. Docker is not needed for the offline checks.

## Offline configuration check

Non-secret settings are in `config.example.toml`. Copy it to ignored
`config.local.toml` for local changes. Required connection credentials come from the
environment; `.env.example` documents their names but is not automatically loaded.

These deliberately fake values are sufficient for the offline check below. They do
not connect to MongoDB, S3 or the website. They are not valid service credentials:

```powershell
$env:KEDRA_MONGO_URI = 'mongodb://example:example@localhost:27017'
$env:KEDRA_S3_ACCESS_KEY_ID = 'offline-example'
$env:KEDRA_S3_SECRET_ACCESS_KEY = 'offline-example'
.\.venv\Scripts\python.exe -m kedra check-config --config config.example.toml --start-date 2024-01-15 --end-date 2024-03-02
```

The command validates settings and prints a small JSON partition summary without
credentials. Both input dates are included. `partition_size` accepts `month` or
`day`. Invalid dates, configuration or missing environment values return exit code 2.

MongoDB URI options and database/collection names are validated by PyMongo using a
temporary client with `connect=False`; database and collection handles do not create
anything on the server. The minimum PyMongo version is 4.17, the tested baseline for
deferring SRV DNS lookups as well as connections. Driver warnings about URI options
are treated as errors without echoing their values. Passing this check does not prove
DNS records, authentication, permissions or service availability.

## Validation

```powershell
uv lock --check --cache-dir .uv-cache
uv pip check --python .venv/Scripts/python.exe --cache-dir .uv-cache
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
```

Tests use synthetic inputs and no external services. Dependency resolution and import
checks cover Scrapy, pymongo, boto3, BeautifulSoup and Dagster; they do not verify service
connectivity, persistence or live scraping behavior.

## Design decisions and constraints

- Monthly partitions use canonical month-start labels, even for clipped ranges.
  `RecordMetadata` requires an explicit `partition_size`: `month` requires the first
  day of the source date's month; `day` requires the source date itself. Incorrect
  labels fail at construction. Changing between valid modes keeps the source identity
  and metadata hash unchanged because partitioning describes processing, not source content.
- Preserve the card heading as `title` and `identifier`; preserve the distinct source
  reference as `reference_number`. Internal keys use source/body/reference, falling
  back to the source URL when a reference is absent.
- `published_date` keeps the assignment's name, but denotes the website's displayed
  decision/determination date. It is not an inferred true publication timestamp.
- Encode unsafe filename characters reversibly, preserving the original identifier.
  Storage paths are intended to separate source/body/record/asset versions into parent
  directories; no file storage is implemented.

## Freshness limitations

The selected refresh policy uses server validators where available and fetches the
document otherwise. This policy is not implemented or tested against live downloads.
Exact-byte hashes identify downloaded bytes; without a reliable remote change signal,
they cannot establish whether the remote document has changed without another fetch.

Verification-first HTML refresh may transfer unchanged legal content and capture
changing timing comments. The assignment's unconditional no-redownload guarantee
therefore remains unresolved for HTML without validators.
