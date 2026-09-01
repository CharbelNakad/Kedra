# Kedra

Python utilities for the Workplace Relations coding test: configuration validation,
calendar partitions, filtered result discovery, stable document identity and persistent
local storage primitives.

The application can preview configuration offline or use Scrapy to enumerate filtered
result cards without following their document links. Separate administration commands
provision local storage. Document ingestion, transformation and orchestration are not
implemented.

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

## Result discovery (PowerShell)

`discover` sends direct filtered GET requests through Scrapy. It schedules every configured
body for every calendar partition by default, follows only the next filtered pager link,
and never requests a decision/document URL. Use `--body-id` to make an explicit narrower
run; repeat the option to select more than one configured body.

```powershell
.\.venv\Scripts\python.exe -m kedra discover --config config.example.toml `
  --start-date 2025-07-17 --end-date 2025-07-17 --body-id 15376
```

The command still validates the complete configuration, so the three storage environment
variables shown above must be present, although discovery does not connect to storage.
Every output line is a JSON object with a run ID and UTC timestamp. It records scheduled
partitions, parsed pages, discovered metadata, card/listing failures, identity collisions,
reconciled partition counts and a final run summary. The final summary separates advertised
totals, card occurrences, parsed/malformed cards, distinct records, duplicates, failed page
URLs and known missing records. Document, download and storage totals are explicitly marked
as not run because discovery does not perform those stages. Exit code 0 means every selected
body/partition was fully enumerated; 2 means invalid input/configuration; 3 means discovery
was incomplete.

Cards use the verified `li.each-item`, `h2.title`, `p.description`, `span.date`, `.refNO`
and `.link a` selectors. A missing description becomes `null`. Zero results require both the
known no-results region and a trusted zero count; an arbitrary phrase elsewhere in the page
cannot establish a complete empty partition. The tracker rejects changed filters,
skipped/repeated pages, changing totals, malformed cards, identity collisions, count
mismatches and a configured page safety limit. Exact duplicate cards are counted separately
from conflicting cards that share a logical record key.

Scrapy obeys `robots.txt`, disables cookies, uses AutoThrottle plus the configured per-domain
concurrency, delay, timeout, retry count and response-size limit. A 429 response creates a
shared cooldown for that origin: a valid `Retry-After` controls the delay, while a missing or
invalid header uses randomized exponential backoff capped by
`rate_limit_backoff_max_seconds`. Waiting is asynchronous, so it does not block Scrapy's
reactor. The default page safety limit is 1,000 pages per body/partition; reaching it fails
the partition rather than reporting truncated success. Tune these values in TOML, not source.

The bounded 2026-09-01 recheck of the previously known 17 July 2025 WRC result returned
HTTP 200 with an empty body. The command correctly reported `empty_listing_response` and
did not request page 2 or any decision. Offline fixtures still prove the previously observed
10+2 pagination contract, but current positive live discovery remains unverified while the
source returns empty responses. Do not try alternate clients to bypass a source refusal.

## Local storage (PowerShell)

Use Docker Desktop with Linux containers. The storage deployment consists of MongoDB
8.0.29, SeaweedFS 4.44 and an Nginx 1.30.4 S3 gateway; all images are pinned by digest.
Only MongoDB and the gateway publish ports, both bound to `127.0.0.1`.
SeaweedFS's S3, filer and management interfaces stay on a private Docker network.

```powershell
docker desktop start
.\.venv\Scripts\python.exe -m kedra.storage_admin prepare --config config.example.toml
docker compose --env-file .local/compose.env config --quiet
docker compose --env-file .local/compose.env up -d --wait --wait-timeout 90
.\.venv\Scripts\python.exe -m kedra.storage_admin bootstrap --config config.example.toml
```

`prepare` generates random administrator, ingestion and transformation credentials in
ignored `.local/` files. It never replaces existing credentials. The credential manifest
is written first; rerunning `prepare` reconstructs any missing derived secret/profile files
from that manifest and refuses to replace files whose contents differ. `bootstrap` creates
separate buckets/collections, date and logical-identity indexes, a validator for new
Landing documents, restricted Mongo roles and a Landing bucket policy. Repeating these
commands preserves existing data and checks role/policy drift.
The container health checks cover basic process/HTTP readiness; the tests below verify
authenticated storage operations.

| Role | Landing | Other access |
| --- | --- | --- |
| Ingestion | Read and append metadata/objects | Read/write operational checkpoints in `crawl_state` |
| Transformation | Read metadata/objects | Append to the separate transformed collection/bucket |
| Administrator | Provisioning access | Local setup only; never use these credentials for ingestion/transformation |

To customize names or the S3 port, prepare with a modified `config.local.toml` before
first provisioning. Use `--mongo-port` for a different Mongo port. Pass the same config
path to later administration commands. Storage timeouts, retry attempts and object
prefix can change without reprovisioning. Changing provisioned resource names/endpoints
is deliberately rejected rather than silently replacing credentials or data.
Integration tests use `config.example.toml` and the generated local profiles.

The application still reads credentials from its environment. To explicitly load the
restricted ingestion profile into the current PowerShell process:

```powershell
Get-Content .local/ingest.env | ForEach-Object {
    $name, $value = $_ -split '=', 2
    [Environment]::SetEnvironmentVariable($name, $value, 'Process')
}
```

Use `transform.env` for transformation credentials. `.local/credentials.json` also
contains administrator credentials: keep this directory private, back it up securely
and never commit it. Files use owner-only creation modes on POSIX; Windows access
depends on the workspace's inherited ACLs. Docker secrets here are local file mounts,
not an encrypted secret vault.

### Immutable storage and its limits

`ObjectStore.put_if_absent` uses `If-None-Match: *`, then reads and hashes the exact stored
bytes. Existing identical bytes are reused; a conflicting object fails without repair
or overwrite. `LandingVersion` converts a validated `RecordMetadata` and an object receipt
into one canonical Mongo document. Its deterministic `_id` includes the stable record and
asset identities, metadata hash, exact byte hash and document format. `LandingMetadataStore`
re-reads the object and checks its bucket, key, hash and length before inserting that
document. A Mongo validator rejects incomplete or wrongly typed new documents, and a
partial unique index prevents the same logical version from being inserted under another
`_id`. Neither adapter exposes update/delete.

Python calendar dates are stored as BSON datetimes at `00:00:00 UTC`; `datetime.date`
objects are never passed directly to PyMongo. Inclusive source-date queries use
`published_date >= start-at-UTC-midnight` and `< day-after-end-at-UTC-midnight`. The Mongo
client returns timezone-aware UTC datetimes. This convention represents the source's
displayed decision/determination date, not a publication instant.

The gateway requires conditional object PUTs and rejects deletes, copies, multipart
uploads, object-edit queries and competing preconditions. This restriction is needed
because [SeaweedFS 4.44's policy request extraction](https://github.com/seaweedfs/seaweedfs/blob/4.44/weed/s3api/policy_engine/engine.go)
does not expose `s3:if-none-match`, despite supporting conditional writes. The backend
is not published directly. Mongo permissions independently forbid Landing updates and
deletes, and transformation credentials cannot append to Landing.

This is a local application permission boundary, not protection against a Docker/host
administrator who can read secrets, bypass the gateway or edit volume files. The S3
gateway intentionally supports a small API subset, applies create-only behavior to
both buckets, and accepts object requests up to 32 MiB. Its limits are in
`infra/s3-gateway.conf`. SeaweedFS volume count/size are configurable through
`KEDRA_SEAWEED_VOLUME_MAX` (default `0`, automatic) and
`KEDRA_SEAWEED_VOLUME_SIZE_MB` (default `128`) in the Compose environment.
Each adapter call holds one object's bytes in memory. This setup has one storage node,
no replication and no automated backup or production scale validation.

Mongo cannot enforce that an S3 object exists and cannot atomically commit an S3 upload.
The typed adapter enforces object-first ordering for application writes, but code that
bypasses it and calls PyMongo directly also bypasses the cross-store check. A future
ingestion operation must upload and verify bytes before inserting metadata. An object
left behind by a failed metadata insert must be reused on retry, never deleted as rollback.
The few synthetic documents written before the validator was introduced remain immutable;
Mongo applies the validator to new writes without rewriting those preserved probes.

### Storage verification and safe stopping

The opt-in tests use only small synthetic samples under `_checks/`, never the source
website. Their object keys and version identities are fixed, so reruns reuse existing
samples instead of appending UUID probes. They verify schema and permission failures,
logical duplicate prevention, BSON date ranges, exact-byte integrity, separate outputs,
missing objects and an unavailable endpoint. Restart verification paginates through every
synthetic document/object and compares the saved metadata/object snapshot, including
hashes and timestamps.

```powershell
.\.venv\Scripts\python.exe -m pytest --storage -m "storage and not persistence" -p no:cacheprovider
docker compose --env-file .local/compose.env restart
docker compose --env-file .local/compose.env up -d --wait --wait-timeout 90
.\.venv\Scripts\python.exe -m pytest --storage -m persistence -p no:cacheprovider
```

For stronger persistence verification,
`docker compose --env-file .local/compose.env up -d --force-recreate --wait` replaces the
containers while retaining their named volumes; run the read-only persistence check
afterward, before running any test that can create its fixed sample for the first time.

Stop without deleting data:

```powershell
docker compose --env-file .local/compose.env stop
```

Resume with `up -d --wait`. Never use `down -v`, delete/prune the project volumes, or
delete `.local/` as a reset procedure. Mongo persists in `mongo_data`/`mongo_config`;
SeaweedFS stores both raw bytes and its filer metadata in `seaweed_data`. If preparation
is interrupted after `.local/credentials.json` exists, rerun `prepare`; it recreates only
missing derived files from the same secrets. If the manifest itself is missing or corrupt,
restore it from backup instead of rotating credentials or resetting storage.

## Validation

```powershell
uv lock --check --cache-dir .uv-cache
uv pip check --python .venv/Scripts/python.exe --cache-dir .uv-cache
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\ruff.exe check .
.\.venv\Scripts\ruff.exe format --check .
```

Default tests use synthetic inputs with DNS/socket access blocked; storage integration
tests are skipped unless `--storage` is supplied. Dependency/import checks do not prove
service behavior. The opt-in checks above exercise local storage only; no tests yet
prove live scraping or the complete ingestion/transformation pipeline.

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
  directories; source-specific asset paths will be constructed during ingestion.

## Freshness limitations

The selected refresh policy uses server validators where available and fetches the
document otherwise. This policy is not implemented or tested against live downloads.
Exact-byte hashes identify downloaded bytes; without a reliable remote change signal,
they cannot establish whether the remote document has changed without another fetch.

Verification-first HTML refresh may transfer unchanged legal content and capture
changing timing comments. The assignment's unconditional no-redownload guarantee
therefore remains unresolved for HTML without validators.
