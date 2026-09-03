# Manual end-to-end testing

This guide validates the application without using pytest. Complete the shared setup once,
then choose either the local guide or the live data guide.

| Path | What it does | When to use it |
| --- | --- | --- |
| Local guide | Runs the production Scrapy, Dagster, MongoDB and S3-compatible storage code against a deterministic loopback source, then exercises 1,000 records in memory | Repeatable development, demonstrations and full failure/rerun checks |
| Live data guide | Runs bounded discovery, ingestion and transformation against Workplace Relations | Final proof that the current public site works with the production pipeline |

The local and live guides write to the same persistent storage services but use distinct
source names. Neither guide deletes or overwrites Landing data.

## 1. Shared setup

**What this section does:** creates the locked Python environment, prepares ignored local
credentials, starts MongoDB and SeaweedFS, and verifies the storage schema and permissions.
Complete it before either testing path.

### 1.1 Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop using Linux containers
- PowerShell 7 or Windows PowerShell 5.1
- Free loopback ports 27017, 8333 and 18766

Run all commands from the repository root. On Linux or macOS, replace
`.\.venv\Scripts\python.exe` with `.venv/bin/python` and PowerShell backticks with `\`.

### 1.2 Build the locked Python environment

**What this does:** installs exactly the dependency versions recorded in the lock file.

```powershell
uv sync --locked --python 3.12 --cache-dir .uv-cache
$Python = '.\.venv\Scripts\python.exe'
& $Python -m kedra --help
```

Expected result: `uv` succeeds and the help output lists `check-config`, `discover`,
`ingest`, `transform` and `orchestrate`.

### 1.3 Prepare and start persistent storage

**What this does:** creates local credentials on the first run, starts the Docker services,
then creates or verifies the buckets, collections, validators, indexes and restricted roles.
It reuses existing data on later runs.

```powershell
$StorageConfig = 'config.example.toml'

docker desktop start
& $Python -m kedra.storage_admin prepare --config $StorageConfig
docker compose --env-file .local/compose.env config --quiet
docker compose --env-file .local/compose.env up -d --wait --wait-timeout 90
& $Python -m kedra.storage_admin bootstrap --config $StorageConfig
docker compose --env-file .local/compose.env ps
```

Expected result: `prepare` and `bootstrap` print `"status": "ready"`; MongoDB,
SeaweedFS and the S3 gateway are healthy.

Do not use `docker compose down -v`, delete the named volumes, or remove `.local/` as a
reset procedure. Landing objects and metadata are append-only.

---

## 2. Local validation guide

**What this guide does:** runs the complete application against a deterministic source on
`127.0.0.1`. It covers all four bodies, pagination, HTML/PDF/DOC/DOCX files, an attachment,
exact stored-byte verification, transformation and a validator-backed unchanged rerun. A
separate direct exercise checks the required 1,000-record volume and failure recovery.

**What it proves:** application behavior, storage permissions, immutable writes, format
handling, failure accounting and rerun behavior under controlled conditions.

**What it does not prove:** current Workplace Relations selectors or public-site throughput.
Use the live guide separately for that evidence.

### 2.1 Start the deterministic source in terminal A

**What this does:** starts a local HTTP source with stable pages, assets and ETags. Keep this
terminal open while completing sections 2.2 through 2.5.

```powershell
$Python = '.\.venv\Scripts\python.exe'
& $Python -B scripts/demo_source.py
```

The first JSON line should report:

- four configured body IDs;
- two listing pages per body;
- 12 logical records and 13 required assets;
- HTML, PDF, DOC and DOCX responses; and
- one HTML wrapper with a required PDF attachment.

Subsequent lines record every listing and asset response.

### 2.2 Run the complete local pipeline in terminal B

**What this does:** runs ingestion and transformation as dependent Dagster operations.
Ingestion must complete and produce a manifest before transformation starts.

Open another PowerShell terminal in the repository root:

```powershell
$Python = '.\.venv\Scripts\python.exe'
$FirstSummaryPath = '.local/manual-first-orchestration.jsonl'

& $Python -B -m kedra orchestrate `
  --config config.demo.toml `
  --start-date 2025-07-17 --end-date 2025-07-17 `
  --ingest-env .local/ingest.env `
  --transform-env .local/transform.env `
  --run-directory .local/manual-runs |
  Tee-Object -FilePath $FirstSummaryPath

if ($LASTEXITCODE -ne 0) { throw 'The first local orchestration run did not complete.' }
$FirstRun = Get-Content $FirstSummaryPath | Select-Object -Last 1 | ConvertFrom-Json
$FirstRun | Format-List
```

Expected orchestration result:

```text
ingestion_status       complete
transformation_status  complete
complete               True
```

### 2.3 Check the local run totals

**What this does:** reads the final JSON event from each stage and checks that every advertised
record and required asset was accounted for.

```powershell
$FirstIngestion = Get-Content $FirstRun.ingestion_manifest_path |
  Select-Object -Last 1 | ConvertFrom-Json
$FirstTransform = Get-Content $FirstRun.transformation_log_path |
  Select-Object -Last 1 | ConvertFrom-Json

$FirstIngestion | Select-Object advertised_total, card_occurrences, distinct_records, `
  successfully_available_records, failed_documents, downloaded_files, stored_files, `
  created_objects, inserted_metadata_versions, complete | Format-List

$FirstTransform | Select-Object selected_assets, successfully_transformed_assets, `
  html_transformed, binary_copied, failed_assets, created_objects, `
  inserted_metadata_versions, complete | Format-List
```

Expected first-run values:

| Check | Expected |
| --- | ---: |
| Advertised / card / distinct / successful records | 12 / 12 / 12 / 12 |
| Failed documents | 0 |
| Downloaded / stored Landing assets | 13 / 13 |
| New Landing objects / metadata versions | 13 / 13 |
| Selected / successful transformed assets | 13 / 13 |
| HTML transformed / binary copied | 4 / 9 |
| New transformed objects / metadata versions | 13 / 13 |

The thirteenth asset is the PDF attachment belonging to the HTML wrapper record.

### 2.4 Verify Mongo metadata and exact object bytes

**What this does:** uses the restricted transformation profile to read stored metadata and
objects. The inspector recomputes every SHA-256 and length, resolves every transformed link,
compares binary outputs byte-for-byte, validates `identifier.ext` filenames and exports one
sample per format.

```powershell
$LocalInspectionPath = '.local/manual-storage-inspection.jsonl'

& $Python -B scripts/inspect_storage.py `
  --config config.demo.toml `
  --profile .local/transform.env `
  --source kedra-manual-demo-v1 `
  --export-directory .local/manual-export `
  --export-samples-per-format 1 |
  Tee-Object -FilePath $LocalInspectionPath

if ($LASTEXITCODE -ne 0) { throw 'Local storage inspection failed.' }
$LocalInspection = Get-Content $LocalInspectionPath |
  Select-Object -Last 1 | ConvertFrom-Json
$LocalInspection | ConvertTo-Json -Depth 6
```

Expected inspection values:

```text
complete                                           true
landing.metadata_versions                         13
landing.logical_records                           12
landing.verified_objects                          13
landing.listed_prefix_objects                     13
landing.unreferenced_prefix_objects                0
transformed.metadata_versions                     13
transformed.verified_objects                      13
transformed.listed_prefix_objects                 13
transformed.unreferenced_prefix_objects            0
cross_checks.resolved_transformed_to_landing_links 13
cross_checks.binary_exact_copies                    9
cross_checks.html_outputs_with_new_hash             4
```

List and inspect the exported bytes:

```powershell
Get-ChildItem .local/manual-export -Recurse -File |
  Select-Object FullName, Length

$RawHtml = Get-ChildItem .local/manual-export/landing/html/*.html |
  Select-Object -First 1
$CleanHtml = Get-ChildItem .local/manual-export/transformed/html/*.html |
  Select-Object -First 1
Get-Content $RawHtml.FullName
Get-Content $CleanHtml.FullName
```

The raw HTML contains the demo header, footer, navigation and button. The transformed HTML
retains the decision heading, legal paragraph and table while removing that page chrome. A
wrapper output contains a clean attachment index. PDF, DOC and DOCX bytes remain unchanged.

Optional direct Mongo inspection:

```powershell
$RootPassword = (Get-Content .local/mongo-root-password -Raw).Trim()
docker compose --env-file .local/compose.env exec -T mongo `
  mongosh --quiet --username kedra-admin --password $RootPassword `
  --authenticationDatabase admin kedra --eval `
  'const source="kedra-manual-demo-v1";
   printjson({
     landing: db.landing_metadata.countDocuments({source}),
     transformed: db.transformed_metadata.countDocuments({source})
   });
   printjson(db.landing_metadata.findOne(
     {source},
     {_id:0, identifier:1, body_id:1, published_date:1, partition_date:1,
      object_bucket:1, object_key:1, file_hash:1, document_format:1}
   ));'
```

Expected counts are 13 Landing and 13 transformed metadata versions. The sample document
shows the source identifier, date, partition, object path, format and exact file hash.

### 2.5 Prove an unchanged local rerun

**What this does:** repeats the identical scope while the local source returns trustworthy
ETags. It proves that unchanged document bodies are not transferred and no immutable version
is duplicated.

```powershell
$SecondSummaryPath = '.local/manual-second-orchestration.jsonl'

& $Python -B -m kedra orchestrate `
  --config config.demo.toml `
  --start-date 2025-07-17 --end-date 2025-07-17 `
  --ingest-env .local/ingest.env `
  --transform-env .local/transform.env `
  --run-directory .local/manual-runs |
  Tee-Object -FilePath $SecondSummaryPath

if ($LASTEXITCODE -ne 0) { throw 'The local rerun did not complete.' }
$SecondRun = Get-Content $SecondSummaryPath | Select-Object -Last 1 | ConvertFrom-Json
$SecondIngestion = Get-Content $SecondRun.ingestion_manifest_path |
  Select-Object -Last 1 | ConvertFrom-Json
$SecondTransform = Get-Content $SecondRun.transformation_log_path |
  Select-Object -Last 1 | ConvertFrom-Json

$SecondIngestion | Select-Object downloaded_files, not_modified_files, `
  records_reused_without_download, created_objects, inserted_metadata_versions, `
  reused_objects, reused_metadata_versions, complete | Format-List

$SecondTransform | Select-Object created_objects, inserted_metadata_versions, `
  reused_objects, reused_metadata_versions, complete | Format-List
```

Expected rerun values:

| Check | Expected |
| --- | ---: |
| Downloaded document bodies | 0 |
| HTTP 304 / not-modified assets | 13 |
| Records reused without a document body | 12 |
| New Landing objects / metadata | 0 / 0 |
| Reused Landing objects / metadata | 13 / 13 |
| New transformed objects / metadata | 0 / 0 |
| Reused transformed objects / metadata | 13 / 13 |

Terminal A should show 13 asset responses with `"status": 304` and
`"response_body_bytes": 0`. Listing pages are fetched again so new or removed decisions can
still be detected. Re-run section 2.4; counts must remain unchanged and all hashes must pass.

### 2.6 Exercise 1,000 records and recovery

**What this does:** generates 1,000 deterministic records without contacting WRC or Docker.
It measures bounded concurrency and Python allocations while injecting retryable and terminal
failures, recovering the missing records, rerunning persistence and rerunning transformation.

Stop the demo source with Ctrl+C, or leave it running; this exercise uses another loopback
port.

```powershell
& $Python -B scripts/reliability_exercise.py --records 1000
```

The command must exit 0 with one `reliability_exercise_summary` containing:

- `records: 1000` and `pages: 100`;
- 250 HTML, PDF, DOC and DOCX assets;
- recovered 429, 503 and timeout attempts;
- 998 initial successes and two explicitly failed records;
- recovery creating only the two missing versions;
- an unchanged persistence rerun reusing all 1,000 versions;
- transformation creating, then reusing, all 1,000 outputs;
- maximum active requests no greater than configured concurrency; and
- `complete: true`.

Elapsed time is measured rather than asserted because workstation performance varies. Peak
memory is Python allocation data from `tracemalloc`; it excludes native libraries and
Docker. The exercise uses in-memory adapters, so it cannot pollute Landing.

### 2.7 Local evidence checklist

The local path is complete when all of these are true:

- The first orchestration run completes with 12 records and 13 assets.
- The inspector validates every Mongo-to-object reference, hash and length.
- Four HTML outputs have new hashes and nine binary outputs are byte-identical.
- The unchanged rerun transfers zero asset bodies and creates zero versions.
- The 1,000-record exercise reports bounded concurrency, complete accounting and successful
  recovery.

---

## 3. Live Workplace Relations guide

**What this guide does:** first checks current listing selectors and pagination without
downloading documents. It then runs the production Dagster pipeline for one body and one day,
inspects the exact run manifest and verifies all matching stored bytes.

**What it proves:** the current public search, real decision downloads, immutable Landing
writes and transformation work for the exact body/date scope tested.

**What it does not prove:** all historical dates, every document format, or the maximum safe
public request rate. Keep this test bounded and do not turn it into a bulk scrape.

As of 2 September 2026, body `15376` on 17 July 2025 advertises 12 decisions across two
listing pages. This is a useful small sample because it proves real pagination.

`config.example.toml` uses a neutral `Kedra/0.1` user-agent. A previous user-agent string
containing `discovery` received zero-byte HTTP 200 responses. The current direct HTTP path
works without browser impersonation, cookies, JavaScript or a session bootstrap.

### 3.1 Load the live profile and choose the bounded scope

**What this does:** loads the restricted ingestion credentials into the current PowerShell
process and declares one body/day for every subsequent live command.

```powershell
$Python = '.\.venv\Scripts\python.exe'

Get-Content .local/ingest.env | ForEach-Object {
  $Name, $Value = $_ -split '=', 2
  [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}

$LiveStart = '2025-07-17'
$LiveEnd = '2025-07-17'
$LiveBody = '15376'
```

### 3.2 Verify live discovery before downloading documents

**What this does:** sends only the bounded listing requests and validates the advertised
count, parsed cards and pagination. It does not download or store decision files.

```powershell
$LiveDiscoveryPath = '.local/live-wrc-discovery.jsonl'

& $Python -B -m kedra discover --config config.example.toml `
  --start-date $LiveStart --end-date $LiveEnd --body-id $LiveBody |
  Tee-Object -FilePath $LiveDiscoveryPath

if ($LASTEXITCODE -ne 0) { throw 'Live WRC discovery did not complete.' }
$LiveDiscoveryEvents = Get-Content $LiveDiscoveryPath | ConvertFrom-Json
$LivePartition = $LiveDiscoveryEvents |
  Where-Object event -eq 'discovery_summary' | Select-Object -Last 1
$LiveDiscovery = $LiveDiscoveryEvents |
  Where-Object event -eq 'discovery_run_summary' | Select-Object -Last 1

$LivePartition | Select-Object pages_seen, advertised_total, card_occurrences, `
  distinct_records, failed_listing_pages, known_missing_records, complete
$LiveDiscovery | Select-Object body_partition_count, advertised_total, distinct_records, `
  failed_listing_pages, known_missing_records, complete
```

Expected current result: two pages, 12 advertised/card/distinct records, no failed or missing
records, and `complete=True` in both summaries.

If the public count has legitimately changed, the advertised total and card occurrences must
still reconcile. Stop before ingestion if the JSONL contains `soft_block_detected`,
`empty_listing_response`, an HTTP failure, changed selectors, incomplete pagination or
missing records. Empty HTTP 200 responses are never interpreted as zero results.

### 3.3 Run live ingestion and transformation

**What this does:** repeats the bounded listing request, downloads every required decision
asset, appends exact bytes and metadata to Landing, and writes a completed ingestion manifest.
Only then does transformation write to its separate bucket and collection.

```powershell
$LiveRunSummaryPath = '.local/live-wrc-orchestration-summary.jsonl'

& $Python -B -m kedra orchestrate --config config.example.toml `
  --start-date $LiveStart --end-date $LiveEnd --body-id $LiveBody `
  --ingest-env .local/ingest.env `
  --transform-env .local/transform.env `
  --run-directory .local/live-wrc-runs |
  Tee-Object -FilePath $LiveRunSummaryPath

if ($LASTEXITCODE -ne 0) { throw 'The bounded live WRC pipeline did not complete.' }
$LiveRun = Get-Content $LiveRunSummaryPath | Select-Object -Last 1 | ConvertFrom-Json
$LiveIngestion = Get-Content $LiveRun.ingestion_manifest_path |
  Select-Object -Last 1 | ConvertFrom-Json
$LiveTransformation = Get-Content $LiveRun.transformation_log_path |
  Select-Object -Last 1 | ConvertFrom-Json

$LiveRun | Select-Object ingestion_status, transformation_status, complete
$LiveIngestion | Select-Object advertised_total, card_occurrences, distinct_records, `
  successfully_available_records, failed_documents, downloaded_files, stored_files, `
  created_objects, inserted_metadata_versions, reused_objects, reused_metadata_versions, `
  complete
$LiveTransformation | Select-Object selected_assets, successfully_transformed_assets, `
  failed_assets, html_transformed, binary_copied, created_objects, `
  inserted_metadata_versions, reused_objects, reused_metadata_versions, complete
```

Require `complete=True` at the orchestration, ingestion and transformation boundaries. For
the verified scope, the current result is:

| Check | Verified result |
| --- | ---: |
| Advertised / card / distinct / successful decisions | 12 / 12 / 12 / 12 |
| Listing pages | 2 |
| Failed documents | 0 |
| Downloaded / stored assets | 12 / 12 |
| Successfully transformed assets | 12 |
| HTML transformed / binary copied | 12 / 0 |

This scope happens to contain one HTML asset per decision. Another bounded scope may have more
assets than decisions because a decision can contain attachments or continuation pages.

### 3.4 Inspect the exact live run

**What this does:** reads the manifest selected by Dagster, so the displayed records, object
paths and hashes belong to this run rather than older data in the persistent stores.

```powershell
$LiveEvents = Get-Content $LiveRun.ingestion_manifest_path | ConvertFrom-Json
$LiveRecords = $LiveEvents | Where-Object event -eq 'record_discovered'
$LiveAssets = $LiveEvents | Where-Object event -eq 'asset_stored'

$LiveRecords | Select-Object title, description, reference_number, published_date, `
  partition_date, source_url | Format-Table -AutoSize

$LiveAssets | Select-Object document_format, asset_role, size_bytes, file_hash, `
  object_bucket, object_key, landing_version_id, object_created, metadata_created |
  Format-List

"records=$($LiveRecords.Count) assets=$($LiveAssets.Count)"
```

Expected for this scope: 12 records and 12 assets. Every asset must name `kedra-landing`,
have a 64-character SHA-256 `file_hash`, use a deterministic object key containing that hash,
and have a nonblank Landing version ID.

### 3.5 Verify the stored live bytes

**What this does:** performs a read-only verification through the restricted transformation
role. It reads every matching object, recomputes hashes and lengths, validates transformed
provenance and compares binary outputs byte-for-byte when present.

```powershell
$LiveInspectionPath = '.local/live-wrc-storage-inspection.jsonl'

& $Python -B scripts/inspect_storage.py `
  --config config.example.toml `
  --profile .local/transform.env `
  --source workplace-relations |
  Tee-Object -FilePath $LiveInspectionPath

if ($LASTEXITCODE -ne 0) { throw 'Live storage inspection failed.' }
$LiveInspection = Get-Content $LiveInspectionPath |
  Select-Object -Last 1 | ConvertFrom-Json
$LiveInspection | ConvertTo-Json -Depth 6
```

Require `complete=True`. The aggregate counts can exceed 12 if persistent storage contains
older immutable WRC runs. Use section 3.4 for exact per-run counts.

### 3.6 Prove a safe live rerun

**What this does:** repeats the exact public scope without deleting state. It proves that
identical bytes reuse existing immutable objects and metadata.

```powershell
$LiveRerunSummaryPath = '.local/live-wrc-orchestration-rerun-summary.jsonl'

& $Python -B -m kedra orchestrate --config config.example.toml `
  --start-date $LiveStart --end-date $LiveEnd --body-id $LiveBody `
  --ingest-env .local/ingest.env `
  --transform-env .local/transform.env `
  --run-directory .local/live-wrc-runs |
  Tee-Object -FilePath $LiveRerunSummaryPath

if ($LASTEXITCODE -ne 0) { throw 'The bounded live WRC rerun did not complete.' }
$LiveRerun = Get-Content $LiveRerunSummaryPath | Select-Object -Last 1 | ConvertFrom-Json
$LiveRerunIngestion = Get-Content $LiveRerun.ingestion_manifest_path |
  Select-Object -Last 1 | ConvertFrom-Json
$LiveRerunTransformation = Get-Content $LiveRerun.transformation_log_path |
  Select-Object -Last 1 | ConvertFrom-Json

$LiveRerun | Select-Object ingestion_status, transformation_status, complete
$LiveRerunIngestion | Select-Object downloaded_files, not_modified_files, `
  stored_files, failed_documents, created_objects, reused_objects, `
  inserted_metadata_versions, reused_metadata_versions, complete
$LiveRerunTransformation | Select-Object selected_assets, failed_assets, created_objects, `
  reused_objects, inserted_metadata_versions, reused_metadata_versions, complete
```

Require complete summaries and zero failures. Identical downloaded bytes must create zero new
objects and metadata versions.

WRC HTML currently supplies no trustworthy `ETag` or `Last-Modified`, so the freshness-first
policy downloads it again to detect changes. If volatile raw HTML changes, creating a new
immutable Landing version is correct; the run must never overwrite or delete the old version.
This is different from the local ETag-backed rerun in section 2.5.

### 3.7 Test an evaluator-supplied range

**What this does:** applies the same safe sequence to a requested date range: discovery first,
then ingestion only after all listing partitions reconcile.

Run `discover` for the exact inclusive dates with no `--body-id`. Review every partition
summary and the final totals. Only after discovery completes should you run `orchestrate`
with the same dates and no `--body-id`, covering all four configured bodies. The application
splits the range by body and calendar month.

Do not replace those partitions with the single all-body URL from 2 December 2024 through
2 September 2026. Its first page advertised 4,911 results, creating an unnecessarily large
pagination and retry unit. Do not run an unbounded scrape merely to force the total above
1,000.

If a live run exits 3, its successfully stored immutable versions remain valid. Preserve the
JSONL manifest, use its failed URLs and reasons to diagnose the cause, then rerun the same
scope. Never delete Landing records or reset volumes to obtain a clean rerun.

### 3.8 Live evidence checklist

The live path is complete when all of these are true:

- Discovery reconciles advertised results, cards and pagination before any document download.
- Orchestration, ingestion and transformation each report `complete=True`.
- The ingestion manifest accounts for every decision and asset with zero unexplained failures.
- Every asset has an immutable object key, exact hash, byte length and Landing version ID.
- The storage inspector returns `complete=True`.
- A rerun creates no duplicate version when the downloaded bytes are identical.

---

## 4. Requirement evidence map

**What this section does:** maps each main assignment behavior to the manual evidence produced
by the two guides.

| Assignment behavior | Evidence |
| --- | --- |
| Scrapy direct HTTP with limits | Local and live manifests; configuration records delay, concurrency, timeout, retry and size limits |
| Four bodies and date partitions | Local run covers all four body IDs; listing events record exact `from`, `to` and `body` filters |
| Complete pagination and failure accounting | Local two-page fixtures, live two-page sample and the 1,000-record injected failures |
| Required metadata | Manifest and Mongo views include identifier/title, description, reference, date, source URL, partition and provenance |
| MongoDB and S3-compatible storage in Docker | Readable Mongo documents and hash-verified SeaweedFS objects |
| Immutable Landing Zone | Reruns reuse identical versions; no workflow deletes or overwrites Landing |
| PDF/DOC/DOCX unchanged and HTML cleaned | Local inspector reports nine exact binary copies and four newly hashed HTML outputs |
| Deterministic paths and SHA-256 | Manifest object keys plus inspector recomputation over stored bytes |
| Separate dependent tasks | Dagster logs show transformation begins only after a completed ingestion manifest |
| Separate transformed storage | Distinct bucket and collection with links back to immutable Landing versions |
| Live Workplace Relations data | Bounded discovery and orchestration for body `15376` on 17 July 2025 |
| Approximately 500-1,000 records | Direct 1,000-record exercise with all formats, recovery, rerun and resource measurements |
| Design for much larger scale | `ARCHITECTURE.md` documents partition queues, host budgets, streaming, replication, workers, monitoring and backups |

The assignment asks for validation at approximately 500-1,000 documents and a design that
could evolve to roughly 1,000 times that size. It does not require downloading more than
1,000 public documents during routine validation.

## 5. Known limitations

**What this section does:** states what the evidence does not establish, so test results are
not overstated.

- The bounded 2 September 2026 validation completed 12 WRC decisions for body `15376` on
  17 July 2025. It proves only that scope, not every historical date or public throughput.
- A local hash identifies bytes only after they arrive. If HTML has no ETag or Last-Modified,
  the freshness-first policy downloads it again. The literal “do not re-download unchanged
  files” guarantee cannot be met for validator-free HTML without accepting stale-cache risk.
- The 1,000-record exercise uses in-memory adapters. Docker-backed storage and permissions are
  proven by the smaller deterministic and live runs, not a 1,000-object persistent load test.
- The local deployment is single-process and single-node. Replication, automated backups,
  distributed queues and disaster recovery are design proposals rather than validated runtime
  features.
- Live DOC, DOCX and multi-page continuation layouts remain unobserved. The scraper rejects
  unrecognized and off-host assets rather than silently accepting them.

## 6. Safe shutdown and later reuse

**What this section does:** stops processes while preserving all immutable evidence and local
credentials.

Stop `scripts/demo_source.py` with Ctrl+C if it is running. Stop storage without deleting
data:

```powershell
docker compose --env-file .local/compose.env stop
```

Resume later with:

```powershell
docker compose --env-file .local/compose.env up -d --wait --wait-timeout 90
& $Python -m kedra.storage_admin bootstrap --config config.example.toml
```

Never delete Landing records or volumes as a cleanup step.
