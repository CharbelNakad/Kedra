# Manual end-to-end testing

This guide is the recommended manual validation path. It runs the production Scrapy, Dagster,
MongoDB and S3-compatible storage code without using pytest. The first walkthrough uses a
deterministic loopback source so that it is repeatable even when the public Workplace
Relations site is unavailable or has changed. A separate 1,000-record exercise covers the
assignment's evaluation volume. The final live section uses the production configuration to
scrape a bounded Workplace Relations sample through Scrapy, Landing storage and transformation.

## What this proves

| Evidence | What it demonstrates | What it does not claim |
| --- | --- | --- |
| Manual Docker-backed walkthrough | Four bodies, date filters, pagination, Scrapy downloads, all four formats, a wrapper/attachment, immutable Landing writes, transformation, Mongo/S3 inspection and an idempotent rerun | Current WRC selectors or public-site throughput |
| Direct 1,000-record exercise | 100 listing pages, bounded concurrency/memory, retries, exact failure accounting, recovery and transformation/rerun reuse | Docker capacity, production durability or a live scrape |
| Bounded WRC run | Current selectors, Scrapy document downloads, Landing writes and transformation for the exact body/date scope tested | Other dates, unobserved formats or a maximum safe request rate |

The assignment states a validation volume of approximately 500-1,000 documents and asks for
a design that could evolve to roughly 1,000 times that size. It does not require downloading
more than 1,000 public documents as routine validation.

## Prerequisites

- Python 3.12
- [uv](https://docs.astral.sh/uv/)
- Docker Desktop using Linux containers
- PowerShell 7 or Windows PowerShell 5.1
- Free loopback ports 27017, 8333 and 18766

Run every command from the repository root. On Linux or macOS, replace
`.\.venv\Scripts\python.exe` with `.venv/bin/python` and PowerShell backticks with `\`.

## 1. Build the locked Python environment

```powershell
uv sync --locked --python 3.12 --cache-dir .uv-cache
$Python = '.\.venv\Scripts\python.exe'
```

Expected result: `uv` finishes successfully and `$Python -m kedra --help` lists
`check-config`, `discover`, `ingest`, `transform` and `orchestrate`.

## 2. Prepare and start persistent local storage

`prepare` generates ignored local credentials on the first run and reuses them afterward.
`bootstrap` creates or verifies the two buckets, three collections, validators, indexes and
restricted ingestion/transformation roles. Neither command resets storage.

```powershell
docker desktop start
& $Python -m kedra.storage_admin prepare --config config.demo.toml
docker compose --env-file .local/compose.env config --quiet
docker compose --env-file .local/compose.env up -d --wait --wait-timeout 90
& $Python -m kedra.storage_admin bootstrap --config config.demo.toml
docker compose --env-file .local/compose.env ps
```

Expected result: `prepare` and `bootstrap` print `"status": "ready"`; MongoDB, SeaweedFS and
the S3 gateway are healthy. Existing volumes and records are retained.

Do not use `docker compose down -v`, delete the named volumes, or remove `.local/` as a reset
procedure. Landing objects and metadata are deliberately append-only.

## 3. Start the deterministic source in terminal A

```powershell
$Python = '.\.venv\Scripts\python.exe'
& $Python -B scripts/demo_source.py
```

Leave this terminal open. The first JSON line reports 12 records, four body IDs and the
`2025-07-17` source date. Subsequent lines show each listing or asset HTTP response. The
server binds only to `127.0.0.1`.

The sample contains:

- all four configured body IDs;
- two listing pages per body;
- 12 logical records and 13 required assets;
- HTML, PDF, DOC and DOCX responses; and
- one empty HTML wrapper whose PDF attachment is also required.

## 4. Run the complete pipeline in terminal B

Open a second PowerShell terminal in the repository root:

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

if ($LASTEXITCODE -ne 0) { throw 'The first orchestration run did not complete.' }
$FirstRun = Get-Content $FirstSummaryPath | Select-Object -Last 1 | ConvertFrom-Json
$FirstRun | Format-List
```

Dagster must show `ingest_landing` succeeding before `transform_landing` starts. The final
`orchestration_run_summary` must contain:

```text
ingestion_status       complete
transformation_status  complete
complete               True
```

Inspect the final structured summary from each stage:

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

| Field | Expected |
| --- | ---: |
| Advertised/card/distinct/successful records | 12 / 12 / 12 / 12 |
| Failed documents | 0 |
| Downloaded/stored Landing assets | 13 / 13 |
| New Landing objects/metadata versions | 13 / 13 |
| Selected/successful transformed assets | 13 / 13 |
| HTML transformed / binary copied | 4 / 9 |
| New transformed objects/metadata versions | 13 / 13 |

The extra asset is the required PDF belonging to the HTML wrapper record.

## 5. Read Mongo metadata and exact object bytes

The inspector uses the restricted transformation profile. It performs only Mongo reads and
S3 `GET` operations, recomputes every SHA-256, checks every stored byte length, verifies all
transformed-to-Landing links, compares binary outputs byte-for-byte, checks
`identifier.ext` filenames and exports one sample per format from each bucket.

```powershell
& $Python -B scripts/inspect_storage.py `
  --config config.demo.toml `
  --profile .local/transform.env `
  --source kedra-manual-demo-v1 `
  --export-directory .local/manual-export `
  --export-samples-per-format 1
```

Expected inspection values:

```text
complete                                      true
landing.metadata_versions                    13
landing.logical_records                      12
landing.verified_objects                     13
landing.listed_prefix_objects                13
landing.unreferenced_prefix_objects           0
transformed.metadata_versions                13
transformed.verified_objects                 13
transformed.listed_prefix_objects            13
transformed.unreferenced_prefix_objects       0
cross_checks.resolved_transformed_to_landing_links  13
cross_checks.binary_exact_copies               9
cross_checks.html_outputs_with_new_hash        4
```

List and open the actual exported object bytes:

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

A substantive raw HTML sample contains demo site header/footer/navigation/button content.
The transformed sample retains the decision heading, legal paragraph and table while removing
that chrome. A wrapper sample instead contains a clean attachment index. PDF/DOC/DOCX bytes
are copied unchanged; the PDF sample is a valid one-page PDF and the DOCX sample is a valid
ZIP package containing WordprocessingML.

For a direct Mongo view, use the local administrator only for this read-only inspection. The
pipeline itself uses the restricted profiles:

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
shows the date, partition, bucket, object key, format and exact file hash required by the PDF.

## 6. Prove the unchanged rerun

Keep terminal A running and repeat the orchestration command into a second summary file:

```powershell
$SecondSummaryPath = '.local/manual-second-orchestration.jsonl'

& $Python -B -m kedra orchestrate `
  --config config.demo.toml `
  --start-date 2025-07-17 --end-date 2025-07-17 `
  --ingest-env .local/ingest.env `
  --transform-env .local/transform.env `
  --run-directory .local/manual-runs |
  Tee-Object -FilePath $SecondSummaryPath

if ($LASTEXITCODE -ne 0) { throw 'The second orchestration run did not complete.' }
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

Terminal A must show 13 asset responses with `"status": 304` and
`"response_body_bytes": 0`. Listing pages are fetched again so the pipeline can still detect
new or removed decisions. Run the storage inspector again; both collections and both object
sets must remain at 13 versions with all hashes valid.

This is the strongest form of the idempotency requirement because the controlled source
provides trustworthy ETags. See "Known limits" below for validator-free HTML.

## 7. Exercise 1,000 records directly

Stop the demo source with Ctrl+C, or leave it running; this exercise uses another loopback
port and does not contact WRC or Docker. Invoke the standalone exercise directly, not through
the automated test suite:

```powershell
& $Python -B scripts/reliability_exercise.py --records 1000
```

The command must exit 0 with one JSON `reliability_exercise_summary` containing:

- `records: 1000` and `pages: 100`;
- 250 HTML, PDF, DOC and DOCX assets;
- recovered 429, 503 and timeout attempts;
- a first pass of 998 successful and two explicitly failed records;
- recovery creating only the two missing objects and metadata versions;
- an unchanged persistence rerun reusing all 1,000 versions;
- transformation creating, then reusing, all 1,000 outputs;
- maximum active requests no greater than the configured concurrency; and
- `complete: true`.

Elapsed time is measured, not asserted, because workstation performance varies. Peak memory
is Python allocation data from `tracemalloc`; it excludes native libraries and Docker. The
exercise uses in-memory storage so it cannot pollute or delete Landing. The smaller manual
walkthrough above separately proves the real Docker storage and permission path.

## 8. Scrape a bounded Workplace Relations sample

`config.example.toml` points to the public Workplace Relations search and configures the
neutral `Kedra/0.1` user-agent that currently receives complete HTML. The earlier user-agent
containing `discovery` caused a zero-byte HTTP 200. The fix does not impersonate a browser:
the same direct HTTP transport works without cookies, JavaScript or a session bootstrap.

Keep the first public run to one known body and one day. As of 2 September 2026, body `15376`
on 17 July 2025 advertises 12 records across two listing pages. This is large enough to prove
real pagination and document downloading without turning manual validation into a bulk scrape.

### 8.1 Verify live discovery before downloading documents

Load the restricted ingestion profile because configuration validation requires complete
settings even though `discover` does not open storage:

```powershell
Get-Content .local/ingest.env | ForEach-Object {
  $Name, $Value = $_ -split '=', 2
  [Environment]::SetEnvironmentVariable($Name, $Value, 'Process')
}

$LiveStart = '2025-07-17'
$LiveEnd = '2025-07-17'
$LiveBody = '15376'
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

Expected current result: `pages_seen=2`, `advertised_total=12`, `distinct_records=12`, no
failed or missing records, and both summaries complete. If the count has legitimately changed,
the advertised total and received cards must still reconcile. If the log instead contains
`soft_block_detected`, `empty_listing_response`, an HTTP failure, changed selectors or
incomplete pagination, stop and preserve the JSONL evidence. Empty HTTP 200 responses receive
only the configured bounded cooldown/retries and are never interpreted as zero results.

### 8.2 Run real WRC ingestion and transformation

The following command runs the production Dagster dependency. Ingestion repeats the bounded
listing request, downloads every required decision asset, appends the exact response bytes and
metadata to the immutable Landing bucket/collection, and writes a completed manifest.
Transformation then reads only those manifested Landing versions and appends outputs to the
separate transformed bucket/collection.

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
$LiveIngestion | Select-Object advertised_total, distinct_records, `
  successfully_available_records, failed_documents, downloaded_files, stored_files, complete
$LiveTransformation | Select-Object selected_assets, successful_assets, failed_assets, `
  html_assets, binary_assets, complete
```

Require `complete=True` at all three boundaries. The ingestion count is decisions; the file
count may be larger when a decision has attachments or continuation pages. Inspect the live
objects and hashes with the same commands from step 5. Rerunning this exact command is safe:
it reuses identical immutable versions, uses validators when WRC supplies them and fetches
validator-free HTML again so freshness is not guessed.

### 8.3 Use an evaluator-supplied range

First run `discover` over the exact inclusive dates with no `--body-id`, then review every
partition summary and the final totals. Only after discovery reconciles should you run the
same `orchestrate` command with those dates and no `--body-id`, so all four configured bodies
are covered. The application automatically splits the range by body and calendar month. Do
not replace that with the single all-body URL from 2 December 2024 through 2 September 2026:
its first page advertised 4,911 results, creating an unnecessarily large pagination and retry
unit. Do not run an unbounded scrape merely to force the total above 1,000.

If a live run exits 3, its successfully stored immutable versions remain valid. Use the failed
URLs and reasons in the manifest, correct the cause and rerun the same scope. Never delete
Landing records or reset volumes to obtain a clean rerun.

## Requirement sign-off

After the commands above, the following evidence should be available:

| PDF requirement | Manual evidence |
| --- | --- |
| Scrapy, direct HTTP, rate limits | Demo source request log plus ingestion manifest; configuration shows delay, concurrency, timeout, retry and size caps |
| Every body and date partitions | Four body IDs in the orchestration summary; exact `from`, `to`, `body` filters in listing events; daily demo partition and configurable monthly production default |
| Metadata fields | Mongo document contains identifier/title, description, reference, date, source URL, partition and provenance |
| NoSQL and object storage in Docker | Queryable Mongo documents and hash-verified bytes read from separate SeaweedFS buckets |
| PDF/DOC/DOCX unchanged; HTML cleaned | Nine byte-identical binary outputs and four newly hashed HTML outputs; exported samples are directly inspectable |
| Paths and SHA-256 | Mongo sample plus inspector recomputation over every stored object |
| Idempotent rerun | Zero new versions, 13 zero-body 304 responses and 13 reused outputs |
| Structured logs and failure accounting | JSONL manifests/summaries; the 1,000-record exercise injects and reconciles transient and terminal failures |
| Separate dependent tasks | Dagster log shows transformation receiving the completed ingestion manifest only after ingestion succeeds |
| Separate transformed storage | Distinct bucket and collection names plus transformed metadata links back to immutable Landing versions |
| Live Workplace Relations scrape | Bounded live discovery plus orchestration manifests show reconciled WRC listings, downloaded decision assets and transformed outputs |
| 500-1,000 document reliability | Direct 1,000-record, 100-page exercise with all formats, recovery, rerun and resource bounds |
| Design for much larger scale | `ARCHITECTURE.md` explains queued partition work, per-host budgets, streaming, replication, workers, monitoring and backups; it is a design, not a load-test claim |

## Known limitations

- The bounded 2026-09-02 check revalidated one 12-record WRC listing and one substantive HTML
  decision. A successful execution of step 8 adds live storage/transformation evidence only
  for that exact scope; it does not establish other dates, all formats or public throughput.
- A local file hash can identify bytes only after receiving them. If an HTML response has no
  ETag or Last-Modified value, the freshness-first policy downloads the body again to detect
  changes. The unconditional "do not re-download unchanged files" wording therefore remains
  a literal gap for validator-free HTML. Trusting a local cache would avoid the transfer but
  could silently retain stale legal content.
- The 1,000-record exercise uses in-memory adapters. Docker-backed behavior is proven on the
  smaller deterministic run, not by a 1,000-object persistent load test.
- The local deployment is single-process and single-node, with no replication, automated
  backup, distributed queue or production disaster-recovery validation.
- Live DOC/DOCX and multi-page continuation layouts remain unobserved. The scraper rejects
  unrecognized or off-host assets rather than silently accepting them.

## Safe shutdown and later reuse

Stop the loopback source with Ctrl+C. To stop the storage services without deleting data:

```powershell
docker compose --env-file .local/compose.env stop
```

Resume later with `up -d --wait`; rerun `prepare` and `bootstrap` to verify configuration and
permissions. Never delete Landing records or volumes as a cleanup step.
