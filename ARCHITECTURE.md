# Architecture

**Flow:** Dagster runs `Scrapy ingestion -> completed JSONL manifest -> transformation`.
Ingestion writes exact source bytes to an immutable Landing bucket and canonical metadata to
MongoDB. Transformation reads only the Landing version IDs in the completed manifest, copies
PDF/DOC/DOCX bytes, extracts substantive HTML with BeautifulSoup, and appends results to a
separate bucket and collection. Mutable HTTP validators live outside Landing.

## Date partitions

Calendar months are the default because the source accepts inclusive date filters and the
expected volume is modest. A month limits retry scope and produces useful progress without
the request overhead of one query per day. The first and last months are clipped to the
requested inclusive bounds, while `partition_date` remains the calendar month start so an
overlapping rerun has a stable label. Daily partitions remain configurable for unusually
dense or repeatedly failing periods. Each body is queried separately for every partition,
and advertised totals must reconcile with all pages and cards before the partition succeeds.

## Retries and rate limiting

Direct Scrapy HTTP exposes configurable per-domain concurrency, delay, timeouts, response
cap, retry count and an honest user-agent. AutoThrottle stays within the concurrency limit. A
shared origin cooldown honors 429 `Retry-After`; otherwise it uses randomized exponential
backoff. A zero-byte 200 uses the same bounded retry, then fails visibly. Other retries cover
only 408, 500, 502, 503, 504, 522 and 524. Permanent content, format and integrity failures
make the run incomplete. A pagination cap fails instead of truncating a large result set.

## Deduplication and recovery

A logical record key uses source, body and reference number, with a canonical-URL fallback;
conflicting metadata under one key is an identity collision. Each asset version combines
that key, asset identity, stable metadata hash, detected format and SHA-256 of the exact
downloaded bytes. Object keys are deterministic. Create-only object writes are read back and
verified before create-only Mongo metadata is inserted, so an interrupted metadata write can
reuse its orphan object on retry. Transformation versions also include the Landing version
and transform version. Landing objects and metadata are never updated or deleted.

Server `ETag` or `Last-Modified` values enable conditional requests and zero-body 304 reuse.
The sampled HTML supplied neither validator. Under the selected freshness-first policy it
must be fetched again before a local hash can prove equality, so the literal guarantee of no
unchanged HTML re-download is not achievable without accepting stale-cache risk or obtaining
a trustworthy upstream change signal.

## Supporting 50+ sources

Keep the storage/version contracts and add one source adapter per search, metadata and
document-layout contract. Schedule durable source/partition units instead of one large run;
apply a global rate budget per host, isolate retries and checkpoints by source, and stream
indexed metadata in bounded batches. Replace the single-node local stores with managed,
replicated object storage and MongoDB, then add queues, worker autoscaling, monitoring,
backups, schema/transform version migration and per-source quality metrics. Cross-source
entity matching should be a separate derived-data stage so it cannot weaken raw provenance
or Landing immutability.
