# Tremor Ingestion Architecture and Status

## Goal
Build a reliable ingestion path for replayed vendor slices, starting with a poller-only phase and deferring database writes to a later phase.

## Current System Architecture

### 1) Vendor Replay Source
- Poll manifest endpoint: `/v2/lastupdate.txt`
- Manifest line contract: `<bytes> <sha1> <url>`
- Slice files are zipped CSVs for:
  - `events`
  - `mentions`
  - `articles`
- Replay timing is fast; effective cadence is one slice every ~0.45s.

### 2) Ingestion Poller (Implemented)
File: `src/ingest/poller.py`

Responsibilities currently implemented:
- Poll manifest repeatedly.
- Parse and validate manifest lines.
- Process all manifest entries (not a limited subset).
- Deterministically order work by:
  - `slice_ts`
  - file type order (`events`, `mentions`, `articles`)
  - URL (stable tie-break)
- Download each file using the exact URL from the manifest.
- Verify payload integrity:
  - byte length check
  - SHA1 check
- Open ZIP and parse CSV rows in memory.
- Keep a `(slice_ts, file_type)` seen set to avoid reprocessing.
- Emit structured JSON logs for observability.

Out of scope by design (not implemented yet):
- Any database writes
- Any durable checkpoint persistence

### 3) Time and Lag Handling (Implemented)
- Timestamp parsing supports:
  - compact format: `YYYYMMDDHHMMSS`
  - ISO format with `Z`
- Timestamps are normalized to UTC-aware datetimes.
- Poller fetches `/simulated_now` and logs `lag_seconds` per processed result.

### 5) Transient Failure Retry and Backoff (Implemented)
- Poller now retries transient network/HTTP failures for:
  - manifest polling
  - slice file downloads
- Retry policy:
  - exponential backoff
  - defaults: `max_retries=2`, `backoff_initial_seconds=0.2`, `backoff_max_seconds=2.0`
  - transient statuses: `429, 500, 502, 503, 504`
- Retry behavior is configurable via CLI flags:
  - `--max-retries`
  - `--backoff-initial-seconds`
  - `--backoff-max-seconds`

### 4) Data Model Definition (Implemented)
File: `src/schema.sql`

Current status:
- DDL created for `events`, `mentions`, `articles`.
- Indexes added.
- `mentions` uses composite primary key:
  - `(event_id, mention_time, source_domain, slice_ts)`

## Verification and Tests

### Runtime Verification Completed
- Live smoke runs validated:
  - deterministic ordering behavior
  - successful integrity checks
  - lag logging with parse/timezone fixes

### Automated Tests Added
File: `tests/test_poller.py`

Coverage currently includes:
- deterministic ordering from unsorted manifest input
- timestamp parsing for compact and ISO formats
- lag check logging and lag value calculation
- guardrail test confirming no DB-driver imports in poller phase
- retry on transient manifest request failures
- retry on transient file download HTTP statuses

Latest known result:
- `python -m pytest -q tests/test_poller.py` passed (`6 passed`)
- Project-level pytest configuration now supports a plain repo-root `python -m pytest -q` run.
- `make test` was added as the canonical test workflow entrypoint.

## What Is Left To Do

## Phase A: Stabilization and Workflow
1. Optionally run a longer soak test and capture lag trend logs.
2. Add CI coverage if/when a CI pipeline is introduced.

## Phase B: Persistence Layer (Next Major Phase)
1. Add DB insert/upsert path for `events`, `mentions`, `articles`.
2. Add idempotent write semantics aligned with table keys.
3. Define transaction boundaries and failure handling strategy.
4. Add durable checkpointing so restart behavior is explicit.

## Phase C: Operational Hardening
1. Add metrics and alert thresholds (lag, parse errors, verify failures).
2. Add runbook notes for outage modes (503, malformed manifest, partial slices).
3. Add end-to-end tests covering poll -> parse -> persist flow.

## Recommended Immediate Next Step
Run a longer vendor soak and review lag/error logs, then begin Phase B when persistence scope is approved.