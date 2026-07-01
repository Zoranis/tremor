# Tremor Operational Runbook

## Purpose
Quick operator actions for overnight incidents in the ingestion pipeline.

## 1) Vendor outage (503 on manifest and file endpoints)
What to watch:
- Repeated manifest_poll errors
- poll_metrics shows manifest_poll_error_count > 0

Immediate actions:
1. Confirm vendor health: curl -s http://localhost:18200/healthz
2. Confirm outage counters: curl -s http://localhost:18200/stats
3. Keep poller running; retry/backoff is built in.
4. If poller is down, restart it and verify logs resume.

Recovery check:
1. manifest_poll returns ok again.
2. processed_file_count rises in poll_metrics.
3. lag_seconds trends downward after recovery.

## 2) Malformed manifest or validation errors
What to watch:
- manifest_parse invalid_line warnings
- manifest_validate missing_file_type warnings

Immediate actions:
1. Inspect the latest manifest: curl -s http://localhost:18200/v2/lastupdate.txt
2. Confirm lines match: <bytes> <sha1> <url>
3. Keep poller running; it skips invalid lines and continues.

Recovery check:
1. Next valid manifest is parsed.
2. file_process parsed_rows events resume.

## 3) Partial slice (one file type missing)
What to watch:
- manifest_validate missing_file_type for a slice_ts
- One file type flat while others continue

Immediate actions:
1. Do not backfill manually yet; provider may publish missing file later.
2. Keep poller running so late files are consumed automatically.

Recovery check:
1. Missing file type appears in later manifest.
2. Matching slice_ts is processed without duplicate writes.

## 4) Restart and idempotency check
When to run:
- After poller restart or host restart

Steps:
1. Restart poller process.
2. Verify checkpoint preload is logged (db_checkpoint loaded).
3. Verify already-completed files are skipped (file_process skip_seen).

Pass criteria:
- No duplicate row growth for already-completed slices.
- New slices continue processing normally.

## Escalation trigger
Escalate to engineering if any condition persists longer than 15 wall-minutes:
- Continuous manifest errors
- No processed files while vendor is healthy
- Lag increasing continuously after vendor recovery

## Soak evidence capture
Run this when preparing defense evidence or validating overnight stability:

1. Start vendor and poller in normal mode.
2. Capture a baseline soak:
	- make soak-capture
3. Run a chaos window and capture again:
	- make vendor-chaos
	- make soak-capture
	- make vendor-calm

Artifacts are written to data/evidence/<run_id>/ with:
- timeline.jsonl (sample-by-sample data)
- summary.json (aggregates)
- notes.md (operator interpretation template)

## Alert threshold baseline (2026-07-01 evidence)
From 30-minute persistence-enabled calm and chaos captures:
- Calm (`20260701T100127Z`): 1 startup transient manifest error sample.
- Chaos (`20260701T103128Z`): 5 manifest error samples during outage windows.

Recommended starting thresholds:
1. `--alert-manifest-error-threshold 3`
2. `--alert-lag-seconds-threshold 1800`

Rationale:
- Threshold `3` suppresses one-off startup/transient failures while still firing during sustained outage windows.
- Lag threshold remains conservative until additional long windows with active file processing are captured.

Execution note:
- Prefer running `tools/run_soak_matrix.sh` from WSL instead of embedding a long quoted one-liner in PowerShell; this avoids quote-escape failures.
