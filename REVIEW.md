# Review of the current Tremor ingestion/dashboard/SQL solution

Written after reading `BRIEF.md`, all other project `.md` docs, and the
actual implementation (poller, storage, schema, dashboard, vendor mock,
compose/Makefile, SQL query pack, tests, and archived soak evidence).

## Bottom line

The ingestion core (manifest polling, integrity checks, ordering, upserts,
checkpointing, retry/backoff, transition-based alerting) is well built and
I agree with most of the design decisions documented in
`ARCHITECTURE_BRIEF.md`. But `ARCHITECTURE.md` / `PROGRESS.md` claiming
**100% complete / all finish-line items done** is not accurate. Tracing the
actual runtime behavior (not just reading the code in isolation) surfaces
a critical, demo-breaking bug in the exact configuration `make run` ships,
plus a live-defense-breaking bug in two of the six required SQL queries.
Both slipped through because the soak evidence tested a *different*
configuration than the one that actually ships by default.

---

## Critical findings

### 1. Default `make run` config silently drops ~99% of slices, with no alert

- `compose.yml`'s `ingest-poller` runs with `--poll-profile ${INGEST_POLL_PROFILE:-prod}`,
  and `.env` sets `INGEST_POLL_PROFILE=prod` → poll interval is 60s
  (`src/ingest/poller.py:1155-1157`).
- The vendor's default `REPLAY_SECONDS_PER_SLICE=0.45` (`.env`, `vendor/app/clock.py:24`)
  advances the simulated clock by one new 15-minute slice every **0.45
  wall-seconds**.
- `/v2/lastupdate.txt` only ever advertises the *single current* slice
  (`vendor/app/main.py:_current_slice_ts`, backed by
  `storage.nearest_available_slice`) — there is no backlog of unfetched
  slices in the manifest. Once the clock moves on, the previous slice's
  manifest entry is gone forever; the raw file is still servable via
  direct `GET /v2/{slice_ts}.{type}.csv.zip`, but the poller never learns
  that slice_ts existed because it only reacts to what the manifest lists.
- Net effect: between two consecutive 60s polls, ~133 slices pass
  (60 / 0.45 ≈ 133), and the poller only ever sees and ingests **1 of
  them**. That's roughly 99% data loss under the exact command specified
  as the operational baseline in `BRIEF.md` ("must come up with a single
  command... `make run`").
- **Nothing catches this.** Lag stays near-zero because the poller always
  grabs whatever slice is "current" at poll time (`lag_seconds` in
  `poll_once` compares against `simulated_now`, not against how many
  slices were skipped). The stale-manifest detector
  (`_emit_alert_events` / `alert_state["manifest_repeat_count"]` in
  `src/ingest/poller.py`) only fires when the **same** `slice_ts` repeats
  across polls — it has no notion of a **skipped** slice_ts. The dashboard's
  `v_slice_lag` view (`src/schema.sql`) is built from the same
  `ingest_slice_status` rows and inherits the same blind spot. So the
  overnight producer's screen would show green the entire time while the
  pipeline quietly ingests essentially none of the feed — the exact
  failure mode `BRIEF.md` warns about ("the 06:30 brief goes out blind").
- **Why the soak evidence didn't catch this:** `tools/run_soak_matrix.sh`
  (used to produce the archived evidence under `data/evidence/calm/` and
  `data/evidence/chaos/`) launches the poller with
  `--interval-seconds 0.45` — i.e. dev-speed polling matched to dev-speed
  replay. That's a different configuration than what `compose.yml`/`make
  run` actually deploys (`--poll-profile prod` → 60s). The 30-minute calm
  and chaos soak runs cited as evidence of correctness never exercised the
  shipped default.
- **Suggested fix direction:** either (a) have the vendor's manifest
  endpoint return a bounded backlog of not-yet-served slices instead of
  only the current one, or (b) make the poller compute the full expected
  `slice_ts` sequence between the last-seen and current manifest slice and
  backfill each one directly via `GET /v2/{slice_ts}.{type}.csv.zip`
  (the files remain servable even when unlisted), or (c) at minimum, size
  `REPLAY_SECONDS_PER_SLICE` and poll interval so they can't diverge like
  this, and add a dashboard/alert signal for "manifest slice_ts jumped by
  more than one slice between polls."

### 2. Two of the six required defense SQL queries return zero rows when run live

- `sql/queries/protest_hotspots.sql` filters on
  `event_time >= date_trunc('day', NOW() - INTERVAL '1 day')` (i.e.
  "yesterday"), and `sql/queries/theme_surge.sql` filters on
  `NOW() - INTERVAL '24 hours'` / `NOW() - INTERVAL '8 days'`.
- `NOW()` in Postgres returns the real wall-clock date. But every ingested
  `event_time`/`mention_time` is anchored to the simulated replay window —
  `.env` defaults to `REPLAY_WINDOW_START=2024-01-01`,
  `REPLAY_WINDOW_END=2024-01-08` (`vendor/app/clock.py:25-26`). Unless that
  window happens to be set to actually straddle today's real date, `NOW()`
  and the data's timestamps are years apart.
- `BRIEF.md` is explicit: **"you will provide the SQL queries... and run
  them live against your system."** As written, `protest_hotspots.sql` and
  `theme_surge.sql` will return empty result sets in that live run unless
  the replay window is manually re-pointed at "yesterday relative to
  today" right before the defense — which is fragile and easy to forget,
  especially since `REPLAY_LOOP=true` keeps looping the same fixed
  historical window rather than tracking real time.
- **Suggested fix direction:** derive "now" for these queries from the
  data itself (e.g. `(SELECT MAX(event_time) FROM events)` or a small
  `v_simulated_now` view fed from the vendor's `/simulated_now`) instead of
  Postgres `NOW()`, so the queries are correct regardless of which
  calendar window is being replayed.

### 3. No backfill/gap-recovery after `ingest-poller` downtime

- Because the manifest only ever exposes the current slice (see #1), any
  slice published while the `ingest-poller` container itself is
  restarted, crashed, or otherwise down is never retried — checkpoints
  (`ingest_checkpoints`, `load_seen()`) only dedupe what was already
  fetched; there's no reconciliation pass that walks the known slice_ts
  range and backfills anything missed while the poller wasn't running.
- This directly contradicts the BRIEF's operational baseline: **"survive a
  `docker compose restart` of any one service without losing data,
  dropping a slice, or requiring a human to babysit it back to a healthy
  state."**
- `tools/validate_restart_safety.py` — the tool that's supposed to prove
  this — only restarts the `dashboard` service and checks that `/healthz`
  and `/metrics` return 200. It never restarts `ingest-poller` and never
  asserts slice continuity (e.g. "no gap in `ingest_slice_status` before
  vs. after"), so this gap isn't exercised by the existing validation
  either.
- **Suggested fix direction:** same backfill mechanism as #1 would also
  close this — on startup, compare the last durable checkpoint's slice_ts
  against the vendor's current slice_ts and walk forward through the gap
  by direct file GETs.

---

## Secondary findings

### 4. Outage-alert 60s SLA has no margin against the 60s poll interval

`BRIEF.md` requires the outage alert to fire within 60 wall-seconds of the
first 503. The `prod` profile polls exactly every 60s
(`src/ingest/poller.py:1155-1157`), with threshold auto-tuned to 1 so the
very next poll fires the alert (`main():1159-1161`). But in the worst case
— outage starts just after a poll succeeds — the next poll is ~60s later,
plus whatever time `_get_with_retry`'s backoff adds before that poll gives
up (up to a couple seconds with defaults). That can push detection past
the 60s bound with no safety margin at all. A dedicated lighter-weight
health check on a faster cadence (or simply polling faster than the SLA
threshold, e.g. 15-20s) would give real margin.

### 5. `geographic_overlay.sql` / `theme_surge.sql` join on `slice_ts` as a stand-in for a real relationship

Per the schema, `articles` has no lat/lon (only `location_country`), and
`mentions` has no link to `articles` (it only joins to `events` via
`event_id`). Both queries work around this by joining on `slice_ts` (a
15-minute time bucket) instead of a real spatial or entity key:

- `geographic_overlay.sql` joins `events e JOIN articles a ON
  a.slice_ts = e.slice_ts`, which credits every event's lat/lon bucket
  with a cross-product against every protest article published in that
  same 15-minute slice — not articles actually about that location.
- `theme_surge.sql` joins `articles a JOIN mentions m ON m.slice_ts =
  a.slice_ts` for the same reason (acknowledged in
  `sql/queries/README.md`).

Both produce a multiplicative, not additive, count — the numbers will
likely look implausible (or wildly skewed by whichever slice happened to
have a lot of articles) when run live. This is a real data-model gap
(articles genuinely don't carry the geo/entity keys the brief's questions
5 and 3 imply), so I'd flag it explicitly rather than let the "which
outlet over-amplified" style scrutiny discover it live. A defensible
fallback for #5 would be bucketing by `location_country` (which both
`events` and `articles` do have) instead of lat/lon.

### 6. Minor

- `sql/queries/outlet_amplification.sql` extracts a domain from
  `events.source_url` via `regexp_replace(split_part(source_url, '/', 3),
  '^www\\.', '')` but never lowercases it before joining to
  `mentions.source_domain` (which the spec guarantees is already
  lowercased). Any capitalization difference in `source_url`'s host silently
  drops that domain from the join — undercounting rather than erroring.
  Trivial fix: wrap in `lower(...)`.
- `src/ingest/storage.py`'s `_insert_events`/`_insert_mentions`/`_insert_articles`
  issue one `cur.execute()` per row instead of `executemany`/batched
  `COPY`. Fine at current slice sizes (tens to low hundreds of rows), but
  worth a note if replay speed or slice size ever grows — not urgent.

---

## What I agree with

- The core poller design (`src/ingest/poller.py`) is careful: deterministic
  processing order, byte+SHA1 verification as a hard gate before parsing,
  DB-optional architecture (pure ingestion engine + pluggable persist
  callback), and exponential backoff on transient HTTP/network errors are
  all sound choices, well tested in `tests/test_poller.py`.
- Idempotency design is solid: `(slice_ts, file_type)` as the natural
  dedup unit, upserts keyed appropriately per table
  (`event_id`; composite `(event_id, mention_time, source_domain,
  slice_ts)` for mentions; `article_id`), and durable checkpoints that
  preload on restart.
- Dropping the strict FK from `mentions.event_id` to `events.event_id` is
  the right call given the vendor's non-atomic per-file-type publishing —
  well justified in `ARCHITECTURE_BRIEF.md` section G.
- Alerting is transition-based (fires once, clears once) rather than
  spamming every poll — good operational hygiene for a tired 03:00 human.
- Schema indexing (`slice_ts`, `event_type + location_country`,
  `primary_theme + article_time`) matches the query shapes the six
  analyst questions actually need.

---

## Priority if picking a next step

1. Fix #1 (slice-skip data loss) — this is the one that would be caught
   immediately by anyone running `make run` as documented and comparing
   `slices_served` on the vendor's `/healthz` against rows actually landing
   in Postgres.
2. Fix #2 (`NOW()` vs. simulated time) — five-minute fix, but a live-defense
   embarrassment if missed.
3. Fix #3 (restart backfill) — same root cause/fix as #1, do them together.
4. Everything else is real but lower stakes than the above three.
