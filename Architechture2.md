# Architecture2: Missing Requirements Implementation Plan

Date: 2026-07-01
Source of truth compared: BRIEF.md vs current repository implementation/docs.

## Missing Items (Gap List)

1. Operational dashboard is not implemented.
- No always-on UI service exists in compose.
- No dashboard panels for the required four operator signals.

2. Required dashboard metrics are not fully computed as first-class data.
- Slice lag is not tracked as "latest manifest slice minus latest fully processed slice (all 3 file types)".
- Rolling 5-minute ingestion rate per file type is not materialized for visualization.
- Manifest poll health (success rate + latency over last N polls) is not exposed as a dashboard-ready metric endpoint/table.
- Outage alert SLA (fire within 60s of first 503, clear within 60s of recovery) is not explicitly implemented/tested against wall-clock timing.

3. Analyst SQL surface deliverables are missing.
- No shipped SQL query pack for the six defense questions.
- No curated SQL views/materialized views to simplify analyst use.
- No documented/validated query runtime targets for editorial-meeting iteration.

4. Pipeline self-audit query support is incomplete.
- Stale-manifest degraded windows are not durably persisted in DB for direct SQL auditing.
- No audit table/view proving degraded fraction over a chosen window.

5. Full system operational baseline is not complete.
- make run currently starts vendor services only, not a full newsroom stack (ingest + storage + dashboard).
- Compose does not yet include Postgres, ingest runner, and dashboard service as integrated production path.
- "Restart any one service without data loss or dropped slice" is not validated for full multi-service stack.

6. Brief-required polling policy mismatch.
- Brief expectation is poll manifest once per minute; current poller defaults to 0.45s interval.
- Need explicit "production profile" config matching brief while preserving fast-dev profile.

7. Evidence package for missing requirements is incomplete.
- No archived dashboard screenshots/export.
- No automated evidence for outage-alert timing SLA and dashboard refresh continuity.

## Architecture2 Plan for Missing Implementations

## Phase 1: Complete Data/Metric Contract

Goal: define canonical data needed by dashboard + SQL surface.

Deliverables:
1. Add new DB tables for pipeline observability:
- ingest_manifest_polls
- ingest_file_attempts
- ingest_alert_events
- ingest_slice_status (per slice_ts with events/mentions/articles completion flags)
- ingest_degraded_windows (stale_manifest, outage)

2. Add views for operator/analyst convenience:
- v_slice_completion
- v_slice_lag
- v_ingestion_rate_5m_by_type
- v_manifest_poll_health
- v_pipeline_self_audit

3. Persist stale-manifest detection as durable events (not logs only).

Acceptance criteria:
- Every manifest poll and file attempt produces DB-observable telemetry.
- Slice "fully processed" can be computed from DB state alone.

## Phase 2: Implement Operational Dashboard

Goal: always-on single-screen dashboard for overnight producer.

Deliverables:
1. Add dashboard service in compose (recommended: Grafana + Prometheus exporter, or lightweight Streamlit backed by SQL).
2. Panels required by brief:
- Slice lag with clear red threshold.
- 5-minute ingestion rate per file type (events/mentions/articles).
- Manifest poll health (success rate and latency over last N polls).
- Outage alert status timeline with active/cleared markers.
3. Auto-refresh every 5-10s and resilient restart behavior.

Acceptance criteria:
- Dashboard runs continuously without manual refresh.
- During chaos outage, alert appears <=60s after first 503 and clears <=60s after recovery.

## Phase 3: Implement Analyst SQL Surface

Goal: provide direct SQL that producers can run live for editorial questions.

Deliverables:
1. Create sql/queries/ with six required queries:
- protest_hotspots.sql
- bilateral_trend.sql
- theme_surge.sql
- outlet_amplification.sql
- geographic_overlay.sql
- pipeline_self_audit.sql

2. Add optional helper views/materialized views for speed and readability.
3. Add SQL README explaining parameters and expected output columns.

Acceptance criteria:
- Each query runs against current DB schema and returns non-empty results on replay data.
- Query pack can be executed live without data-engineer intervention.

## Phase 4: Operational Baseline Integration

Goal: one-command full stack startup and restart safety.

Deliverables:
1. Extend compose.yml with:
- postgres
- ingest service (poller)
- dashboard service
2. Update Makefile:
- make run brings up full stack.
- make run-dev (fast replay profile) and make run-prod-profile (1-minute polling).
3. Add healthchecks and startup dependencies.

Acceptance criteria:
- Fresh machine: make run yields healthy end-to-end system.
- docker compose restart of any single service recovers automatically with no duplicate or dropped slices.

## Phase 5: Tests and Evidence for Defense

Goal: prove missing requirements are truly complete.

Deliverables:
1. Add tests:
- outage alert fire/clear timing SLA tests
- stale-manifest persistence tests
- slice completion correctness under partial/late slices
- restart/idempotency for full stack path
2. Add evidence artifacts:
- calm + chaos dashboard screenshots/exports
- SLA timing report for outage alerts
- SQL query outputs for all six required questions

Acceptance criteria:
- Evidence folder contains reproducible artifacts aligned to BRIEF definition of done.

## Suggested File Additions

- src/ingest/metrics.py
- src/ingest/audit.py
- src/schema_observability.sql
- sql/queries/protest_hotspots.sql
- sql/queries/bilateral_trend.sql
- sql/queries/theme_surge.sql
- sql/queries/outlet_amplification.sql
- sql/queries/geographic_overlay.sql
- sql/queries/pipeline_self_audit.sql
- dashboards/ (provisioning JSON or app code)

## Recommended Execution Order (1-week sprint)

1. Day 1-2: observability schema + durable telemetry + stale-manifest persistence.
2. Day 3: dashboard service and required four panels.
3. Day 4: analyst SQL query pack + helper views.
4. Day 5: compose/make integration for one-command run + restart tests.
5. Day 6-7: chaos validation, evidence capture, documentation alignment.

## Definition of Done for Architecture2

All BRIEF gaps above are closed when:
- dashboard is live and auto-refreshing,
- six SQL questions are runnable and documented,
- outage/stale states are queryable and evidenced,
- make run starts full stack,
- restart safety is demonstrated with archived proof.
