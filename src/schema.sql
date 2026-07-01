CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    event_time TIMESTAMPTZ,
    actor_country CHAR(3),
    target_country CHAR(3),
    event_type TEXT,
    intensity REAL,
    location_country CHAR(3),
    location_lat REAL,
    location_lon REAL,
    source_url TEXT,
    slice_ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mentions (
    event_id INTEGER NOT NULL,
    mention_time TIMESTAMPTZ,
    source_domain TEXT,
    tone REAL,
    slice_ts TEXT NOT NULL,
    PRIMARY KEY (event_id, mention_time, source_domain, slice_ts)
);

CREATE TABLE IF NOT EXISTS articles (
    article_id TEXT PRIMARY KEY,
    article_time TIMESTAMPTZ,
    source_domain TEXT,
    primary_theme TEXT,
    location_country CHAR(3),
    slice_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_slice_ts ON events (slice_ts);
CREATE INDEX IF NOT EXISTS idx_mentions_slice_ts ON mentions (slice_ts);
CREATE INDEX IF NOT EXISTS idx_articles_slice_ts ON articles (slice_ts);

CREATE INDEX IF NOT EXISTS idx_mentions_event_id ON mentions (event_id);

CREATE INDEX IF NOT EXISTS idx_events_event_type_location_country ON events (event_type, location_country);

CREATE INDEX IF NOT EXISTS idx_articles_primary_theme_article_time ON articles (primary_theme, article_time);

CREATE TABLE IF NOT EXISTS ingest_manifest_polls (
    id BIGSERIAL PRIMARY KEY,
    polled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status_code INTEGER,
    success BOOLEAN NOT NULL,
    latency_ms DOUBLE PRECISION,
    line_count INTEGER NOT NULL DEFAULT 0,
    manifest_latest_slice TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_manifest_polls_polled_at ON ingest_manifest_polls (polled_at DESC);

CREATE TABLE IF NOT EXISTS ingest_file_attempts (
    id BIGSERIAL PRIMARY KEY,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    slice_ts TEXT NOT NULL,
    file_type TEXT NOT NULL,
    url TEXT,
    status_code INTEGER,
    success BOOLEAN NOT NULL,
    outcome TEXT NOT NULL,
    latency_ms DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ingest_file_attempts_attempted_at ON ingest_file_attempts (attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_ingest_file_attempts_slice_file ON ingest_file_attempts (slice_ts, file_type);

CREATE TABLE IF NOT EXISTS ingest_alert_events (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_name TEXT NOT NULL,
    file_type TEXT,
    state TEXT NOT NULL,
    value DOUBLE PRECISION,
    threshold DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ingest_alert_events_observed_at ON ingest_alert_events (observed_at DESC);

CREATE TABLE IF NOT EXISTS ingest_slice_status (
    slice_ts TEXT PRIMARY KEY,
    manifest_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    events_done BOOLEAN NOT NULL DEFAULT FALSE,
    mentions_done BOOLEAN NOT NULL DEFAULT FALSE,
    articles_done BOOLEAN NOT NULL DEFAULT FALSE,
    fully_processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingest_slice_status_fully_processed_at ON ingest_slice_status (fully_processed_at DESC);

CREATE TABLE IF NOT EXISTS ingest_degraded_windows (
    id BIGSERIAL PRIMARY KEY,
    degraded_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_ingest_degraded_windows_type_active ON ingest_degraded_windows (degraded_type, active);
CREATE INDEX IF NOT EXISTS idx_ingest_degraded_windows_started_at ON ingest_degraded_windows (started_at DESC);

CREATE OR REPLACE VIEW v_slice_completion AS
SELECT
    slice_ts,
    manifest_seen_at,
    events_done,
    mentions_done,
    articles_done,
    fully_processed_at,
    (events_done AND mentions_done AND articles_done) AS is_fully_processed
FROM ingest_slice_status;

CREATE OR REPLACE VIEW v_slice_lag AS
WITH latest_manifest AS (
    SELECT MAX(slice_ts) AS manifest_latest_slice FROM ingest_slice_status
),
latest_fully AS (
    SELECT MAX(slice_ts) AS latest_fully_processed_slice
    FROM ingest_slice_status
    WHERE fully_processed_at IS NOT NULL
)
SELECT
    latest_manifest.manifest_latest_slice,
    latest_fully.latest_fully_processed_slice,
    CASE
        WHEN latest_manifest.manifest_latest_slice IS NULL OR latest_fully.latest_fully_processed_slice IS NULL THEN NULL
        ELSE EXTRACT(
            EPOCH FROM (
                to_timestamp(latest_manifest.manifest_latest_slice, 'YYYYMMDDHH24MISS')
                - to_timestamp(latest_fully.latest_fully_processed_slice, 'YYYYMMDDHH24MISS')
            )
        )
    END AS lag_seconds,
    CASE
        WHEN latest_manifest.manifest_latest_slice IS NULL OR latest_fully.latest_fully_processed_slice IS NULL THEN NULL
        ELSE EXTRACT(
            EPOCH FROM (
                to_timestamp(latest_manifest.manifest_latest_slice, 'YYYYMMDDHH24MISS')
                - to_timestamp(latest_fully.latest_fully_processed_slice, 'YYYYMMDDHH24MISS')
            )
        ) / 900.0
    END AS lag_slices
FROM latest_manifest
CROSS JOIN latest_fully;

CREATE OR REPLACE VIEW v_ingestion_rate_5m_by_type AS
SELECT
    file_type,
    COUNT(DISTINCT slice_ts) FILTER (WHERE attempted_at >= NOW() - INTERVAL '5 minutes' AND success) AS slices_5m,
    COUNT(DISTINCT slice_ts) FILTER (WHERE attempted_at >= NOW() - INTERVAL '5 minutes' AND success) / 5.0 AS slices_per_min_5m
FROM ingest_file_attempts
GROUP BY file_type;

CREATE OR REPLACE VIEW v_manifest_poll_health AS
WITH recent AS (
    SELECT *
    FROM ingest_manifest_polls
    ORDER BY polled_at DESC
    LIMIT 120
)
SELECT
    COUNT(*) AS polls_n,
    COALESCE(AVG(CASE WHEN success THEN 1.0 ELSE 0.0 END), 0.0) AS success_rate,
    COALESCE(AVG(latency_ms), 0.0) AS avg_latency_ms,
    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms), 0.0) AS p95_latency_ms,
    COALESCE(SUM(CASE WHEN status_code = 503 THEN 1 ELSE 0 END), 0) AS outage_503_count
FROM recent;

CREATE OR REPLACE VIEW v_pipeline_self_audit AS
WITH totals AS (
    SELECT
        COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at))), 0.0) AS degraded_seconds,
        COALESCE(MIN(started_at), NOW()) AS min_t,
        COALESCE(MAX(COALESCE(ended_at, NOW())), NOW()) AS max_t
    FROM ingest_degraded_windows
    WHERE degraded_type = 'stale_manifest'
),
window_span AS (
    SELECT EXTRACT(EPOCH FROM (max_t - min_t)) AS observed_seconds
    FROM totals
)
SELECT
    totals.degraded_seconds,
    window_span.observed_seconds,
    CASE
        WHEN window_span.observed_seconds <= 0 THEN 0.0
        ELSE totals.degraded_seconds / window_span.observed_seconds
    END AS degraded_fraction
FROM totals, window_span;
