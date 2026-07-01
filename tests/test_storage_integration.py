import os

import pytest

psycopg = pytest.importorskip("psycopg")
connect = psycopg.connect

from src.ingest.storage import PersistableResult, PostgresIngestStore


def _test_dsn() -> str:
    dsn = os.getenv("TREMOR_TEST_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not dsn:
        pytest.skip("set TREMOR_TEST_DATABASE_URL (or DATABASE_URL) to run Postgres integration tests")
    return dsn


def _reset_tables(dsn: str) -> None:
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE ingest_checkpoints, mentions, articles, events RESTART IDENTITY CASCADE")


@pytest.fixture
def pg_store():
    dsn = _test_dsn()
    print(f"[pg_store] opening store for DSN: {dsn}")
    store = PostgresIngestStore(dsn=dsn)
    store.open()
    print("[pg_store] resetting tables before test")
    _reset_tables(dsn)
    try:
        yield store, dsn
    finally:
        print("[pg_store] closing store")
        store.close()
        print("[pg_store] resetting tables after test")
        _reset_tables(dsn)


def test_persist_events_upserts_and_updates_checkpoint(pg_store):
    store, dsn = pg_store

    initial = PersistableResult(
        slice_ts="20240101100000",
        file_type="events",
        rows=[
            {
                "event_id": "12345",
                "event_time": "2024-01-01T10:00:00Z",
                "actor_country": "USA",
                "target_country": "CHN",
                "event_type": "statement",
                "intensity": "1.5",
                "location_country": "USA",
                "location_lat": "40.71",
                "location_lon": "-74.01",
                "source_url": "https://example.com/a",
            }
        ],
    )

    update = PersistableResult(
        slice_ts="20240101101500",
        file_type="events",
        rows=[
            {
                "event_id": "12345",
                "event_time": "2024-01-01T10:15:00Z",
                "actor_country": "USA",
                "target_country": "CHN",
                "event_type": "agreement",
                "intensity": "3.0",
                "location_country": "USA",
                "location_lat": "40.71",
                "location_lon": "-74.01",
                "source_url": "https://example.com/b",
            }
        ],
    )

    store.persist(initial)
    store.persist(update)

    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT event_type, intensity, source_url, slice_ts FROM events WHERE event_id = 12345")
            row = cur.fetchone()
            assert row == ("agreement", 3.0, "https://example.com/b", "20240101101500")

    seen = store.load_seen()
    assert ("20240101100000", "events") in seen
    assert ("20240101101500", "events") in seen


def test_persist_mentions_articles_and_load_seen(pg_store):
    store, dsn = pg_store

    store.persist(
        PersistableResult(
            slice_ts="20240101100000",
            file_type="events",
            rows=[
                {
                    "event_id": "222",
                    "event_time": "2024-01-01T10:00:00Z",
                    "actor_country": "FRA",
                    "target_country": "DEU",
                    "event_type": "protest",
                    "intensity": "-2.0",
                    "location_country": "FRA",
                    "location_lat": "48.85",
                    "location_lon": "2.35",
                    "source_url": "https://example.com/event",
                }
            ],
        )
    )

    store.persist(
        PersistableResult(
            slice_ts="20240101100000",
            file_type="mentions",
            rows=[
                {
                    "event_id": "222",
                    "mention_time": "2024-01-01T10:01:00Z",
                    "source_domain": "news.example",
                    "tone": "-5.5",
                }
            ],
        )
    )

    store.persist(
        PersistableResult(
            slice_ts="20240101100000",
            file_type="articles",
            rows=[
                {
                    "article_id": "20240101100000-0",
                    "article_time": "2024-01-01T10:00:30Z",
                    "source_domain": "news.example",
                    "primary_theme": "protest",
                    "location_country": "FRA",
                }
            ],
        )
    )

    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mentions")
            mention_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM articles")
            article_count = cur.fetchone()[0]

    assert mention_count == 1
    assert article_count == 1

    seen = store.load_seen()
    assert ("20240101100000", "events") in seen
    assert ("20240101100000", "mentions") in seen
    assert ("20240101100000", "articles") in seen


def test_persist_rolls_back_on_invalid_row(pg_store):
    store, dsn = pg_store

    with pytest.raises(Exception):
        store.persist(
            PersistableResult(
                slice_ts="20240101100000",
                file_type="mentions",
                rows=[
                    {
                        "event_id": None,
                        "mention_time": "2024-01-01T10:01:00Z",
                        "source_domain": "broken.example",
                        "tone": "0.0",
                    }
                ],
            )
        )

    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM ingest_checkpoints WHERE slice_ts = %s AND file_type = %s",
                ("20240101100000", "mentions"),
            )
            checkpoint_count = cur.fetchone()[0]

    assert checkpoint_count == 0


def test_restart_load_seen_prevents_duplicate_writes(pg_store):
    store, dsn = pg_store

    first_result = PersistableResult(
        slice_ts="20240101103000",
        file_type="events",
        rows=[
            {
                "event_id": "9001",
                "event_time": "2024-01-01T10:30:00Z",
                "actor_country": "USA",
                "target_country": "CAN",
                "event_type": "statement",
                "intensity": "1.0",
                "location_country": "USA",
                "location_lat": "38.90",
                "location_lon": "-77.04",
                "source_url": "https://example.com/9001-a",
            }
        ],
    )

    store.persist(first_result)
    store.close()

    restarted = PostgresIngestStore(dsn=dsn)
    restarted.open()
    try:
        seen = restarted.load_seen()
        assert ("20240101103000", "events") in seen

        # Simulate poller restart behavior: if checkpoint is already seen,
        # skip persisting duplicate slice/file payloads.
        if ("20240101103000", "events") not in seen:
            restarted.persist(first_result)
    finally:
        restarted.close()

    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM events WHERE event_id = 9001")
            row_count = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM ingest_checkpoints WHERE slice_ts = %s AND file_type = %s",
                ("20240101103000", "events"),
            )
            checkpoint_count = cur.fetchone()[0]

    assert row_count == 1
    assert checkpoint_count == 1


def test_compact_checkpoints_retains_latest_per_file_type(pg_store):
    store, dsn = pg_store

    # Create done checkpoints across multiple file types/slices.
    for slice_ts in ["20240101100000", "20240101101500", "20240101103000"]:
        event_id = int(slice_ts[-4:])
        store.persist(
            PersistableResult(
                slice_ts=slice_ts,
                file_type="events",
                rows=[
                    {
                        "event_id": str(event_id),
                        "event_time": "2024-01-01T10:00:00Z",
                        "actor_country": "USA",
                        "target_country": "MEX",
                        "event_type": "statement",
                        "intensity": "1.0",
                        "location_country": "USA",
                        "location_lat": "34.05",
                        "location_lon": "-118.24",
                        "source_url": f"https://example.com/events-{slice_ts}",
                    }
                ],
            )
        )

        store.persist(
            PersistableResult(
                slice_ts=slice_ts,
                file_type="articles",
                rows=[
                    {
                        "article_id": f"article-{slice_ts}",
                        "article_time": "2024-01-01T10:00:30Z",
                        "source_domain": "news.example",
                        "primary_theme": "theme",
                        "location_country": "USA",
                    }
                ],
            )
        )

    deleted_count = store.compact_checkpoints(retain_per_file_type=1)
    assert deleted_count == 4

    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT slice_ts, file_type
                FROM ingest_checkpoints
                ORDER BY file_type, slice_ts
                """
            )
            remaining = cur.fetchall()

    assert remaining == [
        ("20240101103000", "articles"),
        ("20240101103000", "events"),
    ]


def _seed_brief_dataset(store: PostgresIngestStore) -> None:
    print("[brief-seed] preparing seed rows")
    rows_events = [
        {
            "event_id": "1001",
            "event_time": "2024-01-01T06:00:00Z",
            "actor_country": "USA",
            "target_country": "CHN",
            "event_type": "protest",
            "intensity": "-4.0",
            "location_country": "THA",
            "location_lat": "13.75",
            "location_lon": "100.50",
            "source_url": "https://example.com/e1",
        },
        {
            "event_id": "1002",
            "event_time": "2024-01-01T08:00:00Z",
            "actor_country": "USA",
            "target_country": "CHN",
            "event_type": "clash",
            "intensity": "-6.0",
            "location_country": "THA",
            "location_lat": "13.20",
            "location_lon": "100.10",
            "source_url": "https://example.com/e2",
        },
        {
            "event_id": "1003",
            "event_time": "2024-01-02T06:00:00Z",
            "actor_country": "USA",
            "target_country": "CHN",
            "event_type": "agreement",
            "intensity": "3.0",
            "location_country": "USA",
            "location_lat": "40.00",
            "location_lon": "-75.00",
            "source_url": "https://example.com/e3",
        },
    ]

    rows_mentions = [
        {
            "event_id": "1001",
            "mention_time": "2024-01-01T06:05:00Z",
            "source_domain": "desk.example",
            "tone": "-2.0",
        },
        {
            "event_id": "1001",
            "mention_time": "2024-01-01T06:06:00Z",
            "source_domain": "desk.example",
            "tone": "-1.0",
        },
        {
            "event_id": "1002",
            "mention_time": "2024-01-01T08:10:00Z",
            "source_domain": "wire.example",
            "tone": "-4.0",
        },
        {
            "event_id": "1003",
            "mention_time": "2024-01-02T06:10:00Z",
            "source_domain": "desk.example",
            "tone": "2.0",
        },
    ]

    rows_articles = [
        {
            "article_id": "20240101060000-0",
            "article_time": "2024-01-01T06:00:00Z",
            "source_domain": "desk.example",
            "primary_theme": "protest",
            "location_country": "THA",
        },
        {
            "article_id": "20240101080000-0",
            "article_time": "2024-01-01T08:00:00Z",
            "source_domain": "wire.example",
            "primary_theme": "conflict",
            "location_country": "THA",
        },
        {
            "article_id": "20240102060000-0",
            "article_time": "2024-01-02T06:00:00Z",
            "source_domain": "desk.example",
            "primary_theme": "protest",
            "location_country": "USA",
        },
    ]

    store.persist(PersistableResult(slice_ts="20240101060000", file_type="events", rows=rows_events))
    store.persist(PersistableResult(slice_ts="20240101060000", file_type="mentions", rows=rows_mentions))
    store.persist(PersistableResult(slice_ts="20240101060000", file_type="articles", rows=rows_articles))
    print("[brief-seed] persisted events/mentions/articles")


@pytest.fixture
def brief_seed(pg_store):
    store, dsn = pg_store
    _seed_brief_dataset(store)
    return dsn


def _run_query(dsn: str, label: str, sql: str):
    print(f"[brief-query] start: {label}")
    with connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    print(f"[brief-query] done: {label}; rows={len(rows)}")
    return rows


def test_brief_query_protest_hotspots(brief_seed):
    rows = _run_query(
        brief_seed,
        "protest_hotspots",
        """
        SELECT location_country, COUNT(*) AS event_count
        FROM events
        WHERE event_type IN ('protest', 'clash')
        GROUP BY location_country
        ORDER BY event_count DESC
        LIMIT 10
        """,
    )
    assert len(rows) >= 1


def test_brief_query_bilateral_trend(brief_seed):
    rows = _run_query(
        brief_seed,
        "bilateral_trend",
        """
        SELECT date_trunc('day', event_time) AS day_bucket, AVG(intensity) AS mean_intensity
        FROM events
        WHERE actor_country = 'USA' AND target_country = 'CHN'
        GROUP BY day_bucket
        ORDER BY day_bucket
        """,
    )
    assert len(rows) >= 1


def test_brief_query_theme_surge(brief_seed):
    rows = _run_query(
        brief_seed,
        "theme_surge",
        """
        WITH bounds AS (
            SELECT MAX(article_time) AS max_t FROM articles
        ),
        recent AS (
            SELECT primary_theme, COUNT(*) AS recent_count
            FROM articles, bounds
            WHERE article_time > (bounds.max_t - INTERVAL '24 hours')
            GROUP BY primary_theme
        ),
        baseline AS (
            SELECT primary_theme, COUNT(*)::float / 7.0 AS baseline_daily
            FROM articles, bounds
            WHERE article_time <= (bounds.max_t - INTERVAL '24 hours')
            GROUP BY primary_theme
        )
        SELECT r.primary_theme, r.recent_count, COALESCE(b.baseline_daily, 0) AS baseline_daily
        FROM recent r
        LEFT JOIN baseline b ON b.primary_theme = r.primary_theme
        ORDER BY r.recent_count DESC
        LIMIT 10
        """,
    )
    assert len(rows) >= 1


def test_brief_query_outlet_amplification_ratio(brief_seed):
    rows = _run_query(
        brief_seed,
        "outlet_amplification_ratio",
        """
        SELECT m.source_domain, COUNT(*)::float / NULLIF(COUNT(DISTINCT m.event_id), 0) AS mention_event_ratio
        FROM mentions m
        GROUP BY m.source_domain
        ORDER BY mention_event_ratio DESC
        """,
    )
    assert len(rows) >= 1


def test_brief_query_geographic_overlay(brief_seed):
    rows = _run_query(
        brief_seed,
        "geographic_overlay",
        """
        WITH event_bins AS (
            SELECT floor(location_lat)::int AS lat_bin, floor(location_lon)::int AS lon_bin, COUNT(*) AS event_count
            FROM events
            GROUP BY lat_bin, lon_bin
        ),
        protest_articles AS (
            SELECT location_country, COUNT(*) AS protest_article_count
            FROM articles
            WHERE primary_theme = 'protest'
            GROUP BY location_country
        )
        SELECT e.lat_bin, e.lon_bin, e.event_count, COALESCE(p.protest_article_count, 0)
        FROM event_bins e
        LEFT JOIN protest_articles p ON p.location_country IN (
            SELECT DISTINCT location_country
            FROM events ev
            WHERE floor(ev.location_lat)::int = e.lat_bin
              AND floor(ev.location_lon)::int = e.lon_bin
        )
        ORDER BY e.event_count DESC
        """,
    )
    assert len(rows) >= 1


def test_brief_query_pipeline_self_audit(brief_seed):
    rows = _run_query(
        brief_seed,
        "pipeline_self_audit",
        """
        SELECT slice_ts, file_type, status
        FROM ingest_checkpoints
        WHERE status = 'done'
        ORDER BY slice_ts, file_type
        """,
    )
    assert len(rows) >= 3
