from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from psycopg import connect
from psycopg.abc import Query
from psycopg.connection import Connection


@dataclass(frozen=True)
class PersistableResult:
    slice_ts: str
    file_type: str
    rows: list[dict[str, str]]


class PostgresIngestStore:
    def __init__(self, *, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Connection | None = None

    def open(self) -> None:
        if self._conn is not None:
            return
        self._conn = connect(self._dsn, autocommit=False)
        self._ensure_schema()

    def close(self) -> None:
        if self._conn is None:
            return
        self._conn.close()
        self._conn = None

    def __enter__(self) -> PostgresIngestStore:
        self.open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def load_seen(self) -> set[tuple[str, str]]:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT slice_ts, file_type FROM ingest_checkpoints WHERE status = 'done'"
                )
                return {(str(slice_ts), str(file_type)) for slice_ts, file_type in cur.fetchall()}

    def persist(self, result: PersistableResult) -> None:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                self._mark_manifest_slice_seen(cur, result.slice_ts)

                if result.file_type == "events":
                    self._insert_events(cur, result.rows, result.slice_ts)
                elif result.file_type == "mentions":
                    self._insert_mentions(cur, result.rows, result.slice_ts)
                elif result.file_type == "articles":
                    self._insert_articles(cur, result.rows, result.slice_ts)
                else:
                    raise ValueError(f"unsupported file_type: {result.file_type}")

                cur.execute(
                    """
                    INSERT INTO ingest_checkpoints (slice_ts, file_type, status)
                    VALUES (%s, %s, 'done')
                    ON CONFLICT (slice_ts, file_type)
                    DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
                    """,
                    (result.slice_ts, result.file_type),
                )
                self._mark_slice_file_done(cur, result.slice_ts, result.file_type)

    def mark_manifest_slice_seen(self, *, slice_ts: str) -> None:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                self._mark_manifest_slice_seen(cur, slice_ts)

    def record_manifest_poll(
        self,
        *,
        status_code: int | None,
        success: bool,
        latency_ms: float,
        line_count: int,
        manifest_latest_slice: str | None,
    ) -> None:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingest_manifest_polls (
                        polled_at,
                        status_code,
                        success,
                        latency_ms,
                        line_count,
                        manifest_latest_slice
                    )
                    VALUES (NOW(), %s, %s, %s, %s, %s)
                    """,
                    (
                        status_code,
                        success,
                        latency_ms,
                        line_count,
                        manifest_latest_slice,
                    ),
                )

    def record_file_attempt(
        self,
        *,
        slice_ts: str,
        file_type: str,
        url: str,
        status_code: int | None,
        success: bool,
        outcome: str,
        latency_ms: float,
    ) -> None:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingest_file_attempts (
                        attempted_at,
                        slice_ts,
                        file_type,
                        url,
                        status_code,
                        success,
                        outcome,
                        latency_ms
                    )
                    VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        slice_ts,
                        file_type,
                        url,
                        status_code,
                        success,
                        outcome,
                        latency_ms,
                    ),
                )

    def record_alert_event(
        self,
        *,
        alert_name: str,
        file_type: str | None,
        state: str,
        value: float,
        threshold: float,
    ) -> None:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingest_alert_events (
                        observed_at,
                        alert_name,
                        file_type,
                        state,
                        value,
                        threshold
                    )
                    VALUES (NOW(), %s, %s, %s, %s, %s)
                    """,
                    (alert_name, file_type, state, value, threshold),
                )

    def set_degraded_state(self, *, degraded_type: str, active: bool) -> None:
        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                if active:
                    cur.execute(
                        """
                        INSERT INTO ingest_degraded_windows (degraded_type, started_at, active)
                        SELECT %s, NOW(), TRUE
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM ingest_degraded_windows
                            WHERE degraded_type = %s AND active = TRUE
                        )
                        """,
                        (degraded_type, degraded_type),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE ingest_degraded_windows
                        SET active = FALSE,
                            ended_at = COALESCE(ended_at, NOW())
                        WHERE id = (
                            SELECT id
                            FROM ingest_degraded_windows
                            WHERE degraded_type = %s AND active = TRUE
                            ORDER BY started_at DESC
                            LIMIT 1
                        )
                        """,
                        (degraded_type,),
                    )

    def compact_checkpoints(self, *, retain_per_file_type: int) -> int:
        if retain_per_file_type <= 0:
            return 0

        conn = self._require_conn()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH ranked AS (
                        SELECT
                            slice_ts,
                            file_type,
                            ROW_NUMBER() OVER (
                                PARTITION BY file_type
                                ORDER BY slice_ts DESC
                            ) AS rn
                        FROM ingest_checkpoints
                        WHERE status = 'done'
                    ),
                    doomed AS (
                        SELECT slice_ts, file_type
                        FROM ranked
                        WHERE rn > %s
                    )
                    DELETE FROM ingest_checkpoints i
                    USING doomed d
                    WHERE i.slice_ts = d.slice_ts
                      AND i.file_type = d.file_type
                    """,
                    (retain_per_file_type,),
                )
                return cur.rowcount if cur.rowcount is not None else 0

    def _require_conn(self) -> Connection:
        if self._conn is None:
            raise RuntimeError("database is not open")
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self._require_conn()
        schema_sql = self._read_schema_sql()
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(schema_sql)
                # Mentions can legitimately reference event ids that are not present
                # in the current events slice; keep ingest resilient by removing
                # the strict FK if it exists from earlier schema versions.
                cur.execute(
                    "ALTER TABLE IF EXISTS mentions DROP CONSTRAINT IF EXISTS mentions_event_id_fkey"
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ingest_checkpoints (
                        slice_ts TEXT NOT NULL,
                        file_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (slice_ts, file_type)
                    )
                    """
                )

    @staticmethod
    def _mark_manifest_slice_seen(cur, slice_ts: str) -> None:
        cur.execute(
            """
            INSERT INTO ingest_slice_status (slice_ts, manifest_seen_at)
            VALUES (%s, NOW())
            ON CONFLICT (slice_ts)
            DO UPDATE SET manifest_seen_at = LEAST(ingest_slice_status.manifest_seen_at, NOW())
            """,
            (slice_ts,),
        )

    @staticmethod
    def _mark_slice_file_done(cur, slice_ts: str, file_type: str) -> None:
        if file_type not in {"events", "mentions", "articles"}:
            return

        set_clause = {
            "events": "events_done = TRUE",
            "mentions": "mentions_done = TRUE",
            "articles": "articles_done = TRUE",
        }[file_type]

        cur.execute(
            f"""
            INSERT INTO ingest_slice_status (slice_ts, manifest_seen_at, {file_type}_done)
            VALUES (%s, NOW(), TRUE)
            ON CONFLICT (slice_ts)
            DO UPDATE SET {set_clause}
            """,
            (slice_ts,),
        )

        cur.execute(
            """
            UPDATE ingest_slice_status
            SET fully_processed_at = COALESCE(fully_processed_at, NOW())
            WHERE slice_ts = %s
              AND events_done = TRUE
              AND mentions_done = TRUE
              AND articles_done = TRUE
            """,
            (slice_ts,),
        )

    def _read_schema_sql(self) -> Query:
        here = Path(__file__).resolve()
        schema_path = here.parent.parent / "schema.sql"
        return schema_path.read_text(encoding="utf-8")

    @staticmethod
    def _insert_events(cur, rows: Iterable[dict[str, str]], slice_ts: str) -> None:
        for row in rows:
            cur.execute(
                """
                INSERT INTO events (
                    event_id,
                    event_time,
                    actor_country,
                    target_country,
                    event_type,
                    intensity,
                    location_country,
                    location_lat,
                    location_lon,
                    source_url,
                    slice_ts
                )
                VALUES (%(event_id)s, %(event_time)s, %(actor_country)s, %(target_country)s,
                        %(event_type)s, %(intensity)s, %(location_country)s,
                        %(location_lat)s, %(location_lon)s, %(source_url)s, %(slice_ts)s)
                ON CONFLICT (event_id)
                DO UPDATE SET
                    event_time = EXCLUDED.event_time,
                    actor_country = EXCLUDED.actor_country,
                    target_country = EXCLUDED.target_country,
                    event_type = EXCLUDED.event_type,
                    intensity = EXCLUDED.intensity,
                    location_country = EXCLUDED.location_country,
                    location_lat = EXCLUDED.location_lat,
                    location_lon = EXCLUDED.location_lon,
                    source_url = EXCLUDED.source_url,
                    slice_ts = EXCLUDED.slice_ts
                """,
                {
                    "event_id": row.get("event_id"),
                    "event_time": row.get("event_time"),
                    "actor_country": row.get("actor_country") or None,
                    "target_country": row.get("target_country") or None,
                    "event_type": row.get("event_type"),
                    "intensity": row.get("intensity"),
                    "location_country": row.get("location_country") or None,
                    "location_lat": row.get("location_lat"),
                    "location_lon": row.get("location_lon"),
                    "source_url": row.get("source_url"),
                    "slice_ts": slice_ts,
                },
            )

    @staticmethod
    def _insert_mentions(cur, rows: Iterable[dict[str, str]], slice_ts: str) -> None:
        for row in rows:
            cur.execute(
                """
                INSERT INTO mentions (
                    event_id,
                    mention_time,
                    source_domain,
                    tone,
                    slice_ts
                )
                VALUES (%(event_id)s, %(mention_time)s, %(source_domain)s, %(tone)s, %(slice_ts)s)
                ON CONFLICT (event_id, mention_time, source_domain, slice_ts)
                DO UPDATE SET tone = EXCLUDED.tone
                """,
                {
                    "event_id": row.get("event_id"),
                    "mention_time": row.get("mention_time"),
                    "source_domain": row.get("source_domain"),
                    "tone": row.get("tone"),
                    "slice_ts": slice_ts,
                },
            )

    @staticmethod
    def _insert_articles(cur, rows: Iterable[dict[str, str]], slice_ts: str) -> None:
        for row in rows:
            cur.execute(
                """
                INSERT INTO articles (
                    article_id,
                    article_time,
                    source_domain,
                    primary_theme,
                    location_country,
                    slice_ts
                )
                VALUES (%(article_id)s, %(article_time)s, %(source_domain)s,
                        %(primary_theme)s, %(location_country)s, %(slice_ts)s)
                ON CONFLICT (article_id)
                DO UPDATE SET
                    article_time = EXCLUDED.article_time,
                    source_domain = EXCLUDED.source_domain,
                    primary_theme = EXCLUDED.primary_theme,
                    location_country = EXCLUDED.location_country,
                    slice_ts = EXCLUDED.slice_ts
                """,
                {
                    "article_id": row.get("article_id"),
                    "article_time": row.get("article_time"),
                    "source_domain": row.get("source_domain"),
                    "primary_theme": row.get("primary_theme"),
                    "location_country": row.get("location_country") or None,
                    "slice_ts": slice_ts,
                },
            )