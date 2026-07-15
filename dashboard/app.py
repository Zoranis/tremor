from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from psycopg import connect


DATABASE_URL = os.getenv("DATABASE_URL", "")
BASE_DIR = Path(__file__).resolve().parent
INDEX_HTML = (BASE_DIR / "templates" / "index.html").read_text(encoding="utf-8")

app = FastAPI(title="Tremor Ops Dashboard API", version="1.0")
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


def _query_one(sql: str) -> dict[str, Any]:
    if not DATABASE_URL:
        return {}
    with connect(DATABASE_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if row is None:
                return {}
            cols = [c.name for c in cur.description]
            return dict(zip(cols, row))


def _query_many(sql: str) -> list[dict[str, Any]]:
    if not DATABASE_URL:
        return []
    with connect(DATABASE_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            cols = [c.name for c in cur.description]
            return [dict(zip(cols, row)) for row in rows]


@app.get("/healthz")
def healthz() -> dict[str, Any]:
    return {"ok": True, "database_configured": bool(DATABASE_URL)}


@app.get("/metrics")
def metrics() -> dict[str, Any]:
    return {
        "slice_lag": _query_one("SELECT * FROM v_slice_lag"),
        "ingestion_rate_5m_by_type": _query_many(
            "SELECT file_type, slices_5m, slices_per_min_5m FROM v_ingestion_rate_5m_by_type ORDER BY file_type"
        ),
        "manifest_poll_health": _query_one(
            "SELECT polls_n, success_rate, avg_latency_ms, p95_latency_ms, outage_503_count FROM v_manifest_poll_health"
        ),
        "active_alerts": _query_many(
            """
            SELECT alert_name, file_type, state, observed_at, value, threshold
            FROM ingest_alert_events
            WHERE state = 'firing'
              AND observed_at >= NOW() - INTERVAL '60 minutes'
            ORDER BY observed_at DESC
            LIMIT 50
            """
        ),
        "degraded_windows": _query_many(
            """
            SELECT degraded_type, started_at, ended_at, active
            FROM ingest_degraded_windows
            ORDER BY started_at DESC
            LIMIT 100
            """
        ),
    }


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    return INDEX_HTML
