from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from psycopg import connect


DATABASE_URL = os.getenv("DATABASE_URL", "")
app = FastAPI(title="Tremor Ops Dashboard API", version="1.0")


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
        "manifest_poll_health": _query_one("SELECT * FROM v_manifest_poll_health"),
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
        return """
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Tremor Overnight Dashboard</title>
    <style>
        :root {
            --bg: #0e1726;
            --panel: #14213d;
            --text: #f5f7fa;
            --muted: #9fb3c8;
            --ok: #35d07f;
            --warn: #ffbf3f;
            --bad: #ff5f57;
            --accent: #35b4ff;
        }
        body {
            margin: 0;
            font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
            background: radial-gradient(1200px 600px at 20% -10%, #1c3f66, var(--bg));
            color: var(--text);
        }
        .wrap {
            max-width: 1100px;
            margin: 0 auto;
            padding: 16px;
            display: grid;
            gap: 12px;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        }
        .title {
            grid-column: 1 / -1;
            display: flex;
            justify-content: space-between;
            align-items: baseline;
        }
        .title h1 {
            margin: 0;
            font-size: 1.2rem;
            letter-spacing: 0.02em;
        }
        .panel {
            background: linear-gradient(180deg, #173056, var(--panel));
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 10px;
            padding: 12px;
            min-height: 120px;
            box-shadow: 0 8px 20px rgba(0, 0, 0, 0.25);
        }
        .panel h2 {
            margin: 0 0 10px;
            font-size: 0.95rem;
            color: var(--muted);
            font-weight: 600;
        }
        .metric {
            font-size: 1.7rem;
            font-weight: 700;
        }
        .ok { color: var(--ok); }
        .warn { color: var(--warn); }
        .bad { color: var(--bad); }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        th, td {
            text-align: left;
            padding: 4px 2px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.08);
        }
        .muted { color: var(--muted); }
        @media (max-width: 700px) {
            .metric { font-size: 1.4rem; }
        }
    </style>
</head>
<body>
    <div class="wrap">
        <div class="title">
            <h1>Tremor Overnight Ops Dashboard</h1>
            <div class="muted" id="updated">refreshing...</div>
        </div>

        <section class="panel">
            <h2>Slice Lag</h2>
            <div id="lagValue" class="metric">-</div>
            <div class="muted" id="lagSlices"></div>
        </section>

        <section class="panel">
            <h2>Manifest Poll Health</h2>
            <div id="pollSuccess" class="metric">-</div>
            <div class="muted" id="pollLatency"></div>
        </section>

        <section class="panel">
            <h2>Outage Alert</h2>
            <div id="outageState" class="metric">-</div>
            <div class="muted" id="outageDetail"></div>
        </section>

        <section class="panel" style="grid-column: 1 / -1;">
            <h2>5m Ingestion Rate by File Type</h2>
            <table>
                <thead><tr><th>File Type</th><th>Slices (5m)</th><th>Slices/Min</th></tr></thead>
                <tbody id="rateRows"></tbody>
            </table>
        </section>

        <section class="panel" style="grid-column: 1 / -1;">
            <h2>Recent Degraded Windows</h2>
            <table>
                <thead><tr><th>Type</th><th>Start</th><th>End</th><th>Active</th></tr></thead>
                <tbody id="degradedRows"></tbody>
            </table>
        </section>
    </div>

    <script>
        async function refresh() {
            const res = await fetch('/metrics', { cache: 'no-store' });
            const data = await res.json();

            const lag = data.slice_lag || {};
            const lagSeconds = lag.lag_seconds == null ? null : Number(lag.lag_seconds);
            const lagEl = document.getElementById('lagValue');
            if (lagSeconds == null) {
                lagEl.textContent = 'n/a';
                lagEl.className = 'metric muted';
            } else {
                lagEl.textContent = Math.round(lagSeconds) + 's';
                lagEl.className = 'metric ' + (lagSeconds >= 1800 ? 'bad' : lagSeconds >= 900 ? 'warn' : 'ok');
            }
            document.getElementById('lagSlices').textContent = 'lag_slices=' + (lag.lag_slices == null ? 'n/a' : Number(lag.lag_slices).toFixed(2));

            const health = data.manifest_poll_health || {};
            const successRate = Number(health.success_rate || 0);
            const successPct = Math.round(successRate * 100);
            const pollEl = document.getElementById('pollSuccess');
            pollEl.textContent = successPct + '% success';
            pollEl.className = 'metric ' + (successPct < 90 ? 'bad' : successPct < 98 ? 'warn' : 'ok');
            document.getElementById('pollLatency').textContent = 'avg=' + Number(health.avg_latency_ms || 0).toFixed(1) + 'ms, p95=' + Number(health.p95_latency_ms || 0).toFixed(1) + 'ms, n=' + Number(health.polls_n || 0);

            const alerts = Array.isArray(data.active_alerts) ? data.active_alerts : [];
            const activeOutage = alerts.find(a => a.alert_name === 'vendor_feed_down');
            const outageEl = document.getElementById('outageState');
            if (activeOutage) {
                outageEl.textContent = 'ACTIVE';
                outageEl.className = 'metric bad';
                document.getElementById('outageDetail').textContent = 'vendor_feed_down observed_at=' + activeOutage.observed_at;
            } else {
                outageEl.textContent = 'CLEAR';
                outageEl.className = 'metric ok';
                document.getElementById('outageDetail').textContent = 'No active outage alert in last 60m';
            }

            const rateRows = document.getElementById('rateRows');
            rateRows.innerHTML = '';
            (data.ingestion_rate_5m_by_type || []).forEach(r => {
                const tr = document.createElement('tr');
                tr.innerHTML = '<td>' + r.file_type + '</td><td>' + r.slices_5m + '</td><td>' + Number(r.slices_per_min_5m || 0).toFixed(2) + '</td>';
                rateRows.appendChild(tr);
            });

            const degradedRows = document.getElementById('degradedRows');
            degradedRows.innerHTML = '';
            (data.degraded_windows || []).slice(0, 10).forEach(w => {
                const tr = document.createElement('tr');
                tr.innerHTML = '<td>' + w.degraded_type + '</td><td>' + w.started_at + '</td><td>' + (w.ended_at || '-') + '</td><td>' + (w.active ? 'yes' : 'no') + '</td>';
                degradedRows.appendChild(tr);
            });

            document.getElementById('updated').textContent = 'updated ' + new Date().toLocaleTimeString();
        }

        setInterval(refresh, 5000);
        refresh();
    </script>
</body>
</html>
        """
