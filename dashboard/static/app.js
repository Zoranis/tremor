const MAX_POINTS = 60; // 5 minutes of history at a 5s refresh cadence
const history = {
    lag: [],
    pollSuccess: [],
    pollLatency: [],
    ingest: { events: [], mentions: [], articles: [] },
};
let missedTicks = 0;

function pushCapped(arr, point) {
    arr.push(point);
    if (arr.length > MAX_POINTS) arr.shift();
}

function fmtTime(d) {
    return d.toLocaleTimeString(undefined, { hour12: false });
}

function statusClass(level) {
    return level; // 'good' | 'warning' | 'serious' | 'critical'
}

// Shared lightweight sparkline: single series, de-emphasis line, accent last-point marker.
function renderSparkline(containerEl, points, accentVar) {
    containerEl.innerHTML = '';
    if (points.length < 2) {
        containerEl.innerHTML = '<div class="muted" style="font-size:0.75rem;">collecting…</div>';
        return;
    }
    const W = 200, H = 32, PAD = 3;
    const values = points.map(p => p.v);
    const min = Math.min(...values), max = Math.max(...values);
    const span = (max - min) || 1;
    const xs = points.map((_, i) => PAD + (i / (points.length - 1)) * (W - PAD * 2));
    const ys = points.map(p => H - PAD - ((p.v - min) / span) * (H - PAD * 2));
    const d = xs.map((x, i) => (i === 0 ? 'M' : 'L') + x.toFixed(1) + ',' + ys[i].toFixed(1)).join(' ');
    const lastX = xs[xs.length - 1], lastY = ys[ys.length - 1];
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
    svg.setAttribute('preserveAspectRatio', 'none');
    svg.innerHTML = `
        <path d="${d}" fill="none" stroke="var(--de-emphasis)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />
        <circle cx="${lastX.toFixed(1)}" cy="${lastY.toFixed(1)}" r="4" fill="${accentVar}" stroke="var(--surface-1)" stroke-width="2" />
    `;
    containerEl.appendChild(svg);
}

// Multi-series line chart with legend, crosshair + tooltip, direct end-labels.
function renderMultiLine(svgEl, tooltipEl, series) {
    const W = 600, H = 190, PAD_L = 8, PAD_R = 64, PAD_T = 10, PAD_B = 10;
    svgEl.innerHTML = '';
    const n = series[0].points.length;
    if (n < 2) {
        svgEl.innerHTML = `<text x="${W/2}" y="${H/2}" text-anchor="middle" fill="var(--text-muted)" font-size="13">collecting data&#8230;</text>`;
        return;
    }
    const allValues = series.flatMap(s => s.points.map(p => p.v));
    const min = Math.min(0, ...allValues);
    const max = Math.max(...allValues, 1);
    const span = (max - min) || 1;
    const xAt = i => PAD_L + (i / (n - 1)) * (W - PAD_L - PAD_R);
    const yAt = v => H - PAD_B - ((v - min) / span) * (H - PAD_T - PAD_B);

    let svgInner = '';
    // gridlines (hairline, recessive)
    const gridSteps = 3;
    for (let g = 0; g <= gridSteps; g++) {
        const gy = PAD_T + (g / gridSteps) * (H - PAD_T - PAD_B);
        svgInner += `<line x1="${PAD_L}" y1="${gy.toFixed(1)}" x2="${W - PAD_R}" y2="${gy.toFixed(1)}" stroke="var(--hairline)" stroke-width="1" />`;
    }

    series.forEach(s => {
        const d = s.points.map((p, i) => (i === 0 ? 'M' : 'L') + xAt(i).toFixed(1) + ',' + yAt(p.v).toFixed(1)).join(' ');
        svgInner += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" />`;
        const lastPoint = s.points[s.points.length - 1];
        const lx = xAt(n - 1), ly = yAt(lastPoint.v);
        svgInner += `<circle cx="${lx.toFixed(1)}" cy="${ly.toFixed(1)}" r="4" fill="${s.color}" stroke="var(--surface-1)" stroke-width="2" />`;
        svgInner += `<text x="${(lx + 8).toFixed(1)}" y="${(ly + 4).toFixed(1)}" font-size="11" fill="var(--text-secondary)">${lastPoint.v.toFixed(1)}</text>`;
    });

    // crosshair (hidden until hover)
    svgInner += `<line id="crosshair" x1="0" y1="${PAD_T}" x2="0" y2="${H - PAD_B}" stroke="var(--text-muted)" stroke-width="1" opacity="0" />`;
    // transparent hit rect drives pointer tracking
    svgInner += `<rect id="hitrect" x="${PAD_L}" y="0" width="${W - PAD_L - PAD_R}" height="${H}" fill="transparent" />`;

    svgEl.setAttribute('viewBox', `0 0 ${W} ${H}`);
    svgEl.innerHTML = svgInner;

    const hitrect = svgEl.querySelector('#hitrect');
    const crosshair = svgEl.querySelector('#crosshair');

    function handleMove(evt) {
        const rect = svgEl.getBoundingClientRect();
        const scaleX = W / rect.width;
        const localX = (evt.clientX - rect.left) * scaleX;
        let idx = Math.round(((localX - PAD_L) / (W - PAD_L - PAD_R)) * (n - 1));
        idx = Math.max(0, Math.min(n - 1, idx));
        const cx = xAt(idx);
        crosshair.setAttribute('x1', cx.toFixed(1));
        crosshair.setAttribute('x2', cx.toFixed(1));
        crosshair.setAttribute('opacity', '1');

        const t = series[0].points[idx].t;
        let html = `<div class="t-time">${fmtTime(t)}</div>`;
        series.forEach(s => {
            const v = s.points[idx].v;
            html += `<div class="t-row"><span class="t-key" style="background:${s.color}"></span><span>${s.label}</span><span class="t-val">${v.toFixed(2)}</span></div>`;
        });
        tooltipEl.innerHTML = html;
        tooltipEl.classList.add('show');
        const wrapRect = svgEl.parentElement.getBoundingClientRect();
        const px = (evt.clientX - wrapRect.left);
        const py = (evt.clientY - wrapRect.top);
        tooltipEl.style.left = Math.min(px + 12, wrapRect.width - 170) + 'px';
        tooltipEl.style.top = Math.max(py - 50, 0) + 'px';
    }
    hitrect.addEventListener('pointermove', handleMove);
    hitrect.addEventListener('pointerleave', () => {
        crosshair.setAttribute('opacity', '0');
        tooltipEl.classList.remove('show');
    });
}

function pollLagLevel(lagSeconds) {
    if (lagSeconds == null) return 'muted';
    if (lagSeconds >= 1800) return 'critical';
    if (lagSeconds >= 900) return 'warning';
    return 'good';
}
function pollHealthLevel(successRate, pollsN) {
    if (!pollsN) return 'muted'; // no polls recorded yet - not the same as failing
    if (successRate < 0.90) return 'critical';
    if (successRate < 0.98) return 'warning';
    return 'good';
}

async function refresh() {
    let data;
    try {
        const res = await fetch('/metrics', { cache: 'no-store' });
        if (!res.ok) throw new Error('bad status');
        data = await res.json();
        missedTicks = 0;
        document.getElementById('connLost').classList.remove('show');
    } catch (err) {
        missedTicks += 1;
        if (missedTicks >= 3) {
            document.getElementById('connLost').classList.add('show');
        }
        document.getElementById('updated').textContent = 'last successful fetch failed';
        return;
    }

    const now = new Date();

    // ---- Slice lag ----
    const lag = data.slice_lag || {};
    const lagSeconds = lag.lag_seconds == null ? null : Number(lag.lag_seconds);
    const lagSlices = lag.lag_slices == null ? null : Number(lag.lag_slices);
    const lagLevel = pollLagLevel(lagSeconds);
    const lagEl = document.getElementById('lagValue');
    const lagIcon = document.getElementById('lagIcon');
    lagEl.className = lagLevel;
    if (lagSeconds == null) {
        lagEl.textContent = 'n/a';
        lagIcon.textContent = '';
    } else {
        lagEl.textContent = Math.round(lagSeconds) + 's';
        lagIcon.textContent = lagLevel === 'good' ? '●' : lagLevel === 'warning' ? '▲' : '✕';
        lagIcon.className = 'icon ' + lagLevel;
        pushCapped(history.lag, { t: now, v: lagSeconds });
    }
    document.getElementById('lagCaption').textContent = lagSlices == null
        ? 'no slices fully processed yet'
        : lagSlices.toFixed(2) + ' slices behind manifest (>=1.0 = falling behind)';
    renderSparkline(document.getElementById('lagSpark'), history.lag, `var(--status-${lagLevel === 'muted' ? 'good' : lagLevel})`);

    // ---- Manifest poll health ----
    const health = data.manifest_poll_health || {};
    const pollsN = Number(health.polls_n || 0);
    const successRate = Number(health.success_rate || 0);
    const pollLevel = pollHealthLevel(successRate, pollsN);
    const pollEl = document.getElementById('pollValue');
    const pollIcon = document.getElementById('pollIcon');
    pollEl.className = pollLevel === 'muted' ? 'muted' : pollLevel;
    pollEl.textContent = pollsN === 0 ? 'n/a' : Math.round(successRate * 100) + '%';
    pollIcon.textContent = pollLevel === 'muted' ? '' : pollLevel === 'good' ? '●' : pollLevel === 'warning' ? '▲' : '✕';
    pollIcon.className = 'icon ' + pollLevel;
    document.getElementById('pollCaption').textContent = pollsN === 0
        ? 'collecting…'
        : 'n=' + pollsN + ' polls · ' + Number(health.outage_503_count || 0) + ' × 503';
    if (pollsN > 0) {
        pushCapped(history.pollSuccess, { t: now, v: successRate * 100 });
    }
    renderSparkline(document.getElementById('pollSpark'), history.pollSuccess, `var(--status-${pollLevel === 'muted' ? 'good' : pollLevel})`);

    const p95 = Number(health.p95_latency_ms || 0);
    document.getElementById('latValue').textContent = p95.toFixed(0) + ' ms';
    document.getElementById('latCaption').textContent = 'avg ' + Number(health.avg_latency_ms || 0).toFixed(0) + ' ms';
    pushCapped(history.pollLatency, { t: now, v: p95 });
    renderSparkline(document.getElementById('latSpark'), history.pollLatency, 'var(--series-1)');

    // ---- Outage alert ----
    const alerts = Array.isArray(data.active_alerts) ? data.active_alerts : [];
    const activeOutage = alerts.find(a => a.alert_name === 'vendor_feed_down');
    const outageEl = document.getElementById('outageValue');
    const outageIcon = document.getElementById('outageIcon');
    if (activeOutage) {
        outageEl.textContent = 'ACTIVE';
        outageEl.className = 'critical';
        outageIcon.textContent = '✕';
        outageIcon.className = 'icon critical';
        document.getElementById('outageCaption').textContent = 'firing since ' + fmtTime(new Date(activeOutage.observed_at));
    } else {
        outageEl.textContent = 'CLEAR';
        outageEl.className = 'good';
        outageIcon.textContent = '●';
        outageIcon.className = 'icon good';
        document.getElementById('outageCaption').textContent = 'no active outage alert in last 60m';
    }

    // ---- Overall banner: worst-of lag / poll / outage / any other firing alert ----
    const otherAlerts = alerts.filter(a => a.alert_name !== 'vendor_feed_down');
    let overall = 'good', reason = 'all systems nominal';
    if (activeOutage) {
        overall = 'critical'; reason = 'vendor feed outage — manifest/file endpoint unreachable';
    } else if (lagLevel === 'critical') {
        overall = 'critical'; reason = 'pipeline has fallen more than 2 slices behind';
    } else if (lagLevel === 'warning' || pollLevel === 'critical') {
        overall = 'serious'; reason = lagLevel === 'warning' ? 'slice lag elevated — 1+ slice behind' : 'manifest poll success rate degraded';
    } else if (otherAlerts.length > 0) {
        overall = 'serious'; reason = otherAlerts.length + ' alert(s) firing: ' + [...new Set(otherAlerts.map(a => a.alert_name))].join(', ');
    } else if (pollLevel === 'warning' && pollsN > 0) {
        overall = 'warning'; reason = 'manifest poll success rate dipping';
    }
    const bannerLabels = { good: 'HEALTHY', warning: 'DEGRADED', serious: 'DEGRADED', critical: 'DOWN' };
    const bannerIcons = { good: '●', warning: '▲', serious: '▲', critical: '✕' };
    const bannerEl = document.getElementById('banner');
    bannerEl.className = 'banner ' + overall;
    document.getElementById('bannerIcon').innerHTML = bannerIcons[overall];
    document.getElementById('bannerText').textContent = bannerLabels[overall] + ' — ' + reason;
    document.getElementById('bannerSub').textContent = 'as of ' + fmtTime(now);

    // ---- Per-file-type ingestion rate (multi-line) ----
    const byType = {};
    (data.ingestion_rate_5m_by_type || []).forEach(r => { byType[r.file_type] = Number(r.slices_per_min_5m || 0); });
    ['events', 'mentions', 'articles'].forEach(ft => {
        pushCapped(history.ingest[ft], { t: now, v: byType[ft] || 0 });
    });
    const seriesSpec = [
        { key: 'events', label: 'events', color: 'var(--series-1)' },
        { key: 'mentions', label: 'mentions', color: 'var(--series-2)' },
        { key: 'articles', label: 'articles', color: 'var(--series-3)' },
    ];
    const legendEl = document.getElementById('ingestLegend');
    legendEl.innerHTML = seriesSpec.map(s =>
        `<span class="legend-item"><span class="legend-key" style="background:${s.color}"></span>${s.label}</span>`
    ).join('');
    renderMultiLine(
        document.getElementById('ingestSvg'),
        document.getElementById('ingestTooltip'),
        seriesSpec.map(s => ({ label: s.label, color: s.color, points: history.ingest[s.key] }))
    );
    const tbody = document.getElementById('ingestTableBody');
    tbody.innerHTML = '';
    const n = history.ingest.events.length;
    for (let i = n - 1; i >= 0; i--) {
        const tr = document.createElement('tr');
        const tCell = document.createElement('td'); tCell.textContent = fmtTime(history.ingest.events[i].t);
        const eCell = document.createElement('td'); eCell.className = 'num'; eCell.textContent = history.ingest.events[i].v.toFixed(2);
        const mCell = document.createElement('td'); mCell.className = 'num'; mCell.textContent = history.ingest.mentions[i].v.toFixed(2);
        const aCell = document.createElement('td'); aCell.className = 'num'; aCell.textContent = history.ingest.articles[i].v.toFixed(2);
        tr.append(tCell, eCell, mCell, aCell);
        tbody.appendChild(tr);
    }

    // ---- Active alerts list ----
    const alertsList = document.getElementById('alertsList');
    alertsList.innerHTML = '';
    if (alerts.length === 0) {
        const div = document.createElement('div');
        div.className = 'empty-state good';
        div.textContent = '● no active alerts';
        alertsList.appendChild(div);
    } else {
        alerts.forEach(a => {
            const row = document.createElement('div');
            row.className = 'alert-row';
            const icon = document.createElement('span');
            icon.className = 'icon critical';
            icon.textContent = '✕';
            const name = document.createElement('span');
            name.className = 'name';
            name.textContent = a.alert_name + (a.file_type ? ' (' + a.file_type + ')' : '');
            const detail = document.createElement('span');
            detail.className = 'detail';
            detail.textContent = fmtTime(new Date(a.observed_at)) + (a.value != null ? ' · value=' + Number(a.value).toFixed(2) : '') + (a.threshold != null ? ' · threshold=' + Number(a.threshold).toFixed(2) : '');
            row.append(icon, name, detail);
            alertsList.appendChild(row);
        });
    }

    // ---- Degraded windows ----
    const degradedRows = document.getElementById('degradedRows');
    degradedRows.innerHTML = '';
    const windows = (data.degraded_windows || []).slice(0, 10);
    if (windows.length === 0) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = 4; td.className = 'empty-state'; td.textContent = 'no degraded windows recorded';
        tr.appendChild(td);
        degradedRows.appendChild(tr);
    } else {
        windows.forEach(w => {
            const tr = document.createElement('tr');
            const c1 = document.createElement('td'); c1.textContent = w.degraded_type;
            const c2 = document.createElement('td'); c2.textContent = fmtTime(new Date(w.started_at));
            const c3 = document.createElement('td'); c3.textContent = w.ended_at ? fmtTime(new Date(w.ended_at)) : '-';
            const c4 = document.createElement('td'); c4.textContent = w.active ? 'yes' : 'no'; c4.className = w.active ? 'warning' : 'muted';
            tr.append(c1, c2, c3, c4);
            degradedRows.appendChild(tr);
        });
    }

    document.getElementById('asOf').textContent = 'as of ' + fmtTime(now);
    document.getElementById('updated').textContent = 'updated ' + fmtTime(now);
}

document.getElementById('toggleIngestTable').addEventListener('click', () => {
    const panel = document.getElementById('ingestTablePanel');
    const showing = panel.classList.toggle('show');
    document.getElementById('toggleIngestTable').textContent = showing ? 'View as chart' : 'View as table';
});

setInterval(refresh, 5000);
refresh();
