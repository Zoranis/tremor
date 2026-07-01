-- Pipeline degraded windows due to stale manifest and degraded fraction.
SELECT
    degraded_type,
    started_at,
    ended_at,
    active,
    EXTRACT(EPOCH FROM (COALESCE(ended_at, NOW()) - started_at)) AS degraded_seconds
FROM ingest_degraded_windows
WHERE degraded_type = 'stale_manifest'
ORDER BY started_at DESC;

-- Summary fraction for the observed stale-manifest window.
SELECT * FROM v_pipeline_self_audit;
