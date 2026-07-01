-- Mention-to-event ratio by source_domain versus global median.
WITH domain_mentions AS (
    SELECT
        source_domain,
        COUNT(*)::float AS mention_count
    FROM mentions
    GROUP BY source_domain
),
domain_events AS (
    SELECT
        regexp_replace(split_part(source_url, '/', 3), '^www\\.', '') AS source_domain,
        COUNT(*)::float AS event_count
    FROM events
    WHERE source_url IS NOT NULL
    GROUP BY regexp_replace(split_part(source_url, '/', 3), '^www\\.', '')
),
ratios AS (
    SELECT
        m.source_domain,
        m.mention_count,
        COALESCE(e.event_count, 0.0) AS event_count,
        CASE
            WHEN COALESCE(e.event_count, 0.0) = 0.0 THEN NULL
            ELSE m.mention_count / e.event_count
        END AS mention_to_event_ratio
    FROM domain_mentions m
    LEFT JOIN domain_events e ON e.source_domain = m.source_domain
),
ratio_median AS (
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY mention_to_event_ratio) AS median_ratio
    FROM ratios
    WHERE mention_to_event_ratio IS NOT NULL
)
SELECT
    r.source_domain,
    r.mention_count,
    r.event_count,
    r.mention_to_event_ratio,
    rm.median_ratio,
    CASE
        WHEN rm.median_ratio IS NULL OR rm.median_ratio = 0 THEN NULL
        ELSE r.mention_to_event_ratio / rm.median_ratio
    END AS ratio_vs_median
FROM ratios r
CROSS JOIN ratio_median rm
ORDER BY ratio_vs_median DESC NULLS LAST, r.mention_count DESC;
