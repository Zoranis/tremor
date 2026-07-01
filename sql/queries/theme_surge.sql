-- Top 10 themes by growth: last 24h vs trailing 7-day baseline.
WITH current_window AS (
    SELECT
        primary_theme,
        COUNT(*)::float AS mentions_24h
    FROM articles a
    JOIN mentions m ON m.slice_ts = a.slice_ts
    WHERE m.mention_time >= NOW() - INTERVAL '24 hours'
    GROUP BY primary_theme
),
baseline_window AS (
    SELECT
        primary_theme,
        COUNT(*)::float / 7.0 AS baseline_daily_mentions
    FROM articles a
    JOIN mentions m ON m.slice_ts = a.slice_ts
    WHERE m.mention_time >= NOW() - INTERVAL '8 days'
      AND m.mention_time < NOW() - INTERVAL '24 hours'
    GROUP BY primary_theme
)
SELECT
    COALESCE(c.primary_theme, b.primary_theme) AS primary_theme,
    COALESCE(c.mentions_24h, 0.0) AS mentions_24h,
    COALESCE(b.baseline_daily_mentions, 0.0) AS baseline_daily_mentions,
    CASE
        WHEN COALESCE(b.baseline_daily_mentions, 0.0) = 0.0 THEN NULL
        ELSE COALESCE(c.mentions_24h, 0.0) / b.baseline_daily_mentions
    END AS growth_ratio
FROM current_window c
FULL OUTER JOIN baseline_window b ON b.primary_theme = c.primary_theme
ORDER BY growth_ratio DESC NULLS LAST, mentions_24h DESC
LIMIT 10;
