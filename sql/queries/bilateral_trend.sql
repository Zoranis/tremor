-- Daily mean intensity for a chosen actor/target pair.
-- Replace 'USA' and 'CHN' as needed.
SELECT
    date_trunc('day', event_time)::date AS day,
    AVG(intensity) AS mean_intensity,
    COUNT(*) AS event_count
FROM events
WHERE actor_country = 'USA'
  AND target_country = 'CHN'
GROUP BY day
ORDER BY day;
