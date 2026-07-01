-- Yesterday's top 10 countries by protest/clash event volume.
WITH yesterday_events AS (
    SELECT location_country
    FROM events
    WHERE event_time >= date_trunc('day', NOW() - INTERVAL '1 day')
      AND event_time < date_trunc('day', NOW())
      AND event_type IN ('protest', 'clash')
)
SELECT
    location_country,
    COUNT(*) AS event_volume
FROM yesterday_events
GROUP BY location_country
ORDER BY event_volume DESC, location_country
LIMIT 10;
