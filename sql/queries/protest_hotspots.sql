-- Yesterday's top 10 countries by protest/clash event volume.
-- "Yesterday" is anchored to the latest ingested event_time, not wall-clock
-- NOW() -- the replay window tracks its own simulated clock (fixed
-- historical dates), not the calendar date the query happens to run on.
WITH latest AS (
    SELECT MAX(event_time) AS ts FROM events
),
yesterday_events AS (
    SELECT e.location_country
    FROM events e, latest
    WHERE e.event_time >= date_trunc('day', latest.ts - INTERVAL '1 day')
      AND e.event_time < date_trunc('day', latest.ts)
      AND e.event_type IN ('protest', 'clash')
)
SELECT
    location_country,
    COUNT(*) AS event_volume
FROM yesterday_events
GROUP BY location_country
ORDER BY event_volume DESC, location_country
LIMIT 10;
