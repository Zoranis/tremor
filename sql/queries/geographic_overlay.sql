-- Event volume and protest-article volume by 1-degree lat/lon bucket.
WITH event_buckets AS (
    SELECT
        floor(location_lat)::int AS lat_bucket,
        floor(location_lon)::int AS lon_bucket,
        COUNT(*) AS event_volume
    FROM events
    WHERE location_lat IS NOT NULL
      AND location_lon IS NOT NULL
    GROUP BY floor(location_lat)::int, floor(location_lon)::int
),
protest_article_buckets AS (
    SELECT
        floor(e.location_lat)::int AS lat_bucket,
        floor(e.location_lon)::int AS lon_bucket,
        COUNT(*) AS protest_article_volume
    FROM events e
    JOIN articles a ON a.slice_ts = e.slice_ts
    WHERE e.location_lat IS NOT NULL
      AND e.location_lon IS NOT NULL
      AND a.primary_theme = 'protest'
    GROUP BY floor(e.location_lat)::int, floor(e.location_lon)::int
)
SELECT
    COALESCE(e.lat_bucket, p.lat_bucket) AS lat_bucket,
    COALESCE(e.lon_bucket, p.lon_bucket) AS lon_bucket,
    COALESCE(e.event_volume, 0) AS event_volume,
    COALESCE(p.protest_article_volume, 0) AS protest_article_volume,
    COALESCE(p.protest_article_volume, 0) - COALESCE(e.event_volume, 0) AS article_minus_event_gap
FROM event_buckets e
FULL OUTER JOIN protest_article_buckets p
  ON p.lat_bucket = e.lat_bucket
 AND p.lon_bucket = e.lon_bucket
ORDER BY article_minus_event_gap DESC, protest_article_volume DESC;
