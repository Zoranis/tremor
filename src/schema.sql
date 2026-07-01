CREATE TABLE IF NOT EXISTS events (
    event_id INTEGER PRIMARY KEY,
    event_time TIMESTAMPTZ,
    actor_country CHAR(3),
    target_country CHAR(3),
    event_type TEXT,
    intensity REAL,
    location_country CHAR(3),
    location_lat REAL,
    location_lon REAL,
    source_url TEXT,
    slice_ts TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mentions (
    event_id INTEGER NOT NULL REFERENCES events(event_id),
    mention_time TIMESTAMPTZ,
    source_domain TEXT,
    tone REAL,
    slice_ts TEXT NOT NULL,
    PRIMARY KEY (event_id, mention_time, source_domain, slice_ts)
);

CREATE TABLE IF NOT EXISTS articles (
    article_id TEXT PRIMARY KEY,
    article_time TIMESTAMPTZ,
    source_domain TEXT,
    primary_theme TEXT,
    location_country CHAR(3),
    slice_ts TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_slice_ts ON events (slice_ts);
CREATE INDEX IF NOT EXISTS idx_mentions_slice_ts ON mentions (slice_ts);
CREATE INDEX IF NOT EXISTS idx_articles_slice_ts ON articles (slice_ts);

CREATE INDEX IF NOT EXISTS idx_mentions_event_id ON mentions (event_id);

CREATE INDEX IF NOT EXISTS idx_events_event_type_location_country ON events (event_type, location_country);

CREATE INDEX IF NOT EXISTS idx_articles_primary_theme_article_time ON articles (primary_theme, article_time);
