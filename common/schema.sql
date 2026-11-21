-- All tables are created automatically by the consumers on start-up (common/db.py runs this file).
-- Every statement is idempotent, so running it again is safe.

-- Part 3: one row per trip event, columns match the event fields.
CREATE TABLE IF NOT EXISTS trips (
    trip_id           TEXT PRIMARY KEY,
    event_time        TIMESTAMPTZ NOT NULL,
    pickup_zone       TEXT NOT NULL,
    dropoff_zone      TEXT NOT NULL,
    service_type      TEXT NOT NULL,
    status            TEXT NOT NULL,          -- completed | rider_cancelled | driver_cancelled
    distance_km       DOUBLE PRECISION NOT NULL,
    duration_min      DOUBLE PRECISION NOT NULL,
    eta_min           DOUBLE PRECISION NOT NULL,
    surge_multiplier  DOUBLE PRECISION NOT NULL,
    fare_usd          DOUBLE PRECISION NOT NULL,
    tip_usd           DOUBLE PRECISION NOT NULL,
    payment_method    TEXT NOT NULL,
    driver_rating     DOUBLE PRECISION,       -- NULL when the trip was cancelled
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS trips_event_time_idx ON trips (event_time DESC);

-- Part 4: tumbling one-minute window per pickup zone, upserted while the window is open.
CREATE TABLE IF NOT EXISTS zone_minute_metrics (
    window_start   TIMESTAMPTZ NOT NULL,
    pickup_zone    TEXT NOT NULL,
    trips          INTEGER NOT NULL,
    completed      INTEGER NOT NULL,
    cancelled      INTEGER NOT NULL,
    revenue_usd    DOUBLE PRECISION NOT NULL,
    avg_surge      DOUBLE PRECISION NOT NULL,
    avg_eta_min    DOUBLE PRECISION NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, pickup_zone)
);

-- Part 4: the last Kafka offset per partition already counted into zone_minute_metrics.
-- Written in the same transaction as the aggregates, so a replayed message is skipped
-- instead of being added to the totals a second time.
CREATE TABLE IF NOT EXISTS stream_offsets (
    consumer_group TEXT NOT NULL,
    topic          TEXT NOT NULL,
    partition      INTEGER NOT NULL,
    last_offset    BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition)
);

-- Part 4: alert rule fired from the windowed metrics.
CREATE TABLE IF NOT EXISTS surge_alerts (
    window_start   TIMESTAMPTZ NOT NULL,
    pickup_zone    TEXT NOT NULL,
    rule           TEXT NOT NULL,             -- high_surge | high_cancellations
    value          DOUBLE PRECISION NOT NULL,
    trips          INTEGER NOT NULL,
    message        TEXT NOT NULL,
    alerted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, pickup_zone, rule)
);

-- Extension B: anomalous fares flagged by the streaming detector.
CREATE TABLE IF NOT EXISTS fare_anomalies (
    trip_id        TEXT PRIMARY KEY,
    event_time     TIMESTAMPTZ NOT NULL,
    pickup_zone    TEXT NOT NULL,
    service_type   TEXT NOT NULL,
    fare_usd       DOUBLE PRECISION NOT NULL,
    distance_km    DOUBLE PRECISION NOT NULL,
    fare_per_km    DOUBLE PRECISION NOT NULL,
    speed_kmh      DOUBLE PRECISION NOT NULL,
    robust_z       DOUBLE PRECISION NOT NULL,
    reason         TEXT NOT NULL,
    detected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Extension B: completion probability predicted for each trip *before* the model sees its outcome.
CREATE TABLE IF NOT EXISTS completion_predictions (
    trip_id        TEXT PRIMARY KEY,
    event_time     TIMESTAMPTZ NOT NULL,
    pickup_zone    TEXT NOT NULL,
    service_type   TEXT NOT NULL,
    p_complete     DOUBLE PRECISION NOT NULL,
    completed      BOOLEAN NOT NULL,
    predicted_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS completion_predictions_time_idx ON completion_predictions (event_time DESC);

-- Extension B: rolling (prequential) evaluation of the online model.
CREATE TABLE IF NOT EXISTS model_metrics (
    recorded_at        TIMESTAMPTZ PRIMARY KEY DEFAULT now(),
    trips_seen         INTEGER NOT NULL,
    window_size        INTEGER NOT NULL,
    accuracy           DOUBLE PRECISION NOT NULL,
    baseline_accuracy  DOUBLE PRECISION NOT NULL,
    log_loss           DOUBLE PRECISION NOT NULL,
    baseline_log_loss  DOUBLE PRECISION NOT NULL,
    auc                DOUBLE PRECISION
);

-- Supabase exposes the public schema through its REST API. Turning on row level security
-- with no policies blocks that API, while the postgres role used here still has full access.
ALTER TABLE trips                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE zone_minute_metrics    ENABLE ROW LEVEL SECURITY;
ALTER TABLE surge_alerts           ENABLE ROW LEVEL SECURITY;
ALTER TABLE fare_anomalies         ENABLE ROW LEVEL SECURITY;
ALTER TABLE completion_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_metrics          ENABLE ROW LEVEL SECURITY;
ALTER TABLE stream_offsets         ENABLE ROW LEVEL SECURITY;
