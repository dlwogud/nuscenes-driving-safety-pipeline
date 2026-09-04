-- Sink tables for the Flink driving-safety job. Only flagged events are stored;
-- the raw stream itself is not archived (originals stay in the scene JSONs).

CREATE TABLE IF NOT EXISTS safety_events (
    vehicle_id    TEXT,
    event_ts      TIMESTAMP,
    event_type    TEXT,
    speed_kmh     DOUBLE PRECISION,
    accel_lon_min DOUBLE PRECISION,
    accel_lat_max DOUBLE PRECISION,
    brake         INTEGER,
    throttle      INTEGER
);

CREATE INDEX IF NOT EXISTS idx_safety_events_vehicle_ts
    ON safety_events (vehicle_id, event_ts);

CREATE TABLE IF NOT EXISTS driving_windows (
    vehicle_id   TEXT,
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    harsh_brakes BIGINT,
    sharp_turns  BIGINT,
    avg_speed    DOUBLE PRECISION,
    record_cnt   BIGINT,
    flag         TEXT
);

CREATE INDEX IF NOT EXISTS idx_driving_windows_vehicle_start
    ON driving_windows (vehicle_id, window_start);
