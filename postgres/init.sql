-- Sink tables for the Flink driving-safety job. Only flagged manoeuvres are
-- stored; the raw stream itself is not archived (originals stay in the scene
-- JSONs). One row per manoeuvre, not per 100 ms bin — see flink/safety.sql.

CREATE TABLE IF NOT EXISTS safety_events (
    vehicle_id    TEXT,
    event_type    TEXT,
    episode_start TIMESTAMP,
    episode_end   TIMESTAMP,
    duration_s    DOUBLE PRECISION,
    peak_value    DOUBLE PRECISION,
    bins          BIGINT,
    entry_speed   DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_safety_events_vehicle_ts
    ON safety_events (vehicle_id, episode_start);

CREATE TABLE IF NOT EXISTS driving_windows (
    vehicle_id   TEXT,
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    harsh_brakes BIGINT,
    sharp_turns  BIGINT,
    harsh_accels BIGINT,
    episode_cnt  BIGINT,
    avg_speed    DOUBLE PRECISION,
    flag         TEXT
);

CREATE INDEX IF NOT EXISTS idx_driving_windows_vehicle_start
    ON driving_windows (vehicle_id, window_start);
