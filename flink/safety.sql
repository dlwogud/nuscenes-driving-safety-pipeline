-- Driving-safety detection over the unified 10 Hz CAN stream.
-- Executed statement-by-statement by DrivingSafetyJob at cluster start.
--
-- Detection boundary (내부구조_사전 #3): the validator upstream already removed
-- the physically impossible, so everything here is real driving — these rules
-- flag the physically possible but DANGEROUS.
--
-- Thresholds are quoted, not invented, and verified against all 979 scenes:
--   HARSH_BRAKE  accel_lon_min < -4.0 m/s² (0.4 G)  — hard braking; comfortable
--                braking stays near 2.5 m/s². Fires on 0.04% of records.
--   SHARP_TURN   |accel_lat_max| > 3.0 m/s² (0.3 G) — fleet-telematics harsh
--                cornering convention. Fires on 0.09% of records.
--   PEDAL_MISUSE brake pressure > 0 while throttle > 50% — pedal misapplication.
--   speed_kmh > 10 guards exclude parking-lot noise.

CREATE TABLE vehicle_source (
    vehicle_id      STRING,
    ts_us           BIGINT,
    speed_kmh       DOUBLE,
    yaw_rate        DOUBLE,
    brake           INT,
    throttle        INT,
    accel_lon       DOUBLE,
    accel_lon_min   DOUBLE,
    accel_lat       DOUBLE,
    accel_lat_max   DOUBLE,
    steering_deg    DOUBLE,
    wheel_delta_rpm DOUBLE,
    wheel_rpm_mean  DOUBLE,
    brake_sensor    DOUBLE,
    throttle_sensor DOUBLE,
    -- event time comes from the record itself (µs -> ms), not from arrival time
    event_ts AS TO_TIMESTAMP_LTZ(ts_us / 1000, 3),
    -- 5 s grace: records later than this miss their window (see 사전 #6)
    WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'vehicle-can-data',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-safety-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE safety_events_sink (
    vehicle_id    STRING,
    event_ts      TIMESTAMP(3),
    event_type    STRING,
    speed_kmh     DOUBLE,
    accel_lon_min DOUBLE,
    accel_lat_max DOUBLE,
    brake         INT,
    throttle      INT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/vehicle_db',
    'table-name' = 'safety_events',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE driving_windows_sink (
    vehicle_id   STRING,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    harsh_brakes BIGINT,
    sharp_turns  BIGINT,
    avg_speed    DOUBLE,
    record_cnt   BIGINT,
    flag         STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/vehicle_db',
    'table-name' = 'driving_windows',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'driver' = 'org.postgresql.Driver'
);

-- Event-level rules: only flagged rows are stored, the stream itself is not archived.
INSERT INTO safety_events_sink
SELECT
    vehicle_id,
    CAST(event_ts AS TIMESTAMP(3)),
    CASE
        WHEN accel_lon_min < -4.0 AND speed_kmh > 10      THEN 'HARSH_BRAKE'
        WHEN ABS(accel_lat_max) > 3.0 AND speed_kmh > 10  THEN 'SHARP_TURN'
        ELSE 'PEDAL_MISUSE'
    END AS event_type,
    speed_kmh,
    accel_lon_min,
    accel_lat_max,
    brake,
    throttle
FROM vehicle_source
WHERE (accel_lon_min < -4.0 AND speed_kmh > 10)
   OR (ABS(accel_lat_max) > 3.0 AND speed_kmh > 10)
   OR (brake > 0 AND throttle > 50);

-- 30 s tumbling window per vehicle: a single harsh event may be an evasive
-- maneuver; three or more inside 30 s is a driving pattern.
INSERT INTO driving_windows_sink
SELECT
    vehicle_id,
    window_start,
    window_end,
    SUM(CASE WHEN accel_lon_min < -4.0 AND speed_kmh > 10 THEN 1 ELSE 0 END)     AS harsh_brakes,
    SUM(CASE WHEN ABS(accel_lat_max) > 3.0 AND speed_kmh > 10 THEN 1 ELSE 0 END) AS sharp_turns,
    ROUND(AVG(speed_kmh), 1) AS avg_speed,
    COUNT(*)                 AS record_cnt,
    'AGGRESSIVE_DRIVING'     AS flag
FROM TABLE(
    TUMBLE(TABLE vehicle_source, DESCRIPTOR(event_ts), INTERVAL '30' SECOND)
)
GROUP BY vehicle_id, window_start, window_end
HAVING SUM(CASE WHEN accel_lon_min < -4.0 AND speed_kmh > 10 THEN 1 ELSE 0 END)
     + SUM(CASE WHEN ABS(accel_lat_max) > 3.0 AND speed_kmh > 10 THEN 1 ELSE 0 END) >= 3;
