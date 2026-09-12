-- Driving-safety detection over the unified 10 Hz CAN stream.
-- Executed statement-by-statement by DrivingSafetyJob at cluster start.
--
-- Detection boundary (내부구조_사전 #3): the validator upstream already removed
-- the physically impossible, so everything here is real driving — these rules
-- flag the physically possible but DANGEROUS.
--
-- EPISODES, NOT BINS (내부구조_사전 #8). A manoeuvre lasts about a second, which
-- is ten 100 ms bins, and the signal wobbles across the threshold while it does.
-- Counting bins therefore reported one hard brake as up to ten events and one
-- corner as three, which in turn made the 30 s window flag a single corner as
-- aggressive driving. MATCH_RECOGNIZE collapses each manoeuvre into one row
-- using two thresholds: the episode is ENTERED on the strong threshold and HELD
-- while the signal stays past a weaker one, so brief dips do not split it.
--
-- Entry thresholds are quoted from vehicle dynamics and were then checked against
-- all 979 scenes so that all three rules sit at the same selectivity — the top
-- ~0.1% of bins above 10 km/h — rather than each being arbitrarily strict or loose:
--   HARSH_BRAKE  enter -4.0 m/s² (0.4 G) — comfortable braking is ~2.5   [top 0.12%]
--   SHARP_TURN   enter  3.0 m/s² lateral — everyday cornering is ~2      [top 0.10%]
--   HARSH_ACCEL  enter  2.5 m/s² — low end of the industry harsh-accel
--                                  range (2.5-3.5)                      [top 0.10%]
-- Each hold threshold is 75% of its entry value, and an episode must span at
-- least two bins (200 ms) to count as a manoeuvre — commercial telematics
-- likewise require a harsh event to persist, usually for longer than this.
-- Single-bin crossings are not noise: 96% of them have the accelerator applied,
-- so they are real but weak, gaining a third of the speed a longer episode does
-- and showing no speed gain at all in half of the cases.
-- speed > 10 km/h guards exclude parking-lot manoeuvres throughout.

CREATE TABLE vehicle_source (
    vehicle_id      STRING,
    ts_us           BIGINT,
    speed_kmh       DOUBLE,
    yaw_rate        DOUBLE,
    brake           INT,
    throttle        INT,
    accel_lon       DOUBLE,
    accel_lon_min   DOUBLE,
    accel_lon_max   DOUBLE,
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
    event_type    STRING,
    episode_start TIMESTAMP(3),
    episode_end   TIMESTAMP(3),
    duration_s    DOUBLE,
    peak_value    DOUBLE,
    bins          BIGINT,
    entry_speed   DOUBLE
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
    harsh_accels BIGINT,
    episode_cnt  BIGINT,
    avg_speed    DOUBLE,
    flag         STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/vehicle_db',
    'table-name' = 'driving_windows',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'driver' = 'org.postgresql.Driver'
);

-- One row per braking manoeuvre. LAST(E.<col>, 1) is the previous row already
-- matched to E: NULL on the first row, which is what makes the entry threshold
-- apply only there and the weaker hold threshold apply afterwards.
CREATE VIEW harsh_brake_episodes AS
SELECT vehicle_id, 'HARSH_BRAKE' AS event_type,
       episode_start, episode_end, peak_value, bins, entry_speed
FROM vehicle_source
MATCH_RECOGNIZE (
    PARTITION BY vehicle_id
    ORDER BY event_ts
    MEASURES
        FIRST(E.event_ts)     AS episode_start,
        LAST(E.event_ts)      AS episode_end,
        MIN(E.accel_lon_min)  AS peak_value,
        COUNT(E.event_ts)     AS bins,
        FIRST(E.speed_kmh)    AS entry_speed
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E{2,})
    DEFINE
        E AS E.speed_kmh > 10 AND (
                 (LAST(E.accel_lon_min, 1) IS NULL     AND E.accel_lon_min < -4.0)
              OR (LAST(E.accel_lon_min, 1) IS NOT NULL AND E.accel_lon_min < -3.0)
             )
);

CREATE VIEW sharp_turn_episodes AS
SELECT vehicle_id, 'SHARP_TURN' AS event_type,
       episode_start, episode_end, peak_value, bins, entry_speed
FROM vehicle_source
MATCH_RECOGNIZE (
    PARTITION BY vehicle_id
    ORDER BY event_ts
    MEASURES
        FIRST(E.event_ts)          AS episode_start,
        LAST(E.event_ts)           AS episode_end,
        MAX(ABS(E.accel_lat_max))  AS peak_value,
        COUNT(E.event_ts)          AS bins,
        FIRST(E.speed_kmh)         AS entry_speed
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E{2,})
    DEFINE
        E AS E.speed_kmh > 10 AND (
                 (LAST(E.accel_lat_max, 1) IS NULL     AND ABS(E.accel_lat_max) > 3.0)
              OR (LAST(E.accel_lat_max, 1) IS NOT NULL AND ABS(E.accel_lat_max) > 2.25)
             )
);

CREATE VIEW harsh_accel_episodes AS
SELECT vehicle_id, 'HARSH_ACCEL' AS event_type,
       episode_start, episode_end, peak_value, bins, entry_speed
FROM vehicle_source
MATCH_RECOGNIZE (
    PARTITION BY vehicle_id
    ORDER BY event_ts
    MEASURES
        FIRST(E.event_ts)     AS episode_start,
        LAST(E.event_ts)      AS episode_end,
        MAX(E.accel_lon_max)  AS peak_value,
        COUNT(E.event_ts)     AS bins,
        FIRST(E.speed_kmh)    AS entry_speed
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E{2,})
    DEFINE
        E AS E.speed_kmh > 10 AND (
                 (LAST(E.accel_lon_max, 1) IS NULL     AND E.accel_lon_max > 2.5)
              OR (LAST(E.accel_lon_max, 1) IS NOT NULL AND E.accel_lon_max > 1.9)
             )
);

CREATE VIEW safety_episodes AS
SELECT * FROM harsh_brake_episodes
UNION ALL
SELECT * FROM sharp_turn_episodes
UNION ALL
SELECT * FROM harsh_accel_episodes;

-- Each row is one manoeuvre. bins x 100 ms is how long it lasted.
INSERT INTO safety_events_sink
SELECT
    vehicle_id,
    event_type,
    episode_start,
    episode_end,
    CAST(bins AS DOUBLE) * 0.1 AS duration_s,
    ROUND(peak_value, 3),
    bins,
    entry_speed
FROM safety_episodes;

-- 30 s tumbling window per vehicle, now counting manoeuvres rather than bins:
-- three separate harsh manoeuvres inside 30 s is a driving pattern, whereas a
-- single corner that wobbles across the threshold is not.
INSERT INTO driving_windows_sink
SELECT
    vehicle_id,
    window_start,
    window_end,
    SUM(CASE WHEN event_type = 'HARSH_BRAKE' THEN 1 ELSE 0 END) AS harsh_brakes,
    SUM(CASE WHEN event_type = 'SHARP_TURN'  THEN 1 ELSE 0 END) AS sharp_turns,
    SUM(CASE WHEN event_type = 'HARSH_ACCEL' THEN 1 ELSE 0 END) AS harsh_accels,
    COUNT(*)                       AS episode_cnt,
    ROUND(AVG(entry_speed), 1)     AS avg_speed,
    'AGGRESSIVE_DRIVING'           AS flag
FROM TABLE(
    TUMBLE(TABLE safety_episodes, DESCRIPTOR(episode_end), INTERVAL '30' SECOND)
)
GROUP BY vehicle_id, window_start, window_end
HAVING COUNT(*) >= 3;
