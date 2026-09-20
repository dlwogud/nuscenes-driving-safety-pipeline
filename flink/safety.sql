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

-- Only speed_kmh, accel_lon_min, accel_lon_max and accel_lat_max are read by the
-- rules below. The remaining columns are declared so the record keeps describing
-- the vehicle's state rather than just the current rule set: they are what makes
-- a quarantined record readable, and what a new rule would otherwise have to be
-- threaded back through the loader and this schema to obtain.
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
    -- Event time comes from the record itself (µs -> ms), not from arrival time.
    -- Cast away the local time zone: TUMBLE rejects a TIMESTAMP_LTZ rowtime once
    -- it has travelled through MATCH_RECOGNIZE and UNION ALL.
    event_ts AS CAST(TO_TIMESTAMP_LTZ(ts_us / 1000, 3) AS TIMESTAMP(3)),
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
-- TERM is the first row that breaks the hold condition. Flink refuses a pattern
-- ending in a greedy quantifier, so the episode has to be closed by a row that
-- does not belong to it; that row can never start the next episode either,
-- since failing the hold threshold also fails the stricter entry one.
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
    PATTERN (E{2,} TERM)
    DEFINE
        E AS E.speed_kmh > 10 AND (
                 (LAST(E.accel_lon_min, 1) IS NULL     AND E.accel_lon_min < -4.0)
              OR (LAST(E.accel_lon_min, 1) IS NOT NULL AND E.accel_lon_min < -3.0)
             ),
        TERM AS TERM.speed_kmh <= 10
             OR TERM.accel_lon_min IS NULL
             OR TERM.accel_lon_min >= -3.0
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
    PATTERN (E{2,} TERM)
    DEFINE
        E AS E.speed_kmh > 10 AND (
                 (LAST(E.accel_lat_max, 1) IS NULL     AND ABS(E.accel_lat_max) > 3.0)
              OR (LAST(E.accel_lat_max, 1) IS NOT NULL AND ABS(E.accel_lat_max) > 2.25)
             ),
        TERM AS TERM.speed_kmh <= 10
             OR TERM.accel_lat_max IS NULL
             OR ABS(TERM.accel_lat_max) <= 2.25
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
    PATTERN (E{2,} TERM)
    DEFINE
        E AS E.speed_kmh > 10 AND (
                 (LAST(E.accel_lon_max, 1) IS NULL     AND E.accel_lon_max > 2.5)
              OR (LAST(E.accel_lon_max, 1) IS NOT NULL AND E.accel_lon_max > 1.9)
             ),
        TERM AS TERM.speed_kmh <= 10
             OR TERM.accel_lon_max IS NULL
             OR TERM.accel_lon_max <= 1.9
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

-- A burst of manoeuvres per vehicle, counted as manoeuvres rather than threshold
-- crossings — a single corner wobbling across the line is no longer three events.
--
-- SESSION rather than TUMBLE. The intent is "two harsh manoeuvres close together",
-- but a tumbling window asks "two in the same fixed box", and those differ:
-- scene-0056's two manoeuvres are 12 s apart yet a boundary fell between them, so
-- the rule missed a case it was written to catch. A session asks the intended
-- question, and its start and end describe the burst rather than a grid cell.
--
-- The 10 s gap comes from how manoeuvres actually cluster here: of the 11 pairs
-- that share a scene, eight are within 2 s of each other (median 1.7 s) — a brake
-- followed by a swerve, not two unrelated events. Any gap from 5 to 10 s selects
-- the same 10 scenes, so the choice sits on a plateau rather than a knife edge;
-- beyond 15 s it stops constraining anything, because a scene is only 20 s long
-- and every pair in it then qualifies regardless of spacing.
--
-- Two manoeuvres is the threshold rather than three: a scene supplies about 20 s
-- of driving, so at three the rule fires on none of the 979 scenes.
-- Grouped-window syntax rather than the TUMBLE(TABLE ...) table function: the
-- newer form rejects a rowtime that has passed through MATCH_RECOGNIZE and
-- UNION ALL, even though it still carries the ROWTIME marker.
INSERT INTO driving_windows_sink
SELECT
    vehicle_id,
    -- The burst's own extent, not SESSION_START/END: the session's end carries
    -- the 10 s idle gap that closed it, which would overstate how long the
    -- driving actually lasted.
    MIN(episode_start) AS window_start,
    MAX(episode_end)   AS window_end,
    SUM(CASE WHEN event_type = 'HARSH_BRAKE' THEN 1 ELSE 0 END) AS harsh_brakes,
    SUM(CASE WHEN event_type = 'SHARP_TURN'  THEN 1 ELSE 0 END) AS sharp_turns,
    SUM(CASE WHEN event_type = 'HARSH_ACCEL' THEN 1 ELSE 0 END) AS harsh_accels,
    COUNT(*)                       AS episode_cnt,
    ROUND(AVG(entry_speed), 1)     AS avg_speed,
    'AGGRESSIVE_DRIVING'           AS flag
FROM safety_episodes
GROUP BY vehicle_id, SESSION(episode_end, INTERVAL '10' SECOND)
HAVING COUNT(*) >= 2;
