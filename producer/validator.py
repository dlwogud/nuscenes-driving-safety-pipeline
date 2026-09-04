"""Data-quality gate between scene_loader and the Kafka producer.

Each record is checked and given a list of failure reasons. Clean records go to
the main topic; records with any reason go to the dead-letter topic instead,
so nothing is silently dropped and every rejection stays inspectable.

Boundary rule (see 내부구조_사전 #3): reject only the PHYSICALLY IMPOSSIBLE
(sensor faults). Physically possible but dangerous driving (hard braking,
sharp steering) is not a data error — detecting it is the Flink job's role.
"""

import json
import sys
from collections import Counter

REQUIRED_FIELDS = ("vehicle_id", "ts_us", "speed_kmh", "accel_lon", "steering_deg")

# Renault Zoe: ~1.94 m tire circumference -> wheel rpm 165 ≈ 19 km/h
KMH_PER_RPM = 1.94 * 60 / 1000
SPEED_WHEEL_TOLERANCE = 0.35  # 35% relative mismatch allowed (tire wear, slip, noise)
MAX_STALENESS_S = 0.5  # dashboard is 2 Hz + forward fill -> value may be this old


def check_required(rec):
    # a present-but-None field (empty source channel, see scene-0419) is missing too
    missing = [f for f in REQUIRED_FIELDS if rec.get(f) is None]
    if missing:
        return "missing:" + ",".join(missing)


def check_speed_range(rec):
    # TODO(재형): 범위 검증 룰 직접 구현하기.
    speed_kmh=rec.get("speed_kmh")
    if speed_kmh is None:
        return None
    if speed_kmh<0 or speed_kmh>200:
        return "speed_out_of_range"
    
    #   speed_kmh가 0 미만이거나 200 초과면 "speed_out_of_range" 반환, 정상이면 None.
    #   주의: rec.get("speed_kmh")가 None이면 결측이라 여기선 통과(None 반환) —
    #   결측 처리는 check_required 담당이므로 책임을 섞지 않는다.
    return None


def check_accel_range(rec):
    for field in ("accel_lon", "accel_lon_min", "accel_lat", "accel_lat_max"):
        v = rec.get(field)
        if v is not None and abs(v) > 15:  # |1.5G| — beyond any road vehicle
            return f"accel_impossible:{field}={v}"


def check_yaw_range(rec):
    v = rec.get("yaw_rate")
    if v is not None and abs(v) > 120:  # deg/s; a road car spin tops out well below
        return f"yaw_impossible:{v}"


def check_speed_vs_wheels(rec):
    """Cross-check: dashboard speed and wheel rpm must tell the same story.

    The dashboard channel is 2 Hz and forward-filled, so its value can be up to
    ~500 ms stale. While accelerating/braking the two channels legitimately
    disagree by (accel × staleness), so that much extra gap is allowed —
    otherwise every hard brake would be flagged as a sensor fault.
    """
    speed, rpm = rec.get("speed_kmh"), rec.get("wheel_rpm_mean")
    # Below ~5 km/h the dashboard readout is quantized/held while the wheels
    # already move (stop-and-go), so the ratio is meaningless: full-sweep showed
    # every sub-5 mismatch was this artifact, and no safety rule fires at
    # walking pace anyway.
    if speed is None or rpm is None or speed < 5:
        return None
    wheel_kmh = rpm * KMH_PER_RPM
    staleness_kmh = abs(rec.get("accel_lon") or 0) * MAX_STALENESS_S * 3.6
    if abs(wheel_kmh - speed) > speed * SPEED_WHEEL_TOLERANCE + staleness_kmh:
        return f"speed_wheel_mismatch:dash={speed},wheel={round(wheel_kmh, 1)}"


PER_RECORD_CHECKS = (
    check_required,
    check_speed_range,
    check_accel_range,
    check_yaw_range,
    check_speed_vs_wheels,
)


def validate(rec) -> list[str]:
    """Reasons this record should go to the DLQ. Empty list = clean."""
    return [r for check in PER_RECORD_CHECKS if (r := check(rec))]


def validate_stream(records):
    """Split a scene's records into (clean, rejected-with-reasons).

    Also enforces the one stateful check — event time must move forward.
    """
    clean, rejected = [], []
    prev_ts = None
    for rec in records:
        reasons = validate(rec)
        ts = rec.get("ts_us")
        if prev_ts is not None and ts is not None and ts <= prev_ts:
            reasons.append(f"time_regression:{ts}<= {prev_ts}")
        prev_ts = ts if ts is not None else prev_ts
        (rejected if reasons else clean).append((rec, reasons) if reasons else rec)
    return clean, rejected


if __name__ == "__main__":
    from scene_loader import load_scene

    scene = sys.argv[1] if len(sys.argv) > 1 else "scene-0001"
    clean, rejected = validate_stream(load_scene(scene))
    print(f"{scene}: clean={len(clean)} rejected={len(rejected)}")
    reason_counts = Counter(r.split(":")[0] for _, reasons in rejected for r in reasons)
    for reason, n in reason_counts.most_common():
        print(f"  {reason}: {n}")
    if rejected:
        print("sample reject:", json.dumps(rejected[0], ensure_ascii=False)[:300])
