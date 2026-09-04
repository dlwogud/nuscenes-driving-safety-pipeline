"""Load one nuScenes CAN bus scene and resample every channel onto a common 10 Hz grid.

Input : scene name (e.g. "scene-0001") + directory holding the raw channel JSONs
Output: list of flat dicts, one per 100 ms bin, ready for Kafka

Aggregation policy (see 내부구조_사전 #2):
  - slow channels (vehicle_monitor, 2 Hz)  -> forward fill (last known value)
  - fast channels (ms_imu 100 Hz, zoesensors ~950 Hz, zoe_veh_info 100 Hz)
    -> per-bin mean, plus min/max for spike-sensitive fields so that a momentary
       hard-brake spike inside a bin survives the averaging
"""

import json
import sys
from bisect import bisect_right
from pathlib import Path
from statistics import fmean

DATA_DIR = Path.home() / "Downloads" / "can_bus" / "can_bus"
BIN_US = 100_000  # 100 ms grid -> 10 Hz


def _load(scene: str, channel: str) -> list[dict]:
    path = DATA_DIR / f"{scene}_{channel}.json"
    records = json.loads(path.read_text())
    return sorted(records, key=lambda r: r["utime"])


class ForwardFill:
    """Last known value at or before a given utime; first value before that."""

    def __init__(self, records: list[dict], field: str):
        self.utimes = [r["utime"] for r in records]
        self.values = [r[field] for r in records]

    def at(self, utime: int):
        if not self.values:  # channel entirely absent (e.g. scene-0419's monitor)
            return None
        i = bisect_right(self.utimes, utime)
        return self.values[i - 1] if i > 0 else self.values[0]


def _bin_slices(records: list[dict], start_us: int, n_bins: int) -> list[list[dict]]:
    """Split time-sorted records into n_bins buckets of BIN_US each."""
    bins: list[list[dict]] = [[] for _ in range(n_bins)]
    for r in records:
        i = (r["utime"] - start_us) // BIN_US
        if 0 <= i < n_bins:
            bins[i].append(r)
    return bins


def load_scene(scene: str) -> list[dict]:
    imu = _load(scene, "ms_imu")
    monitor = _load(scene, "vehicle_monitor")
    veh = _load(scene, "zoe_veh_info")
    pedals = _load(scene, "zoesensors")

    # Grid covers the INTERSECTION of the fast channels' time spans: channels
    # stop a few ms apart, and a partial tail bin would emit records missing
    # whole channels. monitor (2 Hz) is excluded — it is forward-filled anyway.
    start_us = max(ch[0]["utime"] for ch in (imu, veh, pedals))
    end_us = min(ch[-1]["utime"] for ch in (imu, veh, pedals))
    n_bins = (end_us - start_us) // BIN_US + 1

    imu_bins = _bin_slices(imu, start_us, n_bins)
    veh_bins = _bin_slices(veh, start_us, n_bins)
    pedal_bins = _bin_slices(pedals, start_us, n_bins)

    ff_speed = ForwardFill(monitor, "vehicle_speed")
    ff_yaw = ForwardFill(monitor, "yaw_rate")
    ff_brake = ForwardFill(monitor, "brake")
    ff_throttle = ForwardFill(monitor, "throttle")

    out: list[dict] = []
    for i in range(n_bins):
        if not imu_bins[i] and not veh_bins[i]:
            continue  # trailing/leading gap with no fast-channel data

        ts_us = start_us + i * BIN_US
        rec: dict = {
            "vehicle_id": scene,
            "ts_us": ts_us,
            # 2 Hz dashboard channel: forward-filled
            "speed_kmh": ff_speed.at(ts_us),
            "yaw_rate": ff_yaw.at(ts_us),
            "brake": ff_brake.at(ts_us),
            "throttle": ff_throttle.at(ts_us),
        }

        if imu_bins[i]:
            lon = [r["linear_accel"][0] for r in imu_bins[i]]
            lat = [r["linear_accel"][1] for r in imu_bins[i]]
            rec["accel_lon"] = round(fmean(lon), 4)
            rec["accel_lon_min"] = round(min(lon), 4)  # hard-brake spike survives here
            rec["accel_lat"] = round(fmean(lat), 4)
            rec["accel_lat_max"] = round(max(lat, key=abs), 4)

        if veh_bins[i]:
            rec["steering_deg"] = round(fmean(r["steer_corrected"] for r in veh_bins[i]), 2)
            rec["wheel_delta_rpm"] = round(
                fmean(abs(r["RL_wheel_speed"] - r["RR_wheel_speed"]) for r in veh_bins[i]), 2
            )
            # mean of all four wheels; validator cross-checks this against speed_kmh
            rec["wheel_rpm_mean"] = round(
                fmean(
                    (r["FL_wheel_speed"] + r["FR_wheel_speed"]
                     + r["RL_wheel_speed"] + r["RR_wheel_speed"]) / 4
                    for r in veh_bins[i]
                ), 2
            )

        if pedal_bins[i]:
            rec["brake_sensor"] = round(fmean(r["brake_sensor"] for r in pedal_bins[i]), 4)
            rec["throttle_sensor"] = round(fmean(r["throttle_sensor"] for r in pedal_bins[i]), 4)

        out.append(rec)

    return out


if __name__ == "__main__":
    scene = sys.argv[1] if len(sys.argv) > 1 else "scene-0001"
    records = load_scene(scene)
    span_s = (records[-1]["ts_us"] - records[0]["ts_us"]) / 1e6
    print(f"{scene}: {len(records)} records over {span_s:.1f}s")
    for r in records[:3]:
        print(json.dumps(r, ensure_ascii=False))
