"""Replay nuScenes scenes into Kafka at their original real-time pace.

Pipeline position: scene_loader (가공) -> validator (검증) -> HERE (전송).
Clean records go to the main topic, rejected ones to the DLQ topic with their
reasons attached. Several scenes replay concurrently, each acting as one
vehicle — messages are keyed by vehicle_id so per-vehicle ordering survives
partitioning (see 내부구조_사전 #5).

Timestamps are rebased to "now": every scene starts at launch time, so
concurrently replayed 2018 recordings behave like vehicles driving today and
Flink's watermark advances naturally.

Usage:
  python3 replay_producer.py scene-0001 scene-0002        # specific scenes
  python3 replay_producer.py --count 5 --speed 10         # first 5 scenes, 10x
  python3 replay_producer.py --count 2 --dry-run          # no Kafka, print only
"""

import argparse
import heapq
import json
import time

from scene_loader import DATA_DIR, load_scene
from validator import validate_stream

MAIN_TOPIC = "vehicle-can-data"
DLQ_TOPIC = "vehicle-can-dlq"
BOOTSTRAP = "localhost:29092"
DRY_RUN_SAMPLE = 5  # messages printed in a dry run, enough to eyeball the shape
LATE_TOLERANCE_S = 0.001  # a message this far past its slot counts as falling behind


def scene_timeline(scene: str, now_us: int, offset_s: float = 0.0):
    """Yield (rel_seconds, topic, record) for one scene, timestamps rebased.

    offset_s staggers this scene's start so concurrently replayed vehicles do not
    all begin in the same millisecond. A fleet whose recordings overlap keeps the
    stream — and therefore the watermark — moving past window boundaries.
    """
    records = load_scene(scene)
    if not records:
        return
    base_us = records[0]["ts_us"]
    clean, rejected = validate_stream(records)

    timeline = [(rec["ts_us"], MAIN_TOPIC, rec) for rec in clean]
    timeline += [
        (rec["ts_us"], DLQ_TOPIC, {"record": rec, "reasons": reasons})
        for rec, reasons in rejected
    ]
    timeline.sort(key=lambda e: e[0])

    for ts_us, topic, payload in timeline:
        offset_us = int(offset_s * 1e6)
        rebased = dict(payload)
        if topic == MAIN_TOPIC:
            rebased["ts_us"] = now_us + (ts_us - base_us) + offset_us
        yield ((ts_us - base_us) / 1e6 + offset_s, topic, rebased)


def replay(scenes: list[str], speed: float, dry_run: bool, stagger: float = 0.0) -> None:
    now_us = time.time_ns() // 1000
    # heapq.merge lazily interleaves the per-scene generators by rel_seconds,
    # which already include each scene's stagger offset, so this is the global
    # send order across the whole fleet.
    merged = heapq.merge(
        *(scene_timeline(s, now_us, i * stagger) for i, s in enumerate(scenes)),
        key=lambda e: e[0],
    )

    producer = None
    if not dry_run:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP,
            key_serializer=lambda k: k.encode(),
            value_serializer=lambda v: json.dumps(v).encode(),
        )

    sent = {MAIN_TOPIC: 0, DLQ_TOPIC: 0}
    printed = 0   # dry-run sample lines; kept apart from `sent`, which reports totals
    behind = 0    # messages whose slot had already passed when they were sent
    first_rel = last_rel = None
    t0 = time.monotonic()
    for rel_s, topic, payload in merged:
        if first_rel is None:
            first_rel = rel_s
        last_rel = rel_s

        # A dry run reports what would be sent; making it wait out the recording
        # would cost 20 s per scene to print a handful of lines.
        if not dry_run:
            wait = rel_s / speed - (time.monotonic() - t0)
            if wait > 0:
                time.sleep(wait)
            elif wait < -LATE_TOLERANCE_S:
                behind += 1

        key = payload["vehicle_id"] if topic == MAIN_TOPIC else payload["record"]["vehicle_id"]
        if dry_run:
            if printed < DRY_RUN_SAMPLE:
                print(f"[dry-run] {rel_s:7.3f}s {topic} key={key} {json.dumps(payload)[:110]}")
                printed += 1
        else:
            producer.send(topic, key=key, value=payload)
        sent[topic] += 1

    if producer:
        producer.flush()
        producer.close()

    took = time.monotonic() - t0
    span = (last_rel - first_rel) if first_rel is not None else 0.0
    totals = f"main={sent[MAIN_TOPIC]} dlq={sent[DLQ_TOPIC]}"
    if dry_run:
        print(f"[dry-run] {len(scenes)} scene(s), {totals} — {span:.1f}s of event time, "
              f"{span / speed:.1f}s of wall clock at x{speed}")
        return

    # Requesting a speed the producer cannot sustain is silent otherwise: the
    # wait simply goes negative and the run reports the speed it was asked for.
    achieved = span / took if took > 0 else 0.0
    print(f"replayed {len(scenes)} scene(s) in {took:.1f}s "
          f"(x{speed} requested, x{achieved:.0f} achieved): {totals}")
    if behind and achieved < speed * 0.8:
        print(f"  ! fell behind on {behind} of {sum(sent.values())} messages — "
              f"x{speed} is beyond what this producer sustains here")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("scenes", nargs="*", help="scene names, e.g. scene-0001")
    parser.add_argument("--count", type=int, help="replay the first N scenes instead")
    parser.add_argument("--speed", type=float, default=1.0, help="replay speed multiplier")
    parser.add_argument("--dry-run", action="store_true", help="print instead of sending")
    parser.add_argument(
        "--stagger",
        type=float,
        default=5.0,
        help="seconds between scene start times (0 = all vehicles start together)",
    )
    args = parser.parse_args()

    scenes = args.scenes
    if args.count:
        all_scenes = sorted({p.name.split("_")[0] for p in DATA_DIR.glob("scene-*_meta.json")})
        scenes = all_scenes[: args.count]
    if not scenes:
        parser.error("give scene names or --count N")

    replay(scenes, args.speed, args.dry_run, args.stagger)


if __name__ == "__main__":
    main()
