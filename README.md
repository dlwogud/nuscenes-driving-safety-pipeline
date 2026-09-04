# nuScenes Driving Safety Pipeline

Real-time driving safety analysis pipeline built on **real autonomous vehicle CAN bus data** ([nuScenes CAN bus expansion](https://www.nuscenes.org/nuscenes#canbus), 979 driving scenes collected by Motional's Renault Zoe AV fleet).

Replays multi-frequency CAN channels (2 Hz – 950 Hz) into Kafka, detects unsafe driving events (harsh braking, sharp steering at speed, yaw-rate spikes) with Flink SQL — both event-level rules and 30-second tumbling-window aggregation — and lands results in PostgreSQL.

> 실제 자율주행 차량에서 수집한 nuScenes CAN 버스 데이터 기반 실시간 주행 안전성 분석 파이프라인.

## Architecture

```
nuScenes CAN JSON (979 scenes)
   → Replay Producer (Python: scene loader / channel merge / validator + DLQ)
   → Kafka (topic: vehicle-can-data, key = vehicle_id)
   → Flink (event rules + 30s TUMBLE window)
   → PostgreSQL
```

## Status

- [ ] Phase 1 — MVP: replay producer, channel unification, safety rules, data validation
- [ ] Phase 2 — Reliability: checkpointing, exactly-once, failure-recovery experiments, GCP + Grafana
- [ ] Phase 3 — Scale: benchmarks, partition scaling, lakehouse sink

## Prior work

v1 of this project ([vehicle-streaming-anomaly-detection](https://github.com/dlwogud/vehicle-streaming-anomaly-detection)) built the same streaming core on simulated engine-vehicle data with dbt/Airflow downstream. v2 is a ground-up redesign around real EV sensor data: new replay producer, new safety-rule domain model, explicit data-quality validation.
