"""Build the Flink job jar and bring the cluster up.

Services are addressed through `docker compose` (not fixed container names) so
this project can coexist with other compose projects on the same machine.
"""

import subprocess
import sys
import time
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
JAR_NAME = "driving-safety-job.jar"
TOPICS = ("vehicle-can-data", "vehicle-can-dlq")


def run_command(command: list[str]) -> int:
    return subprocess.run(command, cwd=ROOT_DIR).returncode


def compose_container_id(service: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "ps", "-q", service],
        cwd=ROOT_DIR,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def wait_for_kafka(timeout_seconds: int = 120) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        container_id = compose_container_id("kafka")
        if container_id:
            result = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "-f",
                    "{{if .State.Health}}{{.State.Health.Status}}"
                    "{{else}}{{.State.Status}}{{end}}",
                    container_id,
                ],
                cwd=ROOT_DIR,
                capture_output=True,
                text=True,
            )
            if result.returncode == 0 and result.stdout.strip() == "healthy":
                return True
        time.sleep(2)
    return False


def create_kafka_topics() -> int:
    for topic in TOPICS:
        rc = run_command(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "bash",
                "-lc",
                (
                    "kafka-topics --bootstrap-server kafka:9092 "
                    "--create --if-not-exists "
                    f"--topic {topic} "
                    "--partitions 3 "
                    "--replication-factor 1"
                ),
            ]
        )
        if rc != 0:
            return rc
    return 0


def run() -> int:
    build_dir = ROOT_DIR / "build"
    build_dir.mkdir(exist_ok=True)

    build_cmd = [
        "docker", "run", "--rm",
        "-v", f"{ROOT_DIR}:/workspace",
        "-w", "/workspace/flink",
        "maven:3.9.9-eclipse-temurin-11",
        "mvn", "-q", "clean", "package",
    ]
    if run_command(build_cmd) != 0:
        return 1

    jar_src = ROOT_DIR / "flink" / "target" / JAR_NAME
    (build_dir / JAR_NAME).write_bytes(jar_src.read_bytes())

    if run_command(
        ["docker", "compose", "up", "-d", "zookeeper", "postgres", "kafka"]
    ) != 0:
        return 1

    if not wait_for_kafka():
        print("kafka did not become healthy in time", file=sys.stderr)
        return 1

    if create_kafka_topics() != 0:
        return 1

    return run_command(
        ["docker", "compose", "up", "-d", "jobmanager", "taskmanager"]
    )


if __name__ == "__main__":
    sys.exit(run())
