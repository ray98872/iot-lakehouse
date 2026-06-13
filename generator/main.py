"""
IoT sensor data generator.

Simulates a fleet of factory machines and continuously streams JSON sensor
payloads (timestamp, machine ID, temperature, vibration) into a MinIO bucket
over its S3-compatible API — a local stand-in for an AWS ECS producer task
writing to S3.

A small fraction of readings are deliberately anomalous (sudden temperature /
vibration spikes) and a small fraction are deliberately malformed, so the
downstream Spark pipeline has real data-quality work to do.
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError

# --------------------------------------------------------------------------
# Configuration (injected by docker-compose)
# --------------------------------------------------------------------------
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_NAME = os.environ.get("BUCKET_NAME", "sensor-data-lake")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw")
MACHINE_COUNT = int(os.environ.get("MACHINE_COUNT", "12"))
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "5"))
ANOMALY_PROBABILITY = float(os.environ.get("ANOMALY_PROBABILITY", "0.02"))
MALFORMED_PROBABILITY = float(os.environ.get("MALFORMED_PROBABILITY", "0.01"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("iot-generator")

_shutdown = False


def _handle_signal(signum, _frame):
    global _shutdown
    log.info("Received signal %s — shutting down gracefully.", signum)
    _shutdown = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# --------------------------------------------------------------------------
# Machine simulation
# --------------------------------------------------------------------------
class Machine:
    """A single machine with a drifting baseline and occasional faults."""

    def __init__(self, machine_id: str):
        self.machine_id = machine_id
        self.base_temp = random.gauss(70.0, 4.0)       # °C
        self.base_vibration = random.gauss(2.5, 0.35)  # mm/s RMS

    def read(self) -> dict:
        # Slow random-walk drift + sensor noise
        self.base_temp += random.gauss(0, 0.05)
        self.base_vibration = max(0.2, self.base_vibration + random.gauss(0, 0.01))

        temperature = self.base_temp + random.gauss(0, 0.8)
        vibration = max(0.0, self.base_vibration + random.gauss(0, 0.12))
        is_injected_anomaly = False

        # Occasionally simulate a developing fault: heat + vibration spike
        if random.random() < ANOMALY_PROBABILITY:
            temperature += random.uniform(15.0, 35.0)
            vibration *= random.uniform(2.5, 4.0)
            is_injected_anomaly = True
            log.warning("Injected anomaly on %s (T=%.1f°C, V=%.2fmm/s)",
                        self.machine_id, temperature, vibration)

        record = {
            "reading_id": str(uuid.uuid4()),
            "machine_id": self.machine_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "temperature_c": round(temperature, 2),
            "vibration_mm_s": round(vibration, 3),
            # Ground-truth label kept for offline evaluation only —
            # the Spark pipeline never uses it to detect.
            "injected_anomaly": is_injected_anomaly,
        }

        # Occasionally emit garbage so the pipeline's quality gate has work
        if random.random() < MALFORMED_PROBABILITY:
            corruption = random.choice(["null_temp", "string_vib", "missing_ts"])
            if corruption == "null_temp":
                record["temperature_c"] = None
            elif corruption == "string_vib":
                record["vibration_mm_s"] = "SENSOR_ERROR"
            else:
                record.pop("timestamp")
            log.warning("Emitted malformed record (%s) on %s", corruption, self.machine_id)

        return record


# --------------------------------------------------------------------------
# MinIO connectivity
# --------------------------------------------------------------------------
def make_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4", retries={"max_attempts": 3}),
        region_name="us-east-1",
    )


def wait_for_minio(s3, max_attempts: int = 30) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            s3.list_buckets()
            log.info("Connected to MinIO at %s", MINIO_ENDPOINT)
            return
        except (EndpointConnectionError, ClientError) as exc:
            log.info("MinIO not ready (attempt %d/%d): %s", attempt, max_attempts, exc)
            time.sleep(2)
    raise RuntimeError(f"Could not reach MinIO at {MINIO_ENDPOINT}")


def ensure_bucket(s3) -> None:
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except ClientError:
        log.info("Bucket %s missing — creating it.", BUCKET_NAME)
        try:
            s3.create_bucket(Bucket=BUCKET_NAME)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------
def main() -> None:
    s3 = make_client()
    wait_for_minio(s3)
    ensure_bucket(s3)

    machines = [Machine(f"machine-{i:03d}") for i in range(1, MACHINE_COUNT + 1)]
    log.info("Streaming %d machines every %.1fs into s3://%s/%s/",
             MACHINE_COUNT, INTERVAL_SECONDS, BUCKET_NAME, RAW_PREFIX)

    batches = 0
    while not _shutdown:
        now = datetime.now(timezone.utc)
        records = [m.read() for m in machines]
        body = "\n".join(json.dumps(r) for r in records)  # newline-delimited JSON

        # Hive-style partitioning so Spark can prune by date/hour
        key = (
            f"{RAW_PREFIX}/"
            f"ingest_date={now:%Y-%m-%d}/hour={now:%H}/"
            f"batch_{now:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:8]}.json"
        )

        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType="application/json",
            )
            batches += 1
            if batches % 12 == 0:
                log.info("Uploaded %d batches (latest: s3://%s/%s)", batches, BUCKET_NAME, key)
        except (EndpointConnectionError, ClientError) as exc:
            log.error("Upload failed, will retry next cycle: %s", exc)

        time.sleep(INTERVAL_SECONDS)

    log.info("Generator stopped after %d batches.", batches)


if __name__ == "__main__":
    main()
