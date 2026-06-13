# IoT Anomaly Detection Lakehouse

**An enterprise AWS + Databricks streaming architecture, replicated 1:1 on a laptop — at £0/month.**

A fleet of simulated factory machines streams temperature and vibration telemetry into an
S3-compatible data lake, where a PySpark pipeline cleans it, computes rolling per-machine
statistics, and flags any reading deviating more than **3 standard deviations** from its
machine's own recent baseline — the statistical core of a predictive-maintenance system.

![Docker](https://img.shields.io/badge/Docker_Compose-multi--container-2496ED?logo=docker&logoColor=white)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5-E25A1C?logo=apachespark&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3_API-C72E49?logo=minio&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Cost](https://img.shields.io/badge/cloud_spend-%C2%A30.00-success)

> 📖 **Interactive write-up & demo:** https://ray98872.github.io/iot-lakehouse/

---

## Architecture

```
                        DOCKER COMPOSE  ·  network: lakehouse
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                                                                             │
 │  ┌─────────────────┐            ┌─────────────────────────────────────┐   │
 │  │  iot-generator  │            │   MinIO  ·  s3://sensor-data-lake    │   │
 │  │  (Python+boto3) │            │  ┌────────────────────────────────┐  │   │
 │  │                 │  PUT JSON  │  │ raw/ingest_date=…/hour=…/*.json│  │   │
 │  │ 12 machines     │ ─────────► │  └───────────────┬────────────────┘  │   │
 │  │ every 5 seconds │  S3 API    │                  │                   │   │
 │  │ + injected      │  :9000     │  ┌───────────────▼────────────────┐  │   │
 │  │   faults &      │            │  │ quarantine/   (malformed data) │  │   │
 │  │   bad records   │            │  │ processed/readings/   (parquet)│  │   │
 │  └─────────────────┘            │  │ processed/anomalies/  (parquet)│  │   │
 │   replaces AWS ECS              │  └───────────────▲────────────────┘  │   │
 │                                 └──────────────────┼───────────────────┘   │
 │                                       replaces AWS S3                      │
 │                                  read s3a:// │     │ write s3a://          │
 │                                              ▼     │                       │
 │                                 ┌──────────────────┴───────────────────┐   │
 │                                 │  spark-notebook  (Jupyter + PySpark) │   │
 │                                 │                                      │   │
 │                                 │  1. schema-enforced JSON ingest      │   │
 │                                 │  2. quality gate → quarantine        │   │
 │                                 │  3. rolling mean/σ per machine       │   │
 │                                 │  4. flag |x−μ| > 3σ  → parquet       │   │
 │                                 └──────────────────────────────────────┘   │
 │                                       replaces Databricks                  │
 └──────────────────────────────────────────────────────────────────────────────┘
        operator UIs:  Jupyter Lab :8888  ·  MinIO console :9001  ·  Spark UI :4040
```

**Data flow:** `iot-generator` → `raw/` (bronze) → quality gate → `quarantine/` (rejects)
→ rolling statistics (silver) → 3σ scoring → `processed/` (gold).
The medallion layout means every stage is independently auditable and rerunnable.

---

## What the system does

1. **Generates telemetry.** A containerised Python service simulates 12 machines, each with
   its own drifting baseline. Every 5 seconds it uploads a newline-delimited JSON batch of
   `{reading_id, machine_id, timestamp, temperature_c, vibration_mm_s}` to MinIO via `boto3`,
   using Hive-style `ingest_date=…/hour=…` partitioning so Spark can prune by time.
   ~2% of readings are injected faults (heat/vibration spikes) and ~1% are deliberately
   malformed — so the pipeline's data-quality stage earns its keep.

2. **Enforces quality.** Spark reads the raw zone with an explicit schema in PERMISSIVE mode.
   Corrupt records, nulls, unparseable timestamps and physically implausible values are
   filtered out and written to a `quarantine/` prefix — a dead-letter zone, not a silent drop.

3. **Detects anomalies statistically.** For each machine, a trailing window (last 20 readings,
   *excluding* the current one) yields a rolling mean and standard deviation per metric.
   A reading is flagged when it deviates from its own machine's baseline by more than 3σ —
   self-calibrating per machine, no hard-coded thresholds, no labelled training data needed.

4. **Publishes results.** Scored readings and an anomalies-only extract are written back to
   MinIO as date-partitioned Parquet — ready for a dashboard, alerting job, or downstream ML.

## Business value: predictive maintenance

Unplanned industrial downtime is routinely estimated to cost manufacturers tens of thousands
of pounds **per hour**. The economics of this pipeline:

- **Fail before the failure.** Bearing wear and overheating show up as vibration and thermal
  drift long before a breakdown. Catching a 3σ excursion early converts an emergency stoppage
  into a scheduled maintenance slot.
- **Per-machine baselines, zero training data.** Because each machine is compared to *its own*
  recent history, a naturally hot machine isn't a false alarm and a quietly degrading one
  can't hide in the fleet average. It works from day one — no labelled failure history required.
- **An auditable lake, not a black box.** Raw, quarantined and scored zones are all retained,
  so every alert can be traced back to the exact readings that produced it — and the same
  lake feeds future ML models (the natural roadmap: this statistical baseline becomes the
  benchmark an Isolation Forest or LSTM has to beat).

## Cloud mapping & cost optimization strategy

This project deliberately mirrors a production AWS + Databricks deployment, component for
component, on free open-source software:

| Enterprise Cloud Component | Role | Local Open-Source Equivalent | Cloud cost (typical) | Local cost |
|---|---|---|---|---|
| **AWS S3** | Data lake object storage | **MinIO** (S3-compatible API) | ~$0.023/GB/mo + requests | **£0** |
| **AWS ECS / Fargate** | Containerised data producer | **Local Docker container** (`iot-generator`) | ~$0.04/vCPU-hr + RAM | **£0** |
| **Databricks** | Managed Spark processing & notebooks | **Apache Spark container** (Jupyter + PySpark) | ~$0.40+/DBU + EC2 | **£0** |
| **AWS IAM keys** | Lake credentials | MinIO access/secret keys | — | **£0** |
| **S3 lifecycle zones** | Bronze/silver/gold layout | Bucket prefixes (`raw/`, `quarantine/`, `processed/`) | — | **£0** |
| **CloudWatch Logs** | Producer observability | `docker compose logs` | $0.50/GB ingested | **£0** |

**Why this works as a strategy — not just a workaround:**

- **API parity, not approximation.** MinIO implements the real S3 API, so the generator uses
  unmodified `boto3` and Spark uses the standard `s3a://` connector with production Hadoop
  settings. Repointing to AWS is a config change — endpoint URL and credentials — with
  **zero application-code changes**.
- **Identical architecture, zero-cost iteration.** Every design decision that matters —
  partitioning scheme, schema enforcement, quarantine pattern, window logic, Parquet layout —
  is developed and tested locally exactly as it would run in the cloud. The expensive
  trial-and-error loop (the one that bills you per DBU and per GB) happens at £0.
- **No idle-cluster burn.** A modest Databricks dev cluster left running idles into hundreds
  of pounds a month. `docker compose down` costs nothing and resurrects in seconds.
- **The skills transfer 1:1.** PySpark window functions, S3A configuration, medallion zoning
  and Compose orchestration are the same muscles used on the real stack.

## Quickstart

Prerequisites: Docker Desktop (or Docker Engine + Compose v2).

```bash
# 1. Clone and enter the project
git clone https://github.com/ray98872/iot-lakehouse.git
cd iot-lakehouse

# 2. Build and start the entire lakehouse (MinIO + bucket init + generator + Spark)
docker compose up -d --build
#    older Docker installs: docker-compose up -d --build

# 3. Watch sensor data flowing into the lake
docker compose logs -f iot-generator

# 4. Open the tools
#    Jupyter Lab     -> http://localhost:8888   (token: lakehouse)
#    MinIO console   -> http://localhost:9001   (minioadmin / minioadmin123)
#    Spark UI        -> http://localhost:4040   (while a job is running)

# 5. Run the pipeline: in Jupyter, open  work/anomaly_detection.ipynb
#    and Run All. (Give the generator ~2 minutes first so there's data.)

# 6. Tear down
docker compose down        # keep the lake's data
docker compose down -v     # wipe everything, full reset
```

> First notebook run downloads the `hadoop-aws` S3A connector from Maven (~1 min, one-off).

## Project structure

```
iot-lakehouse/
├── docker-compose.yml              # the whole ecosystem: MinIO, init job, generator, Spark
├── generator/
│   ├── Dockerfile                  # slim Python 3.12 image, non-root user
│   ├── requirements.txt            # boto3
│   └── main.py                     # machine simulator + S3 uploader (faults included)
├── notebooks/
│   └── anomaly_detection.ipynb     # bronze → quality gate → rolling stats → 3σ → gold
├── index.html                      # interactive write-up & 3σ demo (GitHub Pages)
└── README.md
```

## Tuning

All generator behaviour is environment-driven in `docker-compose.yml`:

| Variable | Default | Effect |
|---|---|---|
| `MACHINE_COUNT` | `12` | Fleet size |
| `INTERVAL_SECONDS` | `5` | Seconds between batches |
| `ANOMALY_PROBABILITY` | `0.02` | Chance a reading is an injected fault |
| `MALFORMED_PROBABILITY` | `0.01` | Chance a record is corrupted |

Pipeline knobs (`ROLLING_WINDOW_SIZE`, `SIGMA_THRESHOLD`, `MIN_WINDOW_SAMPLES`) live at the
top of the notebook.

## Roadmap

Delta Lake tables with ACID upserts and time travel · scheduled orchestration (Airflow) ·
streaming-first execution (the notebook already includes an `availableNow` Structured
Streaming variant) · an ML scorer (Isolation Forest) benchmarked against the 3σ baseline ·
Grafana over the gold zone.

---

Part of [ray98872](https://github.com/ray98872)'s portfolio ·
[Interactive write-up](https://ray98872.github.io/iot-lakehouse/)
