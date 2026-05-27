# Clinical Telemetry & Patient Risk Platform

A distributed pipeline that ingests ICU bedside-monitor vitals at high frequency, classifies every sample with an unsupervised ML model to separate real signal from sensor noise, and persists everything to a time-series database with full operational and model-level observability in Grafana.

![Kotlin](https://img.shields.io/badge/Kotlin-2.0-7F52FF?logo=kotlin&logoColor=white)
![Ktor](https://img.shields.io/badge/Ktor-2.3-087CFA?logo=ktor&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.4-F7931E?logo=scikitlearn&logoColor=white)
![NumPy](https://img.shields.io/badge/NumPy-1.26-013243?logo=numpy&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-2.14-FDB515?logo=timescale&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-2.51-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-8.10-02303A?logo=gradle&logoColor=white)

## Headline Numbers

| Metric | Value |
|---|---|
| Sustained throughput | ~1,500 events/sec |
| End-to-end p99 latency | < 50 ms |
| ML prediction p99 latency | ~1 ms |
| TimescaleDB COPY batch p99 | ~22 ms |
| Pipeline gap at steady state | ~0 events/sec |
| Data loss | 0 |
| Concurrent simulated patients | 100 |
| Total pipeline stages | 4 (decoupled via Kafka) |

## Problem

ICU bedside monitors sample heart rate, blood-oxygen saturation (SpO₂), and respiratory rate several times per second per patient. A 30-bed unit produces hundreds of thousands of measurements per minute. Two engineering problems follow.

1. **Scale.** Generic relational databases collapse under sustained high-frequency inserts that compete with dashboard queries on the same table. A purpose-built time-series store with partitioning, columnar compression, and pre-computed rollups is required.

2. **Sensor noise.** Bedside monitors emit constant false readings (slipped probes, loose cables, electrical interference). Each glitch is a single impossible value. If those reach clinicians as alarms, signal-to-noise drops to the point where real alarms are ignored. This is alarm fatigue, a documented patient-safety risk. Artifact suppression must happen before alarms fire, by a model that is observable and tunable.

## Solution

A four-stage pipeline, each stage decoupled by a Kafka topic so any one component can stall without blocking the others. Inference is unsupervised (no labels required). Every event is persisted, including flagged anomalies, so the model's decisions are fully auditable.

```
Patient sample
  → [Ktor Ingest]        HTTP POST /ingest → Kafka producer (acks=all)
  → [Kafka raw]          telemetry.raw, 16 partitions, keyed by patientId
  → [Python ML Detector] IsolationForest x3 (HR / SpO2 / RR) + 5-D features
  → [Kafka scored]       telemetry.scored with {flagged, anomalyScore, reason}
  → [Kotlin Worker]      Batched COPY into TimescaleDB
  → [TimescaleDB]        Hypertable partitioned by (time, patient_id)
  → [Grafana]            Prometheus + raw SQL dashboards
```

## Architecture

```
┌─────────────────────┐         ┌─────────────────────┐         ┌──────────────────────┐
│  Python Generator   │  HTTP   │  Ktor Ingest        │  Kafka  │  Python ML Detector  │
│  100 patient sims   │ ───────▶│  /ingest (Netty)    │ ───────▶│  IsolationForest x3  │
│  async aiohttp      │         │  Coroutine producer │  raw    │  (HR / SPO2 / RR)    │
└─────────────────────┘         └──────────┬──────────┘         └──────────┬───────────┘
                                           │                                │
                                           ▼                                │ Kafka
                                  ┌────────────────┐                        │ scored
                                  │  Prometheus    │                        ▼
                                  │  scrapes all 3 │              ┌──────────────────────┐
                                  │  services      │              │  Kotlin Worker       │
                                  └────────┬───────┘              │  Consumer + COPY     │
                                           │                      └──────────┬───────────┘
                                           ▼                                 ▼
                                  ┌────────────────┐              ┌──────────────────┐
                                  │   Grafana      │◀─────────────│  TimescaleDB     │
                                  │   dashboards   │   SQL panels │  hypertable      │
                                  └────────────────┘              └──────────────────┘
```

Infrastructure (Kafka, TimescaleDB, Prometheus, Grafana) runs in Docker via Docker Compose. The three application services (ingest, ML detector, worker) run on the host.

## Pipeline Components

### 1. Generator (Python + asyncio)

**Role:** Simulate 100 ICU patients producing realistic vitals at 5 Hz per metric.

**Design:** Each patient is an independent asyncio coroutine maintaining an autoregressive state that mean-reverts toward a personalized baseline with Gaussian noise. Two stochastic failure modes are injected:

- **Deterioration episodes:** 8 to 40-second drift toward unhealthy values. These represent real clinical events and the model must NOT flag them.
- **Sensor artifacts:** Single-sample large deviations. The model MUST flag these.

The presence of both failure modes is what makes the model meaningfully testable.

**Why Python + asyncio:** Trivial concurrency for the I/O-bound HTTP fan-out (300 inflight requests/sec).

### 2. Ingest (Kotlin + Ktor + Netty)

**Role:** Accept POSTs at `/ingest` and forward every reading to Kafka without loss.

**Why Kotlin/Ktor on Netty:** Non-blocking I/O down to the kernel. Each HTTP request is a coroutine that suspends on `await()` instead of parking an OS thread, so the JVM holds thousands of concurrent connections in a small resident set.

**Producer configuration:**

| Setting | Value | Why |
|---|---|---|
| `acks` | `all` | Every message acknowledged by all in-sync replicas before HTTP 200 |
| `enable.idempotence` | `true` | Retried sends are deduplicated by the broker |
| `compression.type` | `zstd` | Reduces network and disk footprint of the log |
| `linger.ms` | `5` | Coalesces records into larger Kafka requests |
| Message key | `patientId` | All events for one patient land on the same partition |

**Why patientId as the key:** Preserves per-patient ordering across both Kafka hops while allowing horizontal consumer parallelism across patients. The downstream ML detector's per-patient rolling state stays sticky to one consumer instance.

### 3. Kafka (KRaft mode, single broker, 16 partitions per topic)

**Role:** Durable transport and backpressure absorber between every stage.

**Why Kafka:** Three properties make it the right transport here:

- **Durability:** Every event is on disk before the producer acks. The ingest never lies about acceptance.
- **Decoupling:** If the ML detector slows down for a refit, Kafka holds the backlog. The ingest is never blocked by a slow consumer.
- **Replay:** Offsets are committed manually, so a failed write doesn't advance past lost data.

**Why KRaft mode:** No ZooKeeper to operate. Single binary, single config.

### 4. ML Detector (Python + scikit-learn)

**Role:** Classify every reading as a sensor artifact or clean signal, in real time, without labeled training data. See [ML Detector Deep Dive](#ml-detector-deep-dive) below.

### 5. Worker (Kotlin + HikariCP + PG CopyManager)

**Role:** Consume `telemetry.scored` and batch-write every event to TimescaleDB.

**Why COPY instead of INSERT:** `COPY ... FROM STDIN WITH (FORMAT text)` streams a tab-separated buffer through a single Postgres protocol command. For 1,000-row batches, COPY is approximately 8 to 10 times faster than batched INSERT.

**Why every event is persisted (flagged and unflagged):** A `flagged` column distinguishes them at read time. This makes the anomaly-marker overlay panel possible. Dropped events cannot be reconstructed later.

**Worker hardening (post-incident):**

| Concern | Fix |
|---|---|
| Half-broken JDBC socket | `socketTimeout=30s`, `tcpKeepAlive=true` |
| Held-too-long connection | HikariCP `leakDetectionThreshold=60s` |
| Slow batch starves Kafka heartbeat | Consumer `max.poll.interval.ms=180s` |
| Transient COPY failure | 3 retries with exponential backoff |
| Permanent COPY failure | NO Kafka offset commit; redeliver next poll |
| Silent crash | `logback.xml` with timestamped stdout, top-level try/catch in `Main.kt` |

The previous design swallowed write errors and committed offsets anyway, causing silent data loss. The redesign closes that path.

### 6. TimescaleDB (PostgreSQL 16 + TimescaleDB 2.14)

**Role:** Persist all telemetry with read patterns optimized for clinical dashboards.

**Why TimescaleDB over vanilla Postgres:** Hypertables and columnar compression are native to time-series workloads. The application writes to one logical table; TimescaleDB transparently partitions into chunks and compresses old ones.

**Schema features:**

- **Hypertable:** Partitioned by time (1-day chunks) and patient ID (8 space partitions). Per-patient queries hit one chunk instead of scanning the table.
- **Columnar compression:** Applied to chunks older than 1 hour. Compressed chunks are 10 to 20 times smaller, segmented by `(patient_id, metric)` to match dashboard query dimensions.
- **Continuous aggregate:** `telemetry_1min` pre-computes minute-level rollups. Long-range dashboard queries hit the rollup, not the raw table.
- **Partial index on flagged rows:** Anomaly-marker queries hit `idx_tel_flagged_patient_time WHERE flagged = TRUE` directly.

### 7. Observability (Prometheus 2.51 + Grafana 10.4)

**Role:** Expose live state of the pipeline AND the model.

**Why pull-based metrics:** Each service exposes `/metrics` in exposition format. Prometheus scrapes every 5 seconds. Simpler to operate than push-based and uniform across Kotlin and Python services (`simpleclient` and `prometheus-client` respectively).

**Why Grafana:** Two data sources in the same dashboard (Prometheus for pipeline counters, TimescaleDB for clinical SQL). Dashboards provisioned as JSON in the repo and reproduced on `docker compose up -d`.

## ML Detector Deep Dive

The detector is the most domain-specific component of the system and the part that solves the alarm-fatigue problem.

### Why Unsupervised

Hand-labeling millions of ICU samples as "real" versus "glitch" is infeasible at scale. Unsupervised models learn the structure of normal data and isolate outliers without ground truth.

### Why IsolationForest

| Property | Value |
|---|---|
| Inference complexity | O(log n) per sample |
| Per-sample latency | ~50 microseconds |
| Tunable parameter | `contamination` (expected anomaly fraction, clinically interpretable) |
| Infrastructure required | CPU only. No GPU, no model registry, no inference server |
| Training time | Seconds on a laptop |

Three models total, one per metric (HR, SpO₂, RR). Pooled across all 100 patients during training.

### Feature Vector (5-D, per sample)

For each `(patient_id, metric)` pair, the detector keeps a rolling 150-sample deque (approximately 30 seconds at 5 Hz). Per new sample, it computes:

| Feature | Definition |
|---|---|
| Raw value | The reading itself |
| Jump magnitude | `abs(value - previous_value)` |
| Local z-score | Stdevs from the local 30-second window mean |
| Local stddev | Variability of the patient's recent readings |
| Population distance | `abs(value - pop_mean) / pop_stddev` |

The window is used only for feature extraction. Training uses the pooled feature buffer across all patients.

### Warmup and Refit

| Stage | Threshold | Behavior |
|---|---|---|
| Cold start | First 1,000 events per metric | Returns `reason="warmup"`, no flagging |
| Warmed up | After `MIN_FIT_SAMPLES=1000` | Fits the model, begins predicting |
| Drift adaptation | Every `REFIT_INTERVAL=5000` events | Refits on the most recent 1,000 feature vectors |

### Tuning `contamination`

`contamination` is the model's decision threshold, expressed as the expected fraction of true anomalies in the input. For ICU vitals the right range is 1 to 5 percent.

| Setting | Effect | Clinical risk |
|---|---|---|
| Too high (e.g. 0.10) | Many normal samples flagged | High. False negatives can be fatal. |
| Too low (e.g. 0.005) | Only extreme outliers flagged | Lower. Residual artifacts caught downstream. |
| 0.03 (default) | Matches simulator artifact injection rate | Reasonable. |

**Rule:** In clinical contexts, false negatives are worse than false positives. Tune toward letting samples through.

### Model Observability

| Metric | Type | Purpose |
|---|---|---|
| `ml_detector_events_scored_total{metric}` | Counter | Per-metric throughput |
| `ml_detector_events_flagged_total{metric}` | Counter | Per-metric anomaly count |
| `ml_detector_prediction_latency_seconds` | Histogram | Feature extraction + predict latency |
| `ml_detector_anomaly_score` | Histogram | Live anomaly-score distribution |
| `ml_detector_model_ready{metric}` | Gauge | 1 if fit, else 0 |
| `ml_detector_refits_total{metric}` | Counter | Total refits per metric |
| `ml_detector_training_buffer_size{metric}` | Gauge | Buffer size before next refit |

The detector exposes the model itself, not just its throughput. Distributional drift, latency tail, and refit frequency are all queryable in Grafana.

## Dashboards

Four rows, two data sources (Prometheus + TimescaleDB), provisioned as JSON in the repo.

### Row 1: Pipeline Health

![Pipeline Health](docs/pipeline-health.png)

| Panel | What it shows |
|---|---|
| **Ingest throughput, stacked by metric** | Per-vital event rate. Flat parallel bands indicate steady production. A dip in one band indicates a metric-specific failure. |
| **End-to-end latency p99** | Three lines for ingest p99 (~15-20 ms), ML predict p99 (~1 ms), TimescaleDB COPY p99 (~10-13 ms). A spike on one line localizes the bottleneck. |
| **Pipeline gap** | `ingest_rate - ml_scored_rate`. Near zero at steady state. Sustained positive gap means the detector is falling behind. |

### Row 2: ML Model Observability

![ML Model Observability](docs/ml-observability.png)

| Panel | What it shows |
|---|---|
| **Models ready** | Count of warmed-up models (target: 3). Drops indicate one or more metrics are in cold-start. |
| **Anomaly detection rate per metric** | Percentage of events flagged, per metric. Target band: 1 to 5%. Sustained drift indicates tuning issues or distribution shift. |
| **Prediction latency p50 / p95 / p99** | All percentiles under 2 ms in healthy state. p99 divergence indicates tail latency (slow refit, GC, contention). |
| **Anomaly-score distribution heatmap** | Density of events per score bucket over time. The closest proxy for "is model behavior drifting?" without a labeled validation set. |

### Row 3: Clinical View (Patient Risk)

![Clinical View](docs/clinical-view.png)

| Panel | What it shows |
|---|---|
| **Top-20 highest-risk patients over time** | One line per patient, rolling average risk score. The legend lists patient IDs with most recent values. |
| **Top-10 leaderboard table** | Highest-risk patients in last 5 min by `(risk, flags, samples)`. Gradient coloring for fast triage. |
| **Current risk score gauge** | Three-tier fallback: rolling 5-min avg → most recent reading → 0. Always renders a meaningful value. |
| **Flagged events in last 5 min** | Stat for the selected patient. High values indicate sustained anomaly activity. |
| **Minutes since last anomaly** | Stat for the selected patient. Combined with the previous panel, indicates patients who recently had activity and have now gone quiet. |

### Row 4: Live Patient Overlay

![Live Patient Overlay](docs/live-overlay.png)

A combined time-series view for one selected patient:

- **Green / yellow / blue lines:** HR, RR, SpO₂ for unflagged readings.
- **Red dots:** Exact moments the IsolationForest flagged a sample as an artifact.

**Implementation:** Two SQL queries against the same `telemetry` table, one filtered on `flagged = FALSE` and one on `flagged = TRUE`. A Grafana field override matches series names beginning with `^anomaly_` and renders them as red points instead of lines.

**Diagnostic value:** This panel doubles as a model-debugging tool. Red dots on top of normal-looking readings indicates over-firing (lower `contamination`). Visible spikes without red dots indicates under-firing (raise it).

## Tech Stack & Rationale

| Layer | Technology | Rationale |
|---|---|---|
| Ingest / Worker language | Kotlin 2.0 | Coroutines, null safety, JVM ecosystem |
| HTTP server | Ktor 2.3 on Netty | Non-blocking I/O, coroutine-native |
| ML / simulator | Python 3.11 + asyncio | Native scikit-learn, low-overhead I/O concurrency |
| ML model | scikit-learn IsolationForest | Unsupervised, O(log n) inference, single clinical knob |
| Numerics | NumPy 1.26 | Vectorized feature math |
| Message broker | Apache Kafka 3.7 (KRaft) | Durable log, decouples every stage |
| Database | TimescaleDB 2.14 on PostgreSQL 16 | Hypertables, columnar compression, continuous aggregates |
| JDBC pool | HikariCP 5 | Fastest JDBC pool on the JVM |
| Metrics | Prometheus 2.51 + prometheus-client + simpleclient | Pull-based, exposition-format standard |
| Visualization | Grafana 10.4 | Dashboards as code, dual datasource |
| Orchestration | Docker Compose v2 | Single-command infrastructure boot |
| Build | Gradle 8.10 + Shadow 8.3.3 | Fat-jar packaging, reproducible builds |

## Key Engineering Decisions

**ML inference is its own service, decoupled by Kafka.** The Kotlin worker has no knowledge of the model; the model has no knowledge of the database. The IsolationForest can be swapped for a transformer, autoencoder, or ensemble without modifying ingest or storage code.

**`patientId` is the Kafka key on both topics.** All events for a single patient route to the same partition, preserving order through both hops while allowing horizontal consumer parallelism across patients.

**Pooled training, per-sample prediction.** One model per metric, trained on data from all patients. Warmup completes in ~30 seconds. New patients receive meaningful predictions immediately because the model encodes population-level normality.

**Every event is persisted, flagged or not.** A `flagged` column distinguishes them at read time. Required for the anomaly-marker overlay panel. Lost data cannot be reconstructed.

**Explicit JDBC socket timeouts and TCP keep-alive.** Closes a silent-hang failure mode where a half-broken Postgres connection blocked COPY indefinitely. Combined with leak detection and retries, failures are now loud and recoverable.

**No Kafka offset commit on failed write.** If a COPY fails after retries, the worker refuses to commit. Kafka redelivers on the next poll. The previous design committed offsets anyway, silently losing data.

**Compression segmented by `(patient_id, metric)`.** Compressed chunks group rows along the dashboard's primary query dimensions, optimizing read performance after compression.

**Grafana dashboards as code.** Datasources, providers, and dashboard JSON versioned in the repository. `docker compose up -d` reproduces the entire observability layer.

## Performance

Measured on a single MacBook Pro (Apple Silicon, 16 GB RAM), steady state after warmup.

| Metric | Value | Notes |
|---|---|---|
| Ingest throughput | ~1,500 events/sec | 100 patients × 5 samples/sec × 3 metrics |
| `/ingest` p99 latency | < 50 ms | p50 ≈ 5 ms |
| ML prediction p99 latency | ~1 ms | Feature extraction + `predict()` per sample |
| ML detection rate | ~3 % | Matches simulator artifact injection rate |
| TimescaleDB COPY batch p99 | ~22 ms | 1,000-row batches |
| Write throughput | ~1,500 rows/sec | All events persisted |
| End-to-end pipeline gap | ~0 events/sec | Ingest rate ≈ ML scored rate |
| Data loss | 0 | `acks=all`, idempotent producer, no commit on failure |

The throughput ceiling is the Python generator's HTTP client, not any pipeline component. Per-patient Kafka partitioning makes every stage horizontally scalable.

## Getting Started

### Prerequisites

- Docker Desktop, running
- JDK 21 (`brew install --cask temurin@21` on macOS)
- Python 3.11+

### Infrastructure

```bash
docker compose up -d
```

Create Kafka topics:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic telemetry.raw    --partitions 16 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic telemetry.scored --partitions 16 --replication-factor 1
```

If upgrading from a pre-ML database, apply the schema migration:

```bash
docker exec -i timescaledb psql -U telemetry -d clinical < sql/migrate-add-flagged.sql
```

If the TimescaleDB volume predates the Grafana read-only role:

```bash
docker exec -i timescaledb psql -U telemetry -d clinical < sql/fix-grafana-permissions.sql
```

### Application Services

Run each in its own terminal.

```bash
# T1: Ktor ingest
cd ingest
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew run

# T2: Python ML detector
cd ml-detector
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python detector.py

# T3: Kotlin worker
cd worker
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew clean shadowJar -q
java -jar build/libs/telemetry-worker-all.jar

# T4: Python generator
cd generator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python simulate.py
```

### Endpoints

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | none |
| Ingest metrics | http://localhost:8080/metrics | none |
| ML detector metrics | http://localhost:9200/metrics | none |
| Worker metrics | http://localhost:9100/metrics | none |

In Grafana: **Dashboards → Browse → Clinical Telemetry: ML Risk & Pipeline Observability**. Panels populate within ~60 seconds.

### Verification

```bash
docker exec -it timescaledb psql -U telemetry -d clinical -c \
  "SELECT flagged, COUNT(*) FROM telemetry \
   WHERE time > NOW() - INTERVAL '2 minutes' GROUP BY flagged;"
```

Expected: two rows (`flagged=true`, `flagged=false`), with the flagged count near 3% of total.

### Shutdown

```bash
docker compose down       # stop containers, preserve data
docker compose down -v    # stop AND wipe TimescaleDB volume
```

## Project Structure

```
clinical-telemetry-platform/
├── docker-compose.yml                # 4-service local data plane
├── prometheus/
│   └── prometheus.yml                # Scrape config for ingest, ml-detector, worker
├── grafana/
│   ├── provisioning/                 # Datasources + dashboard providers
│   └── dashboards/
│       └── clinical-telemetry.json   # 4-row dashboard
├── sql/
│   ├── init.sql                      # Hypertable, compression, continuous aggregate, grafana_ro role
│   ├── migrate-add-flagged.sql       # Schema migration for the ML upgrade
│   └── fix-grafana-permissions.sql   # Grants for legacy DB volumes
├── generator/
│   ├── requirements.txt
│   └── simulate.py                   # 100-patient asyncio simulator
├── ml-detector/
│   ├── requirements.txt
│   └── detector.py                   # Kafka consumer/producer + per-metric IsolationForest
├── ingest/                           # Gradle module: Ktor service
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/clinical/telemetry/
│       ├── Application.kt
│       ├── api/IngestRoutes.kt
│       ├── kafka/TelemetryProducer.kt
│       ├── metrics/Metrics.kt
│       └── model/TelemetryEvent.kt
├── worker/                           # Gradle module: Kafka consumer + Timescale sink
│   ├── build.gradle.kts
│   └── src/main/
│       ├── kotlin/com/clinical/telemetry/worker/
│       │   ├── Main.kt
│       │   ├── Worker.kt             # Consumes telemetry.scored, batches to DB
│       │   ├── MetricsServer.kt
│       │   ├── WorkerMetrics.kt
│       │   ├── model/Events.kt
│       │   └── storage/
│       │       └── TimescaleSink.kt  # HikariCP + PG CopyManager batch writer
│       └── resources/
│           └── logback.xml
└── docs/                             # Dashboard screenshots
```

## Roadmap

- **Online learning.** Replace periodic-refit IsolationForest with an online algorithm (HalfSpaceTrees, streaming Random-Cut Forest) for continuous adaptation.
- **Per-patient personalization.** A second model layer learning each patient's individual baseline, so chronically irregular patients don't trip the population model.
- **Shadow deploys.** Run candidate model versions in parallel and compare flagged-event rates before promotion.
- **Kubernetes deployment.** Helm charts per service, kube-prometheus-stack, custom HPA metric on `kafka_consumergroup_lag`.
- **Chaos engineering.** Chaos Mesh `PodChaos` and `NetworkChaos` against the detector and worker; verify p99 latency under failure.
- **Production hardening.** At-rest encryption, TLS on Kafka, audit logging, PHI de-identification at the ingest boundary.

## License

MIT. Portfolio project. Not in clinical use. Synthetic data only.
