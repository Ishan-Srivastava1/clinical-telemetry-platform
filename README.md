# Clinical Telemetry & Patient Risk Platform

> A production-grade, end-to-end distributed system that ingests, denoises, and persists high-frequency ICU biometric telemetry in real time — built in Kotlin and Python on top of Kafka, Redis, and TimescaleDB.

![Kotlin](https://img.shields.io/badge/Kotlin-2.0-7F52FF?logo=kotlin&logoColor=white)
![Ktor](https://img.shields.io/badge/Ktor-2.3-087CFA?logo=ktor&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?logo=redis&logoColor=white)
![TimescaleDB](https://img.shields.io/badge/TimescaleDB-2.14-FDB515?logo=timescale&logoColor=black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-2.51-E6522C?logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-8.10-02303A?logo=gradle&logoColor=white)

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [What It Does](#what-it-does)
- [Architecture at a Glance](#architecture-at-a-glance)
- [The Data Flow](#the-data-flow)
- [Deep Dive: The Brain](#deep-dive-the-brain)
- [Tech Stack](#tech-stack)
- [Performance & Results](#performance--results)
- [Screenshots](#screenshots)
- [Getting Started](#getting-started)
- [Project Structure](#project-structure)
- [Engineering Decisions Worth Highlighting](#engineering-decisions-worth-highlighting)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why This Exists

In a modern ICU, every bedside monitor continuously streams vital signs — heart rate, blood oxygen saturation (SpO₂), respiratory rate — at multiple samples per second, for every patient on the floor. A 30-bed unit easily produces **hundreds of thousands of data points per minute**, and that data has to be ingested, analyzed, and stored without dropping a single sample, because each one might be the one that catches a deteriorating patient.

Two real-world problems make this much harder than it sounds:

**1. The data scale breaks ordinary databases.** A traditional Postgres or MySQL instance, hit with sustained high-frequency inserts and dashboard queries on the same table, will degrade fast. You need a storage layer purpose-built for time-series workloads, with time-based partitioning, columnar compression, and pre-computed rollups.

**2. Sensor noise causes alarm fatigue.** Bedside monitors glitch constantly — a patient turns over and a SpO₂ probe loses contact, a heart-rate cable wiggles loose, an electrode picks up ambient electrical noise. Each glitch produces a momentary impossible reading that, taken at face value, would fire a critical alarm. Hospitals have published research showing that **clinicians ignore the majority of monitor alarms** because so many are false. The platform layer has a moral obligation to suppress the obvious artifacts *before* they ever reach a human.

This project is a working answer to both problems: an end-to-end pipeline that ingests at high throughput, intelligently filters out sensor noise in real time using a sliding-window statistical model, persists clean data into a time-series database optimized for clinical analytics, and exposes every layer to live observability.

---

## What It Does

In one sentence: **simulated patient monitors send vitals to an HTTP API, which streams them through Kafka into a stateful denoising worker, which writes clean samples to TimescaleDB and exposes the entire pipeline through Prometheus and Grafana.**

In a little more detail:

- A Python simulator spawns 100 synthetic ICU patients, each with a personalized physiological baseline and the ability to enter sustained deterioration episodes or emit brief sensor artifacts.
- A Kotlin/Ktor service running on Netty accepts the resulting HTTP firehose with non-blocking coroutines and republishes every event to Kafka with **acks=all** + **idempotence=true** for zero data loss.
- A Kotlin worker consumes from Kafka, runs each sample through an atomic Redis Lua script that maintains a 30-second per-patient rolling z-score, drops outliers, and batches surviving samples into TimescaleDB via `COPY FROM STDIN` (≈10× faster than `INSERT`).
- TimescaleDB stores data in a hypertable partitioned by both time and patient ID, with native columnar compression and continuous aggregates for dashboard queries.
- Prometheus scrapes both Kotlin services every 5 seconds; Grafana renders the system's pipeline health and the clinical-domain signal side by side.

---

## Architecture at a Glance

```
┌─────────────────────┐         ┌─────────────────────┐         ┌──────────────────────┐
│  Python Generator   │  HTTP   │  Ktor Ingest        │  Kafka  │  Kotlin Worker       │
│  100 patient sims   │ ───────▶│  /ingest (Netty)    │ ───────▶│  Consumer + Redis    │
│  async aiohttp      │         │  Coroutine producer │  raw    │  Lua sliding window  │
└─────────────────────┘         └──────────┬──────────┘         └──────────┬───────────┘
                                           │                                │
                                           ▼                                ▼
                                  ┌────────────────┐              ┌──────────────────┐
                                  │  Prometheus    │              │  TimescaleDB     │
                                  │  /metrics scr. │              │  hypertable      │
                                  └────────┬───────┘              │  COPY batch sink │
                                           │                       └──────────────────┘
                                           ▼
                                  ┌────────────────┐
                                  │   Grafana      │
                                  │  dashboards    │
                                  └────────────────┘
```

Every component is containerized and orchestrated by a single `docker-compose.yml`. The Kotlin services run on the host via Gradle; Prometheus reaches them through `host.docker.internal`.

---

## The Data Flow

### 1. The Edge — Python Patient Simulator

The simulator (`generator/simulate.py`) launches 100 concurrent `asyncio` coroutines, one per simulated patient. Each patient maintains a stateful **autoregressive model** of their own vitals — heart rate, SpO₂, and respiratory rate gently mean-revert toward a personalized healthy baseline, with small Gaussian noise. With low probability, a patient enters a deterioration episode (sustained drift toward unhealthy targets over 8–40 seconds), and independently at every tick has a small chance of emitting a brief sensor artifact (a single-sample huge spike or drop).

This dual-mode generator is deliberate: it produces both the kind of signal you *want* a real clinical pipeline to catch (genuine deterioration) and the kind of noise you want it to *suppress* (sensor glitch). Without both, you can't honestly evaluate the filter downstream.

### 2. The Front Door — Ktor + Netty Ingest API

A Kotlin/Ktor service running on the Netty engine accepts every sample via `POST /ingest`. Netty is non-blocking I/O down to the kernel; coroutines suspend on `await()` rather than parking OS threads, so a single pod sustains thousands of concurrent connections without thread-pool exhaustion.

Each request handler validates the payload, then publishes to Kafka using a `suspendCancellableCoroutine` wrapper around the producer's callback API. The producer is configured for **durability over speed**: `acks=all`, `enable.idempotence=true`, infinite retries, `zstd` compression, and a 5 ms linger window to coalesce records into larger batches. **Patient ID is used as the Kafka message key**, so all events for a given patient hash to the same partition — preserving per-patient ordering while allowing horizontal consumer parallelism across patients.

### 3. The Nervous System — Apache Kafka

Kafka runs in **KRaft mode** (no ZooKeeper) on a single broker with 16 partitions on `telemetry.raw`. In production this would be a 3-broker cluster with replication factor 3 and `min.insync.replicas=2`; the local setup mirrors the production semantics while staying boot-fast on a laptop.

Kafka's role is to **decouple ingestion from processing**. If the worker stalls (GC pause, slow database, a flapping Redis), Kafka absorbs the backpressure into its log — the ingest path never blocks, and no telemetry is lost. When the worker recovers, it catches up.

### 4. The Brain — Kotlin Worker + Redis Sliding Window

This is the most interesting component. See [Deep Dive: The Brain](#deep-dive-the-brain) below for the full story.

The worker is a Kotlin/JVM process that:
- Consumes from `telemetry.raw` with manual offset commits (no auto-commit — we commit only after a successful database flush, guaranteeing at-least-once semantics).
- For each sample, executes an atomic Redis Lua script that maintains a 30-second per-patient rolling z-score and returns a verdict (`flagged` or `ok`).
- Buffers surviving samples up to 1,000 rows or one poll cycle, then flushes via the PostgreSQL `CopyManager` to TimescaleDB.

### 5. The Vault — TimescaleDB

TimescaleDB is PostgreSQL plus a time-series extension. The `telemetry` table is a **hypertable partitioned by both time (1-day chunks) and patient ID (8 space partitions)**, which means per-patient dashboard queries hit a single chunk instead of fanning out across all of them. Three TimescaleDB features carry the load:

- **Hypertables** transparently partition the table; the application code just writes to one logical table.
- **Native columnar compression** kicks in on chunks older than 1 hour (aggressive setting for local dev — production would compress at 7 days). Compressed chunks are typically 10–20× smaller and faster to scan for aggregate queries.
- **Continuous aggregates** pre-compute 1-minute rollups (`telemetry_1min`) that incrementally refresh, so dashboard panels query a tiny materialized view instead of millions of raw rows.

Writes use `COPY ... FROM STDIN WITH (FORMAT text)` instead of batched `INSERT` — roughly an 8–10× throughput multiplier for batches of 1,000+ rows. The sink serializes each batch into a tab-separated buffer and streams it as one COPY command.

### 6. The Command Center — Prometheus + Grafana

Both Kotlin services expose a `/metrics` endpoint in Prometheus exposition format. Prometheus scrapes them every 5 seconds and stores the time-series data; Grafana renders a unified dashboard with:

- **Pipeline throughput** at each hop (events/sec into Ktor, surviving the filter, written to TimescaleDB)
- **Latency quantiles** (p50 / p95 / p99) for the ingest handler, Redis Lua eval, and TimescaleDB COPY batch
- **The clinical-domain signal**: artifact suppression ratio, per-metric flag counts
- **End-to-end pipeline-gap sanity check**: `ingest − written − flagged`, which should hover near zero. Persistent positive values mean consumer lag.
- **Live vitals** rendered directly from TimescaleDB via Grafana's PostgreSQL datasource — the same data a clinician would see on a real patient screen.

---

## Deep Dive: The Brain

The most clinically valuable engineering in this system is the **per-patient sliding-window artifact filter**. Here's how it works and why it matters.

### The clinical motivation

Bedside monitors are notoriously noisy. A genuine cardiac event and a momentarily disconnected SpO₂ probe produce reading patterns that, at a single sample's resolution, can look very similar — both are sudden, large deviations from a patient's baseline. Naively alerting on every such deviation produces hundreds of false alarms per shift per nurse, and the human response is to start ignoring them. **Suppressing the obvious artifacts before they ever become alarms is the highest-value piece of engineering in a clinical telemetry platform.**

### The mechanism

For every `(patient_id, metric)` pair, the worker maintains a **30-second rolling window of recent samples in a Redis sorted set**, scored by epoch-millisecond timestamp. On every new sample, the worker invokes a Lua script that runs atomically inside Redis:

1. **Evict** entries older than 30 seconds (`ZREMRANGEBYSCORE`).
2. **Append** the new sample (`ZADD`).
3. **Refresh** the key's TTL so idle patients auto-evict.
4. **Compute** mean and standard deviation over the surviving window.
5. **Compute** the new sample's z-score: `z = (value − mean) / stddev`.
6. **Return** a JSON verdict: flagged if `|z| > 3.5` and the window has ≥10 prior samples.

### Why each design choice matters

- **Why atomic Lua?** Without atomicity, two workers reading the same window concurrently could compute stale statistics. Redis runs each `EVAL` on a single thread with full isolation — equivalent to a transaction, but cheaper. This makes horizontal worker scaling safe.
- **Why a sorted set scored by timestamp?** It's the only Redis primitive that lets you efficiently both **evict by time range** and **read the full window** in O(log N + window size). A list would require O(N) eviction; a hash wouldn't support ordering at all.
- **Why z-score over fancier models (Kalman, ARIMA)?** Z-score is computationally trivial (the entire Lua eval runs in ~1ms p99 in production), holds up well for short windows, and is **defensible to a clinician**. A black-box model would be a much harder sell in a hospital setting. Sophistication can come later.
- **Why a 10-sample warmup?** You cannot honestly outlier-detect without history. During warmup every sample passes through; the verdict carries `reason: "warmup"` so downstream consumers know the filter hasn't engaged yet.
- **Why a z-threshold of 3.5?** Tunable. 3.5 corresponds to ~0.05% of a normal distribution, which empirically matches the artifact rate of the simulator. In a real deployment this would be re-tuned per metric (HR tolerates more variance than SpO₂).

### The result

In the dashboard screenshots below, the **Artifact Suppression Ratio stat hovers around 3%**, meaning ~3 out of every 100 incoming samples are correctly identified and dropped as artifacts. The per-metric breakdown shows the filter is firing on all three vital signs, not biased toward any one. End-to-end pipeline latency stays under 50ms p99, so the filter is *fast enough to apply at line rate without backpressure*.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Language (services) | **Kotlin 2.0** | First-class coroutines, null safety, great JVM ecosystem |
| HTTP server | **Ktor 2.3 + Netty** | Coroutine-native, non-blocking I/O, minimal overhead |
| Language (simulator) | **Python 3.11 + asyncio + aiohttp** | Trivial to spawn 100+ concurrent HTTP clients on one thread |
| Message broker | **Apache Kafka 3.7 (KRaft)** | Industry-standard durable log, decouples ingest from processing |
| State store / filter | **Redis 7.2 + Lua** | Sub-millisecond atomic scripting, sorted sets for time windows |
| Time-series DB | **TimescaleDB 2.14 (on PostgreSQL 16)** | Hypertables, columnar compression, continuous aggregates |
| Connection pooling | **HikariCP 5** | Fastest JDBC pool in the JVM ecosystem |
| Metrics | **Prometheus 2.51 + Micrometer** | Pull-based, exposition-format standard |
| Visualization | **Grafana 10.4** | Provisioned dashboards as code, PromQL + native PostgreSQL panels |
| Orchestration (local) | **Docker Compose v2** | Single command to bring up the full data plane |
| Build | **Gradle 8.10 + Shadow 8.3.3** | Reproducible builds, fat-jar packaging |

---

## Performance & Results

Numbers measured on a single laptop (MacBook Pro, M-series, 16GB RAM), with all containers and JVM services running locally.

| Metric | Value | Notes |
|---|---|---|
| **End-to-end ingest throughput** | **1,500+ events/sec** | 100 patients × 5 samples/sec × 3 metrics |
| **`/ingest` p99 latency** | **< 50 ms** | p50 ≈ 5ms, p95 ≈ 10ms |
| **Redis Lua eval p99** | **~1.3 ms** | Atomic sliding-window computation per sample |
| **TimescaleDB COPY batch p99** | **~22 ms** | 1,000-row batches via `COPY FROM STDIN` |
| **TimescaleDB write throughput** | **~1.3K rows/sec** | After artifact suppression |
| **Artifact suppression ratio** | **~3%** | Measured `flagged / (flagged + passed)` |
| **End-to-end pipeline gap** | **~0 ops/sec** | Confirms zero consumer lag at steady state |
| **Zero data loss** | ✓ | `acks=all` + `enable.idempotence=true` on producer; manual offset commits on consumer |

The throughput ceiling here is set by the **Python generator's HTTP client**, not by Ktor or Kafka — both are tested to scale roughly linearly with patient count thanks to per-patient Kafka partitioning. A real production deployment behind a load balancer with multiple Ktor pods would scale into the tens of thousands of events per second.

---

## Screenshots

> Place your dashboard screenshots in `docs/` and they'll render below.

### Pipeline Throughput, Latency & Artifact Suppression

![Grafana — pipeline overview](docs/dashboard-overview.png)

The top-left panel shows steady ingest throughput around 450 ops/sec per metric type. Top-right shows ingest p50/p95/p99 latency — note p99 stays under 20ms. Bottom-left is the headline clinical metric: **2.97% artifact suppression**. Bottom-middle shows per-metric flagged-event rates; bottom-right shows Redis Lua eval latency holding at ~1ms p99.

### Storage Performance & Pipeline Health

![Grafana — storage and health](docs/dashboard-storage.png)

TimescaleDB sustaining ~1.3K rows/sec sustained writes with p99 batch latency around 22ms. The bottom panel — the end-to-end pipeline gap — hovers tight against zero, which is the single best confirmation that the consumer is keeping up with the producer in real time.

### Live Patient Vitals from TimescaleDB

![Grafana — live vitals](docs/dashboard-vitals.png)

The bottom panel is rendered directly from TimescaleDB via Grafana's PostgreSQL datasource — the exact same data a clinician would see on a real patient screen. Green is heart rate, blue is SpO₂, yellow is respiratory rate. You can see the simulator entering and exiting deterioration episodes (the HR spikes toward 130bpm, SpO₂ dips toward 85%) — exactly the kind of sustained pattern the system is designed to *preserve*, in contrast to the brief artifact spikes that get filtered out upstream.

---

## Getting Started

### Prerequisites

- **Docker Desktop** (running)
- **JDK 21** (`brew install --cask temurin@21` on macOS)
- **Python 3.11+**
- **Gradle wrapper** (included; no separate install needed)

### One-command boot

From the project root:

```bash
# 1. Start all infrastructure (Kafka, Redis, TimescaleDB, Prometheus, Grafana)
docker compose up -d

# 2. Create the Kafka topics
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --topic telemetry.raw --partitions 16 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --topic telemetry.filtered --partitions 16 --replication-factor 1
```

### Start the services (four terminals)

```bash
# Terminal 1: Ktor ingest API
cd ingest && ./gradlew run

# Terminal 2: Kotlin worker (consumer + filter + sink)
cd worker && ./gradlew run

# Terminal 3: Python data generator
cd generator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python simulate.py
```

### Open the dashboards

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |
| Ingest `/metrics` | http://localhost:8080/metrics | — |
| Worker `/metrics` | http://localhost:9100/metrics | — |

In Grafana, navigate to **Dashboards → Browse → Clinical Telemetry & Patient Risk Platform**. Within 30–60 seconds of the generator running, every panel should populate.

### Verify the end-to-end pipeline

```bash
# Confirm rows are landing in TimescaleDB
docker exec -it timescaledb psql -U telemetry -d clinical -c \
  "SELECT metric, COUNT(*) FROM telemetry
   WHERE time > NOW() - INTERVAL '2 minutes'
   GROUP BY metric ORDER BY metric;"
```

Expected: three rows (HR, RR, SPO2) each with thousands of samples.

### Shutdown

```bash
docker compose down       # stop containers, preserve TimescaleDB data
docker compose down -v    # stop containers AND wipe TimescaleDB volume
```

---

## Project Structure

```
clinical-telemetry-platform/
├── docker-compose.yml                # Full local data plane
├── prometheus/
│   └── prometheus.yml                # Scrape config for both Kotlin services
├── grafana/
│   ├── provisioning/                 # Datasources + dashboard providers
│   └── dashboards/
│       └── clinical-telemetry.json   # The 9-panel dashboard
├── sql/
│   └── init.sql                      # Hypertable, compression policy,
│                                     # continuous aggregate, grafana_ro role
├── generator/
│   ├── requirements.txt
│   └── simulate.py                   # 100-patient asyncio simulator
├── ingest/                           # Gradle module — Ktor service
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/clinical/telemetry/
│       ├── Application.kt            # Ktor bootstrap + /metrics + /healthz
│       ├── api/IngestRoutes.kt       # POST /ingest
│       ├── kafka/TelemetryProducer.kt# Coroutine-friendly Kafka producer
│       ├── metrics/Metrics.kt        # Prometheus counters + histograms
│       └── model/TelemetryEvent.kt
└── worker/                           # Gradle module — Kafka consumer
    ├── build.gradle.kts
    └── src/main/
        ├── kotlin/com/clinical/telemetry/worker/
        │   ├── Main.kt               # Entry point + dependency wiring
        │   ├── Worker.kt             # Kafka consumer loop, filter, batch flush
        │   ├── MetricsServer.kt      # Built-in HttpServer for /metrics
        │   ├── WorkerMetrics.kt      # Prometheus counter + histogram registry
        │   ├── model/Events.kt
        │   └── storage/
        │       └── TimescaleSink.kt  # HikariCP + PG CopyManager batch writer
        └── resources/
            └── sliding_window.lua    # Atomic z-score filter (the "Brain")
```

---

## Engineering Decisions Worth Highlighting

A few of the choices that make this more than a toy:

- **Per-patient Kafka partitioning** — `patientId` is the message key, so all events for a patient route to the same partition. This preserves per-patient ordering (clinically essential) while allowing horizontal worker scaling across the partition count.
- **At-least-once consumer semantics** — `enable.auto.commit=false`. Offsets are committed only after a successful TimescaleDB flush. On a worker crash mid-batch, the consumer replays from the last committed offset; idempotent producer semantics + COPY's all-or-nothing behavior prevent dupes.
- **HikariCP + CopyManager** — connection pool sized for the parallelism of consumer threads; `CopyManager.copyIn()` streams a tab-separated byte buffer directly into the hypertable, which is roughly 8–10× faster than batched `INSERT` for 1,000-row batches.
- **TimescaleDB compression segment-by `(patient_id, metric)`** — compressed chunks group rows by patient and metric, which is the access pattern dashboards actually use. Combined with `compress_orderby time DESC`, scans for "patient X's last hour" are fast even on compressed data.
- **Continuous aggregates with incremental refresh** — dashboard queries hit `telemetry_1min` (pre-aggregated 1-minute rollups), not the raw hypertable. Sub-millisecond panel loads on millions of underlying rows.
- **Atomic Lua scripts via `EVALSHA`** — the script is loaded once at worker startup; subsequent calls use the SHA hash, eliminating per-call script transmission overhead.
- **Provisioned-as-code Grafana** — dashboards and datasources are versioned YAML/JSON in the repo, not snowflake clicks in a UI. A fresh `docker compose up -d` reproduces the entire observability layer.

---

## Roadmap

Things I'd build next if this were a real production deployment:

- **Kubernetes deployment** — Helm charts per service, with the [kube-prometheus-stack](https://github.com/prometheus-community/helm-charts/tree/main/charts/kube-prometheus-stack)'s Prometheus adapter exposing `kafka_consumergroup_lag` as a custom HPA metric.
- **Chaos engineering** — Chaos Mesh `PodChaos` and `NetworkChaos` experiments against the worker deployment, verifying that p99 latency stays under SLO during pod kills and network partitions.
- **Alert dispatch** — a downstream service that consumes from a `telemetry.alerts` topic (currently only counted as a metric) and routes sustained-deterioration events to PagerDuty / Slack with patient context.
- **Smarter filter** — replace the static z-score threshold with a per-patient adaptive baseline that learns a personalized "normal range" from the patient's own history. Possibly Bayesian online change-point detection.
- **Multi-region replication** — TimescaleDB read replicas in a second region for disaster recovery, with Kafka MirrorMaker 2 mirroring the `telemetry.raw` topic.
- **PHI / HIPAA hardening** — at-rest encryption on the TimescaleDB volume, TLS on every hop (Kafka mTLS, Redis ACL + TLS, Postgres SSL), audit logging on every read of the `telemetry` table, and de-identification of patient IDs at the ingest boundary.

---

## License

MIT — see [LICENSE](LICENSE) for details. Built as a portfolio project; not currently used in any real clinical setting.
