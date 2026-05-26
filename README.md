# Clinical Telemetry & Patient Risk Platform

A real-time data pipeline that takes ICU bedside-monitor readings — heart rate, blood-oxygen, breathing rate — runs every single reading through a machine-learning model to figure out whether it's real or sensor noise, then stores it in a time-series database that powers a live clinical dashboard.

100 simulated patients. 1,500 events per second. 1 ML model per vital sign. Sub-50-millisecond end-to-end latency. Zero data loss.

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

---

## Table of Contents

- [The Problem This Solves](#the-problem-this-solves)
- [How It Works (in plain English)](#how-it-works-in-plain-english)
- [Architecture](#architecture)
- [Walking Through the Data, Step by Step](#walking-through-the-data-step-by-step)
- [The ML Part — Why an IsolationForest?](#the-ml-part--why-an-isolationforest)
- [The Dashboard, Panel by Panel](#the-dashboard-panel-by-panel)
- [Tech Stack — and Why Each Piece Is There](#tech-stack--and-why-each-piece-is-there)
- [Performance Numbers](#performance-numbers)
- [Running It Yourself](#running-it-yourself)
- [Project Structure](#project-structure)
- [Engineering Decisions Worth Calling Out](#engineering-decisions-worth-calling-out)
- [Where I'd Take It Next](#where-id-take-it-next)
- [License](#license)

---

## The Problem This Solves

Walk into a modern intensive-care unit and listen for a minute. You'll hear alarms. A *lot* of alarms. Bedside monitors beep when a heart rate dips below a threshold, when blood-oxygen drops, when breathing slows. Each beep is supposed to mean something. In practice, somewhere between 80% and 99% of them are false — they fire because a probe slipped, a cable wiggled, the patient rolled over, or a static shock hit the lead. The clinical research is brutal on this: it's called **alarm fatigue**, and it's a documented patient-safety problem. Nurses literally stop hearing the alarms after a while, because the signal-to-noise ratio is awful.

That's the problem. Now consider the data underneath it. A 30-bed ICU produces hundreds of thousands of vital-sign measurements every minute. Each one needs to be (1) ingested without dropping anything, (2) judged as real or noise *fast enough that the judgment is still useful*, and (3) stored in a way that lets a clinician scroll back through hours of a patient's history and have it render instantly. A regular Postgres table getting hammered with high-frequency inserts while a dashboard queries the same rows will fall over in minutes.

So this project is a working prototype of the data layer that sits underneath that kind of system. It does three things at once:

1. Takes vitals in over HTTP and never loses one.
2. Runs every reading through a machine-learning model that learns what "normal" looks like and flags the obvious sensor glitches **before** they ever become an alarm.
3. Stores everything in a time-series database tuned for clinical dashboards, with full observability on the pipeline *and* the model.

The whole thing runs on a laptop. One `docker compose up` and you've got the infrastructure. Four terminals and you've got the live pipeline.

---

## How It Works (in plain English)

Imagine a small assembly line:

1. **The patients.** A Python script pretends to be 100 ICU patients, each one quietly generating heart rate / SpO₂ / breathing-rate readings five times a second. Sometimes a "patient" silently starts to deteriorate (their heart rate creeps up over 30 seconds, breathing speeds up). Sometimes a "monitor" glitches and spits out a single nonsense reading. Both happen in real ICUs, and a good pipeline has to tell them apart.

2. **The front door.** A small Kotlin web service catches every reading over HTTP. It's built on Ktor + Netty, which means it can hold thousands of concurrent connections without breaking a sweat. Every reading is shoved into Kafka with strict durability settings — `acks=all`, idempotent producer, infinite retries. If a single byte ever gets dropped, that's a bug.

3. **The brain.** A Python service is sitting on the other side of Kafka, eating every reading as it arrives. For each one, it asks an `IsolationForest` model (a type of unsupervised anomaly detector from scikit-learn): "is this normal, or is this nonsense?" It tags the reading with the answer and shoves it back into Kafka on a different topic.

4. **The vault.** A Kotlin worker reads the tagged stream and bulk-writes every reading into TimescaleDB — both the clean ones and the ones the model flagged. We *don't throw the flagged ones away*, because we want the dashboard to be able to show them as red dots on top of a patient's vitals chart later.

5. **The dashboard.** Grafana sits on top of all of it, with two data sources: Prometheus (for "how is the pipeline doing?") and TimescaleDB (for "what's happening with patient P0083 right now?"). Four rows of panels, each answering a different question.

That's the whole system. The rest of this README is the long version of those five paragraphs.

---

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

Kafka, TimescaleDB, Prometheus, and Grafana are all in Docker. The three "code" services — the Kotlin ingest, the Python ML detector, the Kotlin worker — run on the host so you can edit-build-rerun them without container churn. Prometheus reaches them through `host.docker.internal`.

---

## Walking Through the Data, Step by Step

### 1. The simulator — `generator/simulate.py`

Spawns 100 async Python coroutines, one per simulated patient. Each one keeps its own state — its baseline heart rate, baseline SpO₂, baseline breathing rate, and whether it's currently in a deterioration episode. Five times a second it generates three readings (HR, SpO₂, RR) and POSTs them to the ingest API.

Two things make the simulator more than a toy:

- **Autoregressive vitals.** The numbers don't jump randomly; they mean-revert toward each patient's personal baseline with small Gaussian noise. That looks like a real ECG strip, not a uniform-random number generator.
- **Two failure modes.** Each patient occasionally enters a sustained deterioration (slow drift toward unhealthy values), and independently occasionally emits a brief sensor artifact (one rogue sample, then back to normal). The ML model downstream is supposed to flag the artifacts and *not* flag the deterioration. Without both modes, you can't honestly evaluate it.

### 2. The ingest — `ingest/` (Kotlin + Ktor)

A small HTTP service that does one thing: accept a `POST /ingest` and immediately put the payload onto a Kafka topic called `telemetry.raw`. The catch is doing that for ~1,500 requests/sec on a single laptop without dropping anything.

A few decisions that make this fast and safe:

- **Netty engine, coroutines.** Every incoming request is a suspending Kotlin coroutine, not an OS thread. The JVM holds thousands of in-flight requests with a tiny resident set.
- **Patient ID as the Kafka key.** This is more important than it sounds. Kafka uses the message key to decide which partition a record goes to. By keying on `patientId`, *all* events for the same patient end up on the same partition — which means a downstream consumer sees them in order. You can scale out consumers across patients while preserving order *within* a patient.
- **Durability over speed.** `acks=all` + `enable.idempotence=true` + infinite retries. If Kafka momentarily loses a broker, the producer waits and retries. We never lie about whether a reading was accepted.

### 3. Kafka

Kafka here is doing two jobs: it's the durability layer (every reading is on disk before the ingest acks the HTTP request), and it's the **shock absorber** between every two services. If the ML detector is slow for a few seconds while it refits a model, Kafka quietly buffers everything in its log. The ingest never blocks. When the detector catches up, it does so without anyone noticing. Same trick between the detector and the worker.

Running in **KRaft mode** — no ZooKeeper. One broker, 16 partitions each on `telemetry.raw` and `telemetry.scored`.

### 4. The ML detector — `ml-detector/detector.py` (Python)

This is the most interesting service in the project, so it has its own section below ([The ML Part](#the-ml-part--why-an-isolationforest)). The short version: it consumes `telemetry.raw`, runs each reading through an IsolationForest, and republishes the same reading with three extra fields — `flagged`, `anomalyScore`, `reason` — to `telemetry.scored`.

### 5. The worker — `worker/` (Kotlin)

Reads `telemetry.scored` off Kafka in batches, then bulk-inserts everything into TimescaleDB using PostgreSQL's `COPY ... FROM STDIN` protocol. COPY is roughly 8–10× faster than batched INSERTs for batches over a thousand rows, because it streams a tab-separated buffer through a single socket connection instead of round-tripping per row.

The worker writes **every event, flagged or not**, into the same table. The `flagged` column distinguishes them. That single design choice is what makes the "live patient overlay with red dots" panel possible later — you can't render anomaly markers if you've thrown the anomalies away.

It's also been hardened against silent hangs: the JDBC connection has an explicit `socketTimeout=30s` and TCP keep-alive on, HikariCP has leak detection, and the Kafka consumer config sets `max.poll.interval.ms=180s` so a slow batch doesn't get the worker kicked out of the consumer group. There's a `logback.xml` so every batch + every retry shows up in stdout with a timestamp.

### 6. The database — TimescaleDB

TimescaleDB is regular PostgreSQL with an extension that turns a table into a **hypertable** — a logical table backed by many physical "chunks" partitioned by time. The schema here uses 1-day chunks on time AND 8 space partitions on `patient_id`, so a dashboard query for "give me P0083's last 5 minutes" hits exactly one tiny chunk instead of fanning out across a giant table.

Three TimescaleDB features pull their weight:

- **Hypertables** — transparent partitioning. The application writes to one logical table; TimescaleDB routes inserts to the right chunk.
- **Native columnar compression** — kicks in on chunks older than 1 hour. Compressed chunks are 10–20× smaller and faster for aggregate queries.
- **Continuous aggregates** — a 1-minute rollup (`telemetry_1min`) is pre-computed in the background, so dashboards that want "average HR per minute over the last 4 hours" hit a tiny materialized view instead of millions of raw rows.

### 7. The observability layer — Prometheus + Grafana

Every code service exposes `/metrics` in Prometheus exposition format. Prometheus scrapes them every 5 seconds. Grafana renders the panels you'll see below — four rows of them, each answering a different question about a different layer of the stack.

---

## The ML Part — Why an IsolationForest?

This is the heart of the project, so it's worth a few paragraphs.

### The clinical problem, restated

A single weird reading from a bedside monitor could be:

- A real cardiac event (deteriorating patient — must catch).
- A sensor artifact (probe slipped — must ignore).

At a *single sample's resolution*, those two things can look indistinguishable. Both are big, sudden departures from a patient's baseline. A naive "alert if it's far from normal" rule fires on both — and that's how you get the alarm-fatigue problem at the top of this README.

The previous version of this project tried to solve it with a Redis-Lua script computing a 30-second rolling z-score per patient. It was clever, it was fast, and it was *static*. It couldn't learn. So I replaced it with an actual ML model — and the model gets its own service, with its own Kafka topics, so it can be swapped out without touching the ingest or the storage.

### Why specifically IsolationForest?

`IsolationForest` is an unsupervised tree-ensemble algorithm. Three reasons it fits this problem:

1. **It's unsupervised.** I don't have labeled "artifact" vs "real" data. Nobody hand-annotated millions of ICU samples for me. IsolationForest doesn't need labels — it learns what the bulk of the data looks like and flags anything that's "easy to isolate" (i.e., few decision-tree splits needed to separate it from the crowd).
2. **It's fast at inference.** `model.predict()` on a single 5-D feature vector returns in roughly 50 microseconds on a laptop CPU. The whole detector loop including feature extraction sits at ~1 ms p99. That's fast enough that the ML doesn't even register on the end-to-end latency chart.
3. **It has one knob.** The `contamination` parameter says "what fraction of inputs do you expect to be anomalous?" That's a number a model owner and a clinician can actually have a conversation about. It's not a black box.

### How it actually works in this codebase

The detector keeps a small rolling window of the last 150 samples (~30 seconds at 5 Hz) for each `(patient_id, metric)` pair. **That window is used only for feature extraction, not for training.** For every new reading, it builds a 5-dimensional feature vector describing the reading's relationship to the patient's own recent history:

1. The raw value itself.
2. `|value - previous_value|` — how big a single-sample jump.
3. The z-score against the local 30-second window.
4. The local 30-second standard deviation — how stable is this patient right now.
5. Distance from the global population baseline (`|value - pop_mean| / pop_stddev`).

Those 5-D vectors get pooled across **all 100 patients** for training. There's one IsolationForest per metric — HR, SpO₂, RR — three models total. Each one warms up after it's seen 1,000 events for its metric (about ~7 seconds at our throughput), then refits every 5,000 events to adapt to slow drift in the population.

Pooling across patients is important: it means a brand-new patient gets meaningful predictions from minute one, because the model was trained on what "normal" looks like across the population. A truly per-patient model would have to warm up separately for each patient, which is unworkable.

### Tuning `contamination` for clinical use

`contamination` sets the decision threshold. For ICU vitals, the right range is 1–5%, and the direction you err matters a lot:

| Setting | Effect | Clinical risk |
|---|---|---|
| **Too high** (e.g. 0.10) | Aggressive; many normal readings flagged | **Dangerous.** False negatives can kill people. |
| **Too low** (e.g. 0.005) | Conservative; only extreme outliers caught | Manageable — some artifacts slip through, but downstream rate-limiting catches them. |
| **0.03** (default here) | Matches the artifact rate the simulator injects | Reasonable starting point |

In medicine, false negatives are worse than false positives. A noisy alarm has a human in the loop who can ignore it. A *missed* alarm has no second chance. So when in doubt — err toward letting more readings through, not filtering more out.

### Observability, on the model itself

This is the part most "we have ML in production" stories skip. The detector exposes a full Prometheus surface:

- `ml_detector_events_scored_total{metric}` — per-metric throughput counter
- `ml_detector_events_flagged_total{metric}` — per-metric anomaly counter
- `ml_detector_prediction_latency_seconds` — histogram of feature-extraction + predict latency
- `ml_detector_anomaly_score` — histogram of the live anomaly-score distribution
- `ml_detector_model_ready{metric}` — gauge: is this model fit yet?
- `ml_detector_refits_total{metric}` — how many times has it been retrained
- `ml_detector_training_buffer_size{metric}` — current buffer size before next refit

You can answer questions like "is my model getting slower over time?", "is the anomaly-score distribution drifting?", "how often is RR flagging vs HR right now?" without digging through logs. That's the difference between *having* an ML model in production and being able to *operate* one.

---

## The Dashboard, Panel by Panel

Four rows of panels, each one a different angle on the same pipeline. Below are the actual screenshots from the running system, plus what each panel is showing and why it matters.

### Row 1 — Pipeline Health

![Pipeline Health](docs/pipeline-health.png)

This row answers a single question: **is the data plumbing healthy?**

- **Left — Ingest throughput, stacked by metric.** Each colored band is one of the three vital signs (HR / SpO₂ / RR), each running at ~440 events/sec, summing to ~1.4K events/sec total. The fact that the bands are flat and parallel means the simulator is producing evenly and Kafka is accepting everything. A dip in one color would mean a metric-specific problem.
- **Middle — End-to-end latency p99.** Three lines stacked on a millisecond axis: how long the Ktor ingest takes (top, ~15–20ms), how long ML prediction takes (bottom, ~1ms — it's so fast it's almost a flat line on the floor), and how long the TimescaleDB COPY batch takes (~10–13ms). When all three lines are flat and low, the pipeline isn't bottlenecked anywhere. Spikes in any of them tell you exactly which layer to investigate.
- **Right — Pipeline gap.** The cleverest panel in the row. It computes `ingest_rate − ml_scored_rate`. If the ML detector is keeping up with the ingest, this is ~0. A growing positive gap means the detector is falling behind — backpressure is building up in the `telemetry.raw` topic. In this screenshot the gap hovers around ~500 ops/sec, which is just the natural lag from Kafka batching; flat means healthy.

If the top of this row is misbehaving, you don't even need to look at the lower rows — the data isn't flowing.

### Row 2 — ML Model Observability

![ML Model Observability](docs/ml-observability.png)

This row is the part most teams skip. Operating an ML model is different from operating a web service; you need different signals.

- **Top-left — Models ready.** A big green "3" means all three IsolationForests (HR, SpO₂, RR) have finished their warmup and are actively predicting. If this drops below 3, one of the models is in cold-start; predictions for that metric are coming back as "warmup" until enough samples accumulate.
- **Top-middle — Anomaly detection rate per metric.** What percentage of recently-scored events did each model flag? Should hover in the 1–5% band; the screenshot shows all three metrics oscillating in that range, with occasional bursts up to ~14% during anomaly episodes. If a metric got stuck at 0% or 50%, something is broken with that model.
- **Top-right — Prediction latency p50 / p95 / p99.** The whole model loop is sitting under 2 milliseconds at all three percentiles. The p99 line is the canary: if it climbs without the p50 climbing, something pathological is happening at the tail (maybe a refit is taking too long, or feature extraction is getting starved). Here it's a flat ribbon — boringly healthy, which is exactly what you want.
- **Bottom — Anomaly-score distribution over time.** This is the most information-dense panel on the whole dashboard. Each vertical slice is a one-minute window. The color shows how many recently-scored events landed in each "score bucket" — red/orange = score around 0 (boringly normal), purple/blue = score above 0.2 (definitely flagged). Watching this over time tells you whether the model's behavior is *changing* — is the population getting more anomalous, less anomalous, drifting? It's the closest thing to a "is my model okay?" panel you can build without a labeled validation set.

### Row 3 — Clinical View: Patient Risk

![Clinical View](docs/clinical-view.png)

Now we leave the engineering view and switch to the clinician's view. Same data, different lens — instead of "how is the pipeline doing?", these panels ask "**which patients should I be worrying about right now?**"

- **Top-left — Top-20 most anomalous patients, over time.** Each colored line is one patient's average IsolationForest score over the last few minutes. The 20 lines pictured are the patients with the highest risk in the most recent 5-minute window. A line drifting upward means that patient's data is starting to look weirder; a line spiking sharply means a sudden event. The legend on the right gives you their patient IDs and the "Last value" for each — at the moment of the screenshot, P0040 and P0052 are the most-anomalous patients.
- **Top-right — Top-10 leaderboard.** The same answer in tabular form: patient ID, average risk score, total flagged events in the last 5 minutes, total sample count. The "Flags" column is colored red, the "Risk" column is colored on a green-yellow-orange-red gradient. This is the panel you'd glance at as a charge nurse — who's accumulating flags faster than expected?
- **Bottom-left — Selected patient: current risk score gauge.** A live readout for whichever patient is selected in the dropdown at the top of the dashboard. P0093 in the screenshot has a current rolling 5-minute average risk of **-0.161** — well into the "stable, normal" green zone. The needle's position on the dial gives you an instant read; the thresholds are calibrated so green = stable, yellow = slightly elevated, orange = clearly anomalous, red = sustained anomalous behavior.
- **Bottom-middle — Selected patient: flagged events in last 5 min.** P0093 has had **120 flagged events** in the last 5 minutes. That's high — bright red. Note this isn't necessarily a clinical concern by itself; a patient with a slipping SpO₂ probe will rack up flags fast. But combined with the next panel, it gives a picture.
- **Bottom-right — Selected patient: minutes since last anomaly.** P0093's most recent flag was **2.2 minutes ago**. So 120 flags in 5 minutes, last one 2 minutes ago — this patient *was* generating sustained anomalies and has now gone quiet. That's either "their sensor was reseated and they're fine now" or "they're about to enter a different state." Either way, it tells the clinician what just happened.

Read together, these five panels let you go from "is there anything to worry about?" → "which patient?" → "how anomalous, how recent, how long?" — exactly the triage flow a clinical user expects.

### Row 4 — Live Patient Overlay

![Live Patient Overlay](docs/live-overlay.png)

This is my favorite panel. Pick a patient, see their actual vitals — and see the exact moments the ML model fired.

- **Three colored lines** — green = heart rate, yellow = respiratory rate, blue = SpO₂. These are the *clean* readings (where the model said "this is real").
- **Red dots** — these are the exact moments the IsolationForest classified a reading as a sensor artifact. They're plotted on top of the lines using a Grafana field-override (any series whose name starts with `anomaly_` gets rendered as red points instead of a line).

In the screenshot you can clearly see the story of patient P0093. The heart-rate line (green) hovers around 100 bpm normally, but periodically explodes to 200+ for a few seconds — those are sensor artifacts (notice the dense clusters of red dots painted on those spikes). The yellow respiratory-rate line is mostly stable but occasionally drops to near zero (red dots again — also artifacts). The blue SpO₂ line is calm and smooth. The model is doing its job: it's letting the calm baseline through and flagging the impossible spikes.

This panel is also how you'd debug the model itself. If you select a patient and see red dots on top of perfectly normal-looking readings, the model is over-firing — time to lower `contamination`. If you see obvious spikes in the lines with *no* red dots, it's under-firing — time to raise it.

---

## Tech Stack — and Why Each Piece Is There

| Layer | Tech | Why this choice |
|---|---|---|
| Ingest / Worker language | **Kotlin 2.0** | First-class coroutines, null safety, full JVM ecosystem, and `kotlinx.serialization` is a joy compared to Jackson. |
| HTTP server | **Ktor 2.3 + Netty** | Non-blocking down to the kernel. Coroutines suspend instead of parking OS threads — thousands of in-flight requests on a tiny resident set. |
| ML + simulator language | **Python 3.11 + asyncio + aiohttp** | scikit-learn is unbeatable for prototyping a model, and `asyncio` makes the 100-patient simulator a 50-line file. |
| ML model | **scikit-learn IsolationForest** | Unsupervised (no labels needed), O(log n) inference, one tunable knob with a clinical interpretation. |
| Numerics | **NumPy 1.26** | Feature vectors are 5-D arrays; Python loops would dominate the latency budget. |
| Message broker | **Apache Kafka 3.7 (KRaft)** | The durability layer + the shock absorber between every two services. No ZooKeeper to babysit in KRaft mode. |
| Database | **TimescaleDB 2.14 on PostgreSQL 16** | Hypertables, columnar compression, continuous aggregates — purpose-built for time-series workloads while remaining vanilla SQL. |
| JDBC pool | **HikariCP 5** | Fastest JDBC pool in the JVM. Configured here with `socketTimeout` + `tcpKeepAlive` so a half-broken socket can't hang the worker. |
| Metrics | **Prometheus 2.51 + prometheus-client + simpleclient** | Pull-based, exposition-format standard, the same API in Python and Kotlin. |
| Visualization | **Grafana 10.4** | Dashboards as provisioned JSON in the repo. PromQL on top of Prometheus, raw SQL on top of TimescaleDB — both data sources used in the same dashboard. |
| Local orchestration | **Docker Compose v2** | One command brings up Kafka + Timescale + Prometheus + Grafana. |
| Build | **Gradle 8.10 + Shadow 8.3.3** | Reproducible builds, fat-jar packaging for the two Kotlin services. |

---

## Performance Numbers

Measured on a single MacBook Pro (Apple Silicon, 16 GB RAM). Numbers below are steady-state after warmup.

| Metric | Value | Notes |
|---|---|---|
| End-to-end ingest throughput | **~1,500 events/sec** | 100 patients × 5 samples/sec × 3 metrics |
| `/ingest` p99 latency | **< 50 ms** | p50 ≈ 5 ms |
| ML prediction p99 latency | **~1 ms** | Per-sample feature-extraction + `predict()` |
| ML detection rate | **~3 %** | Matches the simulator's injected artifact rate (great validation signal) |
| TimescaleDB COPY batch p99 | **~22 ms** | 1,000-row batches via `COPY FROM STDIN` |
| TimescaleDB write throughput | **~1,500 rows/sec** | All events persisted, including the flagged ones |
| End-to-end pipeline gap | **~0 events/sec** | Ingest rate ≈ ML scored rate at steady state |
| Data loss | **0** | `acks=all`, idempotent producer, manual offset commits, write retries |

The throughput ceiling here is the Python generator's HTTP client — not anything in the pipeline. Kafka, Ktor, the ML detector, and the worker all scale roughly linearly with horizontal worker count thanks to per-patient partitioning. A real deployment behind a load balancer with multiple Ktor pods and multiple ML-detector instances would scale into the tens of thousands of events per second.

---

## Running It Yourself

### What you need installed

- **Docker Desktop**, running
- **JDK 21** — `brew install --cask temurin@21` on macOS
- **Python 3.11+**

### One-time setup

From the project root:

```bash
docker compose up -d
```

That brings up Kafka, TimescaleDB, Prometheus, and Grafana. Then create the two Kafka topics:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic telemetry.raw    --partitions 16 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic telemetry.scored --partitions 16 --replication-factor 1
```

If you're upgrading from a pre-ML version of this project, also apply the schema migration once:

```bash
docker exec -i timescaledb psql -U telemetry -d clinical < sql/migrate-add-flagged.sql
```

And in case your TimescaleDB volume predates the Grafana read-only role:

```bash
docker exec -i timescaledb psql -U telemetry -d clinical < sql/fix-grafana-permissions.sql
```

### Starting the four code services

You'll want four terminal windows.

```bash
# T1 — Ktor ingest
cd ingest
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew run

# T2 — Python ML detector
cd ml-detector
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python detector.py

# T3 — Kotlin worker
cd worker
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew clean shadowJar -q
java -jar build/libs/telemetry-worker-all.jar

# T4 — Python generator
cd generator
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python simulate.py
```

### Where to look

| What | URL | Login |
|---|---|---|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Prometheus | http://localhost:9090 | — |
| Ingest metrics | http://localhost:8080/metrics | — |
| ML-detector metrics | http://localhost:9200/metrics | — |
| Worker metrics | http://localhost:9100/metrics | — |

In Grafana go to **Dashboards → Browse → Clinical Telemetry — ML Risk & Pipeline Observability**. Within ~60 seconds of all four services running, every panel populates.

### A quick sanity check

```bash
docker exec -it timescaledb psql -U telemetry -d clinical -c \
  "SELECT flagged, COUNT(*) FROM telemetry \
   WHERE time > NOW() - INTERVAL '2 minutes' GROUP BY flagged;"
```

You should see two rows — `flagged=true` and `flagged=false`. Roughly 3% of recent events should be flagged.

### Shutdown

```bash
docker compose down       # stop containers, keep the data
docker compose down -v    # stop AND wipe the TimescaleDB volume
```

---

## Project Structure

```
clinical-telemetry-platform/
├── docker-compose.yml                # 4-service local data plane
├── prometheus/
│   └── prometheus.yml                # Scrapes ingest, ml-detector, worker
├── grafana/
│   ├── provisioning/                 # Datasources + dashboard providers
│   └── dashboards/
│       └── clinical-telemetry.json   # The 4-row dashboard
├── sql/
│   ├── init.sql                      # Hypertable, compression, continuous agg, grafana_ro role
│   ├── migrate-add-flagged.sql       # One-shot schema migration for the ML upgrade
│   └── fix-grafana-permissions.sql   # One-shot fix if your DB volume predates the role
├── generator/
│   ├── requirements.txt
│   └── simulate.py                   # 100-patient asyncio simulator
├── ml-detector/                      # Python — IsolationForest service
│   ├── requirements.txt
│   └── detector.py                   # Kafka consumer/producer + per-metric IF model
├── ingest/                           # Gradle module — Ktor service
│   ├── build.gradle.kts
│   └── src/main/kotlin/com/clinical/telemetry/
│       ├── Application.kt
│       ├── api/IngestRoutes.kt
│       ├── kafka/TelemetryProducer.kt
│       ├── metrics/Metrics.kt
│       └── model/TelemetryEvent.kt
└── worker/                           # Gradle module — Kafka consumer + Timescale sink
    ├── build.gradle.kts
    └── src/main/
        ├── kotlin/com/clinical/telemetry/worker/
        │   ├── Main.kt
        │   ├── Worker.kt             # Consumes telemetry.scored, batches to DB
        │   ├── MetricsServer.kt
        │   ├── WorkerMetrics.kt
        │   ├── model/Events.kt       # ScoredEvent shape on the Kafka wire
        │   └── storage/
        │       └── TimescaleSink.kt  # HikariCP + PG CopyManager batch writer
        └── resources/
            └── logback.xml           # Structured logs with timestamps
```

---

## Engineering Decisions Worth Calling Out

A few choices that shaped the project, in case you're reading the code:

**ML inference lives in its own service.** The detector consumes `telemetry.raw` and produces `telemetry.scored`. The Kotlin worker never touches the model; the model never touches the database. I can swap the IsolationForest for a transformer, an autoencoder, or a multi-stage ensemble without changing a line of ingest or storage code. Decoupling through Kafka is doing real work here.

**Patient ID is the Kafka key on both topics.** All events for one patient route to the same partition. Per-patient ordering is preserved through both Kafka hops, even as you scale out consumers. This is the right move for any per-entity time-series problem; it's worth internalizing.

**The detector trains pooled, predicts per-patient.** One model per metric, trained on data from all patients. Cold-start is fast (~30 seconds), and a brand-new patient gets meaningful predictions from minute one because the model already knows what "normal" looks like across the population. A per-patient model would have to warm up separately for everyone.

**Flagged events are persisted, not dropped.** That single decision is what makes the red-dot overlay panel possible. Existing "clean only" queries just add `WHERE flagged = FALSE`. You can't recover dropped data later, so when in doubt, write it down.

**Full ML observability.** Prediction-latency histograms, refit counters, model-ready gauges, training-buffer sizes, *and* the live anomaly-score distribution are all on Prometheus. Not just throughput counters. That's the difference between "we have ML in production" and "we can operate ML in production."

**HikariCP + Postgres `socketTimeout` and `tcpKeepAlive`.** The worker had a silent-hang bug where a half-broken JDBC connection would freeze the entire pipeline. Now there's a 30-second socket-read timeout, OS-level keep-alive on, retries with backoff, and a 60-second connection-leak warning. Failures are loud now, not silent.

**Don't commit Kafka offsets on a failed write.** If a batch fails to land in TimescaleDB, the worker refuses to commit. Kafka redelivers on the next poll. The alternative — committing despite the failure — is silent data loss, which is what the old version did.

**Grafana dashboards live in the repo as provisioned JSON.** Datasources, providers, and the full dashboard. A clean `docker compose up -d` reproduces the entire observability layer. No manual UI clicking.

---

## Where I'd Take It Next

If I were turning this into a real product instead of a portfolio piece, the next things on my list:

- **Online learning.** The IsolationForest currently retrains every 5,000 events. An online algorithm (HalfSpaceTrees, streaming Random-Cut Forest) would adapt continuously instead of in jumps.
- **Per-patient personalization.** A second model layer that learns each patient's own "normal," so a known atrial-fibrillation patient's irregular heart rate doesn't keep tripping the population-level model.
- **Shadow deploys for new model versions.** When changing `contamination` or feature engineering, run the candidate model in parallel with production and compare flagged-event rates before promoting.
- **Kubernetes deployment.** Helm charts per service, the kube-prometheus-stack for monitoring, and a custom HPA metric on `kafka_consumergroup_lag` so the worker scales out automatically under backpressure.
- **Chaos engineering.** Run Chaos Mesh `PodChaos` / `NetworkChaos` against the detector and worker, verify p99 latency stays inside the SLO under failure.
- **Real-world hardening.** At-rest encryption on the TimescaleDB volume, TLS on every Kafka hop, audit logging, PHI de-identification at the ingest boundary.

---

## License

MIT. Built as a portfolio project; not in clinical use anywhere. Synthetic data only.
