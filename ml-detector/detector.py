"""
ml-detector — real-time anomaly detection for ICU telemetry.

Consumes from Kafka topic `telemetry.raw`, classifies every sample as
artifact or clean using a per-metric IsolationForest model, and republishes
the classification verdict to `telemetry.scored`.

Exposes Prometheus metrics on :9200 for ML model observability.

Design choices:
  * One IsolationForest per metric (HR / SPO2 / RR), pooled across patients
    so each model gets enough training data quickly.
  * Per-(patient, metric) rolling deque is used for *feature extraction only*,
    not for training — features capture the sample's relationship to the
    patient's own recent history (delta, z-score, local std).
  * Cold-start handling: each model fits after MIN_FIT_SAMPLES events for
    that metric have been observed. Until then, verdict is "warmup".
  * The model is periodically refit every REFIT_INTERVAL events to adapt to
    drift in the population distribution.
"""
from __future__ import annotations

import json
import os
import signal
import sys
import time
from collections import defaultdict, deque
from typing import Deque, Dict, List, Optional

import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from sklearn.ensemble import IsolationForest


# ----------------------------- Configuration -------------------------------- #

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC       = os.getenv("KAFKA_RAW_TOPIC", "telemetry.raw")
SCORED_TOPIC    = os.getenv("KAFKA_SCORED_TOPIC", "telemetry.scored")
GROUP_ID        = os.getenv("KAFKA_GROUP_ID", "ml-detector")
METRICS_PORT    = int(os.getenv("METRICS_PORT", "9200"))

WINDOW_SIZE     = 150
MIN_FIT_SAMPLES = int(os.getenv("MIN_FIT_SAMPLES", "1000"))
REFIT_INTERVAL  = int(os.getenv("REFIT_INTERVAL", "5000"))
CONTAMINATION   = float(os.getenv("CONTAMINATION", "0.03"))

BASELINES: Dict[str, tuple[float, float]] = {
    "HR":   (75.0, 25.0),
    "SPO2": (97.0,  5.0),
    "RR":   (15.0, 10.0),
}

REPORT_EVERY_SECONDS = 5.0


# --------------------------- Prometheus metrics ----------------------------- #

ml_events_scored = Counter(
    "ml_detector_events_scored_total",
    "Total events scored by the IsolationForest",
    ["metric"],
)
ml_events_flagged = Counter(
    "ml_detector_events_flagged_total",
    "Total events the IsolationForest classified as artifacts",
    ["metric"],
)
ml_prediction_latency = Histogram(
    "ml_detector_prediction_latency_seconds",
    "Per-sample feature-extraction + model.predict latency",
    ["metric"],
    buckets=(0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1),
)
ml_anomaly_score = Histogram(
    "ml_detector_anomaly_score",
    "Distribution of (negated) decision_function scores; larger = more anomalous",
    ["metric"],
    buckets=(-0.3, -0.2, -0.1, -0.05, 0.0, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5),
)
ml_model_ready = Gauge(
    "ml_detector_model_ready",
    "1 if the per-metric model has been fit at least once, else 0",
    ["metric"],
)
ml_refits = Counter(
    "ml_detector_refits_total",
    "Number of (re)fits performed for each per-metric model",
    ["metric"],
)
ml_training_buffer = Gauge(
    "ml_detector_training_buffer_size",
    "Current size of the rolling training-feature buffer per metric",
    ["metric"],
)
ml_consumer_lag_estimate = Gauge(
    "ml_detector_last_event_age_seconds",
    "Wall-clock age (in seconds) of the most recently scored event",
)

for _m in BASELINES:
    ml_model_ready.labels(metric=_m).set(0)
    ml_training_buffer.labels(metric=_m).set(0)


# --------------------------------- State ------------------------------------ #

class MetricState:
    __slots__ = ("history", "last_value")

    def __init__(self) -> None:
        self.history: Deque[float] = deque(maxlen=WINDOW_SIZE)
        self.last_value: Optional[float] = None


patient_state: Dict[str, Dict[str, MetricState]] = defaultdict(
    lambda: defaultdict(MetricState)
)
training_buffer: Dict[str, List[np.ndarray]] = defaultdict(list)
models: Dict[str, Optional[IsolationForest]] = {m: None for m in BASELINES}
since_fit: Dict[str, int] = defaultdict(int)


# ------------------------- Feature extraction ------------------------------- #

def extract_features(value: float, metric: str, state: MetricState) -> np.ndarray:
    baseline_mean, baseline_dev = BASELINES[metric]
    hist = state.history
    n = len(hist)
    if n >= 5:
        arr = np.fromiter(hist, dtype=np.float64, count=n)
        window_mean = float(arr.mean())
        window_std  = float(arr.std())
        z = (value - window_mean) / window_std if window_std > 1e-6 else 0.0
    else:
        window_std = 0.0
        z = 0.0

    delta = abs(value - state.last_value) if state.last_value is not None else 0.0
    baseline_dist = abs(value - baseline_mean) / baseline_dev

    return np.array([value, delta, z, window_std, baseline_dist], dtype=np.float64)


# --------------------------- Model lifecycle -------------------------------- #

def fit_model(metric: str) -> None:
    buf = training_buffer[metric]
    if len(buf) < MIN_FIT_SAMPLES:
        return
    X = np.vstack(buf[-MIN_FIT_SAMPLES:])
    model = IsolationForest(
        n_estimators=100,
        max_samples="auto",
        contamination=CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)
    models[metric] = model
    since_fit[metric] = 0
    training_buffer[metric] = buf[-MIN_FIT_SAMPLES:]
    ml_model_ready.labels(metric=metric).set(1)
    ml_refits.labels(metric=metric).inc()
    print(f"[ml-detector] {metric}: fit on {X.shape[0]} samples "
          f"(contamination={CONTAMINATION})", flush=True)


# ------------------------------ Main loop ----------------------------------- #

_running = True

def _shutdown(*_args) -> None:
    global _running
    _running = False
    print("[ml-detector] shutdown signal received", flush=True)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)


def main() -> None:
    start_http_server(METRICS_PORT)
    print(f"[ml-detector] prometheus metrics on :{METRICS_PORT}/metrics", flush=True)

    consumer = KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=500,
        fetch_min_bytes=16 * 1024,
        fetch_max_wait_ms=50,
        consumer_timeout_ms=-1,
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        acks="all",
        linger_ms=5,
        compression_type="gzip",
        retries=5,
        max_in_flight_requests_per_connection=5,
    )

    print(f"[ml-detector] consuming {RAW_TOPIC} -> producing {SCORED_TOPIC}",
          flush=True)
    print(f"[ml-detector] warmup: {MIN_FIT_SAMPLES} samples per metric "
          f"before first fit", flush=True)

    scored = 0
    flagged = 0
    period_start = time.time()

    try:
        while _running:
            batches = consumer.poll(timeout_ms=500)
            if not batches:
                continue

            for _tp, records in batches.items():
                for rec in records:
                    event = rec.value
                    metric = event.get("metric")
                    if metric not in BASELINES:
                        producer.send(SCORED_TOPIC,
                                      key=event.get("patientId", ""),
                                      value={**event,
                                             "flagged": False,
                                             "anomalyScore": 0.0,
                                             "reason": "unknown_metric"})
                        continue

                    patient_id = event["patientId"]
                    value = float(event["value"])
                    state = patient_state[patient_id][metric]

                    with ml_prediction_latency.labels(metric=metric).time():
                        feats = extract_features(value, metric, state)
                        model = models[metric]
                        if model is None:
                            flag = False
                            score = 0.0
                            reason = "warmup"
                        else:
                            pred = int(model.predict(feats.reshape(1, -1))[0])
                            raw = float(model.decision_function(feats.reshape(1, -1))[0])
                            score = -raw  # larger == more anomalous
                            flag = (pred == -1)
                            reason = "isolation_forest"

                    ml_anomaly_score.labels(metric=metric).observe(score)
                    ml_events_scored.labels(metric=metric).inc()
                    if flag:
                        ml_events_flagged.labels(metric=metric).inc()

                    state.history.append(value)
                    state.last_value = value

                    training_buffer[metric].append(feats)
                    since_fit[metric] += 1
                    ml_training_buffer.labels(metric=metric).set(
                        len(training_buffer[metric])
                    )

                    if model is None and len(training_buffer[metric]) >= MIN_FIT_SAMPLES:
                        fit_model(metric)
                    elif model is not None and since_fit[metric] >= REFIT_INTERVAL:
                        fit_model(metric)

                    out = {
                        **event,
                        "flagged": flag,
                        "anomalyScore": score,
                        "reason": reason,
                    }
                    producer.send(SCORED_TOPIC, key=patient_id, value=out)

                    # Last-event age gauge — useful for detecting consumer stalls
                    ev_ts = event.get("timestamp")
                    if isinstance(ev_ts, (int, float)):
                        ml_consumer_lag_estimate.set(
                            max(0.0, time.time() - ev_ts / 1000.0)
                        )

                    scored += 1
                    if flag:
                        flagged += 1

            producer.flush()
            consumer.commit()

            now = time.time()
            if now - period_start >= REPORT_EVERY_SECONDS:
                elapsed = now - period_start
                rate = scored / elapsed
                pct = (flagged / scored * 100.0) if scored else 0.0
                ready = sum(1 for m in models.values() if m is not None)
                print(f"[ml-detector] scored={scored:6d} ({rate:7.1f}/s)  "
                      f"flagged={flagged} ({pct:5.2f}%)  "
                      f"models_ready={ready}/{len(BASELINES)}", flush=True)
                scored = 0
                flagged = 0
                period_start = now
    finally:
        try:
            producer.flush(timeout=5)
            producer.close(timeout=5)
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass
        print("[ml-detector] stopped", flush=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
