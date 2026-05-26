"""
ICU telemetry simulator.

Spawns NUM_PATIENTS concurrent async clients. Each maintains an autoregressive
state for HR / SpO2 / RR around a personalized baseline, occasionally entering
a "deterioration" episode (sustained drift toward unhealthy values) or emitting
a sensor-artifact spike (single-sample large jump). All samples are POSTed as
JSON to the Ktor ingestion endpoint.
"""

import asyncio
import aiohttp
import random
import time
from datetime import datetime, timezone


def utc_now_ms() -> int:
    """Return current Unix epoch milliseconds in UTC.

    ``time.time()`` already returns Unix epoch seconds (UTC by definition),
    but we route through ``datetime.now(timezone.utc)`` to make the UTC
    contract explicit at the call-site — there is now zero ambiguity that
    timestamps stamped here are timezone-agnostic UTC, which is what
    TimescaleDB's TIMESTAMPTZ column and Grafana's UTC time picker expect.
    """
    return int(datetime.now(timezone.utc).timestamp() * 1000)

ENDPOINT = "http://localhost:8080/ingest"
NUM_PATIENTS = 100
SAMPLES_PER_SECOND_PER_PATIENT = 5   # = ~1,500 events/sec across all three metrics
ANOMALY_ENTER_PROB = 0.005           # per tick, per patient
SENSOR_ARTIFACT_PROB = 0.01          # per tick, per patient
REPORT_EVERY_SECONDS = 5


class PatientSimulator:
    """Stateful autoregressive vitals generator."""

    def __init__(self, patient_id: str):
        self.patient_id = patient_id
        self.device_id = f"dev-{patient_id}"
        # personalized healthy baselines
        self.hr_baseline = random.uniform(60, 90)
        self.spo2_baseline = random.uniform(95, 99)
        self.rr_baseline = random.uniform(12, 18)
        # initial state == baseline
        self.hr = self.hr_baseline
        self.spo2 = self.spo2_baseline
        self.rr = self.rr_baseline
        # anomaly episode state
        self.in_anomaly = False
        self.anomaly_ticks_left = 0

    def _maybe_enter_anomaly(self):
        if not self.in_anomaly and random.random() < ANOMALY_ENTER_PROB:
            self.in_anomaly = True
            self.anomaly_ticks_left = random.randint(40, 200)  # 8-40 sec of drift

    def _step_baseline(self):
        # mean-revert toward healthy baseline with small gaussian noise
        self.hr += (self.hr_baseline - self.hr) * 0.10 + random.gauss(0, 0.8)
        self.spo2 += (self.spo2_baseline - self.spo2) * 0.10 + random.gauss(0, 0.15)
        self.rr += (self.rr_baseline - self.rr) * 0.10 + random.gauss(0, 0.3)

    def _step_anomaly(self):
        # drift toward unhealthy targets
        self.hr += (130 - self.hr) * 0.03 + random.gauss(0, 1.5)
        self.spo2 += (85 - self.spo2) * 0.03 + random.gauss(0, 0.4)
        self.rr += (28 - self.rr) * 0.03 + random.gauss(0, 0.8)
        self.anomaly_ticks_left -= 1
        if self.anomaly_ticks_left <= 0:
            self.in_anomaly = False

    def _maybe_inject_artifact(self):
        if random.random() < SENSOR_ARTIFACT_PROB:
            which = random.choice(("hr", "spo2", "rr"))
            if which == "hr":
                self.hr += random.choice((-1, 1)) * random.uniform(40, 80)
            elif which == "spo2":
                self.spo2 -= random.uniform(20, 30)
            else:
                self.rr += random.choice((-1, 1)) * random.uniform(15, 30)

    def _clamp(self):
        self.hr = max(20, min(220, self.hr))
        self.spo2 = max(50, min(100, self.spo2))
        self.rr = max(4, min(50, self.rr))

    def tick(self):
        self._maybe_enter_anomaly()
        if self.in_anomaly:
            self._step_anomaly()
        else:
            self._step_baseline()
        self._maybe_inject_artifact()
        self._clamp()

    def frames(self, ts_ms: int):
        return (
            {"patientId": self.patient_id, "metric": "HR",
             "value": round(self.hr, 2), "timestamp": ts_ms, "deviceId": self.device_id},
            {"patientId": self.patient_id, "metric": "SPO2",
             "value": round(self.spo2, 2), "timestamp": ts_ms, "deviceId": self.device_id},
            {"patientId": self.patient_id, "metric": "RR",
             "value": round(self.rr, 2), "timestamp": ts_ms, "deviceId": self.device_id},
        )


# Cross-coroutine counters
posted = 0
errors = 0


async def patient_loop(session: aiohttp.ClientSession,
                       sim: PatientSimulator,
                       interval: float):
    global posted, errors
    while True:
        sim.tick()
        ts_ms = utc_now_ms()
        for frame in sim.frames(ts_ms):
            try:
                async with session.post(ENDPOINT, json=frame) as resp:
                    await resp.read()
                    if resp.status >= 300:
                        errors += 1
                    else:
                        posted += 1
            except Exception:
                errors += 1
        await asyncio.sleep(interval)


async def reporter():
    global posted, errors
    last_posted = 0
    while True:
        await asyncio.sleep(REPORT_EVERY_SECONDS)
        delta = posted - last_posted
        last_posted = posted
        print(f"[gen] posted/sec={delta/REPORT_EVERY_SECONDS:8.1f}   "
              f"total_posted={posted:10d}   errors={errors}")


async def main():
    print(f"[gen] Spawning {NUM_PATIENTS} patients at "
          f"{SAMPLES_PER_SECOND_PER_PATIENT} samples/sec each "
          f"(~{NUM_PATIENTS * SAMPLES_PER_SECOND_PER_PATIENT * 3} HTTP req/sec)")
    sims = [PatientSimulator(f"P{i:04d}") for i in range(NUM_PATIENTS)]
    interval = 1.0 / SAMPLES_PER_SECOND_PER_PATIENT

    connector = aiohttp.TCPConnector(limit=NUM_PATIENTS * 4, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [asyncio.create_task(patient_loop(session, s, interval)) for s in sims]
        tasks.append(asyncio.create_task(reporter()))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[gen] stopped")