package com.clinical.telemetry.metrics

import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram

object PrometheusMetrics {
    val registry: PrometheusMeterRegistry =
        PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    private val collector = registry.prometheusRegistry

    val eventsIngested: Counter = Counter.build()
        .name("telemetry_events_ingested_total")
        .help("Telemetry events accepted at /ingest and published to Kafka")
        .labelNames("metric")
        .register(collector)

    val ingestErrors: Counter = Counter.build()
        .name("telemetry_ingest_errors_total")
        .help("Ingest failures (Kafka publish, validation, etc.)")
        .register(collector)

    val ingestLatency: Histogram = Histogram.build()
        .name("telemetry_ingest_latency_seconds")
        .help("End-to-end /ingest handler latency in seconds")
        .buckets(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
        .register(collector)
}