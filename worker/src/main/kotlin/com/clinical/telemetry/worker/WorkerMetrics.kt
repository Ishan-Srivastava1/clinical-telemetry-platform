package com.clinical.telemetry.worker

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import io.prometheus.client.hotspot.DefaultExports

object WorkerMetrics {
    val registry: CollectorRegistry = CollectorRegistry.defaultRegistry

    init {
        DefaultExports.initialize()  // JVM metrics
    }

    val flagged: Counter = Counter.build()
        .name("worker_events_flagged_total")
        .help("Events suppressed as artifact / false alarm by Lua filter")
        .labelNames("metric")
        .register(registry)

    val passed: Counter = Counter.build()
        .name("worker_events_passed_total")
        .help("Events that survived the Redis sliding-window filter")
        .labelNames("metric")
        .register(registry)

    val parseErrors: Counter = Counter.build()
        .name("worker_parse_errors_total")
        .help("JSON deserialization failures on consumed Kafka messages")
        .register(registry)

    val redisErrors: Counter = Counter.build()
        .name("worker_redis_errors_total")
        .help("Redis Lua script invocation failures")
        .register(registry)

    val writeErrors: Counter = Counter.build()
        .name("worker_write_errors_total")
        .help("TimescaleDB batch write failures")
        .register(registry)

    val batchWrites: Counter = Counter.build()
        .name("worker_batch_writes_total")
        .help("Successful TimescaleDB COPY batches")
        .register(registry)

    val rowsWritten: Counter = Counter.build()
        .name("worker_rows_written_total")
        .help("Rows persisted to TimescaleDB via COPY")
        .register(registry)

    val luaLatency: Histogram = Histogram.build()
        .name("worker_lua_latency_seconds")
        .help("Redis Lua sliding-window evaluation latency")
        .buckets(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1)
        .register(registry)

    val pgWriteLatency: Histogram = Histogram.build()
        .name("worker_pg_batch_latency_seconds")
        .help("TimescaleDB batch COPY latency in seconds")
        .buckets(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)
        .register(registry)
}