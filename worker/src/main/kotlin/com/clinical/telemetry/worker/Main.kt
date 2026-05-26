package com.clinical.telemetry.worker

import com.clinical.telemetry.worker.storage.TimescaleSink
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

private val log = LoggerFactory.getLogger("Main")

fun main() {
    log.info("==========================================================")
    log.info(" telemetry-worker starting")
    log.info("==========================================================")

    val kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP")     ?: "localhost:9092"
    val scoredTopic    = System.getenv("KAFKA_SCORED_TOPIC")  ?: "telemetry.scored"
    val groupId        = System.getenv("KAFKA_GROUP_ID")      ?: "telemetry-worker"
    val jdbcUrl        = System.getenv("PG_URL")              ?: "jdbc:postgresql://localhost:5432/clinical"
    val pgUser         = System.getenv("PG_USER")             ?: "telemetry"
    val pgPass         = System.getenv("PG_PASS")             ?: "telemetry_secret"
    val metricsPort    = (System.getenv("METRICS_PORT") ?: "9100").toInt()

    log.info("config: kafka={} topic={} groupId={} pg={} metricsPort={}",
        kafkaBootstrap, scoredTopic, groupId, jdbcUrl, metricsPort)

    val sink = try {
        TimescaleSink(jdbcUrl = jdbcUrl, user = pgUser, pass = pgPass)
    } catch (e: Exception) {
        log.error("FATAL: could not initialise TimescaleSink", e)
        exitProcess(2)
    }

    try {
        startMetricsServer(metricsPort)
    } catch (e: Exception) {
        log.error("FATAL: could not start metrics server on port {}", metricsPort, e)
        sink.close()
        exitProcess(3)
    }

    val worker = Worker(
        kafkaBootstrap = kafkaBootstrap,
        scoredTopic    = scoredTopic,
        groupId        = groupId,
        sink           = sink
    )

    // Graceful shutdown — wake up the poll loop, let it drain, THEN close pool.
    val shutdownHook = Thread({
        log.info("Shutdown signal received — stopping worker")
        worker.stop()
        // Give the worker thread up to 10s to exit its loop cleanly before
        // we yank the connection pool out from under it.
        try { Thread.sleep(10_000) } catch (_: InterruptedException) {}
        sink.close()
        log.info("Shutdown complete")
    }, "worker-shutdown")
    Runtime.getRuntime().addShutdownHook(shutdownHook)

    // === Top-level guard: if ANYTHING escapes worker.run(), we want to ===
    // === know about it loudly instead of the JVM disappearing silently. ===
    try {
        worker.run()
        log.info("worker.run() returned normally; exiting")
    } catch (t: Throwable) {
        log.error("UNCAUGHT EXCEPTION in worker.run() — JVM will exit", t)
        // Print to stderr too in case logback failed to load
        System.err.println("==========================================================")
        System.err.println(" telemetry-worker DIED with: ${t.javaClass.name}: ${t.message}")
        System.err.println("==========================================================")
        t.printStackTrace(System.err)
        exitProcess(1)
    }
}
