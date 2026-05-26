package com.clinical.telemetry.worker.storage

import com.clinical.telemetry.worker.WorkerMetrics
import com.clinical.telemetry.worker.model.ScoredEvent
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.PGConnection
import org.postgresql.copy.CopyManager
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * Streaming batch sink for TimescaleDB. Uses the PostgreSQL CopyManager
 * (COPY ... FROM STDIN) which is ~8-10x faster than INSERT for batches >= 1k.
 *
 * Writes 10 columns:
 *   time, patient_id, metric, value, device_id,
 *   z_score, window_mean, window_std, flagged, reason
 *
 * --------------------------- HARDENING NOTES ------------------------------
 *
 * The previous version of this sink would HANG FOREVER if Postgres became
 * unresponsive (TimescaleDB chunk creation, compression policy, Docker network
 * blip, kernel paging stall). HikariCP cannot detect a half-broken socket if
 * the kernel's TCP stack still considers it open. The driver would block on
 * socket read with no exception, no log, no recovery.
 *
 * Fixes applied here:
 *   - socketTimeout=30s — kills any COPY that doesn't get a response in 30s
 *   - tcpKeepAlive=true — kernel-level dead-peer detection
 *   - loginTimeout=10s — fail fast on initial connect
 *   - keepaliveTime=30s — Hikari probes idle conns every 30s
 *   - validationTimeout=3s — fast pool-borrow validation
 *   - leakDetectionThreshold=60s — log if a conn is held >60s
 *   - retry on transient COPY errors (max 3 attempts, exponential backoff)
 * -------------------------------------------------------------------------
 */
class TimescaleSink(
    jdbcUrl: String,
    user: String,
    pass: String,
    poolSize: Int = 8
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(TimescaleSink::class.java)
    private val tsFormatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    private val ds: HikariDataSource = HikariDataSource(HikariConfig().apply {
        this.jdbcUrl = jdbcUrl
        this.username = user
        this.password = pass

        // Pool sizing
        this.maximumPoolSize = poolSize
        this.minimumIdle     = 2
        this.poolName        = "TimescalePool"

        // === Liveness timeouts — THE FIX for silent hangs ===
        this.connectionTimeout     = 5_000     // wait at most 5s to borrow from pool
        this.validationTimeout     = 3_000     // wait at most 3s to validate on borrow
        this.idleTimeout           = 60_000    // recycle idle conns after 60s
        this.maxLifetime           = 15 * 60_000L // recycle every 15 min (under PG idle limit)
        this.keepaliveTime         = 30_000    // probe each idle conn every 30s
        this.leakDetectionThreshold = 60_000   // SCREAM into the log if a conn is held >60s

        // === pgjdbc-level timeouts — even more important ===
        // socketTimeout: max time the driver will block on any network read.
        // tcpKeepAlive : OS-level TCP keepalive so a dead bridge connection
        //                gets noticed within ~2 hours (Linux default) instead
        //                of never.
        // loginTimeout : fail fast on the initial handshake if Postgres is down.
        // reWriteBatchedInserts: speeds up bulk inserts; harmless for COPY.
        this.addDataSourceProperty("socketTimeout", "30")           // seconds!
        this.addDataSourceProperty("tcpKeepAlive",  "true")
        this.addDataSourceProperty("loginTimeout",  "10")
        this.addDataSourceProperty("reWriteBatchedInserts", "true")
        this.addDataSourceProperty("ApplicationName", "telemetry-worker")
    })

    init {
        log.info("HikariCP pool initialized: url={} user={} maxPool={}", jdbcUrl, user, poolSize)
    }

    /**
     * Write a batch via COPY. Retries up to 3 times on transient SQL errors
     * (network, timeout, lock contention) with exponential backoff.
     * Throws on permanent failure so the caller can decide NOT to commit
     * the Kafka offset.
     */
    fun writeBatch(batch: List<ScoredEvent>) {
        if (batch.isEmpty()) return
        val timer = WorkerMetrics.pgWriteLatency.startTimer()

        var lastError: Exception? = null
        for (attempt in 1..3) {
            try {
                writeBatchOnce(batch)
                timer.observeDuration()
                if (attempt > 1) {
                    log.warn("COPY succeeded on attempt {} (after {} retries)", attempt, attempt - 1)
                }
                return
            } catch (e: Exception) {
                lastError = e
                log.warn("COPY attempt {}/3 failed: {} — {}",
                    attempt, e.javaClass.simpleName, e.message)
                if (attempt < 3) {
                    Thread.sleep((250L * attempt).coerceAtMost(2_000L))
                }
            }
        }
        timer.observeDuration()
        // All retries exhausted — escalate so caller (Worker) can refuse to commit.
        throw RuntimeException(
            "COPY failed after 3 attempts: ${lastError?.message}", lastError
        )
    }

    private fun writeBatchOnce(batch: List<ScoredEvent>) {
        ds.connection.use { conn: Connection ->
            conn.autoCommit = true
            val pg = conn.unwrap(PGConnection::class.java)
            val copy: CopyManager = pg.copyAPI

            val sb = StringBuilder(batch.size * 128)
            for (e in batch) {
                val tsIso = Instant.ofEpochMilli(e.timestamp)
                    .atOffset(ZoneOffset.UTC).format(tsFormatter)
                sb.append(tsIso).append('\t')
                    .append(escape(e.patientId)).append('\t')
                    .append(escape(e.metric.name)).append('\t')
                    .append(e.value).append('\t')
                    .append(escape(e.deviceId)).append('\t')
                    .append(e.anomalyScore).append('\t')
                    .append("\\N").append('\t')          // window_mean (legacy, NULL)
                    .append("\\N").append('\t')          // window_std  (legacy, NULL)
                    .append(if (e.flagged) "t" else "f").append('\t')
                    .append(escape(e.reason)).append('\n')
            }

            copy.copyIn(
                """
                COPY telemetry (time, patient_id, metric, value,
                                device_id, z_score, window_mean, window_std,
                                flagged, reason)
                FROM STDIN WITH (FORMAT text)
                """.trimIndent(),
                ByteArrayInputStream(sb.toString().toByteArray(StandardCharsets.UTF_8))
            )
        }
    }

    /** Escapes the four characters that have meaning in COPY ... FORMAT text. */
    private fun escape(s: String): String =
        s.replace("\\", "\\\\")
         .replace("\t", "\\t")
         .replace("\n", "\\n")
         .replace("\r", "\\r")

    override fun close() {
        log.info("Closing HikariCP pool")
        ds.close()
    }
}
