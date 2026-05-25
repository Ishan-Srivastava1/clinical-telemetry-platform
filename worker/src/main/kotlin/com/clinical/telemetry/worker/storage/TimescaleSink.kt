package com.clinical.telemetry.worker.storage

import com.clinical.telemetry.worker.WorkerMetrics
import com.clinical.telemetry.worker.model.EnrichedEvent
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.postgresql.PGConnection
import org.postgresql.copy.CopyManager
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.sql.Connection
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class TimescaleSink(
    jdbcUrl: String,
    user: String,
    pass: String,
    poolSize: Int = 8
) : AutoCloseable {

    private val tsFormatter: DateTimeFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME

    private val ds: HikariDataSource = HikariDataSource(HikariConfig().apply {
        this.jdbcUrl = jdbcUrl
        this.username = user
        this.password = pass
        this.maximumPoolSize = poolSize
        this.minimumIdle = 2
        this.connectionTimeout = 5_000
        this.idleTimeout = 30_000
        this.poolName = "TimescalePool"
        this.addDataSourceProperty("rewriteBatchedInserts", "true")
        this.addDataSourceProperty("ApplicationName", "telemetry-worker")
    })

    fun writeBatch(batch: List<EnrichedEvent>) {
        if (batch.isEmpty()) return
        val timer = WorkerMetrics.pgWriteLatency.startTimer()
        try {
            ds.connection.use { conn: Connection ->
                conn.autoCommit = true
                val pg = conn.unwrap(PGConnection::class.java)
                val copy: CopyManager = pg.copyAPI

                val sb = StringBuilder(batch.size * 96)
                for (ee in batch) {
                    val tsIso = Instant.ofEpochMilli(ee.event.timestamp)
                        .atOffset(ZoneOffset.UTC).format(tsFormatter)
                    sb.append(tsIso).append('\t')
                        .append(escape(ee.event.patientId)).append('\t')
                        .append(escape(ee.event.metric.name)).append('\t')
                        .append(ee.event.value).append('\t')
                        .append(escape(ee.event.deviceId)).append('\t')
                        .append(ee.verdict.z).append('\t')
                        .append(ee.verdict.mean).append('\t')
                        .append(ee.verdict.stddev).append('\n')
                }

                copy.copyIn(
                    """
                    COPY telemetry (time, patient_id, metric, value,
                                    device_id, z_score, window_mean, window_std)
                    FROM STDIN WITH (FORMAT text)
                    """.trimIndent(),
                    ByteArrayInputStream(sb.toString().toByteArray(StandardCharsets.UTF_8))
                )
            }
        } finally {
            timer.observeDuration()
        }
    }

    private fun escape(s: String): String =
        s.replace("\\", "\\\\")
         .replace("\t", "\\t")
         .replace("\n", "\\n")
         .replace("\r", "\\r")

    override fun close() {
        ds.close()
    }
}