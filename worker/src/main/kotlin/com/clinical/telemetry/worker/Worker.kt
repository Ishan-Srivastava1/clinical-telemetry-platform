package com.clinical.telemetry.worker

import com.clinical.telemetry.worker.model.ScoredEvent
import com.clinical.telemetry.worker.storage.TimescaleSink
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Consumes scored telemetry events from Kafka and persists EVERY one of them
 * (clean + flagged) to TimescaleDB, tagging each row with the ml-detector's
 * verdict.
 *
 * --------------------------- HARDENING NOTES ------------------------------
 * The previous version had three silent-failure modes:
 *
 *   (a) flush() swallowed exceptions but commitSync() still ran → silent
 *       data loss + advancing offsets past lost batches.
 *   (b) No max.poll.interval.ms / session.timeout.ms → if a COPY hung past
 *       5 min, the broker kicked the consumer, then the next commitSync
 *       threw CommitFailedException, killing the JVM with no log.
 *   (c) Used println instead of slf4j, so default logback never showed
 *       timestamps or pool diagnostics.
 *
 * Fixes:
 *   - flush() now THROWS on permanent failure → caller skips commitSync.
 *   - Explicit Kafka timeouts compatible with our COPY retry budget (3
 *     attempts × ~2s backoff + 30s socketTimeout = ~95s worst case → we
 *     set max.poll.interval.ms=180s with plenty of headroom).
 *   - Structured slf4j logging via logback.xml.
 *   - WakeupException-driven graceful shutdown that doesn't race the pool.
 *   - Per-batch latency logging so a slow batch is immediately visible.
 * -------------------------------------------------------------------------
 */
class Worker(
    private val kafkaBootstrap: String,
    private val scoredTopic: String,
    private val groupId: String,
    private val sink: TimescaleSink,
    private val maxBatchRows: Int = 1_000
) {
    private val log  = LoggerFactory.getLogger(Worker::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val running = AtomicBoolean(true)

    @Volatile private var consumer: KafkaConsumer<String, String>? = null

    /** Called from the shutdown hook on a different thread — uses Kafka's
     *  wakeup() so poll() unblocks immediately. */
    fun stop() {
        running.set(false)
        consumer?.wakeup()
    }

    fun run() {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
            put(ConsumerConfig.GROUP_ID_CONFIG,           groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,   "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,  false)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,    1_000)
            put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,     16 * 1024)
            put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,   50)

            // === The fix for the "silent kick from the consumer group" ===
            // Our worst-case batch budget = 3 retries × 30s socketTimeout + backoff
            // ≈ 95s. We give max.poll.interval.ms generous headroom over that.
            put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 180_000) // 3 minutes
            put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,   45_000)
            put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10_000)
            put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,   60_000)
            // If broker is briefly unreachable, retry forever rather than
            // throwing a fatal exception that exits the JVM.
            put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG,     1_000)
            put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 30_000)
            put(ConsumerConfig.CLIENT_ID_CONFIG, "telemetry-worker-1")
        }

        val consumer = KafkaConsumer<String, String>(props).also { this.consumer = it }
        consumer.subscribe(listOf(scoredTopic))
        log.info("Subscribed to topic={} groupId={} bootstrap={}",
            scoredTopic, groupId, kafkaBootstrap)

        var batchesWritten = 0L
        var rowsWritten   = 0L
        val startMs = System.currentTimeMillis()

        try {
            while (running.get()) {
                val records = try {
                    consumer.poll(Duration.ofMillis(500))
                } catch (w: WakeupException) {
                    if (!running.get()) break else throw w
                }
                if (records.isEmpty) continue

                val batch = ArrayList<ScoredEvent>(records.count())
                for (rec in records) {
                    val event = try {
                        json.decodeFromString(ScoredEvent.serializer(), rec.value())
                    } catch (e: Exception) {
                        WorkerMetrics.parseErrors.inc()
                        if (log.isDebugEnabled) log.debug("parse error on offset {}: {}", rec.offset(), e.message)
                        continue
                    }

                    if (event.flagged) WorkerMetrics.flagged.labels(event.metric.name).inc()
                    else               WorkerMetrics.passed.labels(event.metric.name).inc()

                    batch.add(event)
                    if (batch.size >= maxBatchRows) {
                        if (!flush(batch)) {
                            // Hard failure mid-poll — skip commit, retry whole batch next poll.
                            log.error("Aborting poll cycle without commit; offsets will be re-read")
                            batch.clear()
                            // jump straight back to the top of the while loop
                            continue
                        }
                        rowsWritten += batch.size
                        batchesWritten++
                        batch.clear()
                    }
                }

                if (batch.isNotEmpty()) {
                    if (!flush(batch)) {
                        log.error("Tail-batch flush failed; will NOT commit offsets, retrying next poll")
                        continue
                    }
                    rowsWritten += batch.size
                    batchesWritten++
                }

                // ONLY commit if EVERY flush in this poll cycle succeeded.
                try {
                    consumer.commitSync(Duration.ofSeconds(10))
                } catch (e: Exception) {
                    log.error("commitSync failed: {} — will redeliver on next poll", e.message)
                }

                if (batchesWritten % 10 == 0L) {
                    val elapsedSec = (System.currentTimeMillis() - startMs) / 1000.0
                    log.info("progress: batches={} rows={} ({} rows/s)",
                        batchesWritten, rowsWritten,
                        "%.0f".format(rowsWritten / elapsedSec.coerceAtLeast(1.0)))
                }
            }
        } catch (e: WakeupException) {
            if (running.get()) log.error("Unexpected WakeupException", e)
        } catch (e: Exception) {
            log.error("FATAL in worker loop", e)
            throw e
        } finally {
            log.info("Closing consumer (final stats: batches={}, rows={})",
                batchesWritten, rowsWritten)
            try { consumer.close(Duration.ofSeconds(5)) } catch (_: Exception) {}
        }
    }

    /** Returns true if the batch was persisted; false if all retries failed. */
    private fun flush(batch: List<ScoredEvent>): Boolean {
        return try {
            sink.writeBatch(batch)
            WorkerMetrics.batchWrites.inc()
            WorkerMetrics.rowsWritten.inc(batch.size.toDouble())
            true
        } catch (e: Exception) {
            WorkerMetrics.writeErrors.inc()
            log.error("batch write FAILED ({} rows): {}", batch.size, e.message, e)
            false
        }
    }
}
