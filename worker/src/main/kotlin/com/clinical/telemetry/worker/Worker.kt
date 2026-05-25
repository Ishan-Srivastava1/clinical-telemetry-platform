package com.clinical.telemetry.worker

import com.clinical.telemetry.worker.model.EnrichedEvent
import com.clinical.telemetry.worker.model.TelemetryEvent
import com.clinical.telemetry.worker.model.WindowVerdict
import com.clinical.telemetry.worker.storage.TimescaleSink
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import redis.clients.jedis.JedisPool
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import com.clinical.telemetry.worker.WorkerMetrics

class Worker(
    private val kafkaBootstrap: String,
    private val rawTopic: String,
    private val groupId: String,
    private val redisPool: JedisPool,
    private val sink: TimescaleSink,
    private val windowMs: Long = 30_000L,
    private val zThreshold: Double = 3.5,
    private val minWindowCount: Int = 10,
    private val maxBatchRows: Int = 1_000
) {
    private val json = Json { ignoreUnknownKeys = true }
    private val running = AtomicBoolean(true)

    private val luaScript: String =
        Worker::class.java.classLoader
            .getResourceAsStream("sliding_window.lua")
            ?.bufferedReader()?.use { it.readText() }
            ?: error("sliding_window.lua not found on classpath")

    private val luaSha: String = redisPool.resource.use { jedis ->
        jedis.scriptLoad(luaScript)
    }

    fun stop() = running.set(false)

    fun run() {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1_000)
            put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 16 * 1024)
            put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 50)
        }

        val consumer = KafkaConsumer<String, String>(props)
        consumer.subscribe(listOf(rawTopic))
        println("[worker] subscribed to $rawTopic with groupId=$groupId")

        try {
            while (running.get()) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (records.isEmpty) continue

                val batch = ArrayList<EnrichedEvent>(records.count())

                redisPool.resource.use { jedis ->
                    for (rec in records) {
                        val event = try {
                            json.decodeFromString(TelemetryEvent.serializer(), rec.value())
                        } catch (e: Exception) {
                            WorkerMetrics.parseErrors.inc()
                            continue
                        }

                        val key = "tel:${event.metric.name}:${event.patientId}"
                        val verdict = try {
                            val timer = WorkerMetrics.luaLatency.startTimer()
                            val raw = try {
                                jedis.evalsha(
                                    luaSha,
                                    listOf(key),
                                    listOf(
                                        event.timestamp.toString(),
                                        event.value.toString(),
                                        windowMs.toString(),
                                        zThreshold.toString(),
                                        minWindowCount.toString()
                                    )
                                ) as String
                            } finally {
                                timer.observeDuration()
                            }
                            json.decodeFromString(WindowVerdict.serializer(), raw)
                        } catch (e: Exception) {
                            WorkerMetrics.redisErrors.inc()
                            continue
                        }

                        if (verdict.flagged) {
                            WorkerMetrics.flagged.labels(event.metric.name).inc()
                        } else {
                            WorkerMetrics.passed.labels(event.metric.name).inc()
                            batch.add(EnrichedEvent(event, verdict))
                            if (batch.size >= maxBatchRows) {
                                flush(batch); batch.clear()
                            }
                        }
                    }
                }

                if (batch.isNotEmpty()) flush(batch)
                consumer.commitSync()
            }
        } finally {
            consumer.close(Duration.ofSeconds(5))
            println("[worker] consumer closed")
        }
    }

    private fun flush(batch: List<EnrichedEvent>) {
        try {
            sink.writeBatch(batch)
            WorkerMetrics.batchWrites.inc()
            WorkerMetrics.rowsWritten.inc(batch.size.toDouble())
        } catch (e: Exception) {
            WorkerMetrics.writeErrors.inc()
            System.err.println("[worker] batch write failed: ${e.message}")
        }
    }
}