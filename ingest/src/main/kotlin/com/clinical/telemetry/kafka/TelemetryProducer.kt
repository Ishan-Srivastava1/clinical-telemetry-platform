package com.clinical.telemetry.kafka

import com.clinical.telemetry.config.Config
import com.clinical.telemetry.model.TelemetryEvent
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

class TelemetryProducer : AutoCloseable {

    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true
    }

    private val producer: KafkaProducer<String, String> = run {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.kafkaBootstrap)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5)
            put(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE)
            put(ProducerConfig.LINGER_MS_CONFIG, 5)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024)
            put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd")
            put(ProducerConfig.CLIENT_ID_CONFIG, "telemetry-ingest")
        }
        KafkaProducer(props)
    }

    suspend fun publish(event: TelemetryEvent): RecordMetadata =
        suspendCancellableCoroutine { cont ->
            val payload = json.encodeToString(event)
            val record = ProducerRecord(Config.rawTopic, event.patientId, payload)
            try {
                producer.send(record) { meta, ex ->
                    if (ex != null) cont.resumeWithException(ex)
                    else cont.resume(meta)
                }
            } catch (e: Exception) {
                cont.resumeWithException(e)
            }
        }

    override fun close() {
        producer.flush()
        producer.close()
    }
}