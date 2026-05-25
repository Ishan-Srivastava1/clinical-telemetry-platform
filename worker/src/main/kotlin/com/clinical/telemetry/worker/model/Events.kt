package com.clinical.telemetry.worker.model

import kotlinx.serialization.Serializable

@Serializable
enum class MetricType { HR, SPO2, RR }

@Serializable
data class TelemetryEvent(
    val patientId: String,
    val metric: MetricType,
    val value: Double,
    val timestamp: Long,
    val deviceId: String
)

@Serializable
data class WindowVerdict(
    val flagged: Boolean,
    val reason: String,
    val n: Int,
    val mean: Double,
    val stddev: Double,
    val z: Double
)

data class EnrichedEvent(
    val event: TelemetryEvent,
    val verdict: WindowVerdict
)