package com.clinical.telemetry.model

import kotlinx.serialization.Serializable

@Serializable
enum class MetricType { HR, SPO2, RR }

@Serializable
data class TelemetryEvent(
    val patientId: String,
    val metric: MetricType,
    val value: Double,
    val timestamp: Long,   // epoch millis
    val deviceId: String
)