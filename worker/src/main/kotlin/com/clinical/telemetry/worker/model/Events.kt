package com.clinical.telemetry.worker.model

import kotlinx.serialization.Serializable

@Serializable
enum class MetricType { HR, SPO2, RR }

/**
 * Output of the Python ml-detector service, consumed from `telemetry.scored`.
 * `flagged=true` means the IsolationForest classified this sample as an
 * artifact and the worker should drop it.
 */
@Serializable
data class ScoredEvent(
    val patientId: String,
    val metric: MetricType,
    val value: Double,
    val timestamp: Long,
    val deviceId: String,
    val flagged: Boolean = false,
    val anomalyScore: Double = 0.0,
    val reason: String = "ok"
)
