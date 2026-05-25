package com.clinical.telemetry.api

import com.clinical.telemetry.kafka.TelemetryProducer
import com.clinical.telemetry.metrics.PrometheusMetrics
import com.clinical.telemetry.model.TelemetryEvent
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.Route
import io.ktor.server.routing.post

fun Route.ingestRoutes(producer: TelemetryProducer) {

    post("/ingest") {
        val timer = PrometheusMetrics.ingestLatency.startTimer()
        try {
            val event = call.receive<TelemetryEvent>()
            if (event.patientId.isBlank() || event.timestamp <= 0L) {
                PrometheusMetrics.ingestErrors.inc()
                call.respond(HttpStatusCode.BadRequest,
                    mapOf("error" to "missing patientId or timestamp"))
                return@post
            }
            producer.publish(event)
            PrometheusMetrics.eventsIngested.labels(event.metric.name).inc()
            call.respond(HttpStatusCode.Accepted, mapOf("status" to "queued"))
        } catch (e: Exception) {
            PrometheusMetrics.ingestErrors.inc()
            call.respond(HttpStatusCode.ServiceUnavailable,
                mapOf("error" to (e.message ?: "publish failure")))
        } finally {
            timer.observeDuration()
        }
    }
}