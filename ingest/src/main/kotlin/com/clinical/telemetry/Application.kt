package com.clinical.telemetry

import com.clinical.telemetry.api.ingestRoutes
import com.clinical.telemetry.config.Config
import com.clinical.telemetry.kafka.TelemetryProducer
import com.clinical.telemetry.metrics.PrometheusMetrics
import io.ktor.http.ContentType
import io.ktor.serialization.kotlinx.json.json
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json
import org.slf4j.event.Level
import io.ktor.server.application.call

fun main() {
    val producer = TelemetryProducer()
    Runtime.getRuntime().addShutdownHook(Thread { producer.close() })

    embeddedServer(Netty, port = Config.httpPort, host = "0.0.0.0") {
        install(ContentNegotiation) {
            json(Json {
                ignoreUnknownKeys = true
                isLenient = true
            })
        }
        install(CallLogging) {
            level = Level.INFO
            filter { call -> call.request.local.uri != "/metrics" && call.request.local.uri != "/healthz" }
        }
        install(MicrometerMetrics) {
            registry = PrometheusMetrics.registry
        }
        install(StatusPages) {
            exception<Throwable> { call, cause ->
                call.respondText("Internal error: ${cause.message}",
                    status = io.ktor.http.HttpStatusCode.InternalServerError)
            }
        }

        routing {
            get("/healthz") { call.respondText("ok") }
            get("/metrics") {
                call.respondText(
                    text = PrometheusMetrics.registry.scrape(),
                    contentType = ContentType.parse("text/plain; version=0.0.4; charset=utf-8")
                )
            }
            ingestRoutes(producer)
        }
    }.start(wait = true)
}