package com.clinical.telemetry.worker

import com.sun.net.httpserver.HttpServer
import io.prometheus.client.exporter.common.TextFormat
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

fun startMetricsServer(port: Int) {
    val server = HttpServer.create(InetSocketAddress(port), 0)

    server.createContext("/metrics") { exchange ->
        val body = ByteArrayOutputStream()
        OutputStreamWriter(body, StandardCharsets.UTF_8).use { writer ->
            TextFormat.write004(writer, WorkerMetrics.registry.metricFamilySamples())
        }
        val bytes = body.toByteArray()
        exchange.responseHeaders["Content-Type"] = TextFormat.CONTENT_TYPE_004
        exchange.sendResponseHeaders(200, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    server.createContext("/healthz") { exchange ->
        val b = "ok".toByteArray()
        exchange.sendResponseHeaders(200, b.size.toLong())
        exchange.responseBody.use { it.write(b) }
    }

    server.executor = null
    server.start()
    println("[worker] metrics server listening on :$port")
}