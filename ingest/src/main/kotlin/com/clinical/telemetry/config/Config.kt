package com.clinical.telemetry.config

object Config {
    val httpPort: Int = (System.getenv("HTTP_PORT") ?: "8080").toInt()
    val kafkaBootstrap: String = System.getenv("KAFKA_BOOTSTRAP") ?: "localhost:9092"
    val rawTopic: String = System.getenv("KAFKA_RAW_TOPIC") ?: "telemetry.raw"
}