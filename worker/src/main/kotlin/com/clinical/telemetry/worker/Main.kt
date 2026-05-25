package com.clinical.telemetry.worker

import com.clinical.telemetry.worker.storage.TimescaleSink
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

fun main() {
    val kafkaBootstrap = System.getenv("KAFKA_BOOTSTRAP") ?: "localhost:9092"
    val rawTopic       = System.getenv("KAFKA_RAW_TOPIC") ?: "telemetry.raw"
    val groupId        = System.getenv("KAFKA_GROUP_ID") ?: "telemetry-worker"

    val redisHost = System.getenv("REDIS_HOST") ?: "localhost"
    val redisPort = (System.getenv("REDIS_PORT") ?: "6379").toInt()
    val pool = JedisPool(
        JedisPoolConfig().apply { maxTotal = 64; maxIdle = 32; minIdle = 8 },
        redisHost, redisPort
    )

    val sink = TimescaleSink(
        jdbcUrl = System.getenv("PG_URL") ?: "jdbc:postgresql://localhost:5432/clinical",
        user    = System.getenv("PG_USER") ?: "telemetry",
        pass    = System.getenv("PG_PASS") ?: "telemetry_secret"
    )

    startMetricsServer((System.getenv("METRICS_PORT") ?: "9100").toInt())

    val worker = Worker(
        kafkaBootstrap = kafkaBootstrap,
        rawTopic       = rawTopic,
        groupId        = groupId,
        redisPool      = pool,
        sink           = sink
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        println("[worker] shutdown requested")
        worker.stop()
        sink.close()
        pool.close()
    })

    worker.run()
}