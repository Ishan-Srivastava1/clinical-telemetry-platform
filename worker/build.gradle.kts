plugins {
    kotlin("jvm") version "2.0.0"
    kotlin("plugin.serialization") version "2.0.0"
    application
    id("com.gradleup.shadow") version "8.3.0"
}

group = "com.clinical"
version = "1.0.0"

repositories { mavenCentral() }

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("redis.clients:jedis:5.1.2")

    // TimescaleDB / Postgres
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("com.zaxxer:HikariCP:5.1.0")

    // Metrics
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.prometheus:simpleclient_common:0.16.0")
    implementation("io.prometheus:simpleclient_hotspot:0.16.0")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.5.6")
}

application {
    mainClass.set("com.clinical.telemetry.worker.MainKt")
}

kotlin { jvmToolchain(21) }

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("telemetry-worker")
    archiveClassifier.set("all")
    mergeServiceFiles()
}