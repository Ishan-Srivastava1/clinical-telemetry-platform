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
    // Ktor server
    implementation("io.ktor:ktor-server-core-jvm:2.3.10")
    implementation("io.ktor:ktor-server-netty-jvm:2.3.10")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:2.3.10")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:2.3.10")
    implementation("io.ktor:ktor-server-call-logging-jvm:2.3.10")
    implementation("io.ktor:ktor-server-status-pages-jvm:2.3.10")
    implementation("io.ktor:ktor-server-metrics-micrometer-jvm:2.3.10")

    // Kafka client
    implementation("org.apache.kafka:kafka-clients:3.7.0")

    // Coroutines
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")

    // Metrics (Micrometer feeds simpleclient → /metrics scrape)
    implementation("io.micrometer:micrometer-registry-prometheus:1.12.5")
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.prometheus:simpleclient_common:0.16.0")

    // Logging
    implementation("ch.qos.logback:logback-classic:1.5.6")
}

application {
    mainClass.set("com.clinical.telemetry.ApplicationKt")
}

kotlin { jvmToolchain(21) }

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {
    archiveBaseName.set("telemetry-ingest")
    archiveClassifier.set("all")
    mergeServiceFiles()
}