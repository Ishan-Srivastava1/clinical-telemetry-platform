plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    application
    id("com.gradleup.shadow") version "8.3.3"
}

group = "com.clinical"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("org.postgresql:postgresql:42.7.3")
    implementation("com.zaxxer:HikariCP:5.1.0")
    implementation("io.prometheus:simpleclient:0.16.0")
    implementation("io.prometheus:simpleclient_common:0.16.0")
    implementation("io.prometheus:simpleclient_hotspot:0.16.0")
    implementation("ch.qos.logback:logback-classic:1.5.6")
}

application {
    // Gradle 8 / modern best practice: use mainClass, NOT the deprecated mainClassName.
    mainClass.set("com.clinical.telemetry.worker.MainKt")
}

kotlin {
    jvmToolchain(21)
}

// com.gradleup.shadow 8.3.3 reads `application.mainClass` automatically,
// so no manual Main-Class manifest line is needed.
tasks.shadowJar {
    archiveBaseName.set("telemetry-worker")
    archiveClassifier.set("all")
    archiveVersion.set("")
    mergeServiceFiles()
}