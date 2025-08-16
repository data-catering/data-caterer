plugins {
    kotlin("jvm") version "2.1.0"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.2")
    implementation("com.networknt:json-schema-validator:1.0.87")
    implementation(kotlin("stdlib-jdk8"))
}

kotlin {
    jvmToolchain(17)
}
