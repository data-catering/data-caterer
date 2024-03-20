import org.scoverage.ScoverageExtension

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala application project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.5.1/userguide/building_java_projects.html
 * This project uses @Incubating APIs which are subject to change.
 */
val scalaVersion: String by project
val scalaSpecificVersion: String by project
val sparkVersion: String by project
val sparkMajorVersion: String by project


plugins {
    scala
    application

    id("org.scoverage") version "8.0.3"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
    maven {
        url = uri("https://plugins.gradle.org/m2/")
    }
}

tasks.withType<ScalaCompile> {
    targetCompatibility = "11"
}

val basicImpl: Configuration by configurations.creating
val jpackageDep: Configuration by configurations.creating

configurations {
    compileOnly {
        if (System.getenv("JPACKAGE_BUILD") != "true") {
            extendsFrom(jpackageDep)
        }
    }
    implementation {
        extendsFrom(basicImpl)
        if (System.getenv("JPACKAGE_BUILD") == "true") {
            extendsFrom(jpackageDep)
        }
    }
}

dependencies {
    jpackageDep("org.scala-lang:scala-library:$scalaSpecificVersion")
    jpackageDep("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
    jpackageDep(project(":api"))

    // connectors
    // postgres
    basicImpl("org.postgresql:postgresql:42.6.0")
    // mysql
    basicImpl("mysql:mysql-connector-java:8.0.33")
    // cassandra
    basicImpl("com.datastax.spark:spark-cassandra-connector_$scalaVersion:3.3.0") {
        exclude(group = "org.scala-lang")
    }
    // cloud file storage
    basicImpl("org.apache.spark:spark-hadoop-cloud_$scalaVersion:$sparkVersion") {
        exclude(group = "org.scala-lang")
    }
    // hudi - currently only supports spark 3.4.x
//    basicImpl("org.apache.hudi:hudi-spark3.4-bundle_$scalaVersion:0.14.1") {
//        exclude(group = "org.scala-lang")
//    }
    // iceberg
    basicImpl("org.apache.iceberg:iceberg-spark-runtime-${sparkMajorVersion}_$scalaVersion:1.4.3") {
        exclude(group = "org.scala-lang")
    }
    // delta lake
//    basicImpl("io.delta:delta-spark_$scalaVersion:3.1.0") {
//        exclude(group = "org.scala-lang")
//    }

    // data generation helpers
    basicImpl("net.datafaker:datafaker:1.9.0")
    basicImpl("org.reflections:reflections:0.10.2")

    // alert
    basicImpl("com.slack.api:slack-api-client:1.36.1")

    // UI/HTTP server
    basicImpl("com.typesafe.akka:akka-http_$scalaVersion:10.5.3")
    basicImpl("com.typesafe.akka:akka-stream_$scalaVersion:2.8.5")
    basicImpl("com.typesafe.akka:akka-actor-typed_$scalaVersion:2.8.5")
    basicImpl("com.typesafe.akka:akka-http-spray-json_$scalaVersion:10.5.3")

    // misc
    basicImpl("joda-time:joda-time:2.12.5")
    basicImpl("com.google.guava:guava:32.1.3-jre")
    basicImpl("org.asynchttpclient:async-http-client:2.12.3")
    basicImpl("com.github.pureconfig:pureconfig_$scalaVersion:0.17.2") {
        exclude(group = "org.scala-lang")
    }
    basicImpl("com.fasterxml.jackson.core:jackson-databind:2.15.3") {
        version {
            strictly("2.15.3")
        }
    }
    basicImpl("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.3")
    basicImpl("com.fasterxml.jackson.module:jackson-module-scala_$scalaVersion:2.15.3") {
        exclude(group = "org.scala-lang")
    }
    basicImpl("org.scala-lang.modules:scala-xml_$scalaVersion:2.2.0") {
        exclude(group = "org.scala-lang")
    }
}

testing {
    suites {
        // Configure the built-in test suite
        val test by getting(JvmTestSuite::class) {
            // Use JUnit4 test framework
            useJUnit("4.13.2")

            dependencies {
                // Use Scalatest for testing our library
                implementation("org.scalatest:scalatest_$scalaVersion:3.2.17")
                implementation("org.scalatestplus:junit-4-13_$scalaVersion:3.2.17.0")
                implementation("org.scalamock:scalamock_$scalaVersion:5.2.0")
                implementation("org.apache.spark:spark-sql_$scalaVersion:$sparkVersion")
                implementation("org.apache.spark:spark-avro_$scalaVersion:$sparkVersion")
                implementation("org.apache.spark:spark-protobuf_$scalaVersion:$sparkVersion")
                implementation("com.dimafeng:testcontainers-scala_$scalaVersion:0.41.0")
                implementation(project(":api"))

                // Need scala-xml at test runtime
                runtimeOnly("org.scala-lang.modules:scala-xml_$scalaVersion:1.2.0")
            }
        }
    }
}

application {
    // Define the main class for the application.
    mainClass.set("io.github.datacatering.datacaterer.App")
}

sourceSets {
    test {
        resources {
            setSrcDirs(listOf("src/test/resources"))
        }
    }
}

tasks.shadowJar {
    isZip64 = true
    relocate("com.google.common", "shadow.com.google.common")
    val newTransformer = com.github.jengelman.gradle.plugins.shadow.transformers.AppendingTransformer()
    newTransformer.resource = "reference.conf"
    transformers.add(newTransformer)
}

tasks.test {
    finalizedBy(tasks.reportScoverage)
}

configure<ScoverageExtension> {
    scoverageScalaVersion.set(scalaSpecificVersion)
    excludedFiles.add(".*CombinationCalculator.*")
    excludedPackages.add("io.github.datacatering.datacaterer.core.exception.*")
}
