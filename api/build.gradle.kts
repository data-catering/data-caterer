import org.scoverage.ScoverageExtension

/*
 * This file was generated by the Gradle 'init' task.
 *
 * This generated file contains a sample Scala application project to get you started.
 * For more details take a look at the 'Building Java & JVM projects' chapter in the Gradle
 * User Manual available at https://docs.gradle.org/7.5.1/userguide/building_java_projects.html
 * This project uses @Incubating APIs which are subject to change.
 */
val apiArtifactId: String by project
val scalaVersion: String by project
val scalaSpecificVersion: String by project

project.base.archivesName.set(apiArtifactId)

plugins {
    scala
    `java-library`
    `maven-publish`
    signing

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

val basicImpl: Configuration by configurations.creating
val advancedImpl: Configuration by configurations.creating

configurations {
    implementation {
        extendsFrom(basicImpl)
        extendsFrom(advancedImpl)
    }
}

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaSpecificVersion")
    compileOnly("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.17.1")
    compileOnly("com.fasterxml.jackson.module:jackson-module-scala_$scalaVersion:2.17.1")

    api("com.softwaremill.quicklens:quicklens_$scalaVersion:1.9.7") {
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
                implementation("org.scalatest:scalatest_$scalaVersion:3.2.10")
                implementation("org.scalatestplus:junit-4-13_$scalaVersion:3.2.2.0")
                implementation("org.scalamock:scalamock_$scalaVersion:5.2.0")

                // Need scala-xml at test runtime
                runtimeOnly("org.scala-lang.modules:scala-xml_$scalaVersion:1.3.1")
            }
        }
    }
}

sourceSets {
    main {
        scala {
            setSrcDirs(listOf("src/main/scala", "src/main/java"))
        }
        java {
            setSrcDirs(emptyList<String>())
        }
    }
    test {
        scala {
            setSrcDirs(listOf("src/test/scala", "src/test/java"))
        }
        java {
            setSrcDirs(emptyList<String>())
        }
        resources {
            setSrcDirs(listOf("src/test/resources"))
        }
    }
}

java {
    withJavadocJar()
    withSourcesJar()
}

tasks.shadowJar {
    archiveBaseName.set("datacaterer")
    archiveAppendix.set("api")
    archiveVersion.set(project.version.toString())
    archiveClassifier.set("")
    isZip64 = true
}

tasks.test {
    finalizedBy(tasks.reportScoverage)
}

configure<ScoverageExtension> {
    scoverageScalaVersion.set("2.12.15")
}

publishing {
    repositories {
        maven {
            name = "OSSRH"
            url = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
            credentials {
                username = System.getenv("MAVEN_USERNAME")
                password = System.getenv("MAVEN_PASSWORD")
            }
        }
    }
    publications {
        create<MavenPublication>("mavenScala") {
            artifact(tasks.shadowJar)
            artifact(tasks["sourcesJar"])
            artifact(tasks["javadocJar"])
            groupId = project.properties["groupId"].toString()
            artifactId = apiArtifactId

            pom {
                name.set("Data Caterer API")
                description.set("API for discovering, generating and validating data using Data Caterer")
                url.set("https://data.catering/")
                scm {
                    url.set("https://github.com/data-catering/data-caterer")
                    developerConnection.set("git@github.com:data-catering/data-caterer.git")
                }
                developers {
                    developer {
                        id.set("pflooky")
                        name.set("Peter Flook")
                        email.set("peter.flook@data.catering")
                    }
                }
                licenses {
                    license {
                        name.set("Apache 2.0")
                        url.set("https://opensource.org/license/apache-2-0/")
                    }
                }
            }
        }
    }
}

signing {
    val signingKey: String? by project
    val signingKeyId: String? by project
    val signingPassword: String? by project
    useInMemoryPgpKeys(signingKeyId, signingKey, signingPassword)
    sign(publishing.publications["mavenScala"])
}
