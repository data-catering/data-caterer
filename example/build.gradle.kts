val scalaVersion: String by project
val scalaSpecificVersion: String by project
val dataCatererVersion: String by project

plugins {
    scala
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaSpecificVersion")

    compileOnly("io.github.data-catering:data-caterer-api:$dataCatererVersion")
}

tasks.register<ValidateYamlAgainstSchema>("validateYaml") {
    yamlDirectory.set(layout.projectDirectory.dir("docker/data/custom"))
    schemaFile.set(layout.projectDirectory.file("schema/data-caterer-latest.json"))
}

//tasks.build {
//    dependsOn("validateYaml")
//}