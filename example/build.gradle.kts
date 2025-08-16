val scalaVersion: String by project
val scalaSpecificVersion: String by project

plugins {
    scala
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.scala-lang:scala-library:$scalaSpecificVersion")

    compileOnly(project(":api"))
}

tasks.register<ValidateYamlAgainstSchema>("validateYaml") {
    yamlDirectory.set(layout.projectDirectory.dir("docker/data/custom"))
    schemaFile.set(layout.projectDirectory.file("schema/data-caterer-latest.json"))
}

//tasks.build {
//    dependsOn("validateYaml")
//}