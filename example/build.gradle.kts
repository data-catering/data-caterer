plugins {
    scala
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly(libs.scala.library)

    compileOnly(project(":api"))
}

tasks.register<ValidateYamlAgainstSchema>("validateYaml") {
    yamlDirectory.set(layout.projectDirectory.dir("docker/data/custom"))
    schemaFile.set(layout.projectDirectory.file("schema/data-caterer-latest.json"))
}

//tasks.build {
//    dependsOn("validateYaml")
//}