import org.jetbrains.gradle.ext.Application
import org.jetbrains.gradle.ext.runConfigurations
import org.jetbrains.gradle.ext.settings

plugins {
    idea
    id("org.jetbrains.gradle.plugin.idea-ext") version "1.1.7"
    id("io.github.gradle-nexus.publish-plugin") version "1.3.0"
}

group = project.properties["groupId"].toString()
version = project.properties["version"].toString()

idea.project.settings {
    runConfigurations {
        create("GenerateFromManualJson", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_GENERATE_PLAN_AND_TASKS" to "false",
                "PLAN_FILE_PATH" to "app/src/test/resources/sample/plan/account-create-plan.yaml",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task"
            )
        }
        create("GenerateFromMetadata", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_GENERATE_PLAN_AND_TASKS" to "true",
                "ENABLE_GENERATE_DATA" to "false",
                "PLAN_FILE_PATH" to "app/src/test/resources/sample/plan/customer-create-plan.yaml",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task"
            )
        }
        create("GenerateFromMetadataMysql", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_GENERATE_PLAN_AND_TASKS" to "true",
                "ENABLE_GENERATE_DATA" to "true",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task",
                "APPLICATION_CONFIG_PATH" to "app/src/test/resources/sample/conf/mysql.conf"
            )
        }
        create("GenerateFromMetadataWithTracking", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_GENERATE_PLAN_AND_TASKS" to "true",
                "ENABLE_GENERATE_DATA" to "true",
                "ENABLE_RECORD_TRACKING" to "true",
                "PLAN_FILE_PATH" to "app/src/test/resources/sample/plan/customer-create-plan.yaml",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task"
            )
        }
        create("DeleteGeneratedRecords", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_DELETE_GENERATED_RECORDS" to "true",
                "PLAN_FILE_PATH" to "app/src/test/resources/sample/plan/customer-create-plan.yaml",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task"
            )
        }
        create("GenerateLargeData", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_GENERATE_PLAN_AND_TASKS" to "false",
                "ENABLE_GENERATE_DATA" to "true",
                "ENABLE_RECORD_TRACKING" to "true",
                "PLAN_FILE_PATH" to "app/src/test/resources/sample/plan/large-plan.yaml",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task"
            )
        }
        create("ExampleAccountCreatePlan", Application::class.java) {
            mainClass = "com.github.pflooky.datagen.App"
            moduleName = "data-caterer.app.main"
            includeProvidedDependencies = true
            envs = mutableMapOf(
                "ENABLE_GENERATE_PLAN_AND_TASKS" to "false",
                "ENABLE_GENERATE_DATA" to "true",
                "ENABLE_RECORD_TRACKING" to "true",
                "PLAN_FILE_PATH" to "app/src/test/resources/sample/plan/example-account-create-plan.yaml",
                "TASK_FOLDER_PATH" to "app/src/test/resources/sample/task",
                "LOG_LEVEL" to "debug"
            )
        }
    }
}

nexusPublishing {
    repositories {
        create("sonatype") {
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
            username.set(System.getenv("MAVEN_USERNAME"))
            password.set(System.getenv("MAVEN_PASSWORD"))
        }
    }
}
