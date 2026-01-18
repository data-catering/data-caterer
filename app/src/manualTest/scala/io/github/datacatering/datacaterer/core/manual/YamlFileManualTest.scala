package io.github.datacatering.datacaterer.core.manual

import java.io.File

/**
 * Generic manual test that executes any unified YAML configuration file.
 *
 * Usage:
 * {{{
 * # Run with a specific YAML file
 * YAML_FILE=/path/to/my-config.yaml ./gradlew :app:manualTest --tests "*YamlFileManualTest"
 *
 * # Run with debug logging
 * YAML_FILE=/path/to/config.yaml LOG_LEVEL=debug ./gradlew :app:manualTest --tests "*YamlFileManualTest"
 *
 * # Run an example from misc/schema/examples
 * YAML_FILE=misc/schema/examples/kafka-streaming.yaml ./gradlew :app:manualTest --tests "*YamlFileManualTest"
 * }}}
 *
 * Before running:
 * 1. Ensure any required external services are running (Kafka, PostgreSQL, etc.)
 * 2. Set environment variables referenced in the YAML file
 *
 * This is useful for:
 * - Testing new YAML configurations before integrating them
 * - Debugging data generation issues with real external services
 * - Validating unified YAML schema examples
 */
class YamlFileManualTest extends ManualTestSuite {

  private val yamlFilePath: Option[String] = Option(System.getenv("YAML_FILE")).filter(_.nonEmpty)

  test("Execute YAML file from environment variable") {
    yamlFilePath match {
      case Some(path) =>
        val file = resolveFilePath(path)
        if (!file.exists()) {
          fail(s"YAML file not found: ${file.getAbsolutePath}")
        }

        LOGGER.info(s"Executing YAML file: ${file.getAbsolutePath}")
        val results = executeUnifiedYaml(file.getAbsolutePath)

        // Log results
        LOGGER.info(s"Execution completed:")
        LOGGER.info(s"  - Generation results: ${results.generationResults.size}")
        results.generationResults.foreach { genResult =>
          LOGGER.info(s"    - ${genResult.sinkResult.name}: success=${genResult.sinkResult.isSuccess}, records=${genResult.sinkResult.count}")
        }
        LOGGER.info(s"  - Validation results: ${results.validationResults.size}")
        results.validationResults.foreach { valResult =>
          val allPassed = valResult.dataSourceValidationResults.forall(ds =>
            ds.validationResults.forall(_.isSuccess)
          )
          LOGGER.info(s"    - ${valResult.name}: success=$allPassed")
        }

        // Basic assertions
        results should not be null
        results.generationResults.foreach { genResult =>
          withClue(s"Generation failed for ${genResult.sinkResult.name}: ") {
            genResult.sinkResult.isSuccess shouldBe true
          }
        }

      case None =>
        cancel("YAML_FILE environment variable not set. Usage: YAML_FILE=/path/to/config.yaml ./gradlew :app:manualTest --tests \"*YamlFileManualTest\"")
    }
  }

  /**
   * Resolve file path - handles both absolute and relative paths.
   * Relative paths are resolved from the project root.
   */
  private def resolveFilePath(path: String): File = {
    val file = new File(path)
    if (file.isAbsolute) {
      file
    } else {
      // Try to find project root by looking for build.gradle.kts
      var current = new File(System.getProperty("user.dir"))
      while (current != null && !new File(current, "build.gradle.kts").exists()) {
        current = current.getParentFile
      }
      if (current != null) {
        new File(current, path)
      } else {
        // Fall back to current directory
        new File(path)
      }
    }
  }
}
