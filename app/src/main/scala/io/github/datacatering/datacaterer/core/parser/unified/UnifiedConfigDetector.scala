package io.github.datacatering.datacaterer.core.parser.unified

import io.github.datacatering.datacaterer.core.util.FileUtil
import io.github.datacatering.datacaterer.core.util.FileUtil.{getFileContentFromFileSystem, isCloudStoragePath}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.Try

/**
 * Detects whether a YAML file is in unified format or legacy format.
 *
 * Unified format indicators:
 * - Has 'dataSources' array at root level with inline 'connection' objects
 * - May have 'version' field
 *
 * Legacy format indicators:
 * - Has 'tasks' array with 'dataSourceName' references (plan format)
 * - Has 'steps' array without 'dataSources' (task format)
 */
object UnifiedConfigDetector {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  /**
   * Configuration format types
   */
  sealed trait ConfigFormat
  case object UnifiedFormat extends ConfigFormat
  case object LegacyPlanFormat extends ConfigFormat
  case object LegacyTaskFormat extends ConfigFormat
  case object UnknownFormat extends ConfigFormat

  /**
   * Check if a YAML file is in unified format.
   * Handles both local file system and cloud storage paths (with SparkSession).
   */
  def isUnifiedFormat(filePath: String)(implicit sparkSession: SparkSession = null): Boolean = {
    detectFormat(filePath) == UnifiedFormat
  }

  /**
   * Detect the format of a YAML configuration file.
   * Handles both local file system and cloud storage paths (with SparkSession).
   */
  def detectFormat(filePath: String)(implicit sparkSession: SparkSession = null): ConfigFormat = {
    if (sparkSession != null && isCloudStoragePath(filePath)) {
      Try {
        val content = getFileContentFromFileSystem(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration), filePath)
        detectFormatFromContent(content)
      }.getOrElse(UnknownFormat)
    } else {
      val file = new File(filePath)
      if (!file.exists()) return UnknownFormat

      Try {
        val tree = OBJECT_MAPPER.readTree(file)
        detectFormatFromTree(tree)
      }.getOrElse(UnknownFormat)
    }
  }

  /**
   * Detect format from YAML content string
   */
  def detectFormatFromContent(yamlContent: String): ConfigFormat = {
    Try {
      val tree = OBJECT_MAPPER.readTree(yamlContent)
      detectFormatFromTree(tree)
    }.getOrElse(UnknownFormat)
  }

  /**
   * Detect format from Jackson JsonNode tree
   */
  private def detectFormatFromTree(tree: com.fasterxml.jackson.databind.JsonNode): ConfigFormat = {
    // Check for unified format: 'dataSources' array with inline 'connection'
    val hasDataSources = tree.has("dataSources") && tree.get("dataSources").isArray
    val hasVersion = tree.has("version")

    if (hasDataSources) {
      // Verify it's unified by checking if dataSources have inline connection
      val dataSourcesArray = tree.get("dataSources")
      if (dataSourcesArray.size() > 0) {
        val firstDataSource = dataSourcesArray.get(0)
        if (firstDataSource.has("connection")) {
          LOGGER.debug(s"Detected unified format: dataSources with inline connection")
          return UnifiedFormat
        }
      }
      // If dataSources but no inline connection, treat as unknown
      if (hasVersion) {
        LOGGER.debug(s"Detected unified format: has version field")
        return UnifiedFormat
      }
    }

    // Check for legacy plan format: 'tasks' array with 'dataSourceName' references
    val hasLegacyTasks = tree.has("tasks") && {
      val tasks = tree.get("tasks")
      tasks.isArray && tasks.size() > 0 && {
        val firstTask = tasks.get(0)
        firstTask.has("dataSourceName")
      }
    }

    if (hasLegacyTasks) {
      LOGGER.debug(s"Detected legacy plan format: tasks with dataSourceName")
      return LegacyPlanFormat
    }

    // Check for legacy task format: 'steps' array without 'dataSources'
    val hasSteps = tree.has("steps") && tree.get("steps").isArray && !hasDataSources
    if (hasSteps) {
      LOGGER.debug(s"Detected legacy task format: steps array")
      return LegacyTaskFormat
    }

    LOGGER.debug(s"Unknown format detected")
    UnknownFormat
  }
}
