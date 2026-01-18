package io.github.datacatering.datacaterer.core.parser.unified

import io.github.datacatering.datacaterer.api.model.unified.UnifiedConfig
import io.github.datacatering.datacaterer.core.util.FileUtil
import io.github.datacatering.datacaterer.core.util.FileUtil.{getFileContentFromFileSystem, isCloudStoragePath}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.{Failure, Success, Try}

/**
 * Parser for unified YAML configuration files.
 * Handles both local file system and cloud storage paths.
 */
object UnifiedYamlParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  /**
   * Parse a unified YAML configuration file.
   * Handles both local file system and cloud storage paths (with SparkSession).
   */
  def parse(filePath: String)(implicit sparkSession: SparkSession = null): UnifiedConfig = {
    val config = if (sparkSession != null && isCloudStoragePath(filePath)) {
      val fileContent = getFileContentFromFileSystem(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration), filePath)
      parseContent(fileContent)
    } else {
      val file = if (sparkSession != null) FileUtil.getFile(filePath) else new File(filePath)
      if (!file.exists()) {
        throw new java.io.FileNotFoundException(s"Unified config file not found: $filePath")
      }
      Try(OBJECT_MAPPER.readValue(file, classOf[UnifiedConfig])) match {
        case Success(c) => c
        case Failure(ex) =>
          LOGGER.error(s"Failed to parse unified config: $filePath", ex)
          throw ex
      }
    }

    LOGGER.info(s"Parsed unified config: name=${config.name}, dataSources=${config.dataSources.size}, foreignKeys=${config.foreignKeys.size}")
    config
  }

  /**
   * Parse unified config from YAML content string
   */
  def parseContent(yamlContent: String): UnifiedConfig = {
    Try(OBJECT_MAPPER.readValue(yamlContent, classOf[UnifiedConfig])) match {
      case Success(config) =>
        LOGGER.debug(s"Parsed unified config from content: name=${config.name}")
        config
      case Failure(ex) =>
        LOGGER.error("Failed to parse unified config from content", ex)
        throw ex
    }
  }
}
