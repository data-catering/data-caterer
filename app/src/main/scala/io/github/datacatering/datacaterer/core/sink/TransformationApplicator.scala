package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.PATH
import io.github.datacatering.datacaterer.api.model.Step
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}

/**
 * Handles post-write transformations of data files.
 * Supports both per-record and whole-file transformation modes.
 */
class TransformationApplicator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply transformation based on configured mode (per-record or whole-file).
   * Only applies to file-based formats.
   *
   * @param step                Step configuration containing transformation config
   * @param connectionConfig    Connection configuration with path
   * @param format              File format (csv, json, etc.)
   * @param targetFilePath      Path to the file after consolidation
   * @return (final file path, optional exception)
   */
  def applyTransformation(
    step: Step,
    connectionConfig: Map[String, String],
    format: String,
    targetFilePath: Option[String]
  ): (Option[String], Option[Exception]) = {
    import io.github.datacatering.datacaterer.core.transformer.{PerRecordTransformer, WholeFileTransformer}

    // Check if this is a file-based format (transformations only work with files)
    val fileFormats = List("csv", "json", "parquet", "orc", "delta", "iceberg", "hudi")
    if (!fileFormats.contains(format.toLowerCase)) {
      LOGGER.debug(s"Format $format is not file-based, skipping transformation")
      return (targetFilePath, None)
    }

    // Get the file path
    val filePath = targetFilePath.orElse(connectionConfig.get(PATH))

    if (filePath.isEmpty) {
      LOGGER.warn(s"No file path available for transformation")
      return (targetFilePath, None)
    }

    // Check if transformation is configured
    step.transformation match {
      case Some(config) if config.isEnabled =>
        LOGGER.info(s"Applying ${config.mode} transformation for step=${step.name}, file=${filePath.get}")

        Try {
          val transformedPath = if (config.isPerRecord) {
            PerRecordTransformer.transform(config, filePath.get, format)
          } else {
            WholeFileTransformer.transform(config, filePath.get, format)
          }
          transformedPath
        } match {
          case Success(newPath) =>
            (Some(newPath), None)
          case Failure(ex) =>
            LOGGER.error(s"Transformation failed for step=${step.name}", ex)
            val exception = ex match {
              case e: Exception => e
              case t: Throwable => new Exception(t.getMessage, t)
            }
            (targetFilePath, Some(exception))
        }

      case _ =>
        LOGGER.debug(s"No transformation configured for step=${step.name}")
        (targetFilePath, None)
    }
  }
}

/**
 * Companion object providing a shared instance
 */
object TransformationApplicator {
  private val instance = new TransformationApplicator()

  def apply(): TransformationApplicator = instance
}
