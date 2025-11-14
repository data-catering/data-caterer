package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{DELTA, DELTA_LAKE_SPARK_CONF, DRIVER, FORMAT, ICEBERG, ICEBERG_SPARK_CONF, JDBC, POSTGRES_DRIVER, ROWS_PER_SECOND}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, MetadataConfig, SinkResult, Step}
import io.github.datacatering.datacaterer.api.util.ConfigUtil
import io.github.datacatering.datacaterer.core.exception.FailedSaveDataException
import io.github.datacatering.datacaterer.core.model.Constants.{BATCH, DEFAULT_ROWS_PER_SECOND, FAILED, FINISHED, STARTED}
import io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.determineSaveTiming
import io.github.datacatering.datacaterer.core.util.MetadataUtil.getFieldMetadata
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDateTime

/**
 * Main factory for pushing data to various sinks (databases, files, messaging systems, etc.).
 * Coordinates between batch and real-time writers, handles metadata, and manages the overall sink lifecycle.
 *
 * This class has been refactored to delegate specific responsibilities to specialized components:
 * - FileConsolidator: Handles consolidation of Spark part files into single files
 * - BatchSinkWriter: Handles batch data writes with support for multiple formats
 * - PekkoStreamingSinkWriter: Handles real-time/streaming writes with rate limiting
 * - TransformationApplicator: Applies post-write transformations
 */
class SinkFactory(
  val flagsConfig: FlagsConfig,
  val metadataConfig: MetadataConfig,
  val foldersConfig: FoldersConfig
)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private var HAS_LOGGED_COUNT_DISABLE_WARNING = false

  // Delegate components for specific responsibilities
  private val fileConsolidator = FileConsolidator()
  private val transformationApplicator = TransformationApplicator()
  private val batchSinkWriter = new BatchSinkWriter(fileConsolidator, transformationApplicator)
  private val pekkoStreamingSinkWriter = new PekkoStreamingSinkWriter(foldersConfig)

  /**
   * Main entry point for pushing data to a sink (single-batch mode).
   *
   * @param df DataFrame to save
   * @param dataSourceName Name of the data source
   * @param step Step configuration
   * @param startTime Start time for metrics
   * @return SinkResult with success status and metadata
   */
  def pushToSink(df: DataFrame, dataSourceName: String, step: Step, startTime: LocalDateTime): SinkResult = {
    pushToSink(df, dataSourceName, step, startTime, isMultiBatch = false, isLastBatch = true)
  }

  /**
   * Main entry point for pushing data to a sink with multi-batch support.
   *
   * @param df DataFrame to save
   * @param dataSourceName Name of the data source
   * @param step Step configuration
   * @param startTime Start time for metrics
   * @param isMultiBatch Whether this is part of a multi-batch operation
   * @param isLastBatch Whether this is the last batch
   * @return SinkResult with success status and metadata
   */
  def pushToSink(
    df: DataFrame,
    dataSourceName: String,
    step: Step,
    startTime: LocalDateTime,
    isMultiBatch: Boolean,
    isLastBatch: Boolean
  ): SinkResult = {
    val dfWithoutOmitFields = DataFrameOmitUtil.removeOmitFields(df)
    val saveMode = step.options.get("saveMode").map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
    val format = step.options.getOrElse(FORMAT, throw new IllegalArgumentException(s"No format specified for data source: $dataSourceName, step: ${step.name}. Available options: ${step.options.keys.mkString(", ")}"))
    val enrichedConnectionConfig = additionalConnectionConfig(format, step.options)

    val count = if (flagsConfig.enableCount) {
      dfWithoutOmitFields.count().toString
    } else if (!HAS_LOGGED_COUNT_DISABLE_WARNING) {
      LOGGER.warn("Count is disabled. It will help with performance. Defaulting to -1")
      HAS_LOGGED_COUNT_DISABLE_WARNING = true
      "-1"
    } else "-1"

    LOGGER.info(s"Pushing data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, num-records=$count, status=$STARTED")
    saveData(dfWithoutOmitFields, dataSourceName, step, enrichedConnectionConfig, saveMode, format, count, flagsConfig.enableFailOnError, startTime, isMultiBatch, isLastBatch)
  }

  /**
   * Orchestrates the data saving process, delegating to batch or real-time writers.
   */
  private def saveData(
    df: DataFrame,
    dataSourceName: String,
    step: Step,
    connectionConfig: Map[String, String],
    saveMode: SaveMode,
    format: String,
    count: String,
    enableFailOnError: Boolean,
    startTime: LocalDateTime,
    isMultiBatch: Boolean,
    isLastBatch: Boolean
  ): SinkResult = {
    val saveTiming = determineSaveTiming(dataSourceName, format, step.name)
    val baseSinkResult = SinkResult(dataSourceName, format, saveMode.name())

    //TODO might have use case where empty data can be tested, is it okay just to check for empty schema?
    val sinkResult = if (df.schema.isEmpty) {
      LOGGER.debug(s"Generated data schema is empty, not saving to data source, data-source-name=$dataSourceName, format=$format")
      baseSinkResult
    } else if (saveTiming.equalsIgnoreCase(BATCH)) {
      batchSinkWriter.saveBatchData(dataSourceName, df, saveMode, connectionConfig, count, startTime, step, isMultiBatch, isLastBatch)
    } else {
      // Use PekkoStreamingSinkWriter for real-time sinks (HTTP, JMS)
      val rowsPerSecond = step.options.getOrElse(ROWS_PER_SECOND, connectionConfig.getOrElse(ROWS_PER_SECOND, DEFAULT_ROWS_PER_SECOND))
      val rate = Math.max(rowsPerSecond.toInt, 1)
      LOGGER.info(s"Rows per second for generating data, rows-per-second=$rate")
      pekkoStreamingSinkWriter.saveWithRateControl(dataSourceName, df, format, connectionConfig, step, rate, startTime)
    }

    val finalSinkResult = (sinkResult.isSuccess, sinkResult.exception) match {
      case (false, Some(exception)) =>
        LOGGER.error(s"Failed to save data for sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
          s"num-records=$count, status=$FAILED, exception=${exception.getMessage.take(500)}")
        if (enableFailOnError) throw FailedSaveDataException(dataSourceName, step.name, saveMode.name(), count, exception) else baseSinkResult
      case (true, None) =>
        LOGGER.info(s"Successfully saved data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
          s"num-records=$count, status=$FINISHED")
        enrichSinkResultWithMetadata(sinkResult, df, connectionConfig, count.toLong)
      case (isSuccess, optException) =>
        LOGGER.warn(s"Unexpected sink result scenario, is-success=$isSuccess, exception-exists=${optException.isDefined}")
        enrichSinkResultWithMetadata(sinkResult, df, connectionConfig, count.toLong)
    }

    df.unpersist()
    finalSinkResult
  }

  /**
   * Enriches sink result with metadata and sample data if enabled.
   */
  private def enrichSinkResultWithMetadata(
    sinkResult: SinkResult,
    df: DataFrame,
    connectionConfig: Map[String, String],
    count: Long
  ): SinkResult = {
    val cleansedOptions = ConfigUtil.cleanseOptions(connectionConfig)
    val baseResult = sinkResult.copy(options = cleansedOptions, count = count)

    if (flagsConfig.enableSinkMetadata) {
      val sample = df.take(metadataConfig.numGeneratedSamples).map(_.json)
      val fields = getFieldMetadata(sinkResult.name, df, connectionConfig, metadataConfig)
      baseResult.copy(generatedMetadata = fields, sample = sample)
    } else {
      baseResult
    }
  }

  /**
   * Adds format-specific connection configuration.
   * For example, Postgres JDBC needs stringtype=unspecified, Delta/Iceberg need Spark configs.
   */
  private def additionalConnectionConfig(format: String, connectionConfig: Map[String, String]): Map[String, String] = {
    format match {
      case JDBC => if (connectionConfig(DRIVER).equalsIgnoreCase(POSTGRES_DRIVER) && !connectionConfig.contains("stringtype")) {
        connectionConfig ++ Map("stringtype" -> "unspecified")
      } else connectionConfig
      case DELTA => connectionConfig ++ DELTA_LAKE_SPARK_CONF
      case ICEBERG => connectionConfig ++ ICEBERG_SPARK_CONF
      case _ => connectionConfig
    }
  }

  /**
   * Performs final consolidation for any pending multi-batch operations.
   * This should be called after all batches have been processed.
   */
  def finalizePendingConsolidations(): Unit = {
    batchSinkWriter.finalizePendingConsolidations()
  }
}
