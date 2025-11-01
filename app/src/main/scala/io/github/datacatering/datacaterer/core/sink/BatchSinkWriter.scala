package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{FORMAT, ICEBERG, JSON, PARTITIONS, PARTITION_BY, PATH, TABLE, UNWRAP_TOP_LEVEL_ARRAY}
import io.github.datacatering.datacaterer.api.model.{SinkResult, Step}
import io.github.datacatering.datacaterer.core.exception.FailedSaveDataDataFrameV2Exception
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{CreateTableWriter, DataFrame, DataFrameWriter, DataFrameWriterV2, Row, SaveMode, SparkSession}

import java.nio.file.Paths
import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

/**
 * Handles batch data writing to various data sinks with support for:
 * - Multiple formats (JSON, CSV, Parquet, Iceberg, etc.)
 * - File consolidation for single-file outputs
 * - Multi-batch scenarios with deferred consolidation
 * - Partitioning and special format handling
 */
class BatchSinkWriter(
  fileConsolidator: FileConsolidator,
  transformationApplicator: TransformationApplicator
)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Track pending consolidations for multi-batch scenarios
  private val pendingConsolidations = scala.collection.mutable.Map[String, (String, String, String, Map[String, String])]()

  /**
   * Saves a DataFrame in batch mode to the configured sink.
   *
   * @param dataSourceName Name of the data source
   * @param df DataFrame to save
   * @param saveMode Spark SaveMode (Append, Overwrite, etc.)
   * @param connectionConfig Connection configuration including format, path, etc.
   * @param count Record count for logging
   * @param startTime Start time for metrics
   * @param step Step configuration
   * @param isMultiBatch Whether this is part of a multi-batch operation
   * @param isLastBatch Whether this is the last batch
   * @param retry Current retry attempt number
   * @return SinkResult with success status and metrics
   */
  def saveBatchData(
    dataSourceName: String,
    df: DataFrame,
    saveMode: SaveMode,
    connectionConfig: Map[String, String],
    count: String,
    startTime: LocalDateTime,
    step: Step,
    isMultiBatch: Boolean = false,
    isLastBatch: Boolean = true,
    retry: Int = 0
  ): SinkResult = {
    val format = connectionConfig(FORMAT)

    // Check if we need to consolidate files (path has file suffix)
    val pathOpt = connectionConfig.get(PATH)
    val shouldConsolidate = pathOpt.flatMap(fileConsolidator.detectFileSuffix).isDefined

    // Handle consolidation for multi-batch scenarios
    val (actualConnectionConfig, targetFilePath, shouldDeferConsolidation) = if (shouldConsolidate && pathOpt.isDefined) {
      val originalPath = pathOpt.get
      val dataSourceStepKey = s"${dataSourceName}_${originalPath}"

      if (isMultiBatch && !isLastBatch) {
        // For multi-batch scenarios, use a persistent temp directory and defer consolidation
        val tempPath = pendingConsolidations.get(dataSourceStepKey) match {
          case Some((existingTempPath, _, _, _)) =>
            LOGGER.debug(s"Using existing temp directory for multi-batch: $existingTempPath")
            existingTempPath
          case None =>
            val newTempPath = s"${originalPath}_multibatch_temp_${System.currentTimeMillis()}"
            pendingConsolidations.put(dataSourceStepKey, (newTempPath, originalPath, format, connectionConfig))
            LOGGER.info(s"Created temp directory for multi-batch scenario: $newTempPath, original: $originalPath")
            newTempPath
        }
        (connectionConfig ++ Map(PATH -> tempPath), Some(originalPath), true)
      } else if (isMultiBatch && isLastBatch) {
        // Last batch in multi-batch scenario - use existing temp directory and consolidate after
        pendingConsolidations.get(dataSourceStepKey) match {
          case Some((tempPath, _, _, _)) =>
            LOGGER.info(s"Last batch in multi-batch scenario, will consolidate from: $tempPath to: $originalPath")
            (connectionConfig ++ Map(PATH -> tempPath), Some(originalPath), false)
          case None =>
            // Fallback if no temp directory was created (shouldn't happen)
            val tempPath = s"${originalPath}_spark_temp_${System.currentTimeMillis()}"
            LOGGER.warn(s"No temp directory found for last batch, creating new one: $tempPath")
            (connectionConfig ++ Map(PATH -> tempPath), Some(originalPath), false)
        }
      } else {
        // Single batch scenario - consolidate immediately
        val tempPath = s"${originalPath}_spark_temp_${System.currentTimeMillis()}"
        LOGGER.info(s"Detected file suffix in path: $originalPath. Will consolidate part files after Spark write.")
        (connectionConfig ++ Map(PATH -> tempPath), Some(originalPath), false)
      }
    } else {
      (connectionConfig, None, false)
    }

    // if format is iceberg, need to use dataframev2 api for partition and writing
    actualConnectionConfig.filter(_._1.startsWith("spark.sql"))
      .foreach(conf => df.sqlContext.setConf(conf._1, conf._2))
    val trySaveData = if (format == ICEBERG) {
      Try(tryPartitionAndSaveDfV2(df, saveMode, actualConnectionConfig))
    } else if (format == JSON) {
      // Special-case: allow unwrapping top-level array to emit a bare JSON array file
      val tryMaybeUnwrap = Try(trySaveJsonPossiblyUnwrapped(df, saveMode, actualConnectionConfig))
      tryMaybeUnwrap
    } else {
      // If consolidation is needed, use coalesce(1) to generate a single part file
      val dfToWrite = if (shouldConsolidate) {
        LOGGER.debug(s"Using coalesce(1) for single file output")
        df.coalesce(1)
      } else {
        df
      }
      val partitionedDf = partitionDf(dfToWrite, actualConnectionConfig)
      Try(partitionedDf
        .format(format)
        .mode(saveMode)
        .options(actualConnectionConfig)
        .save())
    }

    // Handle consolidation based on multi-batch scenario
    val tryConsolidation = if (trySaveData.isSuccess && shouldConsolidate && targetFilePath.isDefined) {
      if (shouldDeferConsolidation) {
        // Defer consolidation for multi-batch scenarios (not last batch)
        LOGGER.debug(s"Deferring consolidation for multi-batch scenario, data written to temp directory")
        Success(())
      } else {
        // Perform consolidation (single batch or last batch of multi-batch)
        val tempPath = actualConnectionConfig(PATH)
        val originalPath = targetFilePath.get
        val dataSourceStepKey = s"${dataSourceName}_${originalPath}"

        // If this is the last batch, remove from pending consolidations
        if (isMultiBatch && isLastBatch) {
          pendingConsolidations.remove(dataSourceStepKey)
        }

        val consolidationResult = Try(fileConsolidator.consolidatePartFiles(tempPath, originalPath, format, connectionConfig))
        consolidationResult match {
          case Failure(exception) =>
            LOGGER.error(s"Failed to consolidate part files from $tempPath to $originalPath", exception)
            // Clean up temp directory even if consolidation failed
            Try(fileConsolidator.cleanupDirectory(Paths.get(tempPath)))
          case Success(_) =>
            LOGGER.info(s"Successfully consolidated files to $originalPath")
            // Clean up temp directory after successful consolidation
            Try(fileConsolidator.cleanupDirectory(Paths.get(tempPath))) match {
              case Failure(cleanupException) =>
                LOGGER.warn(s"Failed to clean up temporary directory: $tempPath", cleanupException)
              case Success(_) =>
                LOGGER.debug(s"Cleaned up temporary directory: $tempPath")
            }
        }
        consolidationResult
      }
    } else {
      Success(())
    }

    val optException = (trySaveData, tryConsolidation) match {
      case (Failure(exception), _) => Some(exception)
      case (_, Failure(exception)) => Some(exception)
      case _ => None
    }

    // Apply transformation if configured and successful so far
    val (finalFilePath, transformException) = if (trySaveData.isSuccess && tryConsolidation.isSuccess) {
      transformationApplicator.applyTransformation(step, connectionConfig, format, targetFilePath)
    } else {
      (targetFilePath, None)
    }

    val finalException = optException.orElse(transformException)

    if (trySaveData.isFailure && retry < 3) {
      LOGGER.info(s"Retrying save to data source, data-source-name=$dataSourceName, retry=$retry")
      val connectionConfigWithBatchSize = connectionConfig ++ Map("batchsize" -> "1")
      saveBatchData(dataSourceName, df, saveMode, connectionConfigWithBatchSize, count, startTime, step, isMultiBatch, isLastBatch, retry + 1)
    } else {
      val isSuccess = trySaveData.isSuccess && tryConsolidation.isSuccess && transformException.isEmpty
      SinkResult(dataSourceName, format, saveMode.name(), exception = finalException, isSuccess = isSuccess)
    }
  }

  /**
   * Saves JSON data with optional top-level array unwrapping.
   * If the DataFrame has a single array field with unwrapTopLevelArray metadata,
   * it will be written as a bare JSON array instead of individual JSON objects.
   */
  private def trySaveJsonPossiblyUnwrapped(df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String]): Unit = {
    val shouldUnwrap = detectTopLevelArrayToUnwrap(df)
    val pathOpt = connectionConfig.get(PATH)
    val shouldConsolidate = pathOpt.flatMap(fileConsolidator.detectFileSuffix).isDefined

    shouldUnwrap match {
      case Some(arrayFieldName) =>
        // Write a single file containing the JSON array string using Spark text writer
        // We keep directory semantics consistent with other sinks
        val path = connectionConfig.getOrElse(PATH, throw new IllegalArgumentException("Missing path for JSON sink"))
        val jsonArrayDf = df.selectExpr(s"TO_JSON(`" + arrayFieldName + "`) AS value").coalesce(1)
        jsonArrayDf.write.mode(saveMode).text(path)
      case None =>
        // Default JSON behavior
        val dfToWrite = if (shouldConsolidate) {
          LOGGER.debug(s"Using coalesce(1) for single file JSON output")
          df.coalesce(1)
        } else {
          df
        }
        val partitionedDf = partitionDf(dfToWrite, connectionConfig)
        partitionedDf
          .format(JSON)
          .mode(saveMode)
          .options(connectionConfig)
          .save()
    }
  }

  /**
   * Detects if a DataFrame should have its top-level array unwrapped.
   * Returns the field name if:
   * - DataFrame has exactly one field
   * - That field is an array type
   * - The field has unwrapTopLevelArray metadata set to true
   */
  private def detectTopLevelArrayToUnwrap(df: DataFrame): Option[String] = {
    val fields = df.schema.fields
    if (fields.length == 1) {
      val f = fields.head
      val hasFlag = f.metadata.contains(UNWRAP_TOP_LEVEL_ARRAY) && f.metadata.getString(UNWRAP_TOP_LEVEL_ARRAY).equalsIgnoreCase("true")
      val isArray = f.dataType.typeName == "array"
      if (hasFlag && isArray) {
        LOGGER.debug(s"Field ${f.name} is an array and unwrapTopLevelArray is true")
        Some(f.name)
      } else {
        LOGGER.debug(s"Field ${f.name} is not an array or unwrapTopLevelArray is not true")
        None
      }
    } else {
      LOGGER.debug(s"Multiple fields found for JSON, not unwrapping top level array if set")
      None
    }
  }

  /**
   * Applies partitioning configuration to a DataFrame writer.
   * Supports both repartition (number of partitions) and partitionBy (partition columns).
   */
  private def partitionDf(df: DataFrame, stepOptions: Map[String, String]): DataFrameWriter[Row] = {
    val partitionDf = stepOptions.get(PARTITIONS)
      .map(partitionNum => df.repartition(partitionNum.toInt)).getOrElse(df)
    stepOptions.get(PARTITION_BY)
      .map(partitionCols => partitionDf.write.partitionBy(partitionCols.split(",").map(_.trim): _*))
      .getOrElse(partitionDf.write)
  }

  /**
   * Saves data using DataFrame V2 API (required for Iceberg and other catalog-based formats).
   * Handles partitioning and different save modes.
   */
  private def tryPartitionAndSaveDfV2(df: DataFrame, saveMode: SaveMode, stepOptions: Map[String, String]): Unit = {
    val tableName = s"$ICEBERG.${stepOptions(TABLE)}"
    val repartitionDf = stepOptions.get(PARTITIONS)
      .map(partitionNum => df.repartition(partitionNum.toInt)).getOrElse(df)
    val baseTable = repartitionDf.writeTo(tableName).options(stepOptions)

    stepOptions.get(PARTITION_BY)
      .map(partitionCols => {
        val spt = partitionCols.split(",").map(c => col(c.trim))

        val partitionedDf = if (spt.length > 1) {
          baseTable.partitionedBy(spt.head, spt.tail: _*)
        } else if (spt.length == 1) {
          baseTable.partitionedBy(spt.head)
        } else {
          baseTable
        }

        saveDataframeV2(saveMode, tableName, baseTable, partitionedDf)
      })
      .getOrElse(saveDataframeV2(saveMode, tableName, baseTable, baseTable))
  }

  /**
   * Executes the actual save operation using DataFrame V2 API.
   * Handles different save modes with appropriate table creation/append logic.
   */
  private def saveDataframeV2(saveMode: SaveMode, tableName: String, baseDf: DataFrameWriterV2[Row], partitionedDf: CreateTableWriter[Row]): Unit = {
    saveMode match {
      case SaveMode.Append | SaveMode.Ignore =>
        val tryCreate = Try(partitionedDf.create())
        tryCreate match {
          case Failure(exception) =>
            if (exception.isInstanceOf[TableAlreadyExistsException]) {
              LOGGER.debug(s"Table already exists, appending to existing table, table-name=$tableName")
              baseDf.append()
            } else {
              throw FailedSaveDataDataFrameV2Exception(tableName, saveMode.name(), exception)
            }
          case Success(_) =>
            LOGGER.debug(s"Successfully created partitioned table, table-name=$tableName")
        }
      case SaveMode.Overwrite => baseDf.overwritePartitions()
      case SaveMode.ErrorIfExists => partitionedDf.create()
    }
  }

  /**
   * Performs final consolidation for any pending multi-batch operations.
   * This should be called after all batches have been processed.
   */
  def finalizePendingConsolidations(): Unit = {
    if (pendingConsolidations.nonEmpty) {
      LOGGER.info(s"Finalizing ${pendingConsolidations.size} pending consolidations")

      pendingConsolidations.foreach { case (dataSourceStepKey, (tempPath, originalPath, format, connectionConfig)) =>
        LOGGER.info(s"Performing final consolidation for $dataSourceStepKey: $tempPath -> $originalPath")

        Try(fileConsolidator.consolidatePartFiles(tempPath, originalPath, format, connectionConfig)) match {
          case Success(_) =>
            LOGGER.info(s"Successfully consolidated final files to $originalPath")
            // Clean up temp directory after successful consolidation
            Try(fileConsolidator.cleanupDirectory(Paths.get(tempPath))) match {
              case Failure(cleanupException) =>
                LOGGER.warn(s"Failed to clean up temporary directory: $tempPath", cleanupException)
              case Success(_) =>
                LOGGER.debug(s"Cleaned up temporary directory: $tempPath")
            }
          case Failure(exception) =>
            LOGGER.error(s"Failed to perform final consolidation from $tempPath to $originalPath", exception)
            // Clean up temp directory even if consolidation failed
            Try(fileConsolidator.cleanupDirectory(Paths.get(tempPath)))
        }
      }

      pendingConsolidations.clear()
    }
  }
}
