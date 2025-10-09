package io.github.datacatering.datacaterer.core.sink

import com.google.common.util.concurrent.RateLimiter
import io.github.datacatering.datacaterer.api.model.Constants.{CSV, DELTA, DELTA_LAKE_SPARK_CONF, DRIVER, FORMAT, ICEBERG, ICEBERG_SPARK_CONF, JDBC, JSON, OMIT, PARTITIONS, PARTITION_BY, PATH, POSTGRES_DRIVER, RATE, ROWS_PER_SECOND, SAVE_MODE, TABLE, UNWRAP_TOP_LEVEL_ARRAY}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, MetadataConfig, SinkResult, Step}
import io.github.datacatering.datacaterer.api.util.ConfigUtil
import io.github.datacatering.datacaterer.core.exception.{FailedSaveDataDataFrameV2Exception, FailedSaveDataException}
import io.github.datacatering.datacaterer.core.model.Constants.{BATCH, DEFAULT_ROWS_PER_SECOND, FAILED, FINISHED, PER_FIELD_INDEX_FIELD, STARTED}
import io.github.datacatering.datacaterer.core.model.RealTimeSinkResult
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.determineSaveTiming
import io.github.datacatering.datacaterer.core.util.MetadataUtil.getFieldMetadata
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanValidationIdentifier
import org.apache.log4j.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{CreateTableWriter, DataFrame, DataFrameWriter, DataFrameWriterV2, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalDateTime
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class SinkFactory(
                   val flagsConfig: FlagsConfig,
                   val metadataConfig: MetadataConfig,
                   val foldersConfig: FoldersConfig
                 )(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private var HAS_LOGGED_COUNT_DISABLE_WARNING = false

  def pushToSink(df: DataFrame, dataSourceName: String, step: Step, startTime: LocalDateTime): SinkResult = {
    val dfWithoutOmitFields = removeOmitFields(df)
    val saveMode = step.options.get(SAVE_MODE).map(_.toLowerCase.capitalize).map(SaveMode.valueOf).getOrElse(SaveMode.Append)
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
    saveData(dfWithoutOmitFields, dataSourceName, step, enrichedConnectionConfig, saveMode, format, count, flagsConfig.enableFailOnError, startTime)
  }

  private def saveData(df: DataFrame, dataSourceName: String, step: Step, connectionConfig: Map[String, String],
                       saveMode: SaveMode, format: String, count: String, enableFailOnError: Boolean, startTime: LocalDateTime): SinkResult = {
    val saveTiming = determineSaveTiming(dataSourceName, format, step.name)
    val baseSinkResult = SinkResult(dataSourceName, format, saveMode.name())
    //TODO might have use case where empty data can be tested, is it okay just to check for empty schema?
    val sinkResult = if (df.schema.isEmpty) {
      LOGGER.debug(s"Generated data schema is empty, not saving to data source, data-source-name=$dataSourceName, format=$format")
      baseSinkResult
    } else if (saveTiming.equalsIgnoreCase(BATCH)) {
      saveBatchData(dataSourceName, df, saveMode, connectionConfig, count, startTime)
    } else {
      saveRealTimeData(dataSourceName, df, format, connectionConfig, step, count, startTime)
    }

    val finalSinkResult = (sinkResult.isSuccess, sinkResult.exception) match {
      case (false, Some(exception)) =>
        LOGGER.error(s"Failed to save data for sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
          s"num-records=$count, status=$FAILED, exception=${exception.getMessage.take(500)}")
        if (enableFailOnError) throw FailedSaveDataException(dataSourceName, step.name, saveMode.name(), count, exception) else baseSinkResult
      case (true, None) =>
        LOGGER.info(s"Successfully saved data to sink, data-source-name=$dataSourceName, step-name=${step.name}, save-mode=${saveMode.name()}, " +
          s"num-records=$count, status=$FINISHED")
        sinkResult
      case (isSuccess, optException) =>
        LOGGER.warn(s"Unexpected sink result scenario, is-success=$isSuccess, exception-exists=${optException.isDefined}")
        sinkResult
    }
    df.unpersist()
    finalSinkResult
  }

  private def saveBatchData(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                            count: String, startTime: LocalDateTime, retry: Int = 0): SinkResult = {
    val format = connectionConfig(FORMAT)

    // Check if we need to consolidate files (path has file suffix)
    val pathOpt = connectionConfig.get(PATH)
    val shouldConsolidate = pathOpt.flatMap(detectFileSuffix).isDefined

    // If consolidation is needed, use a temporary directory for Spark output
    val (actualConnectionConfig, targetFilePath) = if (shouldConsolidate && pathOpt.isDefined) {
      val originalPath = pathOpt.get
      val tempPath = s"${originalPath}_spark_temp_${System.currentTimeMillis()}"
      LOGGER.info(s"Detected file suffix in path: $originalPath. Will consolidate part files after Spark write.")
      (connectionConfig ++ Map(PATH -> tempPath), Some(originalPath))
    } else {
      (connectionConfig, None)
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

    // If save was successful and consolidation is needed, consolidate the files
    if (trySaveData.isSuccess && shouldConsolidate && targetFilePath.isDefined) {
      val tempPath = actualConnectionConfig(PATH)
      Try(consolidatePartFiles(tempPath, targetFilePath.get, format, connectionConfig)) match {
        case Failure(exception) =>
          LOGGER.error(s"Failed to consolidate part files from $tempPath to ${targetFilePath.get}", exception)
          // Clean up temp directory even if consolidation failed
          Try(cleanupDirectory(Paths.get(tempPath)))
        case Success(_) =>
          LOGGER.info(s"Successfully consolidated files to ${targetFilePath.get}")
      }
    }

    val optException = trySaveData match {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }
    if (trySaveData.isFailure && retry < 3) {
      LOGGER.info(s"Retrying save to data source, data-source-name=$dataSourceName, retry=$retry")
      val connectionConfigWithBatchSize = connectionConfig ++ Map("batchsize" -> "1")
      saveBatchData(dataSourceName, df, saveMode, connectionConfigWithBatchSize, count, startTime, retry + 1)
    }
    mapToSinkResult(dataSourceName, df, saveMode, connectionConfig, count, format, trySaveData.isSuccess, startTime, optException)
  }

  private def trySaveJsonPossiblyUnwrapped(df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String]): Unit = {
    val shouldUnwrap = detectTopLevelArrayToUnwrap(df)
    val pathOpt = connectionConfig.get(PATH)
    val shouldConsolidate = pathOpt.flatMap(detectFileSuffix).isDefined

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

  private def partitionDf(df: DataFrame, stepOptions: Map[String, String]): DataFrameWriter[Row] = {
    val partitionDf = stepOptions.get(PARTITIONS)
      .map(partitionNum => df.repartition(partitionNum.toInt)).getOrElse(df)
    stepOptions.get(PARTITION_BY)
      .map(partitionCols => partitionDf.write.partitionBy(partitionCols.split(",").map(_.trim): _*))
      .getOrElse(partitionDf.write)
  }

  private def saveRealTimeData(dataSourceName: String, df: DataFrame, format: String, connectionConfig: Map[String, String],
                               step: Step, count: String, startTime: LocalDateTime): SinkResult = {
    val rowsPerSecond = step.options.getOrElse(ROWS_PER_SECOND, connectionConfig.getOrElse(ROWS_PER_SECOND, DEFAULT_ROWS_PER_SECOND))
    LOGGER.info(s"Rows per second for generating data, rows-per-second=$rowsPerSecond")
    saveRealTimePekko(dataSourceName, df, format, connectionConfig, step, rowsPerSecond, count, startTime)
  }

  private def saveRealTimePekko(dataSourceName: String, df: DataFrame, format: String, connectionConfig: Map[String, String],
                                step: Step, rowsPerSecond: String, count: String, startTime: LocalDateTime): SinkResult = {
    implicit val tryEncoder: Encoder[Try[RealTimeSinkResult]] = Encoders.kryo[Try[RealTimeSinkResult]]
    val as = ActorSystem()
    implicit val materializer: Materializer = Materializer(as)

    val permitsPerSecond = Math.max(rowsPerSecond.toInt, 1)
    val dataToPush = df.collect().toList
    val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)
    val pushResults = new mutable.MutableList[Try[RealTimeSinkResult]]()
    val sourceResult = Source(dataToPush)
      .throttle(permitsPerSecond, 1.second)
      .runForeach(row => pushResults += Try(sinkProcessor.pushRowToSink(row)))

    sourceResult.onComplete {
      case Success(_) =>
        LOGGER.debug("Successfully ran real time stream")
        sinkProcessor.close
      case Failure(exception) => throw exception
    }
    val res = sourceResult.map(_ => {
      val CheckExceptionAndSuccess(optException, isSuccess) = checkExceptionAndSuccess(dataSourceName, format, step, count, sparkSession.createDataset(pushResults))
      val someExp = if (optException.count() > 0) optException.head else None
      mapToSinkResult(dataSourceName, df, SaveMode.Append, connectionConfig, count, format, isSuccess, startTime, someExp)
    })
    Await.result(res, 100.seconds)
  }

  case class CheckExceptionAndSuccess(optException: Dataset[Option[Throwable]], isSuccess: Boolean)

  private def checkExceptionAndSuccess(
                                        dataSourceName: String,
                                        format: String,
                                        step: Step,
                                        count: String,
                                        saveResult: Dataset[Try[RealTimeSinkResult]]
                                      ): CheckExceptionAndSuccess = {
    implicit val optionThrowableEncoder: Encoder[Option[Throwable]] = Encoders.kryo[Option[Throwable]]
    val optException = saveResult.map {
      case Failure(exception) => Some(exception)
      case Success(_) => None
    }.filter(_.isDefined).distinct
    val optExceptionCount = optException.count()

    val isSuccess = if (optExceptionCount > 1) {
      LOGGER.error(s"Multiple exceptions occurred when pushing to event sink, data-source-name=$dataSourceName, " +
        s"format=$format, step-name=${step.name}, exception-count=$optExceptionCount, record-count=$count")
      false
    } else if (optExceptionCount == 1) {
      false
    } else {
      true
    }

    saveRealTimeResponses(step, saveResult)
    CheckExceptionAndSuccess(optException, isSuccess)
  }

  @deprecated("Does not keep stable rate")
  private def saveRealTimeGuava(dataSourceName: String, df: DataFrame, format: String, connectionConfig: Map[String, String],
                                step: Step, rowsPerSecond: String, count: String, startTime: LocalDateTime): SinkResult = {
    implicit val tryEncoder: Encoder[Try[RealTimeSinkResult]] = Encoders.kryo[Try[RealTimeSinkResult]]
    val permitsPerSecond = Math.max(rowsPerSecond.toInt, 1)

    val pushResults = df.repartition(1).mapPartitions((partition: Iterator[Row]) => {
      val rateLimiter = RateLimiter.create(permitsPerSecond)
      val rows = partition.toList
      val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)

      val pushResult = rows.map(row => {
        rateLimiter.acquire()
        Try(sinkProcessor.pushRowToSink(row))
      })
      sinkProcessor.close
      pushResult.toIterator
    })
    pushResults.cache()
    val CheckExceptionAndSuccess(optException, isSuccess) = checkExceptionAndSuccess(dataSourceName, format, step, count, pushResults)
    val someExp = if (optException.count() > 0) optException.head else None
    mapToSinkResult(dataSourceName, df, SaveMode.Append, connectionConfig, count, format, isSuccess, startTime, someExp)
  }

  @deprecated("Unstable for JMS connections")
  private def saveRealTimeSpark(df: DataFrame, format: String, connectionConfig: Map[String, String], step: Step, rowsPerSecond: String): Unit = {
    val dfWithIndex = df.selectExpr("*", s"monotonically_increasing_id() AS $PER_FIELD_INDEX_FIELD")
    val rowCount = dfWithIndex.count().toInt
    val readStream = sparkSession.readStream
      .format(RATE).option(ROWS_PER_SECOND, rowsPerSecond)
      .load().limit(rowCount)

    val writeStream = readStream.writeStream
      .foreachBatch((batch: Dataset[Row], id: Long) => {
        LOGGER.info(s"batch num=$id, count=${batch.count()}")
        batch.join(dfWithIndex, batch("value") === dfWithIndex(PER_FIELD_INDEX_FIELD))
          .drop(PER_FIELD_INDEX_FIELD).repartition(3).rdd
          .foreachPartition(partition => {
            val part = partition.toList
            val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)
            part.foreach(sinkProcessor.pushRowToSink)
          })
      }).start()

    writeStream.awaitTermination(getTimeout(rowCount, rowsPerSecond.toInt))
  }

  private def getTimeout(totalRows: Int, rowsPerSecond: Int): Long = totalRows / rowsPerSecond * 1000

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

  private def mapToSinkResult(dataSourceName: String, df: DataFrame, saveMode: SaveMode, connectionConfig: Map[String, String],
                              count: String, format: String, isSuccess: Boolean, startTime: LocalDateTime,
                              optException: Option[Throwable]): SinkResult = {
    val cleansedOptions = ConfigUtil.cleanseOptions(connectionConfig)
    val sinkResult = SinkResult(dataSourceName, format, saveMode.name(), cleansedOptions, count.toLong, isSuccess, Array(), startTime, exception = optException)

    val result = if (flagsConfig.enableSinkMetadata) {
      val sample = df.take(metadataConfig.numGeneratedSamples).map(_.json)
      val fields = getFieldMetadata(dataSourceName, df, connectionConfig, metadataConfig)
      sinkResult.copy(generatedMetadata = fields, sample = sample)
    } else {
      sinkResult
    }
    result
  }

  private def removeOmitFields(df: DataFrame): DataFrame = {
    val dfOmitFields = df.schema.fields
      .filter(field => field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true"))
      .map(_.name)
    val columnsToSelect = df.columns.filter(c => !dfOmitFields.contains(c))
      .map(c => if (c.contains(".")) s"`$c`" else c)
    val dfWithoutOmitFields = df.selectExpr(columnsToSelect: _*)
    if (!dfWithoutOmitFields.storageLevel.useMemory) dfWithoutOmitFields.cache()
    dfWithoutOmitFields
  }

  private def saveRealTimeResponses(step: Step, saveResult: Dataset[Try[RealTimeSinkResult]]): Unit = {
    import sparkSession.implicits._
    LOGGER.debug(s"Attempting to save real time responses for validation, step-name=${step.name}")
    val resultJson = saveResult.map {
      case Success(value) => value.result
      case Failure(exception) => s"""{"exception": "${exception.getMessage}"}"""
    }
    val jsonSchema = sparkSession.read.json(resultJson).schema
    val topLevelFieldNames = jsonSchema.fields.map(f => s"result.${f.name}")
    if (jsonSchema.nonEmpty) {
      LOGGER.debug(s"Schema is non-empty, saving real-time responses for validation, step-name=${step.name}")
      val parsedResult = resultJson.selectExpr(s"FROM_JSON(value, '${jsonSchema.toDDL}') AS result")
        .selectExpr(topLevelFieldNames: _*)
      val cleanStepName = cleanValidationIdentifier(step.name)
      val filePath = s"${foldersConfig.recordTrackingForValidationFolderPath}/$cleanStepName"
      LOGGER.debug(s"Saving real-time responses for validation, step-name=$cleanStepName, file-path=$filePath")
      parsedResult.write
        .mode(SaveMode.Overwrite)
        .json(filePath)
    } else {
      LOGGER.warn("Unable to save real-time responses with empty schema")
    }
  }

  /**
   * Detects if a path contains a file suffix (e.g., .json, .csv, .parquet, etc.)
   * @param path The file path to check
   * @return Some(extension) if a file suffix is detected, None otherwise
   */
  private def detectFileSuffix(path: String): Option[String] = {
    val supportedExtensions = List(".json", ".csv", ".parquet", ".orc", ".xml", ".txt")
    supportedExtensions.find(ext => path.toLowerCase.endsWith(ext))
  }

  /**
   * Consolidates Spark-generated part files into a single file with the specified name.
   * This is useful when users specify a path with a file extension (e.g., output.json)
   * but Spark generates a directory with part files.
   *
   * @param sparkOutputPath The directory path where Spark wrote the part files
   * @param targetFilePath The desired single file path
   * @param format The file format (used for special handling like CSV headers)
   * @param connectionConfig Connection configuration that may contain format-specific options
   */
  private def consolidatePartFiles(sparkOutputPath: String, targetFilePath: String, format: String, connectionConfig: Map[String, String] = Map()): Unit = {
    val outputDir = Paths.get(sparkOutputPath)

    if (Files.exists(outputDir) && Files.isDirectory(outputDir)) {
      LOGGER.info(s"Consolidating part files from $sparkOutputPath to $targetFilePath")

      val partFiles = Files.list(outputDir)
        .filter(p => {
          val fileName = p.getFileName.toString
          fileName.startsWith("part-") && !fileName.endsWith(".crc") && fileName != "_SUCCESS"
        })
        .toArray()
        .map(_.asInstanceOf[java.nio.file.Path])
        .sortBy(_.getFileName.toString)

      if (partFiles.isEmpty) {
        LOGGER.warn(s"No part files found in $sparkOutputPath")
        return
      }

      val targetPath = Paths.get(targetFilePath)
      val targetDir = targetPath.getParent
      if (targetDir != null && !Files.exists(targetDir)) {
        Files.createDirectories(targetDir)
      }

      // If there's only one part file, just move/rename it
      if (partFiles.length == 1) {
        Files.move(partFiles.head, targetPath, StandardCopyOption.REPLACE_EXISTING)
        LOGGER.info(s"Moved single part file to $targetFilePath")
      } else {
        // Multiple part files need to be concatenated
        LOGGER.info(s"Concatenating ${partFiles.length} part files into $targetFilePath")

        // Check if we need special handling for CSV with headers
        val hasHeaders = format.equalsIgnoreCase(CSV) && connectionConfig.get("header").exists(_.equalsIgnoreCase("true"))

        if (hasHeaders) {
          consolidateCsvWithHeaders(partFiles, targetPath)
        } else {
          // Standard concatenation for other formats
          val targetFile = Files.newOutputStream(targetPath)
          try {
            partFiles.foreach { partFile =>
              Files.copy(partFile, targetFile)
            }
          } finally {
            targetFile.close()
          }
        }
      }

      // Clean up the Spark-generated directory
      cleanupDirectory(outputDir)
      LOGGER.info(s"Cleaned up temporary directory: $sparkOutputPath")
    } else {
      LOGGER.warn(s"Spark output directory does not exist or is not a directory: $sparkOutputPath")
    }
  }

  /**
   * Consolidates CSV part files while handling headers correctly.
   * The header from the first file is kept, and headers from subsequent files are skipped.
   *
   * @param partFiles Sorted array of part file paths
   * @param targetPath The target file path
   */
  private def consolidateCsvWithHeaders(partFiles: Array[java.nio.file.Path], targetPath: java.nio.file.Path): Unit = {
    LOGGER.debug(s"Consolidating CSV files with header handling")
    val targetFile = Files.newOutputStream(targetPath)
    try {
      partFiles.zipWithIndex.foreach { case (partFile, index) =>
        val lines = Files.readAllLines(partFile)
        if (index == 0) {
          // First file: write all lines including header
          lines.forEach(line => {
            targetFile.write(line.getBytes("UTF-8"))
            targetFile.write('\n')
          })
        } else {
          // Subsequent files: skip first line (header) and write the rest
          if (lines.size() > 1) {
            lines.subList(1, lines.size()).forEach(line => {
              targetFile.write(line.getBytes("UTF-8"))
              targetFile.write('\n')
            })
          }
        }
      }
    } finally {
      targetFile.close()
    }
  }

  /**
   * Recursively deletes a directory and all its contents
   * @param directory The directory path to delete
   */
  private def cleanupDirectory(directory: java.nio.file.Path): Unit = {
    if (Files.exists(directory)) {
      Files.walk(directory)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(Files.delete)
    }
  }
}
