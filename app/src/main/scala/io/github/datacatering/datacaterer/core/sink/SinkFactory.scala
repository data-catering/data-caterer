package io.github.datacatering.datacaterer.core.sink

import com.google.common.util.concurrent.RateLimiter
import io.github.datacatering.datacaterer.api.model.Constants.{DELTA, DELTA_LAKE_SPARK_CONF, DRIVER, FORMAT, ICEBERG, ICEBERG_SPARK_CONF, JDBC, OMIT, PARTITIONS, PARTITION_BY, POSTGRES_DRIVER, RATE, ROWS_PER_SECOND, SAVE_MODE, TABLE}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, MetadataConfig, SinkResult, Step}
import io.github.datacatering.datacaterer.api.util.ConfigUtil
import io.github.datacatering.datacaterer.core.exception.{FailedSaveDataDataFrameV2Exception, FailedSaveDataException}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.LogHolder
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
    val format = step.options(FORMAT)
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

    // if format is iceberg, need to use dataframev2 api for partition and writing
    connectionConfig.filter(_._1.startsWith("spark.sql"))
      .foreach(conf => df.sqlContext.setConf(conf._1, conf._2))
    val trySaveData = if (format == ICEBERG) {
      Try(tryPartitionAndSaveDfV2(df, saveMode, connectionConfig))
    } else {
      val partitionedDf = partitionDf(df, connectionConfig)
      Try(partitionedDf
        .format(format)
        .mode(saveMode)
        .options(connectionConfig)
        .save())
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

    if (flagsConfig.enableSinkMetadata) {
      val sample = df.take(metadataConfig.numGeneratedSamples).map(_.json)
      val fields = getFieldMetadata(dataSourceName, df, connectionConfig, metadataConfig)
      sinkResult.copy(generatedMetadata = fields, sample = sample)
    } else {
      sinkResult
    }
  }

  private def removeOmitFields(df: DataFrame): DataFrame = {
    val dfOmitFields = df.schema.fields
      .filter(field => field.metadata.contains(OMIT) && field.metadata.getString(OMIT).equalsIgnoreCase("true"))
      .map(_.name)
    val dfWithoutOmitFields = df.selectExpr(df.columns.filter(c => !dfOmitFields.contains(c)): _*)
    if (!dfWithoutOmitFields.storageLevel.useMemory) dfWithoutOmitFields.cache()
    dfWithoutOmitFields
  }

  private def saveRealTimeResponses(step: Step, saveResult: Dataset[Try[RealTimeSinkResult]]): Unit = {
    import sparkSession.implicits._
    val resultJson = saveResult.map {
      case Success(value) => value.result
      case Failure(exception) => s"""{"exception": "${exception.getMessage}"}"""
    }
    val jsonSchema = sparkSession.read.json(resultJson).schema
    val topLevelFieldNames = jsonSchema.fields.map(f => s"result.${f.name}")
    if (jsonSchema.nonEmpty) {
      val parsedResult = resultJson.selectExpr(s"FROM_JSON(value, '${jsonSchema.toDDL}') AS result")
        .selectExpr(topLevelFieldNames: _*)
      val cleanStepName = cleanValidationIdentifier(step.name)
      parsedResult.write
        .mode(SaveMode.Overwrite)
        .json(s"${foldersConfig.recordTrackingForValidationFolderPath}/$cleanStepName")
    } else {
      LOGGER.warn("Unable to save real-time responses with empty schema")
    }
  }
}
