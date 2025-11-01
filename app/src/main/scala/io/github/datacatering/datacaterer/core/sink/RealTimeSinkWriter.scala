package io.github.datacatering.datacaterer.core.sink

import com.google.common.util.concurrent.RateLimiter
import io.github.datacatering.datacaterer.api.model.Constants.{RATE, ROWS_PER_SECOND}
import io.github.datacatering.datacaterer.core.model.Constants.DEFAULT_ROWS_PER_SECOND
import io.github.datacatering.datacaterer.api.model.{FoldersConfig, SinkResult, Step}
import io.github.datacatering.datacaterer.core.model.Constants.PER_FIELD_INDEX_FIELD
import io.github.datacatering.datacaterer.core.model.RealTimeSinkResult
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanValidationIdentifier
import org.apache.log4j.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SaveMode, SparkSession}

import java.time.LocalDateTime
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
 * Handles real-time/streaming data writes with rate limiting.
 * Supports multiple approaches:
 * - Pekko streaming with throttling (recommended)
 * - Guava rate limiting (deprecated)
 * - Spark streaming (deprecated, unstable)
 */
class RealTimeSinkWriter(foldersConfig: FoldersConfig)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Saves data in real-time mode with rate limiting.
   *
   * @param dataSourceName Name of the data source
   * @param df DataFrame to save
   * @param format Data format
   * @param connectionConfig Connection configuration
   * @param step Step configuration
   * @param count Record count for logging
   * @param startTime Start time for metrics
   * @return SinkResult with success status
   */
  def saveRealTimeData(
    dataSourceName: String,
    df: DataFrame,
    format: String,
    connectionConfig: Map[String, String],
    step: Step,
    count: String,
    startTime: LocalDateTime
  ): SinkResult = {
    val rowsPerSecond = step.options.getOrElse(ROWS_PER_SECOND, connectionConfig.getOrElse(ROWS_PER_SECOND, DEFAULT_ROWS_PER_SECOND))
    LOGGER.info(s"Rows per second for generating data, rows-per-second=$rowsPerSecond")
    saveRealTimePekko(dataSourceName, df, format, connectionConfig, step, rowsPerSecond, count, startTime)
  }

  /**
   * Saves data using Pekko streaming with throttling (recommended approach).
   * Provides stable rate limiting and better backpressure handling.
   */
  private def saveRealTimePekko(
    dataSourceName: String,
    df: DataFrame,
    format: String,
    connectionConfig: Map[String, String],
    step: Step,
    rowsPerSecond: String,
    count: String,
    startTime: LocalDateTime
  ): SinkResult = {
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
      SinkResult(dataSourceName, format, SaveMode.Append.name(), exception = someExp, isSuccess = isSuccess)
    })
    Await.result(res, 100.seconds)
  }

  /**
   * Checks for exceptions in real-time sink results and determines overall success.
   *
   * @return CheckExceptionAndSuccess with exception dataset and success flag
   */
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

  /**
   * Saves real-time responses for validation purposes.
   * Parses JSON responses and stores them for later validation.
   */
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
   * Saves data using Guava rate limiter (deprecated).
   * Does not maintain stable rate due to partition-level rate limiting.
   *
   * @deprecated Use saveRealTimePekko instead for better rate stability
   */
  @deprecated("Does not keep stable rate")
  def saveRealTimeGuava(
    dataSourceName: String,
    df: DataFrame,
    format: String,
    connectionConfig: Map[String, String],
    step: Step,
    rowsPerSecond: String,
    count: String,
    startTime: LocalDateTime
  ): SinkResult = {
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
    SinkResult(dataSourceName, format, SaveMode.Append.name(), exception = someExp, isSuccess = isSuccess)
  }

  /**
   * Saves data using Spark streaming (deprecated).
   * Unstable for JMS connections and other streaming sinks.
   *
   * @deprecated Unstable for JMS connections
   */
  @deprecated("Unstable for JMS connections")
  def saveRealTimeSpark(
    df: DataFrame,
    format: String,
    connectionConfig: Map[String, String],
    step: Step,
    rowsPerSecond: String
  ): Unit = {
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

  /**
   * Calculates timeout for streaming operations based on row count and rate.
   */
  private def getTimeout(totalRows: Int, rowsPerSecond: Int): Long = totalRows / rowsPerSecond * 1000

  /**
   * Case class for exception checking results
   */
  case class CheckExceptionAndSuccess(optException: Dataset[Option[Throwable]], isSuccess: Boolean)
}
