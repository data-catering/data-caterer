package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP, JMS}
import io.github.datacatering.datacaterer.api.model.{FoldersConfig, SinkResult, Step}
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanValidationIdentifier
import org.apache.log4j.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Source, Sink => PekkoSink}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDateTime
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
 * Pekko-based streaming sink writer with rate control for real-time sinks.
 * 
 * SinkRouter ensures only real-time formats (HTTP, JMS, Kafka, databases) reach this writer.
 * Streams rows with Pekko throttle for precise rate control.
 */
class PekkoStreamingSinkWriter(foldersConfig: FoldersConfig)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Saves data with Pekko throttling for rate control to real-time sinks.
   * 
   * Note: SinkRouter ensures only real-time formats are routed here.
   * 
   * @param dataSourceName Name of the data source
   * @param df DataFrame containing pre-generated data
   * @param format Data format/sink type (HTTP, JMS, Kafka, etc.)
   * @param connectionConfig Connection configuration
   * @param step Step configuration
   * @param rate Records per second
   * @param startTime Start time for metrics
   * @return SinkResult with success status
   */
  def saveWithRateControl(
    dataSourceName: String,
    df: DataFrame,
    format: String,
    connectionConfig: Map[String, String],
    step: Step,
    rate: Int,
    startTime: LocalDateTime
  ): SinkResult = {
    implicit val as: ActorSystem = ActorSystem()
    implicit val materializer: Materializer = Materializer(as)
    implicit val ec: scala.concurrent.ExecutionContext = as.dispatcher
    val executionStartTime = System.currentTimeMillis()

    try {
      LOGGER.info(s"Starting Pekko streaming for real-time sink, data-source=$dataSourceName, format=$format, rate=$rate/sec")

      val permitsPerSecond = Math.max(rate, 1)
      val dataToPush = df.collect().toList
      val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)

      // Collect responses for validation
      val responses = new mutable.ListBuffer[Try[String]]()

      val sourceResult = format match {
        case HTTP =>
          val httpProcessor = sinkProcessor.asInstanceOf[http.HttpSinkProcessor]
          Source(dataToPush)
            .throttle(permitsPerSecond, 1.second)
            .mapAsync(parallelism = Math.min(permitsPerSecond, 100)) { row =>
              httpProcessor.pushRowToSinkAsync(row).transform(
                success => { responses += Success(success); success },
                failure => { responses += Failure(failure); throw failure }
              )
            }
            .runWith(PekkoSink.ignore)

        case JMS =>
          val jmsProcessor = sinkProcessor.asInstanceOf[jms.JmsSinkProcessor]
          Source(dataToPush)
            .throttle(permitsPerSecond, 1.second)
            .mapAsync(parallelism = Math.min(permitsPerSecond, 100)) { row =>
              jmsProcessor.pushRowToSinkAsync(row).transform(
                success => { responses += Success("{}"); success },
                failure => { responses += Failure(failure); throw failure }
              )
            }
            .runWith(PekkoSink.ignore)

        case _ =>
          Source(dataToPush)
            .throttle(permitsPerSecond, 1.second)
            .runForeach(row => {
              Try(sinkProcessor.pushRowToSink(row)) match {
                case s @ Success(_) => responses += Success("{}")
                case f @ Failure(ex) => responses += f.asInstanceOf[Failure[String]]
              }
            })
      }

      // Calculate dynamic timeout based on record count and rate
      // Add 50% buffer for safety, minimum 10 seconds
      val estimatedDurationSeconds = if (permitsPerSecond > 0) {
        Math.max((dataToPush.size.toDouble / permitsPerSecond) * 1.5, 10.0)
      } else {
        10.0
      }
      val timeoutDuration = Math.min(estimatedDurationSeconds.toInt, 300).seconds // Cap at 5 minutes
      LOGGER.debug(s"Calculated timeout for streaming: ${timeoutDuration.toSeconds}s (records=${dataToPush.size}, rate=$permitsPerSecond/sec)")

      Await.result(sourceResult, timeoutDuration)
      val elapsed = (System.currentTimeMillis() - executionStartTime) / 1000.0
      val actualRate = if (elapsed > 0) dataToPush.size / elapsed else 0.0

      LOGGER.debug("Stream completed, closing sink processor to wait for in-flight requests")
      sinkProcessor.close

      // Save responses for validation
      saveRealTimeResponses(step, responses.toList)

      // Check for exceptions in responses
      val failures = responses.collect { case Failure(ex) => ex }
      val hasFailures = failures.nonEmpty

      if (hasFailures) {
        val failureCount = failures.size
        val firstException = failures.head
        LOGGER.error(s"Exceptions occurred when pushing to sink, data-source-name=$dataSourceName, " +
          s"format=$format, step-name=${step.name}, exception-count=$failureCount, record-count=${dataToPush.size}")

        SinkResult(
          name = dataSourceName,
          format = format,
          saveMode = SaveMode.Append.name(),
          count = dataToPush.size,
          exception = Some(firstException),
          isSuccess = false
        )
      } else {
        LOGGER.info(s"Pekko streaming completed, data-source=$dataSourceName, " +
          s"records-written=${dataToPush.size}, elapsed=${elapsed}s, actual-rate=${actualRate.round}/sec, target-rate=$rate/sec")

        SinkResult(
          name = dataSourceName,
          format = format,
          saveMode = SaveMode.Append.name(),
          count = dataToPush.size,
          isSuccess = true
        )
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed Pekko streaming, data-source=$dataSourceName, error=${ex.getMessage}", ex)
        SinkResult(
          name = dataSourceName,
          format = format,
          saveMode = SaveMode.Append.name(),
          exception = Some(ex),
          isSuccess = false
        )
    } finally {
      as.terminate()
    }
  }

  /**
   * Saves real-time responses for validation purposes.
   * Parses JSON responses and stores them for later validation.
   */
  private def saveRealTimeResponses(step: Step, responses: List[Try[String]]): Unit = {
    import sparkSession.implicits._
    LOGGER.debug(s"Attempting to save real time responses for validation, step-name=${step.name}")

    val resultJson = responses.map {
      case Success(value) => value
      case Failure(exception) => s"""{"exception": "${exception.getMessage}"}"""
    }

    val resultDataset = sparkSession.createDataset(resultJson)
    val jsonSchema = sparkSession.read.json(resultDataset).schema
    val topLevelFieldNames = jsonSchema.fields.map(f => s"result.${f.name}")

    if (jsonSchema.nonEmpty) {
      LOGGER.debug(s"Schema is non-empty, saving real-time responses for validation, step-name=${step.name}")
      val parsedResult = resultDataset.selectExpr(s"FROM_JSON(value, '${jsonSchema.toDDL}') AS result")
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
}
