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
 *
 * Note: This class accepts an optional shared ActorSystem to avoid the overhead of
 * creating/destroying actor systems per call. If no ActorSystem is provided, one will
 * be created and terminated for each call (backwards-compatible behavior).
 *
 * @param foldersConfig Configuration for folder paths
 * @param sharedActorSystem Optional shared ActorSystem for reuse across calls
 * @param sparkSession Implicit SparkSession
 */
class PekkoStreamingSinkWriter(
  foldersConfig: FoldersConfig,
  sharedActorSystem: Option[ActorSystem] = None
)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Maximum parallelism for async operations. Limits concurrent requests to prevent
   * overwhelming downstream services and manage memory usage from in-flight requests.
   */
  private val MAX_ASYNC_PARALLELISM = 100

  /**
   * Maximum timeout duration in seconds for streaming operations.
   * Caps the dynamic timeout to prevent indefinitely long waits.
   */
  private val MAX_STREAMING_TIMEOUT_SECONDS = 300

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
    // Use shared actor system if provided, otherwise create a new one
    val (actorSystem, ownsActorSystem) = sharedActorSystem match {
      case Some(as) => (as, false)
      case None => (ActorSystem("PekkoStreamingSinkWriter"), true)
    }

    implicit val as: ActorSystem = actorSystem
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
            .mapAsync(parallelism = Math.min(permitsPerSecond, MAX_ASYNC_PARALLELISM)) { row =>
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
            .mapAsync(parallelism = Math.min(permitsPerSecond, MAX_ASYNC_PARALLELISM)) { row =>
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
      val timeoutDuration = Math.min(estimatedDurationSeconds.toInt, MAX_STREAMING_TIMEOUT_SECONDS).seconds
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
      // Only terminate the actor system if we created it (not shared)
      if (ownsActorSystem) {
        as.terminate()
      }
    }
  }

  /**
   * Shutdown the shared actor system if one was provided.
   * Should be called when the writer is no longer needed.
   */
  def shutdown(): Unit = {
    sharedActorSystem.foreach { as =>
      LOGGER.info("Shutting down shared actor system")
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
