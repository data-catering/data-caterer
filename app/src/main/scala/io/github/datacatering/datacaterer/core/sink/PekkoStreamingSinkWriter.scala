package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP, JMS, KAFKA}
import io.github.datacatering.datacaterer.api.model.{FoldersConfig, PerformanceMetrics, SinkResult, Step, StreamingConfig, StreamingMetrics}
import io.github.datacatering.datacaterer.core.sink.memory.{BatchTimestampTracker, BoundedResponseBuffer}
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanValidationIdentifier
import org.apache.log4j.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Flow, Source, Sink => PekkoSink}
import org.apache.pekko.{NotUsed}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.TimeoutException
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
 * @param streamingConfig Configuration for streaming behavior (timeouts, buffer sizes, etc.)
 * @param sharedActorSystem Optional shared ActorSystem for reuse across calls
 * @param sparkSession Implicit SparkSession
 */
class PekkoStreamingSinkWriter(
  foldersConfig: FoldersConfig,
  streamingConfig: StreamingConfig = StreamingConfig(),
  sharedActorSystem: Option[ActorSystem] = None
)(implicit val sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Helper method to log rate limiting progress at regular intervals.
   * Logs progress every ~1 second worth of records or when complete.
   */
  private def logRateLimitingProgress(
                                       recordsSent: Long,
                                       totalRecords: Int,
                                       permitsPerSecond: Int,
                                       executionStartTime: Long,
                                       progressLogInterval: Long,
                                       prefix: String = ""
                                     ): Unit = {
    if (recordsSent % progressLogInterval == 0 || recordsSent == totalRecords) {
      val elapsedMs = System.currentTimeMillis() - executionStartTime
      val currentRate = if (elapsedMs > 0) ((recordsSent * 1000.0 / elapsedMs).round).toInt else 0
      val prefixStr = if (prefix.nonEmpty) s"$prefix " else ""
      LOGGER.debug(s"${prefixStr}rate limiting progress: sent=$recordsSent/$totalRecords, elapsed=${elapsedMs}ms, current-rate=$currentRate/sec, target-rate=$permitsPerSecond/sec")
    }
  }

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
   * @param rateFunction Optional dynamic rate function for pattern-based execution
   * @param totalDurationSeconds Optional total duration for pattern-based execution
   * @return Tuple of (SinkResult, Option[PerformanceMetrics])
   */
  def saveWithRateControl(
    dataSourceName: String,
    df: DataFrame,
    format: String,
    connectionConfig: Map[String, String],
    step: Step,
    rate: Int,
    startTime: LocalDateTime,
    rateFunction: Option[Double => Int] = None,
    totalDurationSeconds: Option[Double] = None
  ): (SinkResult, Option[PerformanceMetrics]) = {
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
      // Validate rate parameter
      if (rate <= 0) {
        LOGGER.warn(s"Invalid rate parameter: $rate. Using default rate of 1/sec")
      }

      val useDynamicRate = rateFunction.isDefined && totalDurationSeconds.isDefined
      val rateInfo = if (useDynamicRate) "dynamic (pattern-based)" else s"$rate/sec"
      LOGGER.info(s"Starting Pekko streaming for real-time sink, data-source=$dataSourceName, format=$format, rate=$rateInfo")
      LOGGER.debug(s"Rate control parameters: rateFunction.isDefined=${rateFunction.isDefined}, totalDurationSeconds=${totalDurationSeconds}, useDynamicRate=$useDynamicRate")

      val permitsPerSecond = Math.max(rate, 1)

      // Get record count first for validation and metrics
      val totalRecords = try {
        df.count()
      } catch {
        case ex: Exception =>
          LOGGER.error(s"Failed to count DataFrame records for data-source=$dataSourceName", ex)
          throw new IllegalStateException(s"Cannot stream data - failed to count records: ${ex.getMessage}", ex)
      }

      if (totalRecords == 0) {
        LOGGER.warn(s"DataFrame is empty for data-source=$dataSourceName. Returning empty result.")
        return (SinkResult(dataSourceName, format, SaveMode.Append.name(), count = 0, isSuccess = true), None)
      }

      // Create iterator after validating non-empty DataFrame
      val dataIterator = try {
        df.rdd.toLocalIterator
      } catch {
        case ex: Exception =>
          LOGGER.error(s"Failed to create toLocalIterator for data-source=$dataSourceName, records=$totalRecords", ex)
          throw new IllegalStateException(s"Cannot stream data - failed to create iterator: ${ex.getMessage}", ex)
      }

      val expectedDurationMs = (totalRecords.toDouble / permitsPerSecond) * 1000
      LOGGER.debug(s"Rate limiting config: permits=$permitsPerSecond/sec, records=$totalRecords, expected-duration=${expectedDurationMs.toLong}ms")
      LOGGER.info(s"Stream initialized successfully: data-source=$dataSourceName, total-records=$totalRecords, target-rate=$permitsPerSecond/sec")

      val sinkProcessor = SinkProcessor.getConnection(format, connectionConfig, step)

      // Track progress for detailed timing logs (thread-safe for mapAsync parallelism)
      val recordsSent = new AtomicLong(0)
      val progressLogInterval = Math.max(permitsPerSecond, 10) // Log every ~1 second worth of records

      // Collect responses for validation (bounded buffer to prevent OOM)
      val responseBuffer = new BoundedResponseBuffer(maxSize = streamingConfig.responseBufferSize)

      // Track stream start time for dynamic rate calculation
      val streamStartTime = System.currentTimeMillis()

      // Batch-aggregated timestamp tracking for memory efficiency
      // Groups timestamps into configurable windows instead of tracking every record
      val timestampTracker = new BatchTimestampTracker(windowMs = streamingConfig.timestampWindowMs)

      // Record per-record metrics with minimal logging overhead
      // Progress logging only at significant intervals (1% or every 10K records, whichever is larger)
      val progressLogThreshold = Math.max(totalRecords / 100, 10000)

      def recordPerRecordMetrics(recordIndex: Long): Unit = {
        timestampTracker.recordTimestamp()

        // Log progress at significant milestones only to reduce overhead
        if (recordIndex % progressLogThreshold == 0 || recordIndex == totalRecords) {
          val elapsedSec = (System.currentTimeMillis() - streamStartTime) / 1000.0
          val currentRate = if (elapsedSec > 0) recordIndex / elapsedSec else 0.0
          LOGGER.info(s"Streaming progress: record=$recordIndex/$totalRecords (${"%.1f".format(recordIndex.toDouble / totalRecords * 100)}%), elapsed=${"%.1f".format(elapsedSec)}s, rate=${"%.1f".format(currentRate)}/sec")
        }
      }

      // Create dynamic throttle flow if rate function is provided
      def createDynamicThrottle[T]: Flow[T, T, NotUsed] = {
        (rateFunction, totalDurationSeconds) match {
          case (Some(rateFn), Some(totalDuration)) =>
            LOGGER.info(s"Using DYNAMIC throttle with on-demand rate calculation (streaming mode)")

            // MEMORY OPTIMIZATION: Calculate emit times on-the-fly instead of pre-computing array
            // Memory usage: O(1) instead of O(totalRecords)
            // For 10M records: ~80 bytes vs ~80 MB
            Flow[T]
              .zipWithIndex
              .mapAsync(1) { elementWithIndex =>
                // Use mapAsync with parallelism=1 to maintain order while using Pekko's scheduler
                // This is more efficient than Thread.sleep in the stream
                import scala.concurrent.{Future, Promise}
                import scala.concurrent.duration._

                val (element, index) = elementWithIndex

                // Calculate target time using stateful approach (via shared mutable state)
                // Note: Safe because mapAsync(1) ensures sequential processing
                val promise = Promise[T]()

                // For dynamic rate, we need to calculate the delay
                // Since we can't maintain mutable state across mapAsync calls easily,
                // we'll use a different approach: approximate based on current position
                val approximateElapsedSeconds = index.toDouble / permitsPerSecond
                val currentRate = Math.max(1, rateFn(approximateElapsedSeconds))
                val targetTimeMs = (approximateElapsedSeconds * 1000).toLong

                val now = System.currentTimeMillis()
                val elapsedMs = now - streamStartTime
                val delayMs = Math.max(0, targetTimeMs - elapsedMs)

                if (delayMs > 0) {
                  // Use Pekko scheduler instead of Thread.sleep for better async behavior
                  as.scheduler.scheduleOnce(delayMs.milliseconds) {
                    promise.success(element)
                  }
                } else {
                  promise.success(element)
                }

                // Log samples for debugging at reduced frequency
                val logInterval = Math.max(totalRecords / 100, 10000)
                if (index < 5 || index == totalRecords - 1 || index % logInterval == 0) {
                  LOGGER.debug(s"Dynamic throttle[${index}]: targetTime=${targetTimeMs}ms, actualDelay=${delayMs}ms, rate@${"%.2f".format(approximateElapsedSeconds)}s=${currentRate}/sec")
                }

                promise.future
              }

          case _ =>
            LOGGER.info(s"Using STANDARD throttle at fixed rate: $permitsPerSecond/sec (rateFunction=${rateFunction.isDefined}, totalDuration=${totalDurationSeconds.isDefined})")
            // No dynamic rate function, use standard throttle
            Flow[T].throttle(permitsPerSecond, 1.second)
        }
      }

      val sourceResult = format match {
        case HTTP =>
          val httpProcessor = sinkProcessor.asInstanceOf[http.HttpSinkProcessor]
          Source.fromIterator(() => dataIterator)
            .via(createDynamicThrottle)
            .mapAsync(parallelism = Math.min(permitsPerSecond, streamingConfig.maxAsyncParallelism)) { row =>
              val currentCount = recordsSent.incrementAndGet()
              recordPerRecordMetrics(currentCount)
              httpProcessor.pushRowToSinkAsync(row).transform(
                success => { responseBuffer.add(Success(success)); success },
                failure => { responseBuffer.add(Failure(failure)); throw failure }
              )
            }
            .runWith(PekkoSink.ignore)

        case JMS =>
          val jmsProcessor = sinkProcessor.asInstanceOf[jms.JmsSinkProcessor]
          Source.fromIterator(() => dataIterator)
            .via(createDynamicThrottle)
            .mapAsync(parallelism = Math.min(permitsPerSecond, streamingConfig.maxAsyncParallelism)) { row =>
              val currentCount = recordsSent.incrementAndGet()
              recordPerRecordMetrics(currentCount)
              jmsProcessor.pushRowToSinkAsync(row).transform(
                success => { responseBuffer.add(Success("{}")); success },
                failure => { responseBuffer.add(Failure(failure)); throw failure }
              )
            }
            .runWith(PekkoSink.ignore)

        case KAFKA =>
          val kafkaProcessor = sinkProcessor.asInstanceOf[kafka.KafkaSinkProcessor]
          Source.fromIterator(() => dataIterator)
            .via(createDynamicThrottle)
            .mapAsync(parallelism = Math.min(permitsPerSecond, streamingConfig.maxAsyncParallelism)) { row =>
              val currentCount = recordsSent.incrementAndGet()
              logRateLimitingProgress(currentCount, totalRecords.toInt, permitsPerSecond, executionStartTime, progressLogInterval, "Kafka")
              recordPerRecordMetrics(currentCount)
              kafkaProcessor.pushRowToSinkAsync(row).transform(
                success => { responseBuffer.add(Success(success)); success },
                failure => { responseBuffer.add(Failure(failure)); throw failure }
              )
            }
            .runWith(PekkoSink.ignore)

        case _ =>
          Source.fromIterator(() => dataIterator)
            .via(createDynamicThrottle)
            .runForeach(row => {
              Try(sinkProcessor.pushRowToSink(row)) match {
                case s @ Success(_) => responseBuffer.add(Success("{}"))
                case f @ Failure(ex) => responseBuffer.add(f.asInstanceOf[Failure[String]])
              }
              val currentCount = recordsSent.incrementAndGet()
              logRateLimitingProgress(currentCount, totalRecords.toInt, permitsPerSecond, executionStartTime, progressLogInterval)
              recordPerRecordMetrics(currentCount)
            })
      }

      // Calculate dynamic timeout based on record count and rate
      // Add 50% buffer for safety, minimum 10 seconds
      val estimatedDurationSeconds = if (permitsPerSecond > 0) {
        Math.max((totalRecords.toDouble / permitsPerSecond) * 1.5, 10.0)
      } else {
        10.0
      }
      val timeoutDuration = Math.min(estimatedDurationSeconds.toInt, streamingConfig.maxTimeoutSeconds).seconds
      LOGGER.info(s"Stream timeout configured: ${timeoutDuration.toSeconds}s for $totalRecords records at $permitsPerSecond/sec (with 50% buffer)")

      try {
        Await.result(sourceResult, timeoutDuration)
      } catch {
        case ex: TimeoutException =>
          val elapsed = (System.currentTimeMillis() - executionStartTime) / 1000.0
          val recordsSentCount = recordsSent.get()
          LOGGER.error(s"Stream timed out after ${elapsed}s, data-source=$dataSourceName, " +
            s"records-sent=$recordsSentCount/$totalRecords (${"%.1f".format(recordsSentCount.toDouble / totalRecords * 100)}%), " +
            s"timeout=${timeoutDuration.toSeconds}s", ex)
          throw new IllegalStateException(s"Stream timeout: sent $recordsSentCount/$totalRecords records in ${elapsed}s", ex)
        case ex: Exception =>
          val elapsed = (System.currentTimeMillis() - executionStartTime) / 1000.0
          val recordsSentCount = recordsSent.get()
          LOGGER.error(s"Stream failed during execution after ${elapsed}s, data-source=$dataSourceName, " +
            s"records-sent=$recordsSentCount/$totalRecords, error=${ex.getMessage}", ex)
          throw ex
      }

      val elapsed = (System.currentTimeMillis() - executionStartTime) / 1000.0
      val actualRate = if (elapsed > 0) totalRecords / elapsed else 0.0

      LOGGER.debug("Stream completed, closing sink processor to wait for in-flight requests")
      try {
        sinkProcessor.close
      } catch {
        case ex: Exception =>
          LOGGER.warn(s"Exception while closing sink processor for data-source=$dataSourceName: ${ex.getMessage}", ex)
          // Continue - don't fail the entire operation if close fails
      }

      // Create StreamingMetrics from per-record timestamps
      val actualStreamStartTime = LocalDateTime.now().minusNanos((System.currentTimeMillis() - streamStartTime) * 1000000)
      val actualStreamEndTime = LocalDateTime.now()

      LOGGER.info(s"Collected streaming metrics: batches=${timestampTracker.getBatchCount}, records=${timestampTracker.getTotalRecords}")

      // Get buffer stats for failure detection and logging
      val stats = responseBuffer.getStats

      // Log eviction warning if responses were dropped
      if (stats.evictedCount > 0) {
        LOGGER.info(s"Response buffer evicted ${stats.evictedCount} responses (kept last ${stats.bufferedCount})")
      }

      // Save responses for validation
      try {
        saveRealTimeResponses(step, responseBuffer.getResponses)
      } catch {
        case ex: Exception =>
          LOGGER.warn(s"Failed to save real-time responses for validation, step-name=${step.name}: ${ex.getMessage}", ex)
          // Continue - don't fail the entire operation if saving responses fails
      }

      // Check for failures using stats
      val hasFailures = stats.failureCount > 0

      if (hasFailures) {
        val failureRate = ("%.2f".format(stats.failureCount.toDouble / stats.totalCount * 100))
        LOGGER.warn(s"Stream completed with failures: ${stats.failureCount} failures out of ${stats.totalCount} records ($failureRate%)")
      }

      // Generate final streaming metrics from batch aggregates
      val finalMetrics = if (timestampTracker.getTotalRecords > 0) {
        val executionType = if (rateFunction.isDefined) "pattern-based" else "constant-rate"
        val streamingMetrics = timestampTracker.finalizeAndGetMetrics(
          startTime = actualStreamStartTime,
          endTime = actualStreamEndTime,
          executionType = executionType
        )

        LOGGER.info(s"Created StreamingMetrics from ${timestampTracker.getBatchCount} batches: " +
          s"records=${streamingMetrics.totalRecords}, " +
          s"duration=${"%.2f".format(streamingMetrics.totalDurationSeconds)}s, " +
          s"avg-throughput=${"%.1f".format(streamingMetrics.averageThroughput)}/sec, " +
          s"peak-throughput=${"%.1f".format(streamingMetrics.peakThroughput)}/sec")

        Some(PerformanceMetrics.fromStreaming(streamingMetrics))
      } else {
        LOGGER.warn(s"No batches collected for streaming metrics")
        None // No batches collected
      }

      val sinkResult = if (hasFailures) {
        // Get first failure from buffered responses for exception reporting
        val firstException = responseBuffer.getResponses.collectFirst { case Failure(ex) => ex }
          .getOrElse(new RuntimeException("Unknown failure - response not in buffer"))

        LOGGER.error(s"Exceptions occurred when pushing to sink, data-source-name=$dataSourceName, " +
          s"format=$format, step-name=${step.name}, exception-count=${stats.failureCount}, record-count=$totalRecords")

        SinkResult(
          name = dataSourceName,
          format = format,
          saveMode = SaveMode.Append.name(),
          count = totalRecords.toInt,
          exception = Some(firstException),
          isSuccess = false
        )
      } else {
        LOGGER.info(s"Pekko streaming completed, data-source=$dataSourceName, " +
          s"records-written=$totalRecords, elapsed=${elapsed}s, actual-rate=${actualRate.round}/sec, target-rate=$rate/sec")

        SinkResult(
          name = dataSourceName,
          format = format,
          saveMode = SaveMode.Append.name(),
          count = totalRecords.toInt,
          isSuccess = true
        )
      }

      (sinkResult, finalMetrics)
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed Pekko streaming, data-source=$dataSourceName, error=${ex.getMessage}", ex)
        val sinkResult = SinkResult(
          name = dataSourceName,
          format = format,
          saveMode = SaveMode.Append.name(),
          exception = Some(ex),
          isSuccess = false
        )
        (sinkResult, None) // No metrics on failure
    } finally {
      // Only terminate the actor system if we created it (not shared)
      if (ownsActorSystem) {
        LOGGER.debug(s"Terminating non-shared actor system for data-source=$dataSourceName")
        try {
          as.terminate()
        } catch {
          case ex: Exception =>
            LOGGER.warn(s"Exception during actor system termination for data-source=$dataSourceName: ${ex.getMessage}", ex)
        }
      } else {
        LOGGER.debug(s"Using shared actor system - skipping termination for data-source=$dataSourceName")
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
      try {
        as.terminate()
        LOGGER.debug("Shared actor system terminated successfully")
      } catch {
        case ex: Exception =>
          LOGGER.error(s"Failed to terminate shared actor system: ${ex.getMessage}", ex)
          throw ex
      }
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
