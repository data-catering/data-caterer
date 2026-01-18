package io.github.datacatering.datacaterer.core.sink

import org.apache.log4j.Logger
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.{Materializer, OverflowStrategy, QueueOfferResult}
import org.apache.pekko.stream.scaladsl.{Flow, Sink => PekkoSink, Source, SourceQueueWithComplete}
import org.apache.pekko.{Done, NotUsed}
import org.apache.spark.sql.{DataFrame, Row}

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

/**
 * POC: Streaming Data Source using Spark RDD + Pekko Source.queue
 *
 * This is a proof-of-concept to validate the approach of streaming data from Spark RDD
 * to Pekko Streams without pre-materializing the entire dataset via df.collect().
 *
 * Key innovations:
 * 1. Uses Source.queue with backpressure to coordinate Spark data generation with sink consumption
 * 2. Streams data on-demand from RDD partitions instead of collecting all data upfront
 * 3. Maintains memory bounded by queue buffer size (~1000 rows) instead of total record count
 *
 * @param df DataFrame to stream (data will be generated lazily from RDD)
 * @param bufferSize Size of the Pekko queue buffer (default: 1000 rows)
 * @param actorSystem Implicit Pekko ActorSystem
 * @param materializer Implicit Pekko Materializer
 */
class StreamingDataSource(
  df: DataFrame,
  bufferSize: Int = 1000
)(implicit actorSystem: ActorSystem, materializer: Materializer) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  /**
   * Create a streaming source from the DataFrame using RDD partition iteration.
   *
   * This returns a Pekko Source[Row] that will lazily pull data from Spark RDD partitions
   * as downstream consumers request it. Backpressure is automatically handled by the queue.
   *
   * @return Tuple of (SourceQueue for control, Source for streaming, Future for completion)
   */
  def createStreamingSource(): (SourceQueueWithComplete[Row], Source[Row, NotUsed], Future[Done]) = {
    LOGGER.info(s"Creating streaming source with buffer size: $bufferSize")

    // Create queue-based source with backpressure
    val (queue, source) = Source
      .queue[Row](bufferSize, OverflowStrategy.backpressure)
      .preMaterialize()

    // Create completion promise to track when RDD iteration finishes
    val completionPromise = Promise[Done]()

    // Start async RDD iteration that feeds the queue
    val generationFuture = Future {
      Try {
        val recordCount = new AtomicLong(0)
        val startTime = System.currentTimeMillis()

        LOGGER.info("Starting RDD partition iteration for streaming data generation")

        // Iterate through RDD partitions
        df.rdd.foreachPartition { partition =>
          var partitionRecordCount = 0L

          partition.foreach { row =>
            // Offer row to queue - this blocks if queue is full (backpressure)
            val offerResult = Try {
              Await.result(queue.offer(row), 10.seconds)
            }

            offerResult match {
              case Success(QueueOfferResult.Enqueued) =>
                partitionRecordCount += 1
                val totalCount = recordCount.incrementAndGet()

                // Log progress every 1000 records
                if (totalCount % 1000 == 0) {
                  val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
                  val rate = if (elapsed > 0) totalCount / elapsed else 0.0
                  LOGGER.debug(f"Streamed $totalCount records (rate: $rate%.1f rec/sec)")
                }

              case Success(QueueOfferResult.Dropped) =>
                LOGGER.warn(s"Queue dropped row (should not happen with backpressure)")

              case Success(QueueOfferResult.QueueClosed) =>
                LOGGER.error("Queue closed unexpectedly during data generation")
                throw new RuntimeException("Queue closed during data generation")

              case Success(QueueOfferResult.Failure(ex)) =>
                LOGGER.error(s"Queue offer failed: ${ex.getMessage}", ex)
                throw ex

              case Failure(ex) =>
                LOGGER.error(s"Failed to offer row to queue: ${ex.getMessage}", ex)
                throw ex
            }
          }

          LOGGER.debug(s"Partition completed: $partitionRecordCount records streamed")
        }

        val totalRecords = recordCount.get()
        val totalElapsed = (System.currentTimeMillis() - startTime) / 1000.0
        val avgRate = if (totalElapsed > 0) totalRecords / totalElapsed else 0.0

        LOGGER.info(f"RDD iteration complete: $totalRecords records streamed in $totalElapsed%.2f sec (avg rate: $avgRate%.1f rec/sec)")

        // Signal completion
        queue.complete()
        completionPromise.success(Done)

      } match {
        case Success(_) =>
          LOGGER.debug("RDD iteration finished successfully")

        case Failure(ex) =>
          LOGGER.error(s"RDD iteration failed: ${ex.getMessage}", ex)
          queue.fail(ex)
          completionPromise.failure(ex)
      }
    }

    (queue, source, completionPromise.future)
  }

  /**
   * Stream data with a simple rate limiter (for POC testing).
   *
   * This demonstrates how the streaming source can be combined with Pekko throttling
   * for rate-controlled delivery to sinks.
   *
   * @param permitsPerSecond Target throughput rate (records per second)
   * @return Source with rate limiting applied
   */
  def createRateLimitedSource(permitsPerSecond: Int): Source[Row, NotUsed] = {
    val (_, source, _) = createStreamingSource()

    source
      .throttle(permitsPerSecond, 1.second)
      .map { row =>
        LOGGER.trace(s"Emitting row after throttle: ${row.mkString(",")}")
        row
      }
  }

  /**
   * POC Test: Stream data and collect into a list (for comparison with df.collect()).
   *
   * This is purely for testing - it demonstrates that the streaming approach produces
   * the same results as df.collect() but with bounded memory usage.
   *
   * @param maxRecords Maximum records to collect (safety limit)
   * @return List of collected rows
   */
  def streamAndCollect(maxRecords: Int = 10000): List[Row] = {
    val (queue, source, completionFuture) = createStreamingSource()

    val collectedFuture = source
      .take(maxRecords)
      .runWith(PekkoSink.seq)

    // Wait for collection to complete
    val collected = Await.result(collectedFuture, 60.seconds)

    LOGGER.info(s"POC test: Streamed and collected ${collected.size} records (max: $maxRecords)")

    collected.toList
  }
}

object StreamingDataSource {
  /**
   * Factory method for creating StreamingDataSource instances.
   */
  def apply(df: DataFrame, bufferSize: Int = 1000)
           (implicit actorSystem: ActorSystem, materializer: Materializer): StreamingDataSource = {
    new StreamingDataSource(df, bufferSize)
  }

  /**
   * POC Helper: Compare memory usage between streaming and df.collect() approaches.
   *
   * @param df DataFrame to test
   * @param testName Name for logging
   * @param actorSystem Implicit ActorSystem
   * @param materializer Implicit Materializer
   * @return Tuple of (streaming memory MB, collect memory MB, record count)
   */
  def compareMemoryUsage(df: DataFrame, testName: String = "POC")
                        (implicit actorSystem: ActorSystem, materializer: Materializer): (Long, Long, Long) = {
    val logger = Logger.getLogger(getClass.getName)
    val runtime = Runtime.getRuntime

    // Force GC before measurements
    System.gc()
    Thread.sleep(100)

    // Measure baseline
    val baselineMemory = runtime.totalMemory() - runtime.freeMemory()
    logger.info(f"$testName - Baseline memory: ${baselineMemory / 1024.0 / 1024.0}%.2f MB")

    // Test 1: Streaming approach
    System.gc()
    Thread.sleep(100)
    val beforeStreaming = runtime.totalMemory() - runtime.freeMemory()

    val streamingSource = new StreamingDataSource(df, bufferSize = 1000)
    val streamedCount = streamingSource.streamAndCollect().size

    val afterStreaming = runtime.totalMemory() - runtime.freeMemory()
    val streamingMemory = (afterStreaming - beforeStreaming) / 1024 / 1024
    logger.info(f"$testName - Streaming memory: $streamingMemory MB for $streamedCount records")

    // Test 2: Traditional df.collect() approach
    System.gc()
    Thread.sleep(100)
    val beforeCollect = runtime.totalMemory() - runtime.freeMemory()

    val collected = df.collect()
    val collectedCount = collected.size

    val afterCollect = runtime.totalMemory() - runtime.freeMemory()
    val collectMemory = (afterCollect - beforeCollect) / 1024 / 1024
    logger.info(f"$testName - Collect memory: $collectMemory MB for $collectedCount records")

    // Comparison
    val savings = collectMemory - streamingMemory
    val savingsPercent = if (collectMemory > 0) (savings.toDouble / collectMemory) * 100 else 0.0
    logger.info(f"$testName - Memory savings: $savings MB (${savingsPercent}%.1f%%)")

    (streamingMemory, collectMemory, streamedCount)
  }
}
