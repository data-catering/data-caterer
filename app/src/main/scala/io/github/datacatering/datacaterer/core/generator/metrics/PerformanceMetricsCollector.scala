package io.github.datacatering.datacaterer.core.generator.metrics

import org.apache.log4j.Logger

import java.time.LocalDateTime
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.JavaConverters._

class PerformanceMetricsCollector {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val batchMetricsQueue = new ConcurrentLinkedQueue[BatchMetrics]()
  @volatile private var startTime: Option[LocalDateTime] = None
  @volatile private var endTime: Option[LocalDateTime] = None

  def recordBatchStart(): LocalDateTime = {
    val now = LocalDateTime.now()
    if (startTime.isEmpty) {
      startTime = Some(now)
      LOGGER.debug(s"Performance metrics collection started at $now")
    }
    now
  }

  def recordBatchEnd(batchNumber: Int, batchStartTime: LocalDateTime, recordsGenerated: Long, phase: String = "execution"): Unit = {
    val now = LocalDateTime.now()
    endTime = Some(now)
    val durationMs = java.time.Duration.between(batchStartTime, now).toMillis

    val batchMetric = BatchMetrics(
      batchNumber = batchNumber,
      startTime = batchStartTime,
      endTime = now,
      recordsGenerated = recordsGenerated,
      batchDurationMs = durationMs,
      phase = phase
    )

    batchMetricsQueue.add(batchMetric)

    if (LOGGER.isDebugEnabled) {
      LOGGER.debug(s"Batch $batchNumber completed (phase=$phase): records=$recordsGenerated, duration=${durationMs}ms, " +
        s"throughput=${batchMetric.throughput} records/sec")
    }
  }

  def getMetrics: PerformanceMetrics = {
    val batches = batchMetricsQueue.asScala.toList
    PerformanceMetrics(
      batchMetrics = batches,
      startTime = startTime,
      endTime = endTime
    )
  }

  /**
   * Get metrics filtered to only include execution phase (excluding warmup/cooldown).
   * Phase 3 feature for accurate performance measurement.
   */
  def getExecutionMetrics: PerformanceMetrics = {
    val allBatches = batchMetricsQueue.asScala.toList
    val executionBatches = allBatches.filter(_.phase == "execution")

    if (executionBatches.nonEmpty) {
      PerformanceMetrics(
        batchMetrics = executionBatches,
        startTime = Some(executionBatches.head.startTime),
        endTime = Some(executionBatches.last.endTime)
      )
    } else {
      // Fall back to all metrics if no execution phase metrics found
      getMetrics
    }
  }

  def reset(): Unit = {
    batchMetricsQueue.clear()
    startTime = None
    endTime = None
    LOGGER.debug("Performance metrics collector reset")
  }
}
