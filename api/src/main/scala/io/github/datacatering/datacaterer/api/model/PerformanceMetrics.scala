package io.github.datacatering.datacaterer.api.model

import java.time.LocalDateTime

case class BatchMetrics(
                         batchNumber: Int,
                         startTime: LocalDateTime,
                         endTime: LocalDateTime,
                         recordsGenerated: Long,
                         batchDurationMs: Long,
                         phase: String = "execution"  // warmup, execution, cooldown
                       ) {
  def throughput: Double = {
    if (batchDurationMs > 0) {
      (recordsGenerated.toDouble / batchDurationMs) * 1000.0 // records per second
    } else {
      0.0
    }
  }
}

case class PerformanceMetrics(
                                batchMetrics: List[BatchMetrics] = List(),
                                streamingMetrics: Option[StreamingMetrics] = None,
                                startTime: Option[LocalDateTime] = None,
                                endTime: Option[LocalDateTime] = None
                              ) {

  /**
   * Get total records from either batch or streaming metrics.
   */
  def totalRecords: Long = {
    streamingMetrics match {
      case Some(streaming) => streaming.totalRecords
      case None => batchMetrics.map(_.recordsGenerated).sum
    }
  }

  /**
   * Get average throughput from either batch or streaming metrics.
   */
  def averageThroughput: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.averageThroughput
      case None =>
        val totalDurationMs = batchMetrics.map(_.batchDurationMs).sum
        if (totalDurationMs > 0) {
          (totalRecords.toDouble / totalDurationMs) * 1000.0 // records per second
        } else {
          0.0
        }
    }
  }

  /**
   * Get maximum throughput from either batch or streaming metrics.
   */
  def maxThroughput: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.peakThroughput
      case None =>
        if (batchMetrics.nonEmpty) {
          batchMetrics.map(_.throughput).max
        } else {
          0.0
        }
    }
  }

  /**
   * Get minimum throughput from either batch or streaming metrics.
   */
  def minThroughput: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.minThroughput
      case None =>
        if (batchMetrics.nonEmpty) {
          batchMetrics.map(_.throughput).min
        } else {
          0.0
        }
    }
  }

  /**
   * Get latency percentiles from either batch or streaming metrics.
   */
  def latencyP50: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.latencyP50
      case None => calculatePercentile(0.50)
    }
  }

  def latencyP75: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.latencyP75
      case None => calculatePercentile(0.75)
    }
  }

  def latencyP90: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.latencyP90
      case None => calculatePercentile(0.90)
    }
  }

  def latencyP95: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.latencyP95
      case None => calculatePercentile(0.95)
    }
  }

  def latencyP99: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.latencyP99
      case None => calculatePercentile(0.99)
    }
  }

  def latencyP999: Double = {
    streamingMetrics match {
      case Some(streaming) => streaming.latencyP999
      case None => calculatePercentile(0.999)
    }
  }

  def totalDurationSeconds: Long = {
    (startTime, endTime) match {
      case (Some(start), Some(end)) =>
        java.time.Duration.between(start, end).getSeconds
      case _ => 0L
    }
  }

  def totalDurationMs: Long = {
    (startTime, endTime) match {
      case (Some(start), Some(end)) =>
        java.time.Duration.between(start, end).toMillis
      case _ => 0L
    }
  }

  def errorRate: Double = 0.0 // Placeholder for future enhancement

  /**
   * Calculate percentile using exact sorting method.
   */
  private def calculatePercentile(percentile: Double): Double = {
    if (batchMetrics.isEmpty) return 0.0

    val latencies = batchMetrics.map(_.batchDurationMs.toDouble)
    val sorted = latencies.sorted
    val index = math.ceil(percentile * sorted.length).toInt - 1
    val safeIndex = math.max(0, math.min(index, sorted.length - 1))
    sorted(safeIndex)
  }

  def addBatchMetric(batchMetric: BatchMetrics): PerformanceMetrics = {
    this.copy(
      batchMetrics = batchMetrics :+ batchMetric,
      startTime = startTime.orElse(Some(batchMetric.startTime)),
      endTime = Some(batchMetric.endTime)
    )
  }

  /**
   * Check if this metrics contains streaming data.
   */
  def isStreaming: Boolean = streamingMetrics.isDefined

  /**
   * Check if this metrics contains batch data.
   */
  def isBatch: Boolean = batchMetrics.nonEmpty

  /**
   * Get time-windowed batches for visualization/reporting.
   * For streaming metrics, converts to time windows. For batch metrics, returns as-is.
   */
  def asTimeWindows(windowMs: Long = 1000L): List[BatchMetrics] = {
    streamingMetrics match {
      case Some(streaming) => streaming.asTimeWindows(windowMs)
      case None => batchMetrics
    }
  }
}

object PerformanceMetrics {
  /**
   * Create PerformanceMetrics from streaming data.
   */
  def fromStreaming(streaming: StreamingMetrics): PerformanceMetrics = {
    PerformanceMetrics(
      batchMetrics = List.empty,
      streamingMetrics = Some(streaming),
      startTime = Some(streaming.startTime),
      endTime = Some(streaming.endTime)
    )
  }

  /**
   * Create PerformanceMetrics from batch data.
   */
  def fromBatches(batches: List[BatchMetrics]): PerformanceMetrics = {
    val start = if (batches.nonEmpty) Some(batches.head.startTime) else None
    val end = if (batches.nonEmpty) Some(batches.last.endTime) else None

    PerformanceMetrics(
      batchMetrics = batches,
      streamingMetrics = None,
      startTime = start,
      endTime = end
    )
  }
}
