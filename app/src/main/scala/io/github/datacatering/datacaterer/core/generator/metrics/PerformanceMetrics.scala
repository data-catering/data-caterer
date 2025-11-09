package io.github.datacatering.datacaterer.core.generator.metrics

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
                                startTime: Option[LocalDateTime] = None,
                                endTime: Option[LocalDateTime] = None
                              ) {

  def totalRecords: Long = batchMetrics.map(_.recordsGenerated).sum

  def averageThroughput: Double = {
    val totalDurationMs = batchMetrics.map(_.batchDurationMs).sum
    if (totalDurationMs > 0) {
      (totalRecords.toDouble / totalDurationMs) * 1000.0 // records per second
    } else {
      0.0
    }
  }

  def maxThroughput: Double = {
    if (batchMetrics.nonEmpty) {
      batchMetrics.map(_.throughput).max
    } else {
      0.0
    }
  }

  def minThroughput: Double = {
    if (batchMetrics.nonEmpty) {
      batchMetrics.map(_.throughput).min
    } else {
      0.0
    }
  }

  def latencyP50: Double = calculatePercentile(0.50)

  def latencyP75: Double = calculatePercentile(0.75)

  def latencyP90: Double = calculatePercentile(0.90)

  def latencyP95: Double = calculatePercentile(0.95)

  def latencyP99: Double = calculatePercentile(0.99)

  def latencyP999: Double = calculatePercentile(0.999)

  def totalDurationSeconds: Long = {
    (startTime, endTime) match {
      case (Some(start), Some(end)) =>
        java.time.Duration.between(start, end).getSeconds
      case _ => 0L
    }
  }

  def errorRate: Double = 0.0 // Placeholder for Phase 3

  /**
   * Calculate percentile using exact or approximate method based on dataset size.
   * For large datasets (>100k samples), uses T-Digest for memory efficiency.
   * For smaller datasets, uses exact sorting.
   * Phase 3 optimization.
   */
  private def calculatePercentile(percentile: Double): Double = {
    if (batchMetrics.isEmpty) return 0.0

    val latencies = batchMetrics.map(_.batchDurationMs.toDouble)

    // Use T-Digest for large datasets (Phase 3 optimization)
    if (latencies.size > TDigest.LARGE_DATASET_THRESHOLD) {
      val digest = TDigest.fromValues(latencies)
      digest.quantile(percentile)
    } else {
      // Exact calculation for smaller datasets
      val sorted = latencies.sorted
      val index = math.ceil(percentile * sorted.length).toInt - 1
      val safeIndex = math.max(0, math.min(index, sorted.length - 1))
      sorted(safeIndex)
    }
  }

  def addBatchMetric(batchMetric: BatchMetrics): PerformanceMetrics = {
    this.copy(
      batchMetrics = batchMetrics :+ batchMetric,
      startTime = startTime.orElse(Some(batchMetric.startTime)),
      endTime = Some(batchMetric.endTime)
    )
  }
}
