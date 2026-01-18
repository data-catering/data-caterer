package io.github.datacatering.datacaterer.api.model

import java.time.LocalDateTime
import scala.collection.immutable.TreeMap

/**
 * Performance metrics specifically for streaming/real-time data generation.
 *
 * Unlike batch execution, streaming has no natural "batches" - records flow continuously.
 * This model captures per-record timestamps and provides methods to analyze throughput
 * over time for pattern validation (ramp, wave, spike patterns).
 *
 * @param recordTimestamps List of timestamps (milliseconds since epoch) when each record was emitted
 * @param startTime Logical start time of the streaming operation
 * @param endTime Logical end time of the streaming operation
 * @param totalRecords Total number of records streamed
 * @param executionType Type of execution strategy (constant, pattern-based, etc.)
 */
case class StreamingMetrics(
  recordTimestamps: List[Long],
  startTime: LocalDateTime,
  endTime: LocalDateTime,
  totalRecords: Long,
  executionType: String = "streaming"
) {

  /**
   * Calculate throughput (records/second) for each time window.
   *
   * @param windowMs Window size in milliseconds (default: 1000ms = 1 second)
   * @return Map of window index to throughput (records per second)
   */
  def throughputByWindow(windowMs: Long = 1000L): Map[Int, Double] = {
    if (recordTimestamps.isEmpty) return Map.empty

    val firstTimestamp = recordTimestamps.head
    val windowedCounts = recordTimestamps
      .groupBy(ts => ((ts - firstTimestamp) / windowMs).toInt)
      .map { case (windowIndex, timestamps) =>
        val throughput = (timestamps.size.toDouble / windowMs) * 1000.0 // Convert to records/second
        (windowIndex, throughput)
      }

    TreeMap(windowedCounts.toSeq: _*) // Sorted by window index
  }

  /**
   * Calculate throughput per second (convenience method).
   *
   * @return Map of second index to throughput
   */
  def throughputBySecond: Map[Int, Double] = throughputByWindow(1000L)

  /**
   * Calculate overall average throughput across entire streaming duration.
   *
   * @return Average throughput in records per second
   */
  def averageThroughput: Double = {
    val durationMs = totalDurationMs
    if (durationMs > 0) {
      (totalRecords.toDouble / durationMs) * 1000.0
    } else {
      0.0
    }
  }

  /**
   * Find the peak throughput (highest records/second in any 1-second window).
   *
   * @return Maximum throughput in records per second
   */
  def peakThroughput: Double = {
    val throughputs = throughputBySecond.values
    if (throughputs.nonEmpty) throughputs.max else 0.0
  }

  /**
   * Find the minimum throughput (lowest records/second in any 1-second window).
   *
   * @return Minimum throughput in records per second
   */
  def minThroughput: Double = {
    val throughputs = throughputBySecond.values
    if (throughputs.nonEmpty) throughputs.min else 0.0
  }

  /**
   * Calculate total duration in milliseconds from recorded timestamps.
   *
   * @return Duration in milliseconds
   */
  def totalDurationMs: Long = {
    if (recordTimestamps.isEmpty) 0L
    else recordTimestamps.last - recordTimestamps.head
  }

  /**
   * Calculate total duration in seconds.
   *
   * @return Duration in seconds
   */
  def totalDurationSeconds: Double = totalDurationMs / 1000.0

  /**
   * Calculate inter-arrival time percentile (time between consecutive records).
   * Useful for understanding latency distribution.
   *
   * @param percentile Percentile to calculate (0.0 to 1.0)
   * @return Inter-arrival time in milliseconds at the given percentile
   */
  def interArrivalTimePercentile(percentile: Double): Double = {
    if (recordTimestamps.size < 2) return 0.0

    val intervals = recordTimestamps.sliding(2).map { case List(a, b) => b - a }.toList.sorted
    val index = math.ceil(percentile * intervals.length).toInt - 1
    val safeIndex = math.max(0, math.min(index, intervals.length - 1))
    intervals(safeIndex).toDouble
  }

  /**
   * Calculate latency percentiles (inter-arrival times).
   */
  def latencyP50: Double = interArrivalTimePercentile(0.50)
  def latencyP75: Double = interArrivalTimePercentile(0.75)
  def latencyP90: Double = interArrivalTimePercentile(0.90)
  def latencyP95: Double = interArrivalTimePercentile(0.95)
  def latencyP99: Double = interArrivalTimePercentile(0.99)
  def latencyP999: Double = interArrivalTimePercentile(0.999)

  /**
   * Convert streaming metrics to time-windowed BatchMetrics for compatibility.
   * This allows existing reporting/visualization code to work with streaming data.
   *
   * @param windowMs Window size in milliseconds (default: 1000ms = 1 second)
   * @return List of BatchMetrics representing time windows
   */
  def asTimeWindows(windowMs: Long = 1000L): List[BatchMetrics] = {
    if (recordTimestamps.isEmpty) return List.empty

    val firstTimestamp = recordTimestamps.head
    val lastTimestamp = recordTimestamps.last
    val totalDuration = lastTimestamp - firstTimestamp
    val numWindows = Math.max(1, (totalDuration / windowMs).toInt + 1)

    val batches = scala.collection.mutable.ListBuffer[BatchMetrics]()

    for (windowIndex <- 0 until numWindows) {
      val windowStart = firstTimestamp + (windowIndex * windowMs)
      val windowEnd = windowStart + windowMs

      // Count records in this window
      val recordsInWindow = recordTimestamps.count(ts => ts >= windowStart && ts < windowEnd)

      if (recordsInWindow > 0) {
        val actualWindowEnd = Math.min(windowEnd, lastTimestamp + 1)
        val actualDuration = actualWindowEnd - windowStart

        // Convert timestamp to LocalDateTime
        val batchStartTime = startTime.plusNanos((windowStart - firstTimestamp) * 1000000)
        val batchEndTime = startTime.plusNanos((actualWindowEnd - firstTimestamp) * 1000000)

        batches += BatchMetrics(
          batchNumber = windowIndex,
          startTime = batchStartTime,
          endTime = batchEndTime,
          recordsGenerated = recordsInWindow,
          batchDurationMs = actualDuration,
          phase = "execution"
        )
      }
    }

    batches.toList
  }

  /**
   * Calculate standard deviation of throughput to measure rate consistency.
   * Lower values indicate more consistent throughput.
   *
   * @return Standard deviation of per-second throughput
   */
  def throughputStdDev: Double = {
    val throughputs = throughputBySecond.values.toList
    if (throughputs.isEmpty) return 0.0

    val mean = throughputs.sum / throughputs.size
    val variance = throughputs.map(t => math.pow(t - mean, 2)).sum / throughputs.size
    math.sqrt(variance)
  }

  /**
   * Check if throughput follows an expected pattern within tolerance.
   *
   * @param expectedRateFunction Function that returns expected rate for a given elapsed time
   * @param tolerancePercent Acceptable deviation percentage (default: 20%)
   * @return Tuple of (overallMatch: Boolean, windowResults: List[(time, expectedRate, actualRate, deviation)])
   */
  def validateRatePattern(
    expectedRateFunction: Double => Double,
    tolerancePercent: Double = 20.0
  ): (Boolean, List[(Double, Double, Double, Double)]) = {
    val throughputByTime = throughputBySecond

    val results = throughputByTime.map { case (second, actualRate) =>
      val timeSeconds = second.toDouble
      val expectedRate = expectedRateFunction(timeSeconds)
      val deviation = if (expectedRate > 0) {
        math.abs(actualRate - expectedRate) / expectedRate * 100.0
      } else {
        0.0
      }
      val withinTolerance = deviation <= tolerancePercent
      (timeSeconds, expectedRate, actualRate, deviation)
    }.toList.sortBy(_._1)

    val overallMatch = results.forall(_._4 <= tolerancePercent)
    (overallMatch, results)
  }
}

object StreamingMetrics {
  /**
   * Create StreamingMetrics from a collection of timestamps.
   *
   * @param timestamps Raw timestamps in milliseconds
   * @param startTime Logical start time
   * @param endTime Logical end time
   * @param executionType Type of execution (constant, pattern, etc.)
   * @return StreamingMetrics instance
   */
  def fromTimestamps(
    timestamps: Seq[Long],
    startTime: LocalDateTime,
    endTime: LocalDateTime,
    executionType: String = "streaming"
  ): StreamingMetrics = {
    val sortedTimestamps = timestamps.toList.sorted
    StreamingMetrics(
      recordTimestamps = sortedTimestamps,
      startTime = startTime,
      endTime = endTime,
      totalRecords = sortedTimestamps.size,
      executionType = executionType
    )
  }

  /**
   * Create StreamingMetrics from batch summaries (memory-efficient approach).
   *
   * Instead of storing per-record timestamps, aggregates batches into
   * synthetic timestamps for metrics calculation. This is a memory-efficient
   * alternative that reduces overhead from O(records) to O(batches).
   *
   * For example: 10M records over 1 hour with 1-second windows = 3600 batches
   * Memory: ~300 KB vs 80 MB for per-record timestamps
   *
   * @param batches List of TimestampBatch representing time windows
   * @param startTime Logical start time
   * @param endTime Logical end time
   * @param executionType Type of execution (constant, pattern, etc.)
   * @return StreamingMetrics instance with synthetic timestamps
   */
  def fromBatches(
    batches: List[Any],
    startTime: LocalDateTime,
    endTime: LocalDateTime,
    executionType: String = "streaming"
  ): StreamingMetrics = {
    // Extract batch data using reflection to avoid circular dependency
    val syntheticTimestamps = batches.flatMap { batchObj =>
      val windowStartMs = batchObj.getClass.getMethod("windowStartMs").invoke(batchObj).asInstanceOf[Long]
      val windowEndMs = batchObj.getClass.getMethod("windowEndMs").invoke(batchObj).asInstanceOf[Long]
      val recordCount = batchObj.getClass.getMethod("recordCount").invoke(batchObj).asInstanceOf[Int]

      val durationMs = windowEndMs - windowStartMs

      // Generate synthetic timestamps: distribute records evenly within window
      val intervalMs = if (recordCount > 1) {
        durationMs.toDouble / (recordCount - 1)
      } else {
        0.0
      }

      (0 until recordCount).map { i =>
        windowStartMs + (i * intervalMs).toLong
      }
    }

    val totalRecords = batches.map { batchObj =>
      batchObj.getClass.getMethod("recordCount").invoke(batchObj).asInstanceOf[Int]
    }.sum

    StreamingMetrics(
      recordTimestamps = syntheticTimestamps.sorted,
      startTime = startTime,
      endTime = endTime,
      totalRecords = totalRecords,
      executionType = executionType
    )
  }
}
