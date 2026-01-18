package io.github.datacatering.datacaterer.core.util.timing

import scala.collection.mutable.ListBuffer

/**
 * In-memory sink for capturing batch execution timestamps to validate timing behavior.
 * This is used in integration tests to verify execution strategies are meeting timing requirements.
 */
class TimingValidatorSink {
  private val batchTimestamps = ListBuffer[(Int, Long, Long)]() // (batchNum, timestamp, recordCount)
  private var executionStartTime: Option[Long] = None
  private var executionEndTime: Option[Long] = None

  /**
   * Mark the start of execution
   */
  def markExecutionStart(): Unit = {
    executionStartTime = Some(System.currentTimeMillis())
  }

  /**
   * Mark the end of execution
   */
  def markExecutionEnd(): Unit = {
    executionEndTime = Some(System.currentTimeMillis())
  }

  /**
   * Record a batch execution with its timestamp and record count
   */
  def recordBatch(batchNumber: Int, recordCount: Long): Unit = {
    batchTimestamps += ((batchNumber, System.currentTimeMillis(), recordCount))
  }

  /**
   * Get all recorded batch timestamps
   */
  def getBatchTimestamps: List[(Int, Long, Long)] = batchTimestamps.toList

  /**
   * Get the total number of batches recorded
   */
  def getBatchCount: Int = batchTimestamps.size

  /**
   * Get the total number of records across all batches
   */
  def getTotalRecords: Long = batchTimestamps.map(_._3).sum

  /**
   * Analyze the total execution duration
   */
  def analyzeDuration(expectedMs: Long, tolerancePercent: Double = 5.0): DurationAnalysis = {
    require(batchTimestamps.nonEmpty, "No batches recorded")

    val actualMs = (executionStartTime, executionEndTime) match {
      case (Some(start), Some(end)) => end - start
      case _ =>
        // Fall back to first/last batch timestamps
        batchTimestamps.last._2 - batchTimestamps.head._2
    }

    DurationAnalysis(actualMs, expectedMs, tolerancePercent)
  }

  /**
   * Analyze the actual rate compared to target rate
   */
  def analyzeRate(targetRate: Double, tolerancePercent: Double = 10.0): RateAnalysis = {
    require(batchTimestamps.nonEmpty, "No batches recorded")

    val timestamps = batchTimestamps.map(_._2)
    val totalRecords = getTotalRecords
    val durationSec = (timestamps.last - timestamps.head) / 1000.0

    val actualRate = if (durationSec > 0) {
      totalRecords / durationSec
    } else {
      totalRecords.toDouble // All in one instant
    }

    RateAnalysis(actualRate, targetRate, tolerancePercent)
  }

  /**
   * Analyze batch interval consistency for constant rate validation
   */
  def analyzeIntervals(expectedIntervalMs: Double, tolerancePercent: Double = 15.0): IntervalAnalysis = {
    require(batchTimestamps.size >= 2, "Need at least 2 batches for interval analysis")

    val timestamps = batchTimestamps.map(_._2)
    val intervals = timestamps.sliding(2).map {
      case Seq(a, b) => b - a
    }.toList

    IntervalAnalysis(intervals, expectedIntervalMs, tolerancePercent)
  }

  /**
   * Analyze pattern conformance by sampling actual rate at multiple time points
   */
  def analyzePattern(
    patternFunction: Double => Int,
    totalDurationSec: Double,
    tolerancePercent: Double = 15.0
  ): PatternAnalysis = {
    require(batchTimestamps.nonEmpty, "No batches recorded")

    val startTime = batchTimestamps.head._2

    val samples = batchTimestamps.map { case (batchNum, timestamp, recordCount) =>
      val elapsedSec = (timestamp - startTime) / 1000.0
      val expectedRate = patternFunction(elapsedSec)
      val actualRate = calculateInstantaneousRate(batchNum)
      PatternSample(elapsedSec, expectedRate, actualRate, tolerancePercent)
    }.toList

    PatternAnalysis(samples)
  }

  /**
   * Calculate the instantaneous rate for a specific batch by looking at a time window around it
   */
  private def calculateInstantaneousRate(batchNumber: Int): Double = {
    val batchIndex = batchTimestamps.indexWhere(_._1 == batchNumber)
    if (batchIndex < 0) return 0.0

    // Use a 3-batch window centered on the target batch for rate calculation
    val windowSize = 3
    val startIndex = Math.max(0, batchIndex - windowSize / 2)
    val endIndex = Math.min(batchTimestamps.size - 1, batchIndex + windowSize / 2)

    if (startIndex == endIndex) {
      // Single batch - use its record count and assume 1 second
      return batchTimestamps(batchIndex)._3.toDouble
    }

    val windowBatches = batchTimestamps.slice(startIndex, endIndex + 1)
    val totalRecords = windowBatches.map(_._3).sum
    val durationSec = (windowBatches.last._2 - windowBatches.head._2) / 1000.0

    if (durationSec > 0) {
      totalRecords / durationSec
    } else {
      totalRecords.toDouble
    }
  }

  /**
   * Print a summary of all recorded batches (for debugging)
   */
  def printSummary(): Unit = {
    println("=" * 60)
    println("Timing Validator Sink Summary")
    println("=" * 60)
    println(s"Total batches: ${batchTimestamps.size}")
    println(s"Total records: $getTotalRecords")
    if (batchTimestamps.nonEmpty) {
      val startTime = batchTimestamps.head._2
      println(s"\nBatch Details:")
      batchTimestamps.foreach { case (batchNum, timestamp, recordCount) =>
        val elapsedMs = timestamp - startTime
        println(f"  Batch $batchNum%3d: t=${elapsedMs}%5dms, records=$recordCount%5d")
      }
    }
    println("=" * 60)
  }
}

object TimingValidatorSink {
  def apply(): TimingValidatorSink = new TimingValidatorSink()
}
