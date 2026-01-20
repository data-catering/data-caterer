package io.github.datacatering.datacaterer.core.sink.memory

import io.github.datacatering.datacaterer.api.model.StreamingMetrics
import java.time.LocalDateTime
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters._

/**
 * Represents a time window batch for aggregated timestamp tracking.
 *
 * Instead of storing individual timestamps for every record, we group records
 * into time windows (typically 1 second). This reduces memory from O(records)
 * to O(duration_seconds).
 *
 * @param windowStartMs Start time of the window in milliseconds since epoch
 * @param windowEndMs End time of the window in milliseconds since epoch
 * @param recordCount Number of records emitted in this window
 */
case class TimestampBatch(
  windowStartMs: Long,
  windowEndMs: Long,
  recordCount: Int
) {
  def durationMs: Long = windowEndMs - windowStartMs
  def throughput: Double = if (durationMs > 0) (recordCount * 1000.0) / durationMs else 0.0
}

/**
 * Memory-efficient timestamp tracker using batch aggregation.
 *
 * Replaces per-record timestamp collection with time-windowed batches.
 * For 10M records over 1 hour: ~3600 batches (~300 KB) vs 80 MB of timestamps.
 *
 * Thread-safe for concurrent access from Pekko stream operations.
 *
 * @param windowMs Window size in milliseconds (default 1000ms = 1 second)
 */
class BatchTimestampTracker(windowMs: Long = 1000) {
  private val batches = new ConcurrentLinkedQueue[TimestampBatch]()

  @volatile private var currentWindowStart: Long = System.currentTimeMillis()
  private val currentWindowCount = new AtomicInteger(0)
  private val streamStartTime: Long = System.currentTimeMillis()

  /**
   * Record that a record was emitted.
   * Automatically flushes the current window if time elapsed exceeds windowMs.
   *
   * Thread-safe: Uses double-checked locking to prevent race conditions during window transitions.
   * Multiple threads can safely call this method concurrently.
   */
  def recordTimestamp(): Unit = {
    val now = System.currentTimeMillis()

    // CRITICAL: Increment BEFORE flush check to prevent race condition
    // If we increment after, concurrent threads can both enter flush path and
    // assign their increments to the NEW window, losing counts from the old window
    currentWindowCount.incrementAndGet()

    // Check if window should be flushed (time-based) - unsynchronized check
    if (now - currentWindowStart >= windowMs) {
      // Double-checked locking: verify condition inside synchronized block
      // This prevents race condition where multiple threads enter flush simultaneously
      flushCurrentWindow(now)
    }
  }

  /**
   * Flush the current window to the batch queue.
   * Synchronized to prevent concurrent flush operations.
   * Uses double-checked locking pattern to avoid duplicate flushes.
   *
   * @param endTime End time for the current window
   */
  private def flushCurrentWindow(endTime: Long): Unit = synchronized {
    // Double-check: another thread may have already flushed
    if (endTime - currentWindowStart >= windowMs) {
      val count = currentWindowCount.getAndSet(0)
      if (count > 0) {
        batches.add(TimestampBatch(currentWindowStart, endTime, count))
        currentWindowStart = endTime
      }
    }
  }

  /**
   * Finalize tracking and generate StreamingMetrics from collected batches.
   *
   * Flushes any remaining records in the current window and converts
   * batch summaries into StreamingMetrics with synthetic timestamps.
   *
   * @param startTime Logical start time of streaming operation
   * @param endTime Logical end time of streaming operation
   * @param executionType Type of execution (constant-rate, pattern-based, etc.)
   * @return StreamingMetrics instance
   */
  def finalizeAndGetMetrics(
    startTime: LocalDateTime,
    endTime: LocalDateTime,
    executionType: String
  ): StreamingMetrics = {
    // Force flush of any remaining records in current window, regardless of duration
    val now = System.currentTimeMillis()
    synchronized {
      val count = currentWindowCount.getAndSet(0)
      if (count > 0) {
        batches.add(TimestampBatch(currentWindowStart, now, count))
        currentWindowStart = now
      }
    }

    val batchList = batches.asScala.toList

    // Convert batches to streaming metrics
    StreamingMetrics.fromBatches(
      batches = batchList,
      startTime = startTime,
      endTime = endTime,
      executionType = executionType
    )
  }

  /**
   * Get the number of time window batches collected.
   *
   * @return Number of batches
   */
  def getBatchCount: Int = batches.size()

  /**
   * Get the total number of records tracked across all batches.
   * Includes records in the current (unflushed) window.
   *
   * @return Total record count
   */
  def getTotalRecords: Int = {
    val flushedCount = batches.asScala.map(_.recordCount).sum
    val currentCount = currentWindowCount.get()
    flushedCount + currentCount
  }
}
