package io.github.datacatering.datacaterer.core.sink.memory

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * Statistics about the response buffer state.
 *
 * @param totalCount Total number of responses added (including evicted)
 * @param successCount Number of successful responses
 * @param failureCount Number of failed responses
 * @param bufferedCount Current number of responses in buffer
 * @param evictedCount Number of responses evicted due to capacity limits
 */
case class ResponseStats(
  totalCount: Long,
  successCount: Long,
  failureCount: Long,
  bufferedCount: Int,
  evictedCount: Long
) {
  def successRate: Double = if (totalCount > 0) successCount.toDouble / totalCount else 0.0
  def failureRate: Double = if (totalCount > 0) failureCount.toDouble / totalCount else 0.0
}

/**
 * Memory-bounded response buffer with LRU eviction.
 *
 * Replaces unbounded ListBuffer to prevent OOM failures in large-scale streaming.
 * Keeps last N responses for debugging while tracking total success/failure counts.
 *
 * Thread-safe for concurrent access from Pekko stream mapAsync operations.
 *
 * @param maxSize Maximum number of responses to retain (default 10,000)
 */
class BoundedResponseBuffer(maxSize: Int = 10000) {
  private val buffer = new ConcurrentLinkedQueue[Try[String]]()
  private val totalCount = new AtomicLong(0)
  private val successCount = new AtomicLong(0)
  private val failureCount = new AtomicLong(0)
  private val evictedCount = new AtomicLong(0)

  /**
   * Add a response to the buffer.
   * Automatically evicts oldest entries when capacity is exceeded (LRU).
   *
   * @param response Response to add (Success or Failure)
   */
  def add(response: Try[String]): Unit = {
    totalCount.incrementAndGet()

    response match {
      case Success(_) => successCount.incrementAndGet()
      case Failure(_) => failureCount.incrementAndGet()
    }

    buffer.add(response)

    // Evict oldest entries if over capacity (LRU)
    while (buffer.size() > maxSize) {
      buffer.poll()
      evictedCount.incrementAndGet()
    }
  }

  /**
   * Get all currently buffered responses.
   *
   * Note: This may not include all responses if eviction has occurred.
   * Use getStats to check total counts.
   *
   * @return List of buffered responses
   */
  def getResponses: List[Try[String]] = buffer.asScala.toList

  /**
   * Get buffer statistics including total counts and eviction info.
   *
   * @return Current buffer statistics
   */
  def getStats: ResponseStats = ResponseStats(
    totalCount = totalCount.get(),
    successCount = successCount.get(),
    failureCount = failureCount.get(),
    bufferedCount = buffer.size(),
    evictedCount = evictedCount.get()
  )

  /**
   * Clear all buffered responses.
   * Does not reset total/success/failure counters.
   */
  def clear(): Unit = {
    buffer.clear()
  }
}
