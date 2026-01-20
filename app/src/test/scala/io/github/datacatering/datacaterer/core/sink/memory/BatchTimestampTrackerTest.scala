package io.github.datacatering.datacaterer.core.sink.memory

import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.log4j.Logger
import org.scalatest.matchers.should.Matchers

import java.time.LocalDateTime
import java.util.concurrent.{CountDownLatch, Executors}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Await}
import scala.concurrent.duration.DurationInt

/**
 * Unit tests for BatchTimestampTracker thread safety and functionality.
 *
 * Tests verify:
 * - Thread-safe concurrent access to recordTimestamp()
 * - Correct batch window flushing
 * - No record count loss during concurrent window transitions
 * - Proper batch aggregation and metrics generation
 *
 * Critical: Tests for issue #1 - thread safety race condition where
 * concurrent threads entering flush path could lose record counts.
 */
class BatchTimestampTrackerTest extends SparkSuite with Matchers {

  private val LOGGER = Logger.getLogger(getClass.getName)

  test("Single-threaded tracking - verify basic functionality") {
    val tracker = new BatchTimestampTracker(windowMs = 100)

    // Record 10 timestamps
    (1 to 10).foreach { _ =>
      tracker.recordTimestamp()
      Thread.sleep(5) // Small delay within window
    }

    // Should still be in first window (< 100ms total)
    tracker.getBatchCount shouldBe 0
    tracker.getTotalRecords shouldBe 10

    // Wait for window to expire
    Thread.sleep(150)
    tracker.recordTimestamp() // Trigger flush

    // Should have flushed first window and started second
    tracker.getBatchCount shouldBe 1
    tracker.getTotalRecords shouldBe 11
  }

  test("Multi-threaded concurrent recording - verify no record loss") {
    val tracker = new BatchTimestampTracker(windowMs = 1000)
    val numThreads = 10
    val recordsPerThread = 100
    val totalExpectedRecords = numThreads * recordsPerThread

    val executor = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    try {
      val latch = new CountDownLatch(numThreads)

      // Launch multiple threads simultaneously
      val futures = (1 to numThreads).map { threadId =>
        Future {
          latch.countDown()
          latch.await() // Wait for all threads to be ready

          // Each thread records timestamps
          (1 to recordsPerThread).foreach { _ =>
            tracker.recordTimestamp()
            Thread.sleep(1) // Small delay to spread across time
          }
        }
      }

      // Wait for all threads to complete
      Await.result(Future.sequence(futures), 30.seconds)

      // Verify no records were lost
      val finalRecordCount = tracker.getTotalRecords
      LOGGER.info(s"Multi-threaded test: expected=$totalExpectedRecords, actual=$finalRecordCount")

      finalRecordCount shouldBe totalExpectedRecords

    } finally {
      executor.shutdown()
    }
  }

  test("Concurrent window transitions - verify no count loss during flush") {
    // CRITICAL TEST: This tests the fix for issue #1
    // Without the fix (increment AFTER flush check), concurrent threads
    // entering the flush path would both increment in the NEW window,
    // losing counts from the old window.

    val tracker = new BatchTimestampTracker(windowMs = 50)
    val numThreads = 20
    val recordsPerThread = 50
    val totalExpectedRecords = numThreads * recordsPerThread

    val executor = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    try {
      val latch = new CountDownLatch(numThreads)

      // Launch threads that will trigger multiple window transitions
      val futures = (1 to numThreads).map { threadId =>
        Future {
          latch.countDown()
          latch.await()

          (1 to recordsPerThread).foreach { i =>
            tracker.recordTimestamp()

            // Introduce occasional small delays to trigger window transitions
            if (i % 10 == 0) {
              Thread.sleep(10)
            }
          }
        }
      }

      Await.result(Future.sequence(futures), 30.seconds)

      // Force final flush
      Thread.sleep(100)
      val metrics = tracker.finalizeAndGetMetrics(
        LocalDateTime.now(),
        LocalDateTime.now(),
        "test"
      )

      val finalRecordCount = metrics.totalRecords
      LOGGER.info(s"Concurrent window transitions: expected=$totalExpectedRecords, actual=$finalRecordCount, batches=${tracker.getBatchCount}")

      // CRITICAL: This should be exact equality
      // Any loss indicates a thread safety bug
      finalRecordCount shouldBe totalExpectedRecords

      // Should have created multiple batches due to window transitions
      tracker.getBatchCount should be > 1

    } finally {
      executor.shutdown()
    }
  }

  test("High-frequency concurrent access - stress test") {
    val tracker = new BatchTimestampTracker(windowMs = 100)
    val numThreads = 50
    val recordsPerThread = 200
    val totalExpectedRecords = numThreads * recordsPerThread

    val executor = Executors.newFixedThreadPool(numThreads)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(executor)

    try {
      val startTime = System.currentTimeMillis()

      val futures = (1 to numThreads).map { _ =>
        Future {
          (1 to recordsPerThread).foreach { _ =>
            tracker.recordTimestamp()
          }
        }
      }

      Await.result(Future.sequence(futures), 60.seconds)

      val duration = System.currentTimeMillis() - startTime
      val finalRecordCount = tracker.getTotalRecords

      LOGGER.info(s"Stress test: $totalExpectedRecords records from $numThreads threads in ${duration}ms")
      LOGGER.info(s"Expected=$totalExpectedRecords, actual=$finalRecordCount, batches=${tracker.getBatchCount}")

      finalRecordCount shouldBe totalExpectedRecords

    } finally {
      executor.shutdown()
    }
  }

  test("Metrics generation - verify correct batch aggregation") {
    val tracker = new BatchTimestampTracker(windowMs = 100)
    val startTime = LocalDateTime.now()

    // Record some timestamps with delays to create multiple windows
    (1 to 10).foreach { _ =>
      tracker.recordTimestamp()
    }

    Thread.sleep(150)

    (1 to 15).foreach { _ =>
      tracker.recordTimestamp()
    }

    Thread.sleep(150)

    (1 to 20).foreach { _ =>
      tracker.recordTimestamp()
    }

    // Verify tracker has all records before finalization
    val totalBeforeFinalize = tracker.getTotalRecords
    val batchCountBefore = tracker.getBatchCount
    LOGGER.info(s"Before finalization: totalRecords=$totalBeforeFinalize, batches=$batchCountBefore")
    totalBeforeFinalize shouldBe 45

    val endTime = LocalDateTime.now()
    val metrics = tracker.finalizeAndGetMetrics(startTime, endTime, "test")

    val batchCountAfter = tracker.getBatchCount
    LOGGER.info(s"After finalization: metrics.totalRecords=${metrics.totalRecords}, batches=$batchCountAfter")
    LOGGER.info(s"recordTimestamps.size=${metrics.recordTimestamps.size}")

    metrics.totalRecords shouldBe 45
    metrics.executionType shouldBe "test"
  }

  test("Empty tracker - verify graceful handling") {
    val tracker = new BatchTimestampTracker(windowMs = 100)

    tracker.getBatchCount shouldBe 0
    tracker.getTotalRecords shouldBe 0

    val metrics = tracker.finalizeAndGetMetrics(
      LocalDateTime.now(),
      LocalDateTime.now(),
      "test"
    )

    metrics.totalRecords shouldBe 0
  }

  test("Window size variations - verify different window sizes work") {
    // Test with very small window
    val smallWindow = new BatchTimestampTracker(windowMs = 10)
    (1 to 100).foreach { _ =>
      smallWindow.recordTimestamp()
      Thread.sleep(1)
    }

    Thread.sleep(20)
    smallWindow.recordTimestamp()

    smallWindow.getBatchCount should be > 5
    smallWindow.getTotalRecords shouldBe 101

    // Test with larger window
    val largeWindow = new BatchTimestampTracker(windowMs = 1000)
    (1 to 100).foreach { _ =>
      largeWindow.recordTimestamp()
      Thread.sleep(1)
    }

    // Should still be in first window
    largeWindow.getBatchCount shouldBe 0
    largeWindow.getTotalRecords shouldBe 100
  }
}
