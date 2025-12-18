package io.github.datacatering.datacaterer.core.generator.execution.rate

import org.apache.log4j.Logger

/**
 * Rate limiter for controlling throughput
 */
class RateLimiter(targetRate: Int, rateUnit: String = "1s") {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val targetRatePerSecond: Double = calculateRatePerSecond(targetRate, rateUnit)
  private val minBatchIntervalMs: Long = calculateMinBatchInterval()

  LOGGER.info(s"Rate limiter initialized: target-rate=$targetRate/$rateUnit, " +
    s"rate-per-second=$targetRatePerSecond, min-batch-interval-ms=${minBatchIntervalMs}ms")

  /**
   * Calculate sleep time needed to maintain target rate
   * @param recordsGenerated number of records generated in this batch
   * @param batchDurationMs time taken to generate the batch
   * @return sleep time in milliseconds
   */
  def calculateSleepTime(recordsGenerated: Long, batchDurationMs: Long): Long = {
    if (recordsGenerated == 0 || targetRatePerSecond <= 0) {
      return 0L
    }

    // Calculate expected time to generate these records at target rate
    val expectedDurationMs = (recordsGenerated.toDouble / targetRatePerSecond * 1000.0).toLong

    // If we're ahead of schedule, sleep to catch up to target rate
    val sleepTime = math.max(0, expectedDurationMs - batchDurationMs)

    if (LOGGER.isDebugEnabled) {
      val currentRate = if (batchDurationMs > 0) {
        (recordsGenerated.toDouble / batchDurationMs) * 1000.0
      } else 0.0
      LOGGER.debug(f"Rate limiter calculation: records=$recordsGenerated, batch-duration=${batchDurationMs}ms, " +
        f"current-rate=$currentRate%.2f/s, target-rate=$targetRatePerSecond%.2f/s, sleep-time=${sleepTime}ms")
    }

    sleepTime
  }

  /**
   * Sleep if needed to maintain target rate
   */
  def throttle(recordsGenerated: Long, batchDurationMs: Long): Unit = {
    val sleepTime = calculateSleepTime(recordsGenerated, batchDurationMs)
    if (sleepTime > 0) {
      LOGGER.debug(s"Throttling: sleeping for ${sleepTime}ms to maintain target rate")
      Thread.sleep(sleepTime)
    }
  }

  private def calculateRatePerSecond(rate: Int, unit: String): Double = {
    // Parse unit like "1s", "100ms", "1m"
    val pattern = """(\d+)([a-z]+)""".r
    unit.toLowerCase match {
      case pattern(value, unitType) =>
        val timeWindowMs = unitType match {
          case "ms" => value.toLong
          case "s" => value.toLong * 1000
          case "m" => value.toLong * 60 * 1000
          case _ => throw new IllegalArgumentException(s"Invalid rate unit: $unit")
        }
        (rate.toDouble / timeWindowMs) * 1000.0 // convert to per second
      case _ => throw new IllegalArgumentException(s"Invalid rate unit format: $unit. Expected format: <number><unit> (e.g., '1s', '100ms')")
    }
  }

  private def calculateMinBatchInterval(): Long = {
    // Minimum interval between batches (in ms) to avoid excessive sleeping
    math.max(100, (100.0 / targetRatePerSecond * 1000.0).toLong)
  }
}
