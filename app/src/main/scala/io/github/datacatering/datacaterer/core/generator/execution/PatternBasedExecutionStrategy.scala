package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.execution.pattern.LoadPattern
import io.github.datacatering.datacaterer.core.generator.execution.rate.{DurationTracker, RateLimiter}
import io.github.datacatering.datacaterer.api.model.PerformanceMetrics
import io.github.datacatering.datacaterer.core.generator.metrics.PerformanceMetricsCollector
import io.github.datacatering.datacaterer.core.parser.LoadPatternParser
import io.github.datacatering.datacaterer.core.util.GeneratorUtil
import org.apache.log4j.Logger

/**
 * Pattern-based execution strategy with dynamic rate adjustment over time.
 * Supports various load patterns: ramp, spike, stepped, wave, breaking point.
 */
class PatternBasedExecutionStrategy(
                                     executableTasks: List[(TaskSummary, Task)]
                                   ) extends ExecutionStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Threshold for rate change detection. Only update the rate limiter when the rate
   * changes by more than this fraction (10%) to avoid excessive rate limiter recreation.
   */
  private val RATE_CHANGE_THRESHOLD = 0.1
  private val metricsCollector = new PerformanceMetricsCollector()

  // Extract pattern configuration from first step with pattern configured
  private val (duration, loadPattern, rateUnit) = extractPatternConfig(executableTasks)

  private val totalDurationSeconds = GeneratorUtil.parseDurationToSeconds(duration)
  private val durationTracker = new DurationTracker(duration)

  // We'll dynamically create rate limiters as needed based on the current pattern rate
  private var currentRateLimiter: Option[RateLimiter] = None
  private var currentRate: Int = 0

  private var currentBatchStartTime: Option[java.time.LocalDateTime] = None
  private var hasStarted = false

  LOGGER.info(s"Pattern-based execution strategy initialized: duration=$duration, pattern=${loadPattern.getClass.getSimpleName}")

  /**
   * Override to use AllUpfront generation mode for pattern-based execution.
   * This enables routing through SinkRouter for proper rate limiting at sink level.
   */
  override def getGenerationMode: GenerationMode = GenerationMode.AllUpfront

  /**
   * Get the metrics collector so streaming sink can collect performance metrics.
   */
  override def getMetricsCollector: Option[PerformanceMetricsCollector] = Some(metricsCollector)

  /**
   * Get the total duration in seconds for this pattern-based execution.
   */
  def getDurationSeconds: Double = totalDurationSeconds

  /**
   * Get a function that calculates the rate at any given time in the execution.
   * @return Function that takes elapsed seconds and returns the rate at that time
   */
  def getRateFunction: Double => Int = {
    (elapsedSeconds: Double) => loadPattern.getRateAt(elapsedSeconds, totalDurationSeconds)
  }

  /**
   * Get the current rate at the start of execution (time = 0).
   */
  def getInitialRate: Int = loadPattern.getRateAt(0.0, totalDurationSeconds)

  /**
   * Get the rate unit for this pattern-based execution.
   */
  def getRateUnit: String = rateUnit

  /**
   * Calculate the average rate across the execution duration.
   * Samples the pattern at multiple points to get a representative average.
   * This is used as a fallback when dynamic rate control is not available,
   * and for calculating expected total record counts.
   */
  def getAverageRate: Int = {
    val samplePoints = 10
    val samples = (0 until samplePoints).map { i =>
      val elapsedSeconds = (totalDurationSeconds / samplePoints) * i
      loadPattern.getRateAt(elapsedSeconds, totalDurationSeconds)
    }
    (samples.sum.toDouble / samplePoints).toInt
  }

  override def calculateNumBatches: Int = {
    // For pattern-based execution, we don't know the exact number of batches upfront
    // Return a large number and rely on shouldContinue to stop execution
    Int.MaxValue
  }

  override def shouldContinue(currentBatch: Int): Boolean = {
    if (!hasStarted) {
      durationTracker.start()
      hasStarted = true
    }
    val shouldContinue = durationTracker.hasTimeRemaining
    if (!shouldContinue) {
      LOGGER.info(s"Pattern-based execution completed: total-batches=${currentBatch - 1}, " +
        s"elapsed-time=${durationTracker.getElapsedTimeMs}ms")
    }
    shouldContinue
  }

  override def onBatchStart(batchNumber: Int): Unit = {
    currentBatchStartTime = Some(metricsCollector.recordBatchStart())

    // Update rate based on elapsed time and pattern
    updateRateBasedOnPattern()
  }

  override def onBatchEnd(batchNumber: Int, recordsGenerated: Long): Unit = {
    currentBatchStartTime.foreach { startTime =>
      // Record metrics
      metricsCollector.recordBatchEnd(batchNumber, startTime, recordsGenerated)

      // Apply rate limiting with current rate
      currentRateLimiter.foreach { limiter =>
        val batchDurationMs = java.time.Duration.between(startTime, java.time.LocalDateTime.now()).toMillis
        limiter.throttle(recordsGenerated, batchDurationMs)
      }
    }
  }

  override def getMetrics: Option[PerformanceMetrics] = {
    Some(metricsCollector.getMetrics)
  }

  /**
   * Update the rate limiter based on the current elapsed time and load pattern.
   * This is called at the start of each batch to adjust the rate dynamically.
   */
  private def updateRateBasedOnPattern(): Unit = {
    val elapsedSeconds = durationTracker.getElapsedTimeMs / 1000.0
    val targetRate = loadPattern.getRateAt(elapsedSeconds, totalDurationSeconds)

    // Only create a new rate limiter if the rate has changed significantly or this is the first time
    val shouldUpdate = currentRateLimiter.isEmpty ||
      math.abs(targetRate - currentRate).toDouble / currentRate > RATE_CHANGE_THRESHOLD

    if (shouldUpdate) {
      currentRate = targetRate
      currentRateLimiter = Some(new RateLimiter(targetRate, rateUnit))
      LOGGER.debug(s"Updated rate to $targetRate records/$rateUnit at ${elapsedSeconds.toInt}s elapsed")
    }
  }

  private def extractPatternConfig(tasks: List[(TaskSummary, Task)]): (String, LoadPattern, String) = {
    // Find first step with pattern configured
    val optPatternStep = tasks.flatMap(_._2.steps).find(_.count.pattern.isDefined)

    optPatternStep match {
      case Some(step) =>
        val count = step.count
        val patternModel = count.pattern.getOrElse(
          throw new IllegalArgumentException("Pattern must be specified")
        )

        val pattern = LoadPatternParser.parse(patternModel) match {
          case Right(p) => p
          case Left(errors) =>
            throw new IllegalArgumentException(s"Failed to parse load pattern: ${errors.mkString(", ")}")
        }

        val duration = count.duration.getOrElse(
          throw new IllegalArgumentException("Duration must be specified for pattern-based execution")
        )

        val rateUnit = count.rateUnit.getOrElse("1s")

        (duration, pattern, rateUnit)

      case None =>
        throw new IllegalArgumentException("No step with pattern configuration found")
    }
  }
}
