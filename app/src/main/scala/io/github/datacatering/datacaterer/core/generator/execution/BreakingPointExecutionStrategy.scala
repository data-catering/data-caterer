package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.execution.pattern.BreakingPointPattern
import io.github.datacatering.datacaterer.core.generator.execution.rate.{DurationTracker, RateLimiter}
import io.github.datacatering.datacaterer.core.generator.metrics.{PerformanceMetrics, PerformanceMetricsCollector}
import io.github.datacatering.datacaterer.core.parser.LoadPatternParser
import io.github.datacatering.datacaterer.core.util.GeneratorUtil
import org.apache.log4j.Logger

/**
 * Breaking point execution strategy that automatically increases load until a breaking condition is met.
 *
 * Features:
 * - Starts at a base rate and progressively increases
 * - Real-time metric evaluation during execution
 * - Automatic stopping when threshold is breached (e.g., error rate > 5%, latency p95 > 2000ms)
 * - Records the breaking point rate for capacity planning
 *
 * Phase 3 implementation with full auto-stop capabilities.
 */
class BreakingPointExecutionStrategy(
                                      executableTasks: List[(TaskSummary, Task)]
                                    ) extends ExecutionStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val metricsCollector = new PerformanceMetricsCollector()

  // Extract breaking point configuration
  private val (duration, breakingPointPattern, rateUnit, breakingCondition) = extractBreakingPointConfig(executableTasks)

  private val totalDurationSeconds = GeneratorUtil.parseDurationToSeconds(duration)
  private val durationTracker = new DurationTracker(duration)

  private var currentRateLimiter: Option[RateLimiter] = None
  private var currentRate: Int = 0
  private var currentBatchStartTime: Option[java.time.LocalDateTime] = None
  private var hasStarted = false

  // Breaking point tracking
  @volatile private var breakingPointReached = false
  @volatile private var breakingPointRate: Option[Int] = None
  @volatile private var breakingPointReason: Option[String] = None

  // Minimum samples before checking breaking conditions (to avoid premature stopping)
  private val MIN_BATCHES_BEFORE_CHECK = 5

  LOGGER.info(s"Breaking point execution strategy initialized: duration=$duration, " +
    s"startRate=${breakingPointPattern.startRate}, increment=${breakingPointPattern.rateIncrement}, " +
    s"interval=${breakingPointPattern.incrementInterval}s")

  override def calculateNumBatches: Int = {
    // For breaking point execution, we don't know the exact number of batches upfront
    // Return a large number and rely on shouldContinue to stop execution
    Int.MaxValue
  }

  override def shouldContinue(currentBatch: Int): Boolean = {
    if (!hasStarted) {
      durationTracker.start()
      hasStarted = true
    }

    // Check if breaking point has been reached
    if (breakingPointReached) {
      LOGGER.info(s"Breaking point reached: rate=${breakingPointRate.getOrElse("unknown")}, " +
        s"reason=${breakingPointReason.getOrElse("unknown")}, batches=$currentBatch")
      return false
    }

    // Check if time limit has been reached
    val hasTimeRemaining = durationTracker.hasTimeRemaining
    if (!hasTimeRemaining) {
      LOGGER.info(s"Breaking point test completed (time limit): max-rate=$currentRate, " +
        s"total-batches=$currentBatch, elapsed-time=${durationTracker.getElapsedTimeMs}ms")
    }

    hasTimeRemaining
  }

  override def onBatchStart(batchNumber: Int): Unit = {
    currentBatchStartTime = Some(metricsCollector.recordBatchStart())

    // Update rate based on elapsed time and breaking point pattern
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

      // Check breaking conditions after minimum samples collected
      if (batchNumber >= MIN_BATCHES_BEFORE_CHECK) {
        checkBreakingConditions(batchNumber)
      }
    }
  }

  override def getMetrics: Option[PerformanceMetrics] = {
    val metrics = metricsCollector.getMetrics

    // Annotate metrics with breaking point information if available
    breakingPointRate.foreach { rate =>
      LOGGER.info(s"Breaking point found at $rate records/$rateUnit")
    }

    Some(metrics)
  }

  /**
   * Update the rate limiter based on the current elapsed time and breaking point pattern.
   */
  private def updateRateBasedOnPattern(): Unit = {
    if (breakingPointReached) return

    val elapsedSeconds = durationTracker.getElapsedTimeMs / 1000.0
    val targetRate = breakingPointPattern.getRateAt(elapsedSeconds, totalDurationSeconds)

    // Only create a new rate limiter if the rate has changed
    if (currentRateLimiter.isEmpty || targetRate != currentRate) {
      currentRate = targetRate
      currentRateLimiter = Some(new RateLimiter(targetRate, rateUnit))
      LOGGER.info(s"Breaking point test: increased rate to $targetRate records/$rateUnit at ${elapsedSeconds.toInt}s elapsed")
    }
  }

  /**
   * Check if any breaking conditions have been met.
   * Breaking conditions can be based on error rate, latency percentiles, or throughput degradation.
   */
  private def checkBreakingConditions(batchNumber: Int): Unit = {
    if (breakingPointReached) return

    breakingCondition.foreach { condition =>
      val metrics = metricsCollector.getMetrics
      val metricName = condition.metric
      val threshold = condition.threshold

      val currentMetricValue = metricName.toLowerCase match {
        case "error_rate" => metrics.errorRate
        case "throughput" => metrics.averageThroughput
        case "latency_p50" => metrics.latencyP50
        case "latency_p75" => metrics.latencyP75
        case "latency_p90" => metrics.latencyP90
        case "latency_p95" => metrics.latencyP95
        case "latency_p99" => metrics.latencyP99
        case _ =>
          LOGGER.warn(s"Unknown metric for breaking condition: $metricName")
          return
      }

      // Check if threshold has been breached
      val thresholdBreached = metricName.toLowerCase match {
        case "error_rate" => currentMetricValue > threshold
        case "throughput" => currentMetricValue < threshold // Breaking point if throughput drops below threshold
        case metric if metric.startsWith("latency") => currentMetricValue > threshold
        case _ => false
      }

      if (thresholdBreached) {
        breakingPointReached = true
        breakingPointRate = Some(currentRate)
        breakingPointReason = Some(s"$metricName exceeded threshold: $currentMetricValue > $threshold")

        LOGGER.warn(s"Breaking point reached at batch $batchNumber: $metricName=$currentMetricValue exceeds threshold=$threshold")
      } else if (LOGGER.isDebugEnabled) {
        LOGGER.debug(s"Breaking condition check at batch $batchNumber: $metricName=$currentMetricValue (threshold=$threshold)")
      }
    }
  }

  private def extractBreakingPointConfig(tasks: List[(TaskSummary, Task)]): (String, BreakingPointPattern, String, Option[BreakingCondition]) = {
    // Find first step with pattern configured
    val optPatternStep = tasks.flatMap(_._2.steps).find(_.count.pattern.isDefined)

    optPatternStep match {
      case Some(step) =>
        val count = step.count
        val patternModel = count.pattern.getOrElse(
          throw new IllegalArgumentException("Pattern must be specified for breaking point execution")
        )

        val pattern = LoadPatternParser.parse(patternModel) match {
          case Right(p: BreakingPointPattern) => p
          case Right(_) =>
            throw new IllegalArgumentException("Breaking point execution strategy requires a breaking point pattern")
          case Left(errors) =>
            throw new IllegalArgumentException(s"Failed to parse breaking point pattern: ${errors.mkString(", ")}")
        }

        val duration = count.duration.getOrElse(
          throw new IllegalArgumentException("Duration must be specified for breaking point execution")
        )

        val rateUnit = count.rateUnit.getOrElse("1s")

        // Extract breaking condition from count options
        val breakingCondition = count.options.get("breakingCondition").flatMap {
          case map: Map[_, _] =>
            try {
              val metric = map.asInstanceOf[Map[String, Any]].getOrElse("metric", "error_rate").toString
              val threshold = map.asInstanceOf[Map[String, Any]].getOrElse("threshold", 0.05) match {
                case d: Double => d
                case i: Int => i.toDouble
                case s: String => s.toDouble
                case _ => 0.05
              }
              Some(BreakingCondition(metric, threshold))
            } catch {
              case e: Exception =>
                LOGGER.warn(s"Failed to parse breaking condition: ${e.getMessage}")
                None
            }
          case _ => None
        }

        (duration, pattern, rateUnit, breakingCondition)

      case None =>
        throw new IllegalArgumentException("No step with breaking point pattern configuration found")
    }
  }
}

/**
 * Breaking condition configuration
 */
case class BreakingCondition(metric: String, threshold: Double)

