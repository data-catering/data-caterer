package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Task, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.execution.rate.{DurationTracker, RateLimiter}
import io.github.datacatering.datacaterer.core.generator.metrics.{PerformanceMetrics, PerformanceMetricsCollector}
import io.github.datacatering.datacaterer.core.util.GeneratorUtil
import org.apache.log4j.Logger

/**
 * Duration-based execution strategy with constant rate limiting
 */
class DurationBasedExecutionStrategy(
                                      executableTasks: List[(TaskSummary, Task)]
                                    ) extends ExecutionStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val metricsCollector = new PerformanceMetricsCollector()

  // Extract duration and rate from first step with duration configured
  private val (duration, rate, rateUnit) = extractDurationConfig(executableTasks)

  private val durationTracker = new DurationTracker(duration)
  private val rateLimiter = rate.map(r => new RateLimiter(r, rateUnit.getOrElse("1s")))

  private var currentBatchStartTime: Option[java.time.LocalDateTime] = None
  private var hasStarted = false

  LOGGER.info(s"Duration-based execution strategy initialized: duration=$duration, rate=${rate.getOrElse("unlimited")}/${rateUnit.getOrElse("1s")}")

  override def calculateNumBatches: Int = {
    // For duration-based, we don't know the exact number of batches upfront
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
      LOGGER.info(s"Duration-based execution completed: total-batches=${currentBatch - 1}, " +
        s"elapsed-time=${durationTracker.getElapsedTimeMs}ms")
    }
    shouldContinue
  }

  override def onBatchStart(batchNumber: Int): Unit = {
    currentBatchStartTime = Some(metricsCollector.recordBatchStart())
  }

  override def onBatchEnd(batchNumber: Int, recordsGenerated: Long): Unit = {
    currentBatchStartTime.foreach { startTime =>
      // Record metrics
      metricsCollector.recordBatchEnd(batchNumber, startTime, recordsGenerated)

      // Apply rate limiting if configured
      rateLimiter.foreach { limiter =>
        val batchDurationMs = java.time.Duration.between(startTime, java.time.LocalDateTime.now()).toMillis
        limiter.throttle(recordsGenerated, batchDurationMs)
      }
    }
  }

  override def getMetrics: Option[PerformanceMetrics] = {
    Some(metricsCollector.getMetrics)
  }

  /**
   * Duration-based execution with rate uses AllUpfront generation mode for streaming
   */
  override def getGenerationMode: GenerationMode = {
    if (rate.isDefined) GenerationMode.AllUpfront else GenerationMode.Batched
  }

  /**
   * Get the duration in seconds for streaming execution
   */
  def getDurationSeconds: Double = GeneratorUtil.parseDurationToSeconds(duration)

  /**
   * Get the target rate per second (if configured)
   */
  def getTargetRate: Option[Int] = rate

  private def extractDurationConfig(tasks: List[(TaskSummary, Task)]): (String, Option[Int], Option[String]) = {
    // Find first step with duration configured
    val optDurationStep = tasks.flatMap(_._2.steps).find(_.count.duration.isDefined)

    optDurationStep match {
      case Some(step) =>
        val count = step.count
        (
          count.duration.getOrElse(throw new IllegalArgumentException("Duration must be specified")),
          count.rate,
          count.rateUnit.orElse(Some("1s"))
        )
      case None =>
        throw new IllegalArgumentException("No step with duration configuration found")
    }
  }
}
