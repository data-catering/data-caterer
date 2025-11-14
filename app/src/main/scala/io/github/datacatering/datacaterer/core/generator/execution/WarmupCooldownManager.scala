package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Plan, TestConfig}
import org.apache.log4j.Logger

/**
 * Manages warmup and cooldown phases for test execution.
 *
 * Warmup Phase:
 * - Pre-test execution period to stabilize system state
 * - Metrics collected during warmup are excluded from final results
 * - Useful for JVM warm-up, cache population, connection pool initialization
 *
 * Cooldown Phase:
 * - Post-test execution period to observe system recovery
 * - Metrics collected during cooldown are excluded from final results
 * - Useful for observing resource cleanup, connection draining
 *
 * Phase 3 feature.
 */
class WarmupCooldownManager(plan: Plan, timeProvider: () => Long = () => System.currentTimeMillis()) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private val testConfig: Option[TestConfig] = plan.testConfig
  private val warmupDurationMs: Long = testConfig.flatMap(_.warmup).map(parseDuration).getOrElse(0L)
  private val cooldownDurationMs: Long = testConfig.flatMap(_.cooldown).map(parseDuration).getOrElse(0L)

  private var testStartTime: Option[Long] = None
  private var warmupEndTime: Option[Long] = None
  private var executionEndTime: Option[Long] = None

  val hasWarmup: Boolean = warmupDurationMs > 0
  val hasCooldown: Boolean = cooldownDurationMs > 0

  // Expose for testing
  protected[execution] def getWarmupDurationMs: Long = warmupDurationMs
  protected[execution] def getCooldownDurationMs: Long = cooldownDurationMs
  protected[execution] def getTestStartTime: Option[Long] = testStartTime
  protected[execution] def getWarmupEndTime: Option[Long] = warmupEndTime
  protected[execution] def getExecutionEndTime: Option[Long] = executionEndTime

  if (hasWarmup) {
    LOGGER.info(s"Warmup phase configured: ${testConfig.flatMap(_.warmup).getOrElse("0s")} (${warmupDurationMs}ms)")
  }

  if (hasCooldown) {
    LOGGER.info(s"Cooldown phase configured: ${testConfig.flatMap(_.cooldown).getOrElse("0s")} (${cooldownDurationMs}ms)")
  }

  /**
   * Mark the start of the test (including warmup phase)
   */
  def startTest(): Unit = {
    testStartTime = Some(timeProvider())
    warmupEndTime = testStartTime.map(_ + warmupDurationMs)
    LOGGER.info(s"Test started at ${testStartTime.get}, warmup will end at ${warmupEndTime.getOrElse("N/A")}")
  }

  /**
   * Mark the end of the main execution phase (before cooldown)
   */
  def endExecution(): Unit = {
    executionEndTime = Some(timeProvider())
    LOGGER.info(s"Main execution phase ended at ${executionEndTime.get}")
  }

  /**
   * Check if currently in warmup phase
   */
  def isInWarmupPhase: Boolean = {
    (testStartTime, warmupEndTime) match {
      case (Some(start), Some(warmupEnd)) =>
        val now = timeProvider()
        now < warmupEnd
      case _ => false
    }
  }

  /**
   * Check if currently in cooldown phase
   */
  def isInCooldownPhase: Boolean = {
    (executionEndTime, testStartTime) match {
      case (Some(execEnd), Some(start)) if hasCooldown =>
        val now = timeProvider()
        val cooldownEnd = execEnd + cooldownDurationMs
        now >= execEnd && now < cooldownEnd
      case _ => false
    }
  }

  /**
   * Check if currently in main execution phase (not warmup, not cooldown)
   */
  def isInExecutionPhase: Boolean = {
    !isInWarmupPhase && !isInCooldownPhase
  }

  /**
   * Check if warmup phase has completed
   */
  def isWarmupComplete: Boolean = {
    warmupEndTime match {
      case Some(warmupEnd) => timeProvider() >= warmupEnd
      case None => true // No warmup configured
    }
  }

  /**
   * Check if cooldown phase should start
   */
  def shouldStartCooldown(mainExecutionComplete: Boolean): Boolean = {
    hasCooldown && mainExecutionComplete && executionEndTime.isEmpty
  }

  /**
   * Check if cooldown phase is complete
   */
  def isCooldownComplete: Boolean = {
    (executionEndTime, hasCooldown) match {
      case (Some(execEnd), true) =>
        val now = timeProvider()
        val cooldownEnd = execEnd + cooldownDurationMs
        now >= cooldownEnd
      case (_, false) => true // No cooldown configured
      case _ => false
    }
  }

  /**
   * Get the current phase name for logging/reporting
   */
  def getCurrentPhase: String = {
    if (isInWarmupPhase) "warmup"
    else if (isInCooldownPhase) "cooldown"
    else if (testStartTime.isDefined) "execution"
    else "not started"
  }

  /**
   * Get remaining warmup time in milliseconds
   */
  def getRemainingWarmupTime: Long = {
    (testStartTime, warmupEndTime) match {
      case (Some(_), Some(warmupEnd)) =>
        val now = timeProvider()
        math.max(0, warmupEnd - now)
      case _ => 0L
    }
  }

  /**
   * Get remaining cooldown time in milliseconds
   */
  def getRemainingCooldownTime: Long = {
    executionEndTime match {
      case Some(execEnd) if hasCooldown =>
        val now = timeProvider()
        val cooldownEnd = execEnd + cooldownDurationMs
        math.max(0, cooldownEnd - now)
      case _ => 0L
    }
  }

  /**
   * Parse duration string to milliseconds.
   * Supports formats like: "30s", "5m", "1h", "2h30m15s"
   */
  private def parseDuration(duration: String): Long = {
    val pattern = """(\d+)([smh])""".r
    val matches = pattern.findAllMatchIn(duration.toLowerCase)

    val seconds = matches.foldLeft(0.0) { (total, m) =>
      val value = m.group(1).toDouble
      val unit = m.group(2)
      val secs = unit match {
        case "s" => value
        case "m" => value * 60
        case "h" => value * 3600
        case _ => 0.0
      }
      total + secs
    }

    (seconds * 1000).toLong
  }

  /**
   * Get summary of warmup/cooldown configuration
   */
  def getSummary: String = {
    val warmupStr = if (hasWarmup) testConfig.flatMap(_.warmup).getOrElse("0s") else "none"
    val cooldownStr = if (hasCooldown) testConfig.flatMap(_.cooldown).getOrElse("0s") else "none"
    s"warmup=$warmupStr, cooldown=$cooldownStr"
  }
}
