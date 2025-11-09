package io.github.datacatering.datacaterer.core.generator.execution.pattern

/**
 * Base trait for load patterns that control the rate of data generation over time.
 * Load patterns enable various testing scenarios like ramp tests, spike tests, and stress tests.
 */
trait LoadPattern {
  /**
   * Calculate the target rate (records per second) at a specific point in time.
   *
   * @param elapsedSeconds The number of seconds elapsed since the test started
   * @param totalDurationSeconds The total duration of the test in seconds
   * @return The target rate in records per second at this point in time
   */
  def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int

  /**
   * Validate that the pattern configuration is valid.
   *
   * @return A list of validation error messages, empty if valid
   */
  def validate(): List[String] = List()
}
