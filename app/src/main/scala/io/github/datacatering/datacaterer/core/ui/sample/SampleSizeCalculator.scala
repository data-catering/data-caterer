package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.Step

/**
 * Utility for calculating effective sample sizes with consistent logic
 *
 * This centralizes the sample size calculation logic that was previously duplicated
 * throughout FastSampleGenerator, ensuring consistent behavior across all generation paths.
 *
 * Priority order for determining sample size:
 * 1. Explicitly provided sampleSize parameter (if present)
 * 2. Step count.records configuration (if present)
 * 3. Default value (10)
 *
 * Additional constraints:
 * - Minimum sample size: 1
 * - Maximum sample size: Configurable cap (default 100) to prevent huge HTTP payloads
 */
object SampleSizeCalculator {

  private val DEFAULT_SAMPLE_SIZE = 10
  private val MIN_SAMPLE_SIZE = 1
  private val DEFAULT_MAX_SAMPLE_SIZE = 100

  /**
   * Calculate effective sample size from request and step configuration
   *
   * @param requestedSize Optional explicitly requested sample size
   * @param step Optional step containing count.records configuration
   * @param defaultSize Default size if no other value is available (default: 10)
   * @return Effective sample size (minimum 1)
   */
  def getEffectiveSampleSize(
    requestedSize: Option[Int],
    step: Option[Step] = None,
    defaultSize: Int = DEFAULT_SAMPLE_SIZE
  ): Int = {
    val size = requestedSize
      .orElse(step.flatMap(_.count.records.map(_.toInt)))
      .getOrElse(defaultSize)

    Math.max(size, MIN_SAMPLE_SIZE)
  }

  /**
   * Calculate effective sample size and apply maximum cap
   *
   * This is used for scenarios where we need to prevent huge HTTP payloads,
   * such as relationship-aware generation where multiple DataFrames are generated.
   *
   * @param requestedSize Optional explicitly requested sample size
   * @param step Optional step containing count.records configuration
   * @param maxSize Maximum allowed sample size (default: 100)
   * @param defaultSize Default size if no other value is available (default: 10)
   * @return Capped effective sample size (between MIN and maxSize)
   */
  def getCappedSampleSize(
    requestedSize: Option[Int],
    step: Option[Step] = None,
    maxSize: Int = DEFAULT_MAX_SAMPLE_SIZE,
    defaultSize: Int = DEFAULT_SAMPLE_SIZE
  ): Int = {
    val effectiveSize = getEffectiveSampleSize(requestedSize, step, defaultSize)
    Math.min(effectiveSize, maxSize)
  }

  /**
   * Validate and sanitize a sample size value
   *
   * @param size Raw sample size value
   * @param maxSize Maximum allowed sample size (default: 100)
   * @return Validated sample size (between MIN and maxSize)
   */
  def validateSampleSize(size: Int, maxSize: Int = DEFAULT_MAX_SAMPLE_SIZE): Int = {
    Math.min(Math.max(size, MIN_SAMPLE_SIZE), maxSize)
  }

  /**
   * Get the default maximum sample size used for capping
   */
  def getDefaultMaxSampleSize: Int = DEFAULT_MAX_SAMPLE_SIZE

  /**
   * Get the default sample size
   */
  def getDefaultSampleSize: Int = DEFAULT_SAMPLE_SIZE
}
