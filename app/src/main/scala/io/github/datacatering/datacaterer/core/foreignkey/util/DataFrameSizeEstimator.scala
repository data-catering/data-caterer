package io.github.datacatering.datacaterer.core.foreignkey.util

import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Utilities for estimating DataFrame sizes and making caching decisions.
 */
object DataFrameSizeEstimator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Decide whether to cache a DataFrame based on estimated size.
   *
   * @param df DataFrame to evaluate
   * @param thresholdMB Size threshold in megabytes
   * @return true if DataFrame should be cached, false otherwise
   */
  def shouldCache(df: DataFrame, thresholdMB: Long): Boolean = {
    try {
      val stats = df.queryExecution.analyzed.stats
      if (stats.sizeInBytes.isValidLong) {
        val sizeMB = stats.sizeInBytes.toLong / (1024 * 1024)
        sizeMB < thresholdMB
      } else {
        // Can't determine size, be conservative
        false
      }
    } catch {
      case _: Exception =>
        // If estimation fails, don't cache
        false
    }
  }

  /**
   * Estimate DataFrame size in bytes.
   *
   * @param df DataFrame to estimate
   * @return Estimated size in bytes
   */
  def estimateSize(df: DataFrame): Long = {
    try {
      // Use Spark's statistics if available
      val stats = df.queryExecution.analyzed.stats
      if (stats.sizeInBytes.isValidLong) {
        stats.sizeInBytes.toLong
      } else {
        // Fallback: estimate based on row count
        // Assume average 100 bytes per row if we can't get accurate size
        val rowCount = df.count()
        rowCount * 100
      }
    } catch {
      case _: Exception =>
        // If we can't estimate, assume large (don't cache)
        LOGGER.debug(s"Unable to estimate DataFrame size, defaulting to no-cache")
        Long.MaxValue
    }
  }

  /**
   * Estimate row count without triggering a full count().
   *
   * @param df DataFrame to evaluate
   * @return Estimated row count
   */
  def estimateRowCount(df: DataFrame): Long = {
    try {
      val stats = df.queryExecution.analyzed.stats
      if (stats.rowCount.isDefined) {
        stats.rowCount.get.toLong
      } else {
        // Sample a small portion to estimate
        val sampleCount = df.sample(withReplacement = false, ForeignKeyConfig.SAMPLE_RATIO_FOR_SIZE_ESTIMATE).count()
        (sampleCount / ForeignKeyConfig.SAMPLE_RATIO_FOR_SIZE_ESTIMATE).toLong
      }
    } catch {
      case _: Exception =>
        // Conservative estimate - don't broadcast
        Long.MaxValue
    }
  }

  /**
   * Determine if broadcast join should be used based on DataFrame size.
   *
   * @param df DataFrame to evaluate
   * @param enabled Whether broadcast optimization is enabled
   * @return true if broadcast join should be used
   */
  def shouldBroadcast(df: DataFrame, enabled: Boolean): Boolean = {
    if (!enabled) return false

    try {
      val rowCount = estimateRowCount(df)
      rowCount < ForeignKeyConfig.BROADCAST_THRESHOLD_ROWS
    } catch {
      case _: Exception => false
    }
  }
}
