package io.github.datacatering.datacaterer.core.foreignkey.config

import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, NullabilityConfig}

/**
 * Configuration for foreign key generation behavior.
 *
 * @param enableBroadcastOptimization Whether to use broadcast joins for small dimension tables
 * @param cacheThresholdMB Only cache DataFrames smaller than this threshold
 * @param seed Optional seed for random number generation to ensure deterministic behavior
 * @param cardinality Optional cardinality configuration for controlling relationship ratios
 * @param nullability Optional nullability configuration for controlling null FK percentage
 */
case class ForeignKeyConfig(
  enableBroadcastOptimization: Boolean = true,
  cacheThresholdMB: Long = 200,
  seed: Option[Long] = None,
  cardinality: Option[CardinalityConfig] = None,
  nullability: Option[NullabilityConfig] = None
)

object ForeignKeyConfig {
  // Default thresholds
  val BROADCAST_THRESHOLD_ROWS: Long = 100000
  val CACHE_SIZE_THRESHOLD_MB: Long = 200
  val SAMPLE_RATIO_FOR_SIZE_ESTIMATE: Double = 0.01

  def default: ForeignKeyConfig = ForeignKeyConfig()

}
