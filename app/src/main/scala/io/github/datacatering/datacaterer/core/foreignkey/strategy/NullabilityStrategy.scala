package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.api.model.NullabilityConfig
import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Strategy for applying nullability configuration to foreign key fields.
 *
 * This is a post-processing strategy that applies null values to FK fields
 * after the initial FK application is complete.
 *
 * Supports different strategies for null distribution:
 * - random: Randomly distribute nulls across records
 * - head: Apply nulls to the first N% of records
 * - tail: Apply nulls to the last N% of records
 */
class NullabilityStrategy extends PostProcessingStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def name: String = "NullabilityStrategy"

  /**
   * Check if this strategy is applicable.
   * Applicable when the relation has nullability configuration.
   */
  override def isApplicable(relation: EnhancedForeignKeyRelation): Boolean = {
    relation.config.nullability.isDefined &&
      relation.config.nullability.get.nullPercentage > 0.0
  }

  /**
   * Apply FK values - this is a no-op for nullability strategy as it's post-processing only.
   */
  override def apply(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {
    // Nullability is applied in postProcess, not in initial FK application
    targetDf
  }

  /**
   * Post-process the DataFrame to apply nullability to FK fields.
   */
  override def postProcess(df: DataFrame, relation: EnhancedForeignKeyRelation): DataFrame = {
    relation.config.nullability match {
      case Some(nullabilityConfig) =>
        applyNullability(df, relation.targetFields, nullabilityConfig, relation.config.seed)
      case None =>
        df
    }
  }

  /**
   * Apply nullability configuration to target DataFrame fields.
   *
   * @param targetDf Target DataFrame
   * @param targetFields List of fields to apply nullability to
   * @param nullabilityConfig Nullability configuration
   * @param seed Optional seed for deterministic random behavior
   * @return DataFrame with nullability applied
   */
  def applyNullability(
    targetDf: DataFrame,
    targetFields: List[String],
    nullabilityConfig: NullabilityConfig,
    seed: Option[Long] = None
  ): DataFrame = {

    val percentage = nullabilityConfig.nullPercentage
    val strategy = nullabilityConfig.strategy.toLowerCase

    if (percentage <= 0.0) {
      LOGGER.debug("Nullability percentage is 0, skipping null FK generation")
      return targetDf
    }

    LOGGER.info(s"Applying nullability: ${percentage * 100}% of records will have null FKs, strategy=$strategy")

    // Create a unique seed for this specific nullability application
    // by combining the plan seed with field names to avoid cross-contamination
    // between different FK relationships that share the same plan seed
    val uniqueSeed = seed.map { s =>
      val fieldHash = targetFields.sorted.mkString(",").hashCode.toLong
      s ^ fieldHash  // XOR to combine seeds while maintaining determinism
    }

    // Add a column to determine which records get null FKs
    // For deterministic behavior with seed, we use a hash-based approach instead of rand()
    // because Spark's rand(seed) is partition-dependent and not truly deterministic across environments
    val withNullFlag = strategy match {
      case "random" =>
        uniqueSeed match {
          case Some(s) =>
            // Use hash-based deterministic selection: hash all columns + seed, then check if < percentage
            // This ensures the same rows are selected regardless of partitioning
            val allCols = targetDf.columns.map(col)
            // Use xxhash64 for better distribution (returns Long), then normalize to [0, 1)
            val hashExpr = xxhash64(allCols :+ lit(s): _*)
            // Convert to unsigned by bitwise AND with max long, then normalize
            val normalizedHash = (hashExpr.bitwiseAND(lit(Long.MaxValue))).cast("double") / lit(Long.MaxValue.toDouble)
            targetDf.withColumn("_should_null_fk", normalizedHash < percentage)
          case None =>
            // No seed provided - use non-deterministic rand()
            targetDf.withColumn("_should_null_fk", rand() < percentage)
        }

      case "head" =>
        // First N% of records get null FKs
        val totalCount = targetDf.count()
        val nullCount = (totalCount * percentage).toLong
        targetDf
          .withColumn("_row_idx", row_number().over(Window.orderBy(lit(1))) - 1)
          .withColumn("_should_null_fk", col("_row_idx") < nullCount)
          .drop("_row_idx")

      case "tail" =>
        // Last N% of records get null FKs
        val totalCount = targetDf.count()
        val validCount = (totalCount * (1.0 - percentage)).toLong
        targetDf
          .withColumn("_row_idx", row_number().over(Window.orderBy(lit(1))) - 1)
          .withColumn("_should_null_fk", col("_row_idx") >= validCount)
          .drop("_row_idx")

      case _ =>
        LOGGER.warn(s"Unknown nullability strategy: $strategy, using random")
        uniqueSeed match {
          case Some(s) =>
            val allCols = targetDf.columns.map(col)
            val hashExpr = xxhash64(allCols :+ lit(s): _*)
            val normalizedHash = (hashExpr.bitwiseAND(lit(Long.MaxValue))).cast("double") / lit(Long.MaxValue.toDouble)
            targetDf.withColumn("_should_null_fk", normalizedHash < percentage)
          case None =>
            targetDf.withColumn("_should_null_fk", rand() < percentage)
        }
    }

    // Apply nulls to target fields
    var result = withNullFlag
    targetFields.foreach { field =>
      result = result.withColumn(field,
        when(col("_should_null_fk"), lit(null).cast(result.schema(field).dataType))
          .otherwise(col(field))
      )
    }

    result.drop("_should_null_fk")
  }
}

/**
 * Companion object for backward compatibility with direct apply() calls.
 */
object NullabilityStrategy {

  private val instance = new NullabilityStrategy()

  /**
   * Apply nullability configuration to target DataFrame fields.
   * Backward-compatible method for direct usage.
   *
   * @param targetDf Target DataFrame
   * @param targetFields List of fields to apply nullability to
   * @param nullabilityConfig Nullability configuration
   * @param seed Optional seed for deterministic random behavior
   * @return DataFrame with nullability applied
   */
  def apply(
    targetDf: DataFrame,
    targetFields: List[String],
    nullabilityConfig: NullabilityConfig,
    seed: Option[Long] = None
  ): DataFrame = {
    instance.applyNullability(targetDf, targetFields, nullabilityConfig, seed)
  }
}
