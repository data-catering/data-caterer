package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import org.apache.spark.sql.DataFrame

/**
 * Strategy trait for applying foreign key values to target DataFrames.
 *
 * Implementations provide different approaches for FK application based on:
 * - Field types (flat vs nested)
 * - Performance characteristics (V1 vs V2)
 * - Special requirements (cardinality, nullability, generation mode)
 */
trait ForeignKeyStrategy {

  /**
   * Apply foreign key values from source to target DataFrame.
   *
   * @param sourceDf Source DataFrame containing foreign key values
   * @param targetDf Target DataFrame to populate with foreign key values
   * @param relation Enhanced FK relationship with configuration
   * @return Target DataFrame with foreign key values applied
   */
  def apply(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame

  /**
   * Check if this strategy is applicable for the given relationship.
   *
   * @param relation Enhanced FK relationship
   * @return true if this strategy can handle the relationship
   */
  def isApplicable(relation: EnhancedForeignKeyRelation): Boolean

  /**
   * Get strategy name for logging and debugging.
   */
  def name: String
}

/**
 * Base trait for strategies that require additional operations after FK application.
 */
trait PostProcessingStrategy extends ForeignKeyStrategy {

  /**
   * Apply post-processing operations like nullability or violations.
   *
   * @param df DataFrame after initial FK application
   * @param relation Enhanced FK relationship
   * @return DataFrame with post-processing applied
   */
  def postProcess(df: DataFrame, relation: EnhancedForeignKeyRelation): DataFrame
}
