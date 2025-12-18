package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.api.model.CardinalityConfig
import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Strategy for applying cardinality configuration to foreign key relationships.
 *
 * This handles one-to-many relationship creation by assigning multiple child records
 * to parent records based on cardinality configuration.
 *
 * IMPORTANT: This strategy assumes row counts are already handled by CardinalityCountAdjustmentProcessor
 * during generation. It only assigns FK values to existing groups, it does NOT expand row counts.
 *
 * Two modes:
 * - Group-based: When target has perField grouping, preserves existing groups
 * - Index-based: When no perField grouping, assigns based on row position
 */
class CardinalityStrategy extends ForeignKeyStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /** Minimum source count required for modulo operations to avoid division by zero */
  private val MIN_SOURCE_COUNT_FOR_MODULO = 1L

  override def name: String = "CardinalityStrategy"

  /**
   * Check if this strategy is applicable.
   * Applicable when the relation has cardinality configuration.
   */
  override def isApplicable(relation: EnhancedForeignKeyRelation): Boolean = {
    relation.config.cardinality.isDefined
  }

  /**
   * Apply cardinality-aware FK assignment.
   */
  override def apply(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {

    relation.config.cardinality match {
      case Some(cardinalityConfig) =>
        applyCardinality(
          sourceDf,
          targetDf,
          relation.sourceFields.zip(relation.targetFields),
          cardinalityConfig,
          relation.config.seed,
          relation.targetPerFieldCount
        )
      case None =>
        targetDf
    }
  }

  /**
   * Apply cardinality configuration to generate one-to-many relationships.
   * This assigns FK values based on cardinality config, preserving existing group structure.
   *
   * STRATEGY:
   * When perField count is configured (e.g., 5 transactions per account_id), the target DataFrame
   * is already generated with groups of records sharing the same FK field value.
   * We preserve these groups by:
   * 1. Getting distinct FK values from the target (these are the group identifiers)
   * 2. Assigning each distinct FK value to a source parent (round-robin)
   * 3. Replacing all occurrences of each target FK value with the corresponding source value
   *
   * This ensures that all records in the same group get the same FK value, maintaining
   * the cardinality while preserving unique non-FK field values.
   *
   * @param sourceDf Source DataFrame (parent records)
   * @param targetDf Target DataFrame (child records)
   * @param fieldMappings List of (sourceField, targetField) tuples
   * @param cardinalityConfig Configuration for cardinality control
   * @param seed Optional seed for deterministic behavior
   * @param targetPerFieldCount Optional perField count configuration from target step
   * @return Target DataFrame with FK values assigned based on cardinality
   */
  def applyCardinality(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    fieldMappings: List[(String, String)],
    cardinalityConfig: CardinalityConfig,
    seed: Option[Long] = None,
    targetPerFieldCount: Option[io.github.datacatering.datacaterer.api.model.PerFieldCount] = None
  ): DataFrame = {

    val sourceFields = fieldMappings.map(_._1)
    val targetFields = fieldMappings.map(_._2)

    LOGGER.info(s"Applying cardinality: min=${cardinalityConfig.min}, max=${cardinalityConfig.max}, " +
      s"ratio=${cardinalityConfig.ratio}, distribution=${cardinalityConfig.distribution}")

    // Get distinct source values
    val distinctSource = sourceDf.select(sourceFields.map(col): _*).distinct()
    val sourceCount = distinctSource.count()

    LOGGER.info(s"Source has $sourceCount distinct parent records")

    // Guard against empty source DataFrame to prevent division by zero in modulo operations
    if (sourceCount == 0) {
      LOGGER.warn("Source DataFrame has no records - cannot apply cardinality. Returning target DataFrame unchanged.")
      return targetDf
    }

    // Check if target has perField config that creates grouping structure
    // If so, use group-based approach which preserves the generated groups
    val hasMatchingPerFieldConfig = targetPerFieldCount.exists { pfc =>
      // Check if the FK fields are part of the perField grouping
      targetFields.exists(pfc.fieldNames.contains)
    }

    if (hasMatchingPerFieldConfig) {
      // Use group-based approach when perField grouping exists
      // This preserves the group structure created during generation (uniform or varying)
      LOGGER.info("Using GROUP-BASED approach: target has perField grouping for FK fields")
      applyCardinalityWithGrouping(sourceDf, targetDf, sourceFields, targetFields, sourceCount)
    } else {
      // Use index-based approach when no perField grouping exists
      // Calculate expected records per parent for index assignment
      val recordsPerParent = cardinalityConfig match {
        case config if config.min.isDefined && config.max.isDefined =>
          // For bounded, use average of min and max
          (config.min.get + config.max.get) / 2.0
        case config if config.ratio.isDefined =>
          config.ratio.get
        case _ =>
          1.0
      }

      // Use ceil to match calculateRequiredCount behavior and avoid generating fewer records than expected
      val recordsPerParentCeiled = math.ceil(recordsPerParent).toLong
      LOGGER.info(s"Using INDEX-BASED approach: assigning FKs by row position ($recordsPerParentCeiled records per parent)")
      applyCardinalityWithIndex(sourceDf, targetDf, sourceFields, targetFields, sourceCount, recordsPerParentCeiled)
    }
  }

  /**
   * Group-based FK assignment: Maps existing FK groups to source values.
   * Used when target has perField grouping that creates the correct cardinality structure.
   */
  private def applyCardinalityWithGrouping(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    sourceFields: List[String],
    targetFields: List[String],
    sourceCount: Long
  ): DataFrame = {

    // Step 1: Get distinct FK value combinations from target
    val distinctTargetFKs = targetDf.select(targetFields.map(col): _*).distinct()
    val distinctTargetCount = distinctTargetFKs.count()

    LOGGER.info(s"Target has $distinctTargetCount distinct FK value groups")

    // Step 2: Add index to both source and distinct target FKs
    val windowSpec = Window.orderBy(lit(1))

    val sourceWithIndex = sourceDf.select(sourceFields.map(col): _*).distinct()
      .withColumn("_fk_idx", row_number().over(windowSpec) - 1)

    // For target, assign indices without modulo to avoid collisions when possible
    // Map distinct target groups to source values in order (0->0, 1->1, etc.)
    // Only use modulo if we have more target groups than source values
    val useModulo = distinctTargetCount > sourceCount
    val targetFKsWithIndex = if (useModulo) {
      LOGGER.info(s"Target has more distinct groups ($distinctTargetCount) than source values ($sourceCount), using modulo")
      distinctTargetFKs
        .withColumn("_target_idx", row_number().over(windowSpec) - 1)
        .withColumn("_fk_idx", (col("_target_idx") % sourceCount).cast("long"))
    } else {
      LOGGER.info(s"Mapping distinct target groups ($distinctTargetCount) 1:1 to source values ($sourceCount)")
      distinctTargetFKs
        .withColumn("_target_idx", row_number().over(windowSpec) - 1)
        .withColumn("_fk_idx", col("_target_idx"))
    }

    // Step 3: Rename source fields to prepare for join
    val sourceFieldsRenamed = sourceFields.foldLeft(sourceWithIndex) { case (df, field) =>
      df.withColumnRenamed(field, s"_src_$field")
    }

    // Step 4: Create mapping table: old FK values -> new FK values
    // Keep original field names for the join key
    val fkMapping = targetFKsWithIndex
      .join(broadcast(sourceFieldsRenamed), Seq("_fk_idx"), "left")
      .select(
        targetFields.map(col) ++
          sourceFields.map(f => col(s"_src_$f")): _*
      )

    // Step 5: Join target with mapping to replace FK values
    // This preserves all non-FK fields while updating only the FK fields
    var result = targetDf.join(fkMapping, targetFields, "left")

    // Step 6: Replace target FK fields with source values
    sourceFields.zip(targetFields).foreach { case (sourceField, targetField) =>
      val srcColName = s"_src_$sourceField"
      // If join succeeded, use source value; otherwise keep target value (shouldn't happen)
      result = result.withColumn(targetField, coalesce(col(srcColName), col(targetField)))
    }

    // Step 7: Clean up and return only original columns
    result.select(targetDf.columns.map(col): _*)
  }

  /**
   * Index-based FK assignment: Assigns FKs based on row position.
   * Used when target doesn't have perField grouping, or when we need explicit cardinality control.
   */
  private def applyCardinalityWithIndex(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    sourceFields: List[String],
    targetFields: List[String],
    sourceCount: Long,
    recordsPerParent: Long
  ): DataFrame = {

    LOGGER.debug(s"INDEX-BASED FK assignment: sourceCount=$sourceCount, recordsPerParent=$recordsPerParent, targetCount=${targetDf.count()}")

    val windowSpec = Window.orderBy(lit(1))

    // Step 1: Add index to source for join
    val sourceWithIndex = sourceDf.select(sourceFields.map(col): _*).distinct()
      .withColumn("_fk_idx", row_number().over(windowSpec) - 1)

    // Step 2: Rename source fields to avoid collision during join
    val sourceFieldsRenamed = sourceFields.foldLeft(sourceWithIndex) { case (df, field) =>
      df.withColumnRenamed(field, s"_src_$field")
    }

    // Step 3: Add grouping logic to target based on row position
    // Each group of recordsPerParent consecutive rows gets the same FK index
    val targetWithGrouping = targetDf
      .withColumn("_row_num", row_number().over(windowSpec) - 1)
      .withColumn("_group_id", floor(col("_row_num") / recordsPerParent))
      .withColumn("_fk_idx", (col("_group_id") % sourceCount).cast("long"))

    // Step 4: Join on FK index to get source values
    val joined = targetWithGrouping.join(broadcast(sourceFieldsRenamed), Seq("_fk_idx"), "left")

    // Validate join succeeded
    if (LOGGER.isDebugEnabled) {
      val nullJoinCount = joined.filter(sourceFields.map(f => col(s"_src_$f").isNull).reduce(_ || _)).count()
      if (nullJoinCount > 0) {
        LOGGER.warn(s"Found $nullJoinCount records with null FK values after join (join failed)")
      }
    }

    // Step 5: Update target FK fields with source values
    var result = joined
    sourceFields.zip(targetFields).foreach { case (sourceField, targetField) =>
      val srcColName = s"_src_$sourceField"
      result = result.withColumn(targetField, col(srcColName))
    }

    // Step 6: Clean up temporary columns and return
    result.select(targetDf.columns.map(col): _*)
  }
}

/**
 * Companion object for backward compatibility.
 */
object CardinalityStrategy {

  private val instance = new CardinalityStrategy()

  /**
   * Apply cardinality configuration to target DataFrame.
   * Backward-compatible method for direct usage.
   */
  def apply(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    fieldMappings: List[(String, String)],
    cardinalityConfig: CardinalityConfig,
    seed: Option[Long] = None,
    targetPerFieldCount: Option[io.github.datacatering.datacaterer.api.model.PerFieldCount] = None
  ): DataFrame = {
    instance.applyCardinality(sourceDf, targetDf, fieldMappings, cardinalityConfig, seed, targetPerFieldCount)
  }
}
