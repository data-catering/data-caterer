package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.ForeignKeyApplicationUtil
import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Strategy for applying different foreign key generation modes.
 *
 * Supports three modes:
 * - all-exist: All records have valid foreign keys (default)
 * - partial: Some percentage of records have invalid/null foreign keys
 * - all-combinations: Generate all combinations of valid/invalid FK patterns
 */
class GenerationModeStrategy(generationMode: String = "all-exist") extends ForeignKeyStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def name: String = s"GenerationModeStrategy($generationMode)"

  /**
   * Check if this strategy is applicable based on generation mode.
   */
  override def isApplicable(relation: EnhancedForeignKeyRelation): Boolean = {
    // This strategy is always applicable - it's selected based on the generation mode
    true
  }

  /**
   * Apply FK values based on the generation mode.
   */
  override def apply(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {

    val mode = generationMode.toLowerCase
    LOGGER.info(s"Applying foreign keys with generation mode: $mode")

    mode match {
      case "all-combinations" =>
        applyAllCombinations(sourceDf, targetDf, relation)

      case "partial" =>
        applyPartial(sourceDf, targetDf, relation)

      case _ => // "all-exist" or default
        applyAllExist(sourceDf, targetDf, relation)
    }
  }

  /**
   * All-exist mode: All records have valid foreign keys.
   */
  private def applyAllExist(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {
    LOGGER.info("Using all-exist mode: all records have valid FKs")

    val config = ForeignKeyConfig.toApplicationConfig(relation.config)

    ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf,
      targetDf,
      relation.sourceFields,
      relation.targetFields,
      config,
      relation.targetPerFieldCount
    )
  }

  /**
   * Partial mode: Some percentage of records have invalid foreign keys.
   * Uses nullability configuration to determine violation percentage.
   */
  private def applyPartial(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {
    LOGGER.info("Using partial mode with configured violations")

    // Configure violation ratio based on nullability if available
    val partialConfig = if (relation.config.nullability.isDefined) {
      relation.config.copy(
        violationRatio = relation.config.nullability.get.nullPercentage,
        violationStrategy = "null"
      )
    } else {
      relation.config
    }

    val config = ForeignKeyConfig.toApplicationConfig(partialConfig)

    ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf,
      targetDf,
      relation.sourceFields,
      relation.targetFields,
      config,
      relation.targetPerFieldCount
    )
  }

  /**
   * All-combinations mode: Generate all FK match patterns.
   * This creates records with all possible combinations of valid/invalid FK fields.
   */
  private def applyAllCombinations(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {
    LOGGER.info("Using all-combinations mode: generating all FK match patterns")

    val fieldMappings = relation.sourceFields.zip(relation.targetFields)

    ForeignKeyApplicationUtil.generateAllForeignKeyCombinations(
      sourceDf,
      targetDf,
      fieldMappings,
      relation.config.seed
    )
  }
}

/**
 * Companion object with factory methods.
 */
object GenerationModeStrategy {

  /**
   * Create strategy for all-exist mode.
   */
  def allExist(): GenerationModeStrategy = new GenerationModeStrategy("all-exist")

  /**
   * Create strategy for partial mode.
   */
  def partial(): GenerationModeStrategy = new GenerationModeStrategy("partial")

  /**
   * Create strategy for all-combinations mode.
   */
  def allCombinations(): GenerationModeStrategy = new GenerationModeStrategy("all-combinations")

  /**
   * Create strategy based on mode string.
   */
  def forMode(mode: String): GenerationModeStrategy = {
    mode.toLowerCase match {
      case "all-combinations" => allCombinations()
      case "partial" => partial()
      case _ => allExist()
    }
  }
}
