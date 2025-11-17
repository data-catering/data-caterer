package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{concat, expr, lit, rand, row_number, floor, col, when}
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}

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

    // Configure for all valid FKs (no violations)
    val config = relation.config.copy(violationRatio = 0.0)
    val relationWithConfig = relation.copy(config = config)

    new DistributedSamplingStrategy().apply(sourceDf, targetDf, relationWithConfig)
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

    val relationWithConfig = relation.copy(config = partialConfig)

    new DistributedSamplingStrategy().apply(sourceDf, targetDf, relationWithConfig)
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
    import org.apache.spark.sql.functions._

    val numFields = relation.fieldMappings.length
    LOGGER.info(s"Generating all FK combinations for $numFields fields (${math.pow(2, numFields).toInt} combinations)")

    if (numFields == 0) {
      LOGGER.warn("No fields to generate combinations for")
      return targetDf
    }

    // Calculate total combinations: 2^n (each field can match or not match)
    val totalCombinations = math.pow(2, numFields).toInt
    val targetCount = targetDf.count()
    val recordsPerCombination = math.max(1, targetCount / totalCombinations)

    LOGGER.info(s"Generating $totalCombinations combinations with ~$recordsPerCombination records each")

    // Add combination ID to target
    val targetWithCombo = targetDf
      .withColumn("_row_id", row_number().over(Window.orderBy(lit(1))) - 1)
      .withColumn("_combination_id", floor(col("_row_id") / recordsPerCombination))

    // First, apply valid FKs to get baseline
    val validConfig = relation.config.copy(violationRatio = 0.0)
    val relationWithValidConfig = relation.copy(config = validConfig)
    val withValidFKs = new DistributedSamplingStrategy().apply(sourceDf, targetWithCombo, relationWithValidConfig)

    // For each combination, decide which fields to invalidate
    var result = withValidFKs
    relation.fieldMappings.zipWithIndex.foreach { case (mapping, fieldIdx) =>
      val targetField = mapping.targetField
      // Bit mask: if bit at position fieldIdx is 0, invalidate this field
      // This ensures we get all 2^n combinations
      val shouldInvalidate = (col("_combination_id") % totalCombinations).bitwiseAND(1 << fieldIdx) === 0

      val invalidValue = generateViolationValue(
        result.schema(targetField).dataType,
        "random",
        relation.config.seed
      )

      result = result.withColumn(targetField,
        when(shouldInvalidate, invalidValue).otherwise(col(targetField))
      )
    }

    result.drop("_row_id", "_combination_id")
  }

  /**
   * Generate a value that violates foreign key integrity based on strategy.
   */
  private def generateViolationValue(dataType: DataType, strategy: String, seed: Option[Long] = None): Column = {
    strategy.toLowerCase match {
      case "null" =>
        lit(null).cast(dataType)

      case "random" =>
        val randExpr = seed.map(s => rand(s)).getOrElse(rand())
        dataType match {
          case StringType =>
            // Use deterministic hash-based approach when seed is available
            seed match {
              case Some(s) => concat(lit("INVALID_"), expr(s"MD5(CONCAT('$s', CAST(monotonically_increasing_id() AS STRING)))"))
              case None => concat(lit("INVALID_"), expr("uuid()"))
            }
          case IntegerType => (randExpr * 999999999).cast(IntegerType)
          case LongType => (randExpr * 999999999999L).cast(LongType)
          case _ => lit(null).cast(dataType)
        }

      case "out_of_range" =>
        dataType match {
          case StringType => lit("OUT_OF_RANGE_VALUE")
          case IntegerType => lit(-999999)
          case LongType => lit(-999999999L)
          case _ => lit(null).cast(dataType)
        }

      case _ =>
        LOGGER.warn(s"Unknown violation strategy: $strategy, using null")
        lit(null).cast(dataType)
    }
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
