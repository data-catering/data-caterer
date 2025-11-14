package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import io.github.datacatering.datacaterer.core.foreignkey.util.{DataFrameSizeEstimator, NestedFieldUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.storage.StorageLevel

/**
 * Distributed sampling strategy for foreign key application.
 *
 * This strategy works for any combination of flat/nested fields by:
 * 1. Creating distinct source value combinations
 * 2. Assigning a random index to each target row
 * 3. Joining on index to get source values
 * 4. Updating both flat and nested fields using the sampled values
 *
 * Never collects data to driver, uses distributed joins for sampling.
 */
class DistributedSamplingStrategy extends ForeignKeyStrategy {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override def name: String = "DistributedSampling"

  override def isApplicable(relation: EnhancedForeignKeyRelation): Boolean = {
    // This strategy works for any field combination, but is primarily for nested or mixed fields
    relation.hasNestedFields
  }

  override def apply(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {

    val sourceFields = relation.sourceFields
    val targetFields = relation.targetFields
    val config = relation.config

    LOGGER.debug(s"Using distributed sampling approach for ${relation.fieldMappings.length} fields (includes nested)")

    // Create a temporary table with all source field combinations
    val distinctSource = sourceDf.select(sourceFields.map(col): _*).distinct()

    // Smart caching
    val shouldCache = DataFrameSizeEstimator.shouldCache(distinctSource, config.cacheThresholdMB)
    if (shouldCache) {
      LOGGER.debug("Caching distinct source combinations (within threshold)")
      distinctSource.persist(StorageLevel.MEMORY_AND_DISK)
    }

    try {
      val sourceCount = distinctSource.count()

      // Decide if we should broadcast
      val useBroadcast = DataFrameSizeEstimator.shouldBroadcast(distinctSource, config.enableBroadcastOptimization)
      if (useBroadcast) {
        LOGGER.debug(s"Using broadcast join for lookup table")
      }

      // Add contiguous index to source (0-based)
      val windowSpec = Window.orderBy(lit(1))
      val sourceWithIndex = distinctSource
        .withColumn("_fk_idx", row_number().over(windowSpec) - 1)

      // Add violation flag to target
      val targetWithViolation = if (config.violationRatio > 0) {
        val randExpr = config.seed.map(s => rand(s)).getOrElse(rand())
        targetDf.withColumn("_fk_violation", randExpr < config.violationRatio)
      } else {
        targetDf.withColumn("_fk_violation", lit(false))
      }

      // Assign random index to each target row (0 to sourceCount-1)
      val randExpr = config.seed.map(s => rand(s)).getOrElse(rand())
      val targetWithIndex = targetWithViolation
        .withColumn("_fk_idx", floor(randExpr * sourceCount).cast(LongType))

      // Rename source fields to avoid ambiguity
      val renamedSource = sourceFields.foldLeft(sourceWithIndex) { case (df, field) =>
        df.withColumnRenamed(field, s"_src_$field")
      }

      // Join to get source values
      val sourceForJoin = if (useBroadcast) {
        broadcast(renamedSource)
      } else {
        renamedSource
      }

      val joined = targetWithIndex.join(sourceForJoin, Seq("_fk_idx"), "left")

      // Now update both flat and nested fields using the sampled values
      var resultDf = joined
      relation.fieldMappings.foreach { mapping =>
        val srcColName = s"_src_${mapping.sourceField}"

        if (mapping.isNested) {
          // Nested field - use struct update
          val sampledValue = when(col("_fk_violation"),
            generateViolationValue(
              NestedFieldUtil.getNestedFieldType(targetDf.schema, mapping.targetField),
              config.violationStrategy,
              config.seed
            )
          ).otherwise(col(srcColName))

          resultDf = NestedFieldUtil.updateNestedField(resultDf, mapping.targetField, sampledValue)
        } else {
          // Flat field - direct update
          val sampledValue = when(col("_fk_violation"),
            generateViolationValue(targetDf.schema(mapping.targetField).dataType, config.violationStrategy, config.seed)
          ).otherwise(col(srcColName))

          resultDf = resultDf.withColumn(mapping.targetField, sampledValue)
        }
      }

      // Clean up temporary columns and return only original schema
      resultDf.select(targetDf.columns.map(col): _*)

    } finally {
      if (shouldCache) {
        distinctSource.unpersist()
      }
    }
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
