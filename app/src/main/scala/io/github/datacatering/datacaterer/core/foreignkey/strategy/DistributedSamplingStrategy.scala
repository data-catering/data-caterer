package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import io.github.datacatering.datacaterer.core.foreignkey.util.{DataFrameSizeEstimator, NestedFieldUtil}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
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
    // This strategy works for any field combination (flat, nested, or mixed)
    true
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

      // Assign random index to each target row (0 to sourceCount-1)
      // Use hash-based approach when seed is provided for deterministic behavior across environments
      // (Spark's rand(seed) is partition-dependent and not truly deterministic)
      val targetWithIndex = config.seed match {
        case Some(s) =>
          val allCols = targetDf.columns.map(col)
          val hashExpr = xxhash64(allCols :+ lit(s): _*)
          // Use absolute hash value modulo sourceCount for uniform distribution
          targetDf.withColumn("_fk_idx", abs(hashExpr) % sourceCount)
        case None =>
          targetDf.withColumn("_fk_idx", floor(rand() * sourceCount).cast(LongType))
      }

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
          resultDf = NestedFieldUtil.updateNestedField(resultDf, mapping.targetField, col(srcColName))
        } else {
          // Flat field - direct update
          resultDf = resultDf.withColumn(mapping.targetField, col(srcColName))
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
}
