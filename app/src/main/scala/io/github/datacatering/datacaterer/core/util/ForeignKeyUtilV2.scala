package io.github.datacatering.datacaterer.core.util

import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.annotation.tailrec

/**
 * Next-generation foreign key utility with improved performance and flexibility.
 *
 * Key improvements:
 * 1. Eliminates collect() bottleneck via distributed sampling
 * 2. Supports configurable integrity violations
 * 3. Automatic broadcast optimization for small dimension tables
 * 4. Handles flat, nested, and mixed field scenarios efficiently
 * 5. Maintains referential integrity guarantees with optional violations
 *
 * Design Philosophy:
 * - Stay distributed: Never collect large datasets to driver
 * - Leverage Spark's optimizer: Use joins instead of SQL expression generation
 * - Support real-world testing: Allow controlled integrity violations
 * - Smart caching: Cache only when beneficial
 */
object ForeignKeyUtilV2 {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Configuration constants
  private val BROADCAST_THRESHOLD_ROWS = 100000  // Broadcast if source has < 100K distinct keys
  private val CACHE_SIZE_THRESHOLD_MB = 200      // Cache only if estimated size < 200MB
  private val SAMPLE_RATIO_FOR_SIZE_ESTIMATE = 0.01

  /**
   * Configuration for foreign key generation behavior.
   *
   * @param violationRatio Fraction of records to generate with invalid foreign keys (0.0 = all valid, 0.1 = 10% invalid)
   * @param violationStrategy How to generate invalid foreign keys: "random", "null", "out_of_range"
   * @param enableBroadcastOptimization Whether to use broadcast joins for small dimension tables
   * @param cacheThresholdMB Only cache DataFrames smaller than this threshold
   * @param seed Optional seed for random number generation to ensure deterministic behavior
   */
  case class ForeignKeyConfig(
    violationRatio: Double = 0.0,
    violationStrategy: String = "random",
    enableBroadcastOptimization: Boolean = true,
    cacheThresholdMB: Long = CACHE_SIZE_THRESHOLD_MB,
    seed: Option[Long] = None
  )

  /**
   * Apply foreign key values from source to target DataFrame using distributed approach.
   *
   * This method NEVER collects data to the driver, instead using distributed joins
   * for sampling. Supports both flat and nested fields efficiently.
   *
   * @param sourceDf Source DataFrame containing foreign key values
   * @param targetDf Target DataFrame to populate with foreign key values
   * @param sourceFields List of source field names to sample from
   * @param targetFields List of target field names to populate (must match sourceFields length)
   * @param config Configuration for FK generation behavior
   * @return Target DataFrame with foreign key values populated
   */
  def applyForeignKeysToTargetDf(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    sourceFields: List[String],
    targetFields: List[String],
    config: ForeignKeyConfig = ForeignKeyConfig()
  ): DataFrame = {

    require(sourceFields.length == targetFields.length,
      s"Source and target field counts must match: source=${sourceFields.length}, target=${targetFields.length}")

    LOGGER.info(s"Applying foreign keys: source fields=${sourceFields.mkString(",")}, target fields=${targetFields.mkString(",")}")
    LOGGER.info(s"FK Config: violations=${config.violationRatio}, strategy=${config.violationStrategy}")

    // Separate nested and flat fields
    val fieldMappings = sourceFields.zip(targetFields)
    val nestedMappings = fieldMappings.filter(_._2.contains("."))
    val flatMappings = fieldMappings.filter(!_._2.contains("."))

    LOGGER.debug(s"Field analysis: flat=${flatMappings.length}, nested=${nestedMappings.length}")

    // Determine approach based on field types
    if (nestedMappings.isEmpty && flatMappings.nonEmpty) {
      // Pure flat fields - use optimized flat field approach
      applyFlatFieldForeignKeys(sourceDf, targetDf, flatMappings, config)
    } else if (nestedMappings.nonEmpty) {
      // Has nested fields - use unified distributed sampling approach
      applyDistributedSamplingForeignKeys(sourceDf, targetDf, fieldMappings, config)
    } else {
      // No fields to process
      LOGGER.warn("No foreign key fields to process")
      targetDf
    }
  }

  /**
   * Optimized approach for flat fields only using crossJoin with sampling.
   *
   * This is the fastest path for simple foreign key relationships without nesting.
   * Uses broadcast joins automatically for small dimension tables.
   */
  private def applyFlatFieldForeignKeys(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    fieldMappings: List[(String, String)],
    config: ForeignKeyConfig
  ): DataFrame = {

    val sourceFields = fieldMappings.map(_._1)
    val targetFields = fieldMappings.map(_._2)

    LOGGER.info(s"Using optimized flat field approach for ${fieldMappings.length} fields")

    // Get distinct source values
    val distinctSource = sourceDf.select(sourceFields.map(col): _*).distinct()

    // Smart caching decision
    val shouldCache = shouldCacheDataFrame(distinctSource, config.cacheThresholdMB)
    if (shouldCache) {
      LOGGER.debug("Caching distinct source values (within threshold)")
      distinctSource.persist(StorageLevel.MEMORY_AND_DISK)
    }

    try {
      // Collect source count to determine sampling strategy
      val sourceCount = distinctSource.count()

      // Decide if we should broadcast (for small dimension tables)
      val useBroadcast = if (config.enableBroadcastOptimization) {
        val shouldBroadcast = sourceCount < BROADCAST_THRESHOLD_ROWS
        if (shouldBroadcast) {
          LOGGER.info(s"Using broadcast for small dimension table (rows: $sourceCount)")
        }
        shouldBroadcast
      } else {
        false
      }

      // Add contiguous index to source for sampling (0-based)
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

      // For each target row, assign an index
      // If no violations, try to use distinct key mapping (like V1) to preserve target's original distribution
      // Otherwise, use random sampling for better distribution with violations
      val targetWithSample = if (config.violationRatio == 0.0) {
        // Check if distinct key mapping makes sense
        val targetFields = fieldMappings.map(_._2)
        val distinctTargetKeys = targetDf.select(targetFields.map(col): _*).distinct()
        val distinctCount = distinctTargetKeys.count()

        // Only use distinct key mapping if target has meaningful variance (>1 distinct value)
        // If all target values are the same (e.g., all "PLACEHOLDER"), fall back to random for better distribution
        if (distinctCount > 1) {
          // Distinct key mapping: map distinct target values to distinct source values
          // This preserves the frequency distribution from the target's original values
          val targetWindowSpec = Window.orderBy(targetFields.map(col): _*)
          val distinctTargetWithIndex = distinctTargetKeys
            .withColumn("_fk_idx", (row_number().over(targetWindowSpec) - 1) % sourceCount)

          // Join target with distinct mapping to get indices
          targetWithViolation.join(distinctTargetWithIndex, targetFields, "left")
        } else {
          // Random: all target values are the same, so use random distribution
          val randExpr = config.seed.map(s => rand(s)).getOrElse(rand())
          targetWithViolation
            .withColumn("_fk_idx", floor(randExpr * sourceCount).cast(LongType))
        }
      } else {
        // Random: pick a random source index (0 to sourceCount-1)
        val randExpr = config.seed.map(s => rand(s)).getOrElse(rand())
        targetWithViolation
          .withColumn("_fk_idx", floor(randExpr * sourceCount).cast(LongType))
      }

      // Join on the index
      val sourceForJoin = if (useBroadcast) {
        broadcast(sourceWithIndex)
      } else {
        sourceWithIndex
      }

      // Rename source fields to avoid ambiguity
      val renamedSource = sourceFields.foldLeft(sourceForJoin) { case (df, field) =>
        df.withColumnRenamed(field, s"_src_$field")
      }

      val joined = targetWithSample.join(renamedSource, Seq("_fk_idx"), "left")

      // Update target fields with source values or violations
      var result = joined
      fieldMappings.foreach { case (sourceField, targetField) =>
        val srcColName = s"_src_$sourceField"
        val updatedValue = when(col("_fk_violation"),
          generateViolationValue(targetDf.schema(targetField).dataType, config.violationStrategy, config.seed)
        ).otherwise(col(srcColName))

        result = result.withColumn(targetField, updatedValue)
      }

      // Clean up temporary columns
      result.select(targetDf.columns.map(col): _*)

    } finally {
      if (shouldCache) {
        distinctSource.unpersist()
      }
    }
  }

  /**
   * Distributed sampling approach that works for any combination of flat/nested fields.
   *
   * Uses simple join with index instead of complex window functions.
   * This is the unified approach that handles all scenarios efficiently.
   */
  private def applyDistributedSamplingForeignKeys(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    fieldMappings: List[(String, String)],
    config: ForeignKeyConfig
  ): DataFrame = {

    val sourceFields = fieldMappings.map(_._1)
    val targetFields = fieldMappings.map(_._2)

    LOGGER.info(s"Using distributed sampling approach for ${fieldMappings.length} fields (includes nested)")

    // Create a temporary table with all source field combinations
    val distinctSource = sourceDf.select(sourceFields.map(col): _*).distinct()

    // Smart caching
    val shouldCache = shouldCacheDataFrame(distinctSource, config.cacheThresholdMB)
    if (shouldCache) {
      LOGGER.debug("Caching distinct source combinations (within threshold)")
      distinctSource.persist(StorageLevel.MEMORY_AND_DISK)
    }

    try {
      val sourceCount = distinctSource.count()

      // Decide if we should broadcast
      val useBroadcast = if (config.enableBroadcastOptimization) {
        sourceCount < BROADCAST_THRESHOLD_ROWS
      } else {
        false
      }

      if (useBroadcast) {
        LOGGER.info(s"Using broadcast join for lookup table")
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
      fieldMappings.foreach { case (sourceField, targetField) =>
        val srcColName = s"_src_$sourceField"

        if (targetField.contains(".")) {
          // Nested field - use struct update
          val sampledValue = when(col("_fk_violation"),
            generateViolationValue(getNestedFieldType(targetDf.schema, targetField), config.violationStrategy, config.seed)
          ).otherwise(col(srcColName))

          resultDf = updateNestedFieldDistributed(resultDf, targetField, sampledValue)
        } else {
          // Flat field - direct update
          val sampledValue = when(col("_fk_violation"),
            generateViolationValue(targetDf.schema(targetField).dataType, config.violationStrategy, config.seed)
          ).otherwise(col(srcColName))

          resultDf = resultDf.withColumn(targetField, sampledValue)
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
   * Update a nested field using struct operations.
   *
   * This is more efficient than string-based SQL expressions and works
   * at arbitrary nesting depth.
   */
  private def updateNestedFieldDistributed(
    df: DataFrame,
    fieldPath: String,
    newValue: Column
  ): DataFrame = {
    val parts = fieldPath.split("\\.")

    if (parts.length == 1) {
      // Not actually nested
      df.withColumn(fieldPath, newValue)
    } else if (parts.length == 2) {
      // Simple nested case: parent.child
      val parent = parts(0)
      val child = parts(1)

      val parentSchema = df.schema(parent).dataType.asInstanceOf[StructType]
      val updatedFields = parentSchema.fields.map { field =>
        if (field.name == child) {
          newValue.alias(child)
        } else {
          col(s"$parent.${field.name}").alias(field.name)
        }
      }

      df.withColumn(parent, struct(updatedFields: _*))
    } else {
      // Deep nesting: recursively build struct
      updateDeepNestedFieldDistributed(df, parts, newValue)
    }
  }

  /**
   * Handle deep nesting (3+ levels) using recursive struct building.
   */
  private def updateDeepNestedFieldDistributed(
    df: DataFrame,
    pathParts: Array[String],
    newValue: Column
  ): DataFrame = {
    val topLevel = pathParts(0)
    val topLevelSchema = df.schema(topLevel).dataType.asInstanceOf[StructType]

    val updatedStruct = buildNestedStructWithUpdate(
      topLevel,
      pathParts.tail,
      topLevelSchema,
      newValue
    )

    df.withColumn(topLevel, updatedStruct)
  }

  /**
   * Recursively build a struct with a field update at arbitrary depth.
   */
  private def buildNestedStructWithUpdate(
    basePath: String,
    remainingPath: Array[String],
    schema: StructType,
    newValue: Column
  ): Column = {
    if (remainingPath.length == 1) {
      // We've reached the target field
      val targetField = remainingPath(0)
      val updatedFields = schema.fields.map { field =>
        if (field.name == targetField) {
          newValue.alias(targetField)
        } else {
          col(s"$basePath.${field.name}").alias(field.name)
        }
      }
      struct(updatedFields: _*)
    } else {
      // Need to go deeper
      val currentField = remainingPath(0)
      val nestedSchema = schema(currentField).dataType.asInstanceOf[StructType]

      val nestedStruct = buildNestedStructWithUpdate(
        s"$basePath.$currentField",
        remainingPath.tail,
        nestedSchema,
        newValue
      )

      val updatedFields = schema.fields.map { field =>
        if (field.name == currentField) {
          nestedStruct.alias(currentField)
        } else {
          col(s"$basePath.${field.name}").alias(field.name)
        }
      }
      struct(updatedFields: _*)
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

  /**
   * Get the data type of a nested field by traversing the schema.
   */
  private def getNestedFieldType(schema: StructType, fieldPath: String): DataType = {
    val parts = fieldPath.split("\\.")

    @tailrec
    def traverse(currentSchema: StructType, remainingParts: List[String]): DataType = {
      remainingParts match {
        case Nil => throw new IllegalArgumentException(s"Empty field path")
        case head :: Nil =>
          currentSchema(head).dataType
        case head :: tail =>
          currentSchema(head).dataType match {
            case nested: StructType => traverse(nested, tail)
            case ArrayType(elementType: StructType, _) => traverse(elementType, tail)
            case other => throw new IllegalArgumentException(s"Cannot traverse non-struct type: $other")
          }
      }
    }

    traverse(schema, parts.toList)
  }

  /**
   * Decide whether to cache a DataFrame based on estimated size.
   */
  private def shouldCacheDataFrame(df: DataFrame, thresholdMB: Long): Boolean = {
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
   * Estimate the row count of a DataFrame without triggering a full count().
   */
  private def estimateRowCount(df: DataFrame): Long = {
    try {
      val stats = df.queryExecution.analyzed.stats
      if (stats.rowCount.isDefined) {
        stats.rowCount.get.toLong
      } else {
        // Sample a small portion to estimate
        val sampleCount = df.sample(withReplacement = false, SAMPLE_RATIO_FOR_SIZE_ESTIMATE).count()
        (sampleCount / SAMPLE_RATIO_FOR_SIZE_ESTIMATE).toLong
      }
    } catch {
      case _: Exception =>
        // Conservative estimate - don't broadcast
        Long.MaxValue
    }
  }

  /**
   * Generate all combinations of valid and invalid foreign keys for testing.
   *
   * This is useful for comprehensive testing scenarios where you want to
   * generate data with both correct and intentionally broken relationships.
   *
   * @param sourceDf Source DataFrame
   * @param targetDf Target DataFrame
   * @param sourceFields Source field names
   * @param targetFields Target field names
   * @return Two DataFrames: (valid FK records, invalid FK records)
   */
  def generateValidAndInvalidCombinations(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    sourceFields: List[String],
    targetFields: List[String]
  ): (DataFrame, DataFrame) = {

    LOGGER.info("Generating both valid and invalid foreign key combinations")

    // Split target into two halves
    val targetCount = targetDf.count()
    val splitRatio = 0.5

    val targetWithId = targetDf.withColumn("_split_id", monotonically_increasing_id())
    val splitPoint = (targetCount * splitRatio).toLong

    val validTarget = targetWithId.filter(col("_split_id") < splitPoint).drop("_split_id")
    val invalidTarget = targetWithId.filter(col("_split_id") >= splitPoint).drop("_split_id")

    // Generate valid FKs
    val validConfig = ForeignKeyConfig(violationRatio = 0.0)
    val validResult = applyForeignKeysToTargetDf(
      sourceDf, validTarget, sourceFields, targetFields, validConfig
    )

    // Generate invalid FKs
    val invalidConfig = ForeignKeyConfig(violationRatio = 1.0, violationStrategy = "random")
    val invalidResult = applyForeignKeysToTargetDf(
      sourceDf, invalidTarget, sourceFields, targetFields, invalidConfig
    )

    (validResult, invalidResult)
  }
}
