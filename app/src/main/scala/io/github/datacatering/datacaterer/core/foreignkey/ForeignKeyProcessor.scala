package io.github.datacatering.datacaterer.core.foreignkey

import io.github.datacatering.datacaterer.api.model.ForeignKey
import io.github.datacatering.datacaterer.core.exception.MissingDataSourceFromForeignKeyException
import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import io.github.datacatering.datacaterer.core.foreignkey.model._
import io.github.datacatering.datacaterer.core.foreignkey.strategy.{CardinalityStrategy, DistributedSamplingStrategy, GenerationModeStrategy, NullabilityStrategy}
import io.github.datacatering.datacaterer.core.foreignkey.util.InsertOrderCalculator
import io.github.datacatering.datacaterer.core.foreignkey.validator.ForeignKeyValidator
import io.github.datacatering.datacaterer.core.model.ForeignKeyWithGenerateAndDelete
import io.github.datacatering.datacaterer.core.util.PlanImplicits.ForeignKeyRelationOps
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Main processor for foreign key operations.
 *
 * This class coordinates the application of foreign key relationships across DataFrames.
 * It provides a clean API that encapsulates validation, strategy selection, and execution.
 *
 * Architecture:
 * - Validates foreign key relationships and data compatibility
 * - Selects appropriate strategies based on field types and configuration
 * - Applies foreign keys using distributed, scalable approaches
 * - Calculates insertion order for proper referential integrity
 *
 * @param useV2 Whether to use V2 implementation (default: true)
 */
class ForeignKeyProcessor(useV2: Boolean = true) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Strategy instances
  private val distributedSamplingStrategy = new DistributedSamplingStrategy()
  private val cardinalityStrategy = new CardinalityStrategy()
  private val nullabilityStrategy = new NullabilityStrategy()

  /**
   * Process foreign key relationships for all DataFrames in the plan.
   *
   * @param context Foreign key context with plan, data, and configuration
   * @return Foreign key result with updated DataFrames and insertion order
   */
  def process(context: ForeignKeyContext): ForeignKeyResult = {
    if (useV2) {
      LOGGER.debug("Using ForeignKeyProcessor V2 implementation")
      processV2(context)
    } else {
      LOGGER.debug("Using ForeignKeyProcessor V1 implementation (deprecated)")
      processV1(context)
    }
  }

  /**
   * V2 implementation using new strategy-based architecture.
   */
  private def processV2(context: ForeignKeyContext): ForeignKeyResult = {
    val plan = context.plan
    val generatedDataMap = context.generatedData
    val executableTasks = context.executableTasks

    val enabledSources = plan.tasks.filter(_.enabled).map(_.dataSourceName)
    val sinkOptions = plan.sinkOptions.get

    // Process each foreign key independently with its own configuration
    // DO NOT use gatherForeignKeyRelations as it aggregates ALL FKs with the same source,
    // which causes configuration cross-contamination
    val foreignKeyRelations = sinkOptions.foreignKeys
      .map(fk => {
        val fkDetails = ForeignKeyWithGenerateAndDelete(fk.source, fk.generate, fk.delete)
        (fk, fkDetails)
      })

    // Filter to enabled and valid foreign keys
    val enabledForeignKeys = foreignKeyRelations
      .filter(fkPair => ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkPair._2))

    var taskDfs = context.generatedData.toList

    // Apply foreign keys
    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap { case (originalFk, foreignKeyDetails) =>
      val sourceDfName = foreignKeyDetails.source.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")

      val optSourceDf = taskDfs.find(task => task._1.equalsIgnoreCase(sourceDfName))
      if (optSourceDf.isEmpty) {
        throw MissingDataSourceFromForeignKeyException(sourceDfName)
      }
      val sourceDf = optSourceDf.get._2

      val sourceDfsWithForeignKey = foreignKeyDetails.generationLinks.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, target=$targetDfName")

        val optTargetDf = taskDfs.find(task => task._1.equalsIgnoreCase(targetDfName))
        if (optTargetDf.isEmpty) {
          throw MissingDataSourceFromForeignKeyException(targetDfName)
        }

        val targetDf = optTargetDf.get._2

        if (ForeignKeyValidator.targetContainsAllFields(target.fields, targetDf)) {
          LOGGER.info(s"Applying foreign key values to target data source using V2, source-data=${foreignKeyDetails.source.dataSource}, target-data=${target.dataSource}")

          // Extract configuration
          val seed = sinkOptions.seed.map(_.toLong)
          val fkConfig = ForeignKeyConfig(
            violationRatio = 0.0,
            violationStrategy = "random",
            enableBroadcastOptimization = true,
            cacheThresholdMB = 200,
            seed = seed,
            cardinality = originalFk.cardinality,
            nullability = originalFk.nullability
          )

          // Look up target step for perField count
          val optTargetStep = executableTasks.flatMap(tasks =>
            tasks
              .find(_._1.dataSourceName == target.dataSource)
              .flatMap(_._2.steps.find(_.name == target.step))
          )
          val targetPerFieldCount = optTargetStep.flatMap(step => step.count.perField)

          // Create enhanced relation
          val relation = EnhancedForeignKeyRelation(
            sourceDataFrameName = sourceDfName,
            sourceFields = foreignKeyDetails.source.fields,
            targetDataFrameName = targetDfName,
            targetFields = target.fields,
            config = fkConfig,
            targetPerFieldCount = targetPerFieldCount
          )

          // Apply FKs using V2 (delegating to existing ForeignKeyUtilV2 for now)
          // This will be replaced with strategy pattern in Phase 4
          val resultDf = applyForeignKeysV2(sourceDf, targetDf, relation, originalFk)

          if (!resultDf.storageLevel.useMemory) resultDf.cache()
          (targetDfName, resultDf)
        } else {
          LOGGER.warn(s"Foreign key data source does not contain all foreign key(s) defined in plan, defaulting to base generated data, " +
            s"target-foreign-key-fields=${target.fields.mkString(",")}, target-columns=${targetDf.columns.mkString(",")}")
          (targetDfName, targetDf)
        }
      })

      // Replace entries in taskDfs instead of appending to avoid duplicates
      sourceDfsWithForeignKey.foreach { case (dfName, df) =>
        taskDfs = taskDfs.filterNot(_._1.equalsIgnoreCase(dfName)) :+ (dfName, df)
      }
      sourceDfsWithForeignKey
    }

    // Calculate insertion order
    val insertOrder = InsertOrderCalculator.getInsertOrder(
      foreignKeyRelations.map(f => (f._2.source.dataFrameName, f._2.generationLinks.map(_.dataFrameName)))
    )

    val insertOrderDfs = insertOrder
      .map(s => {
        foreignKeyAppliedDfs.find(f => f._1.equalsIgnoreCase(s))
          .getOrElse(s -> taskDfs.find(t => t._1.equalsIgnoreCase(s)).get._2)
      })

    val nonForeignKeyTasks = taskDfs.filter(t => !insertOrderDfs.exists(_._1.equalsIgnoreCase(t._1)))

    ForeignKeyResult(
      dataFrames = insertOrderDfs ++ nonForeignKeyTasks,
      insertOrder = insertOrder
    )
  }

  /**
   * Apply foreign keys using V2 implementation with strategy composition.
   * Uses specialized strategies for cardinality, generation mode, and nullability.
   */
  private def applyForeignKeysV2(
    sourceDf: DataFrame,
    targetDf: DataFrame,
    relation: EnhancedForeignKeyRelation,
    originalFk: ForeignKey
  ): DataFrame = {

    // Determine if cardinality was already handled via perField count
    val cardinalityHandledViaPerField = originalFk.cardinality.isDefined &&
      relation.targetPerFieldCount.isDefined && {
        val perFieldNames = relation.targetPerFieldCount.get.fieldNames
        relation.targetFields.exists(perFieldNames.contains)
      }

    var resultDf = targetDf

    // Step 1: Apply cardinality if needed
    if (cardinalityHandledViaPerField) {
      LOGGER.info("Cardinality already handled via perField count - using group-based FK assignment")
      resultDf = cardinalityStrategy.apply(sourceDf, resultDf, relation)

      // Apply generation mode if compatible with cardinality
      val generationMode = originalFk.generationMode.getOrElse("all-exist")
      resultDf = applyGenerationMode(resultDf, generationMode, originalFk, relation)

      // Apply nullability if specified (not in partial mode)
      if (originalFk.nullability.isDefined && generationMode != "partial") {
        LOGGER.info(s"Applying nullability configuration: ${originalFk.nullability.get}")
        resultDf = nullabilityStrategy.postProcess(resultDf, relation)
      }
    } else {
      // Standard FK processing

      // Step 1: Handle cardinality if specified
      if (originalFk.cardinality.isDefined) {
        LOGGER.info(s"Applying cardinality configuration for FK: ${originalFk.cardinality.get}")
        resultDf = cardinalityStrategy.apply(sourceDf, resultDf, relation)
      }

      // Step 2: Apply foreign keys based on generation mode
      val generationMode = originalFk.generationMode.getOrElse("all-exist")
      val generationModeStrategy = GenerationModeStrategy.forMode(generationMode)
      resultDf = generationModeStrategy.apply(sourceDf, resultDf, relation)

      // Step 3: Apply nullability if specified (not in partial mode)
      if (originalFk.nullability.isDefined && generationMode != "partial") {
        LOGGER.info(s"Applying nullability configuration: ${originalFk.nullability.get}")
        resultDf = nullabilityStrategy.postProcess(resultDf, relation)
      }
    }

    resultDf
  }

  /**
   * Apply generation mode for cardinality-aware scenarios.
   *
   * This is a specialized helper for when cardinality is handled via perField count.
   * In this case, the FK values are already assigned by CardinalityStrategy, and we only
   * need to apply violations/nulls on top of the existing structure.
   *
   * Note: This could potentially be refactored into a dedicated strategy in the future,
   * but for now it's kept as a helper method since it's only used in one specific scenario.
   */
  private def applyGenerationMode(
    df: DataFrame,
    generationMode: String,
    originalFk: ForeignKey,
    relation: EnhancedForeignKeyRelation
  ): DataFrame = {
    import org.apache.spark.sql.functions._

    generationMode.toLowerCase match {
      case "partial" =>
        LOGGER.info("Applying partial mode violations while preserving cardinality structure")
        val violationRatio = originalFk.nullability.map(_.nullPercentage).getOrElse(0.1)
        val violationStrategy = if (originalFk.nullability.isDefined) "null" else "random"

        val randExpr = relation.config.seed.map(s => rand(s)).getOrElse(rand())
        val withViolationFlag = df.withColumn("_fk_violation", randExpr < lit(violationRatio))

        var resultWithViolations = withViolationFlag
        relation.targetFields.foreach { field =>
          val dataType = resultWithViolations.schema(field).dataType
          val violationValue = if (violationStrategy == "null") {
            lit(null).cast(dataType)
          } else {
            relation.config.seed match {
              case Some(s) => concat(lit("INVALID_"), expr(s"MD5(CONCAT('$s', CAST(monotonically_increasing_id() AS STRING)))"))
              case None => concat(lit("INVALID_"), expr("uuid()"))
            }
          }

          resultWithViolations = resultWithViolations.withColumn(field,
            when(col("_fk_violation"), violationValue)
              .otherwise(col(field))
          )
        }
        resultWithViolations.drop("_fk_violation")

      case "all-combinations" =>
        LOGGER.warn("all-combinations mode is incompatible with cardinality - using all-exist mode instead")
        df

      case _ => // "all-exist" or default
        LOGGER.debug("Using all-exist mode: cardinality already ensures all FKs are valid")
        df
    }
  }

  /**
   * V1 implementation (deprecated, no longer supported).
   *
   * V1 has been removed as part of the foreign key refactoring.
   * All code should use V2 (which is the default).
   */
  private def processV1(context: ForeignKeyContext): ForeignKeyResult = {
    LOGGER.warn("V1 foreign key implementation has been removed. Falling back to V2 implementation.")
    processV2(context)
  }
}

object ForeignKeyProcessor {
  /**
   * Create a processor with default settings (V2 enabled).
   */
  def apply(): ForeignKeyProcessor = new ForeignKeyProcessor(useV2 = true)

  /**
   * Create a processor with specified version.
   */
  def apply(useV2: Boolean): ForeignKeyProcessor = new ForeignKeyProcessor(useV2)
}
