package io.github.datacatering.datacaterer.core.foreignkey

import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation
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
 */
class ForeignKeyProcessor {

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
    val foreignKeyAppliedDfs = enabledForeignKeys.flatMap { case (_, foreignKeyDetails) =>
      val sourceDfName = foreignKeyDetails.source.dataFrameName
      LOGGER.debug(s"Getting source dataframe, source=$sourceDfName")

      val sourceDf: DataFrame = getDataFrame(sourceDfName, taskDfs)

      val sourceDfsWithForeignKey = foreignKeyDetails.generationLinks.map(target => {
        val targetDfName = target.dataFrameName
        LOGGER.debug(s"Getting target dataframe, target=$targetDfName")

        val targetDf = getDataFrame(targetDfName, taskDfs)

        if (ForeignKeyValidator.targetContainsAllFields(target.fields, targetDf)) {
          LOGGER.info(s"Applying foreign key values to target data source, source-data=${foreignKeyDetails.source.dataSource}, target-data=${target.dataSource}")

          // Extract configuration from target relation
          val seed = sinkOptions.seed.map(_.toLong)
          val fkConfig = ForeignKeyConfig(
            enableBroadcastOptimization = true,
            cacheThresholdMB = 200,
            seed = seed,
            cardinality = target.cardinality,
            nullability = target.nullability
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

          // Apply FKs with strategy pattern
          val resultDf = applyForeignKeys(sourceDf, targetDf, relation, target)

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

  private def getDataFrame(dfName: String, taskDfs: List[(String, DataFrame)]) = {
    val optSourceDf = taskDfs.find(task => task._1.equalsIgnoreCase(dfName))
    if (optSourceDf.isEmpty) {
      throw MissingDataSourceFromForeignKeyException(dfName)
    }

    optSourceDf.get._2
  }

  /**
   * Apply foreign keys using implementation with strategy composition.
   * Uses specialized strategies for cardinality, generation mode, and nullability.
   */
  private def applyForeignKeys(
                                sourceDf: DataFrame,
                                targetDf: DataFrame,
                                relation: EnhancedForeignKeyRelation,
                                target: ForeignKeyRelation
                              ): DataFrame = {

    // Determine if perField grouping is needed for FK assignment
    val needsPerFieldGrouping = relation.targetPerFieldCount.isDefined && {
      val perFieldNames = relation.targetPerFieldCount.get.fieldNames
      relation.targetFields.exists(perFieldNames.contains)
    }

    var resultDf = targetDf

    // Step 1: Handle perField grouping or cardinality
    if (needsPerFieldGrouping) {
      LOGGER.info("PerField grouping detected - using group-based FK assignment")
      resultDf = cardinalityStrategy.apply(sourceDf, resultDf, relation)

      // Apply generation mode if compatible with cardinality
      val generationMode = target.generationMode.getOrElse("all-exist")
      resultDf = applyGenerationMode(resultDf, generationMode, target, relation)

      // Apply nullability if specified (not in partial mode)
      if (target.nullability.isDefined && generationMode != "partial") {
        LOGGER.info(s"Applying nullability configuration: ${target.nullability.get}")
        resultDf = nullabilityStrategy.postProcess(resultDf, relation)
      }
    } else {
      // Standard FK processing

      // Step 1: Handle cardinality if specified
      val cardinalityApplied = target.cardinality.isDefined
      if (cardinalityApplied) {
        LOGGER.info(s"Applying cardinality configuration for FK: ${target.cardinality.get}")
        resultDf = cardinalityStrategy.apply(sourceDf, resultDf, relation)
      }

      // Step 2: Apply foreign keys based on generation mode
      val generationMode = target.generationMode.getOrElse("all-exist")
      if (cardinalityApplied) {
        // Cardinality already assigned FKs, only apply generation mode violations/nulls
        LOGGER.info(s"Cardinality already applied, using specialized generation mode handler for mode=$generationMode")
        resultDf = applyGenerationMode(resultDf, generationMode, target, relation)
      } else {
        // No cardinality, use GenerationModeStrategy to assign FKs
        LOGGER.info(s"No cardinality, using GenerationModeStrategy to assign FKs with mode=$generationMode")
        val generationModeStrategy = GenerationModeStrategy.forMode(generationMode)
        resultDf = generationModeStrategy.apply(sourceDf, resultDf, relation)
      }

      // Step 3: Apply nullability if specified (not in partial mode)
      if (target.nullability.isDefined && generationMode != "partial") {
        LOGGER.info(s"Applying nullability configuration: ${target.nullability.get}")
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
                                   target: ForeignKeyRelation,
                                   relation: EnhancedForeignKeyRelation
                                 ): DataFrame = {
    generationMode.toLowerCase match {
      case "partial" =>
        LOGGER.info("Applying partial mode violations while preserving cardinality structure")
        // Use NullabilityStrategy if configured, otherwise just return the dataframe as-is
        if (target.nullability.isDefined) {
          nullabilityStrategy.postProcess(df, relation)
        } else {
          LOGGER.warn("Partial mode specified but no nullability config provided - no violations will be applied")
          df
        }

      case "all-combinations" =>
        LOGGER.warn("all-combinations mode is incompatible with cardinality - using all-exist mode instead")
        df

      case _ => // "all-exist" or default
        LOGGER.info("Using all-exist mode: cardinality already ensures all FKs are valid (keeping assigned values)")
        df
    }
  }

}

object ForeignKeyProcessor {
  /**
   * Create a processor with default settings.
   */
  def apply(): ForeignKeyProcessor = new ForeignKeyProcessor()
}
