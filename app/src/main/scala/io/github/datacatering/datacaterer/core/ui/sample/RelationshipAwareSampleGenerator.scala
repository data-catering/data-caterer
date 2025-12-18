package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Plan, Step}
import io.github.datacatering.datacaterer.core.foreignkey.ForeignKeyProcessor
import io.github.datacatering.datacaterer.core.foreignkey.model.ForeignKeyContext
import io.github.datacatering.datacaterer.core.generator.DataGeneratorFactory
import io.github.datacatering.datacaterer.core.ui.service.{DataFrameManager, TaskLoaderService}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Handles sample data generation with foreign key relationship awareness
 *
 * This class extracts the complex relationship-aware generation logic from FastSampleGenerator,
 * providing a focused implementation for scenarios where foreign key relationships between
 * steps need to be maintained.
 *
 * Key responsibilities:
 * - Coordinate generation of multiple related steps
 * - Apply foreign key relationships across DataFrames
 * - Manage DataFrame caching and cleanup
 * - Handle fallback to individual generation on errors
 *
 * The relationship-aware generation process:
 * 1. Load all plan steps to establish relationship context
 * 2. Generate DataFrames for all steps
 * 3. Apply foreign key relationships using ForeignKeyUtil
 * 4. Extract requested steps from relationship-processed data
 * 5. Clean up cached DataFrames to prevent memory leaks
 */
object RelationshipAwareSampleGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Generate samples with relationship-aware processing
   *
   * This is the unified method that handles both individual and relationship-aware generation efficiently.
   *
   * @param plan Optional plan for relationship context
   * @param requestedSteps Steps to generate with their data source names
   * @param sampleSize Optional sample size override
   * @param fastMode Whether to use fast generation mode
   * @param enableRelationships Whether to enable foreign key relationship processing
   * @param factory DataGeneratorFactory instance for data generation
   * @param taskDirectory Optional custom task directory
   * @return Map of step keys to (Step, SampleResponseWithDataFrame) tuples
   */
  def generateSamplesWithRelationships(
    plan: Option[Plan],
    requestedSteps: List[(Step, String)], // (step, dataSourceName)
    sampleSize: Option[Int],
    fastMode: Boolean,
    enableRelationships: Boolean,
    factory: DataGeneratorFactory,
    taskDirectory: Option[String] = None
  )(implicit sparkSession: SparkSession): Map[String, (Step, FastSampleGenerator.SampleResponseWithDataFrame)] = {

    if (!enableRelationships || plan.isEmpty) {
      // Simple case: generate each step independently
      LOGGER.debug(s"Generating ${requestedSteps.size} steps independently (relationships disabled or no plan)")
      return generateIndependentSteps(requestedSteps, sampleSize, fastMode, factory)
    }

    // Complex case: generate all plan steps together to handle relationships
    LOGGER.info(s"Generating samples with relationships for plan: ${plan.get.name}")

    try {
      generateWithRelationships(plan.get, requestedSteps, sampleSize, fastMode, factory, taskDirectory)
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Error in relationship-aware generation, falling back to individual generation: ${ex.getMessage}", ex)
        // Fallback to individual generation
        generateIndependentSteps(requestedSteps, sampleSize, fastMode, factory)
    }
  }

  /**
   * Generate steps independently without relationship processing
   */
  private def generateIndependentSteps(
    requestedSteps: List[(Step, String)],
    sampleSize: Option[Int],
    fastMode: Boolean,
    factory: DataGeneratorFactory
  )(implicit sparkSession: SparkSession): Map[String, (Step, FastSampleGenerator.SampleResponseWithDataFrame)] = {
    requestedSteps.map { case (step, dataSourceName) =>
      val format = step.options.getOrElse("format", "json")
      // Cap the sample size even for independent generation to prevent huge HTTP payloads
      val cappedSampleSize = SampleSizeCalculator.getCappedSampleSize(sampleSize, Some(step))
      LOGGER.debug(s"Capped sample size for ${step.name}: $cappedSampleSize (sampleSize=$sampleSize, step.count.records=${step.count.records})")

      val responseWithDf = generateSingleStep(step, cappedSampleSize, fastMode, format, factory)
      val key = s"$dataSourceName/${step.name}"
      (key, (step, responseWithDf))
    }.toMap
  }

  /**
   * Generate steps with foreign key relationship processing
   */
  private def generateWithRelationships(
    plan: Plan,
    requestedSteps: List[(Step, String)],
    sampleSize: Option[Int],
    fastMode: Boolean,
    factory: DataGeneratorFactory,
    taskDirectory: Option[String]
  )(implicit sparkSession: SparkSession): Map[String, (Step, FastSampleGenerator.SampleResponseWithDataFrame)] = {

    // Generate data for all plan steps to establish relationship context
    val allPlanSteps = loadAllPlanSteps(plan, taskDirectory)

    LOGGER.debug(s"All plan steps: ${allPlanSteps.size} steps: ${allPlanSteps.map { case (step, ds) => s"$ds.${step.name}" }.mkString(", ")}")
    LOGGER.debug(s"Requested steps: ${requestedSteps.size} steps: ${requestedSteps.map { case (step, ds) => s"$ds.${step.name}" }.mkString(", ")}")

    // Generate DataFrames for all plan steps
    val allGeneratedData = generateAllPlanSteps(allPlanSteps, sampleSize, factory)

    LOGGER.info(s"Generated ${allGeneratedData.size} DataFrames, applying foreign key relationships")

    // Apply foreign key relationships
    val dataFramesWithForeignKeys = if (plan.sinkOptions.exists(_.foreignKeys.nonEmpty)) {
      val fkProcessor = new ForeignKeyProcessor()
      val fkConfig = io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig()
      val fkContext = ForeignKeyContext(plan, allGeneratedData, executableTasks = None, fkConfig)
      val fkResult = fkProcessor.process(fkContext)
      fkResult.dataFrames
    } else {
      allGeneratedData.toList
    }
    val updatedDataMap = dataFramesWithForeignKeys.toMap

    try {
      // Convert requested steps to response format using relationship-aware data
      val results = convertToResponses(requestedSteps, updatedDataMap, sampleSize, fastMode, factory)
      results
    } finally {
      // Clean up cached DataFrames to prevent memory leaks
      LOGGER.debug(s"Cleaning up ${allGeneratedData.size} cached DataFrames")
      val unpersisted = DataFrameManager.unpersistMultiple(allGeneratedData.keys.toList)
      LOGGER.debug(s"Unpersisted $unpersisted DataFrames")
    }
  }

  /**
   * Load all steps from a plan (across all tasks)
   */
  private def loadAllPlanSteps(
    plan: Plan,
    taskDirectory: Option[String]
  )(implicit sparkSession: SparkSession): List[(Step, String)] = {
    plan.tasks.filter(_.enabled).flatMap { planTask =>
      try {
        val task = TaskLoaderService.findTaskByName(planTask.name, taskDirectory)
        val steps = task.steps.filter(_.enabled).map((_, planTask.dataSourceName))
        LOGGER.debug(s"Found ${steps.size} steps in task ${planTask.name}: ${steps.map(_._1.name).mkString(", ")}")
        steps
      } catch {
        case ex: Exception =>
          LOGGER.warn(s"Failed to load task ${planTask.name}: ${ex.getMessage}")
          List.empty
      }
    }
  }

  /**
   * Generate DataFrames for all plan steps and cache them
   */
  private def generateAllPlanSteps(
    allPlanSteps: List[(Step, String)],
    sampleSize: Option[Int],
    factory: DataGeneratorFactory
  )(implicit sparkSession: SparkSession): Map[String, org.apache.spark.sql.DataFrame] = {
    allPlanSteps.map { case (step, dataSourceName) =>
      // Cap the sample size to prevent huge HTTP payloads
      val cappedSampleSize = SampleSizeCalculator.getCappedSampleSize(sampleSize, Some(step))
      LOGGER.debug(s"Generating step ${step.name} with cappedSampleSize=$cappedSampleSize (sampleSize=$sampleSize, step.count.records=${step.count.records})")

      val df = factory.generateDataForStep(step, dataSourceName, 0L, cappedSampleSize)
      val stepKey = s"$dataSourceName.${step.name}"
      LOGGER.debug(s"Generated step ${step.name}: ${df.count()} records")

      // Use DataFrameManager to track cached DataFrames for proper cleanup
      val cachedDf = DataFrameManager.cacheDataFrame(stepKey, df)
      (stepKey, cachedDf)
    }.toMap
  }

  /**
   * Convert requested steps to SampleResponse format using relationship-aware data
   */
  private def convertToResponses(
    requestedSteps: List[(Step, String)],
    updatedDataMap: Map[String, org.apache.spark.sql.DataFrame],
    sampleSize: Option[Int],
    fastMode: Boolean,
    factory: DataGeneratorFactory
  )(implicit sparkSession: SparkSession): Map[String, (Step, FastSampleGenerator.SampleResponseWithDataFrame)] = {
    requestedSteps.map { case (step, dataSourceName) =>
      val stepKey = s"$dataSourceName.${step.name}"
      LOGGER.debug(s"Looking for step key: $stepKey in updatedDataMap with keys: ${updatedDataMap.keys.mkString(", ")}")

      val df = updatedDataMap.getOrElse(stepKey, {
        LOGGER.warn(s"Step $stepKey not found in relationship-processed data, falling back to individual generation")
        val cappedSampleSize = SampleSizeCalculator.getCappedSampleSize(sampleSize, Some(step))
        factory.generateDataForStep(step, dataSourceName, 0L, cappedSampleSize)
      })

      LOGGER.debug(s"Using dataframe for step $stepKey with ${df.count()} records")

      val format = step.options.getOrElse("format", "json")
      // Use the capped sample size for metadata (consistent with what was actually generated)
      val cappedSampleSize = SampleSizeCalculator.getCappedSampleSize(sampleSize, Some(step))
      val responseWithDf = SampleDataConverter.convertDataFrameToSampleResponse(
        df, cappedSampleSize, fastMode, format, enableRelationships = true, step
      )
      val responseKey = s"$dataSourceName/${step.name}"
      (responseKey, (step, responseWithDf))
    }.toMap
  }

  /**
   * Generate a single step without relationships (helper method)
   */
  private def generateSingleStep(
    step: Step,
    effectiveSampleSize: Int,
    fastMode: Boolean,
    format: String,
    factory: DataGeneratorFactory
  )(implicit sparkSession: SparkSession): FastSampleGenerator.SampleResponseWithDataFrame = {
    val df = factory.generateDataForStep(step, "sample", 0L, effectiveSampleSize.toLong)
    SampleDataConverter.convertDataFrameToSampleResponse(
      df, effectiveSampleSize, fastMode, format, enableRelationships = false, step
    )
  }
}
