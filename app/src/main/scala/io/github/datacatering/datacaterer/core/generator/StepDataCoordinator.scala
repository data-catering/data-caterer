package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_ENABLE_GENERATE_DATA, DEFAULT_ENABLE_REFERENCE_MODE, ENABLE_DATA_GENERATION, ENABLE_REFERENCE_MODE}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.util.{DataSourceReader, StepRecordCount, UniqueFieldsUtil}
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.getDataSourceName
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

/**
 * Coordinates data generation for individual steps.
 * Extracted from BatchDataProcessor to separate data generation concerns.
 * 
 * Responsibilities:
 * - Detect and validate generation vs reference mode
 * - Generate data with proper record counts
 * - Handle unique field constraints
 * - Retry logic for reaching target counts
 * - Read reference data from existing sources
 */
class StepDataCoordinator(
  dataGeneratorFactory: DataGeneratorFactory,
  uniqueFieldUtil: UniqueFieldsUtil,
  connectionConfigsByName: Map[String, Map[String, String]],
  flagsConfig: FlagsConfig
)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val maxRetries = 3

  /**
   * Generate or read data for a single step in a batch.
   * 
   * @param batch Current batch number
   * @param task Task and summary tuple
   * @param step Step configuration
   * @param stepRecords Record tracking for this step
   * @return Tuple of (dataSourceStepName, DataFrame)
   */
  def generateForStep(
    batch: Int,
    task: (TaskSummary, Task),
    step: Step,
    stepRecords: StepRecordCount
  ): (String, DataFrame, StepRecordCount) = {
    val isStepEnabledGenerateData = step.options.get(ENABLE_DATA_GENERATION).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_GENERATE_DATA)
    val isStepEnabledReferenceMode = step.options.get(ENABLE_REFERENCE_MODE).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_REFERENCE_MODE)
    val dataSourceStepName = getDataSourceName(task._1, step)

    // Validate configuration
    if (isStepEnabledReferenceMode && isStepEnabledGenerateData) {
      throw new IllegalArgumentException(
        s"Cannot enable both reference mode and data generation for step: ${step.name} in data source: ${task._1.dataSourceName}. " +
          "Please enable only one mode."
      )
    }

    if (isStepEnabledReferenceMode) {
      val df = readReferenceData(task, step)
      (dataSourceStepName, df, stepRecords)
    } else if (isStepEnabledGenerateData) {
      val (df, updatedStepRecords) = generateData(batch, task, step, dataSourceStepName, stepRecords)
      (dataSourceStepName, df, updatedStepRecords)
    } else {
      LOGGER.debug(s"Step has both data generation and reference mode disabled, data-source=${task._1.dataSourceName}, step-name=${step.name}")
      (dataSourceStepName, sparkSession.emptyDataFrame, stepRecords)
    }
  }

  /**
   * Read reference data from an existing data source.
   */
  def readReferenceData(task: (TaskSummary, Task), step: Step): DataFrame = {
    LOGGER.info(s"Reading reference data for step, data-source=${task._1.dataSourceName}, step-name=${step.name}")
    val dataSourceConfig = connectionConfigsByName.getOrElse(task._1.dataSourceName, Map())

    try {
      // Validate reference mode configuration
      DataSourceReader.validateReferenceMode(step, dataSourceConfig)

      // Read data from the data source
      val referenceDf = DataSourceReader.readDataFromSource(task._1.dataSourceName, step, dataSourceConfig)

      if (referenceDf.schema.isEmpty) {
        LOGGER.warn(s"Reference data source has empty schema, data-source=${task._1.dataSourceName}, step-name=${step.name}")
      }

      val recordCount = if (flagsConfig.enableCount && referenceDf.schema.nonEmpty) {
        referenceDf.count()
      } else {
        -1L // Count disabled or empty schema
      }

      if (recordCount == 0) {
        LOGGER.warn(s"Reference data source contains no records. This may cause issues with foreign key relationships, " +
          s"data-source=${task._1.dataSourceName}, step-name=${step.name}")
      } else if (recordCount > 0) {
        LOGGER.info(s"Successfully loaded reference data, data-source=${task._1.dataSourceName}, step-name=${step.name}, num-records=$recordCount")
      }

      referenceDf
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to read reference data, data-source=${task._1.dataSourceName}, step-name=${step.name}, error=${ex.getMessage}")
        throw new RuntimeException(s"Failed to read reference data for ${task._1.dataSourceName}.${step.name}: ${ex.getMessage}", ex)
    }
  }

  /**
   * Generate all data upfront for a step (used in AllUpfront generation mode).
   * 
   * @param task Task and summary tuple
   * @param step Step configuration
   * @param totalRecords Total records to generate
   * @return Generated DataFrame
   */
  def generateAllUpfront(task: (TaskSummary, Task), step: Step, totalRecords: Long): DataFrame = {
    val dataSourceStepName = getDataSourceName(task._1, step)
    
    LOGGER.info(s"Generating all data upfront for step, data-source=${task._1.dataSourceName}, step-name=${step.name}, total-records=$totalRecords")
    
    val genDf = dataGeneratorFactory.generateDataForStep(step, task._1.dataSourceName, 0, totalRecords)
    val uniqueDf = getUniqueGeneratedRecords(dataSourceStepName, genDf, step)
    genDf.unpersist()
    
    uniqueDf
  }

  /**
   * Generate data for a batch with record tracking and retry logic.
   */
  private def generateData(
    batch: Int,
    task: (TaskSummary, Task),
    step: Step,
    dataSourceStepName: String,
    stepRecords: StepRecordCount
  ): (DataFrame, StepRecordCount) = {
    val currentExpandedRecords = stepRecords.currentNumRecords

    // Calculate precise number of records for this batch to ensure exact total
    val adjustedTotalRecords = stepRecords.numTotalRecords / stepRecords.averagePerCol
    val currentBaseRecords = currentExpandedRecords / stepRecords.averagePerCol
    val remainingAdjustedRecords = adjustedTotalRecords - currentBaseRecords

    val recordsToGenerate = if (remainingAdjustedRecords <= 0) {
      0L
    } else if (stepRecords.remainder > 0 && batch <= stepRecords.remainder) {
      // First 'remainder' batches get base + 1 records
      Math.min(stepRecords.baseRecordsPerBatch + 1, remainingAdjustedRecords)
    } else {
      // Remaining batches get base records
      Math.min(stepRecords.baseRecordsPerBatch, remainingAdjustedRecords)
    }

    // For perField counts, generateDataForStep will expand records via generateRecordsPerField
    // actualRecordsToGenerate is the target number of final records (after perField expansion)
    val actualRecordsToGenerate = recordsToGenerate * stepRecords.averagePerCol
    // startIndex and endIndex are in "base record space" (before perField expansion)
    val startIndex = currentBaseRecords
    val endIndex = startIndex + recordsToGenerate

    LOGGER.debug(s"Batch $batch: startIndex=$startIndex, endIndex=$endIndex, recordsToGenerate=$recordsToGenerate, " +
      s"actualRecordsToGenerate=$actualRecordsToGenerate, remainingAdjustedRecords=$remainingAdjustedRecords, " +
      s"currentExpandedRecords=$currentExpandedRecords, currentBaseRecords=$currentBaseRecords")

    val genDf = dataGeneratorFactory.generateDataForStep(step, task._1.dataSourceName, startIndex, endIndex)
    val initialDf = getUniqueGeneratedRecords(dataSourceStepName, genDf, step)
    if (!initialDf.storageLevel.useMemory) initialDf.cache()
    genDf.unpersist()

    val initialRecordCount = if (flagsConfig.enableCount) initialDf.count() else actualRecordsToGenerate
    val targetNumRecords = actualRecordsToGenerate

    LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${step.name}, " +
      s"target-num-records=$targetNumRecords, actual-num-records=$initialRecordCount, records-to-generate=$recordsToGenerate")

    // If random amount of records, don't try to regenerate more records
    val (finalDf, finalRecordCount) = if (step.count.options.isEmpty && step.count.perField.forall(_.options.isEmpty)) {
      generateRecordsRecursively(batch, step, task, dataSourceStepName, stepRecords, initialDf, initialRecordCount, endIndex, targetNumRecords, 0)
    } else {
      LOGGER.debug("Random amount of records generated, not attempting to generate more records")
      (initialDf, initialRecordCount)
    }

    if (targetNumRecords != finalRecordCount && step.count.options.isEmpty && step.count.perField.forall(_.options.isEmpty)) {
      LOGGER.warn("Unable to reach expected number of records due to reaching max retries. " +
        s"Can be due to limited number of potential unique records, " +
        s"target-num-records=$targetNumRecords, actual-num-records=$finalRecordCount")
    }

    val updatedStepRecords = stepRecords.copy(currentNumRecords = stepRecords.currentNumRecords + finalRecordCount)
    (finalDf, updatedStepRecords)
  }

  /**
   * Recursive function to generate additional records until target is reached.
   */
  @tailrec
  private def generateRecordsRecursively(
    batch: Int,
    step: Step,
    task: (TaskSummary, Task),
    dataSourceStepName: String,
    stepRecords: StepRecordCount,
    currentDf: DataFrame,
    currentRecordCount: Long,
    currentBaseRecordCount: Long,
    targetNumRecords: Long,
    retries: Int
  ): (DataFrame, Long) = {
    LOGGER.debug(s"Record count does not reach expected num records for batch, generating more records until reached, " +
      s"target-num-records=$targetNumRecords, actual-num-records=$currentRecordCount, num-retries=$retries, max-retries=$maxRetries")
    
    if (targetNumRecords == currentRecordCount || retries >= maxRetries) {
      (currentDf, currentRecordCount)
    } else {
      val (newDf, newRecordCount, newBaseRecordCount) = generateAdditionalRecords(
        batch, step, task, dataSourceStepName, stepRecords, currentDf, currentRecordCount, currentBaseRecordCount, targetNumRecords
      )
      generateRecordsRecursively(batch, step, task, dataSourceStepName, stepRecords, newDf, newRecordCount, newBaseRecordCount, targetNumRecords, retries + 1)
    }
  }

  /**
   * Generate additional records to reach target count.
   */
  private def generateAdditionalRecords(
    batch: Int,
    step: Step,
    task: (TaskSummary, Task),
    dataSourceStepName: String,
    stepRecords: StepRecordCount,
    currentDf: DataFrame,
    currentRecordCount: Long,
    currentBaseRecordCount: Long,
    targetNumRecords: Long
  ): (DataFrame, Long, Long) = {
    LOGGER.debug(s"Generating additional records for batch, batch=$batch, step-name=${step.name}, " +
      s"current-record-count=$currentRecordCount, target-num-records=$targetNumRecords")

    if (currentRecordCount >= targetNumRecords) {
      LOGGER.debug(s"No additional records needed, current count meets or exceeds target")
      return (currentDf, currentRecordCount, currentBaseRecordCount)
    }

    // Calculate how many base records we need to generate to reach target
    val expandedRecordsNeeded = targetNumRecords - currentRecordCount
    val baseRecordsNeeded = Math.ceil(expandedRecordsNeeded.toDouble / stepRecords.averagePerCol).toLong
    val newBaseEndIndex = currentBaseRecordCount + baseRecordsNeeded

    val additionalGenDf = dataGeneratorFactory
      .generateDataForStep(step, task._1.dataSourceName, currentBaseRecordCount, newBaseEndIndex)
    val additionalDf = getUniqueGeneratedRecords(dataSourceStepName, additionalGenDf, step)
    if (!additionalDf.storageLevel.useMemory) additionalDf.cache()
    additionalGenDf.unpersist()
    val additionalRecordCount = if (flagsConfig.enableCount) additionalDf.count() else 0
    LOGGER.debug(s"Additional records generated, additional-record-count=$additionalRecordCount")

    // Only union if we actually generated additional records
    val (newDf, newRecordCount, newBaseRecordCount) = if (additionalRecordCount > 0) {
      val unionDf = currentDf.union(additionalDf)
      val finalCount = unionDf.count()
      additionalDf.unpersist()
      (unionDf, finalCount, newBaseEndIndex)
    } else {
      // No additional records were generated, return current DataFrame as-is
      additionalDf.unpersist()
      (currentDf, currentRecordCount, currentBaseRecordCount)
    }

    LOGGER.debug(s"Generated more records for step, batch=$batch, step-name=${step.name}, " +
      s"new-num-records=$additionalRecordCount, actual-num-records=$newRecordCount, current-df-count=${currentDf.count()}")
    (newDf, newRecordCount, newBaseRecordCount)
  }

  /**
   * Apply unique field constraints to generated data.
   */
  private def getUniqueGeneratedRecords(dataSourceStepName: String, genDf: DataFrame, step: Step): DataFrame = {
    if (uniqueFieldUtil.uniqueFieldsDf.exists(u => u._1.getDataSourceName == dataSourceStepName)) {
      LOGGER.debug(s"Ensuring field values are unique since there are fields with isUnique or isPrimaryKey set to true " +
        s"or is defined within foreign keys, data-source-step-name=$dataSourceStepName")
      uniqueFieldUtil.getUniqueFieldsValues(dataSourceStepName, genDf, step)
    } else {
      genDf
    }
  }
}

