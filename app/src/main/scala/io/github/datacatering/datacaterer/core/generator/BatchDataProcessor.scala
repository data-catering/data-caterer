package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_ENABLE_GENERATE_DATA, DEFAULT_ENABLE_REFERENCE_MODE, ENABLE_DATA_GENERATION, ENABLE_REFERENCE_MODE, SAVE_MODE}
import io.github.datacatering.datacaterer.api.model.{DataSourceResult, FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, Plan, Step, Task, TaskSummary, UpstreamDataSourceValidation, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.exception.InvalidRandomSeedException
import io.github.datacatering.datacaterer.core.generator.track.RecordTrackingProcessor
import io.github.datacatering.datacaterer.core.sink.SinkFactory
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.getDataSourceName
import io.github.datacatering.datacaterer.core.util.RecordCountUtil.calculateNumBatches
import io.github.datacatering.datacaterer.core.util.{DataSourceReader, ForeignKeyUtil, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.Serializable
import java.time.{Duration, LocalDateTime}
import java.util.{Locale, Random}
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class BatchDataProcessor(connectionConfigsByName: Map[String, Map[String, String]], foldersConfig: FoldersConfig,
                         metadataConfig: MetadataConfig, flagsConfig: FlagsConfig, generationConfig: GenerationConfig)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val sinkFactory = new SinkFactory(flagsConfig, metadataConfig, foldersConfig)
  private lazy val recordTrackingProcessor = new RecordTrackingProcessor(foldersConfig.recordTrackingFolderPath)
  private lazy val validationRecordTrackingProcessor = new RecordTrackingProcessor(foldersConfig.recordTrackingForValidationFolderPath)
  private lazy val maxRetries = 3

  def splitAndProcess(plan: Plan, executableTasks: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]])
                     (implicit sparkSession: SparkSession): List[DataSourceResult] = {
    val faker = getDataFaker(plan)
    val dataGeneratorFactory = new DataGeneratorFactory(faker, flagsConfig.enableFastGeneration)
    val uniqueFieldUtil = new UniqueFieldsUtil(plan, executableTasks, flagsConfig.enableUniqueCheckOnlyInBatch, generationConfig)
    val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(List())
    var (numBatches, trackRecordsPerStep) = calculateNumBatches(foreignKeys, executableTasks, generationConfig)

    def generateDataForStep(batch: Int, task: (TaskSummary, Task), s: Step): (String, DataFrame) = {
      val isStepEnabledGenerateData = s.options.get(ENABLE_DATA_GENERATION).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_GENERATE_DATA)
      val isStepEnabledReferenceMode = s.options.get(ENABLE_REFERENCE_MODE).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_REFERENCE_MODE)
      val dataSourceStepName = getDataSourceName(task._1, s)
      val dataSourceConfig = connectionConfigsByName.getOrElse(task._1.dataSourceName, Map())
      
      // Validate configuration
      if (isStepEnabledReferenceMode && isStepEnabledGenerateData) {
        throw new IllegalArgumentException(
          s"Cannot enable both reference mode and data generation for step: ${s.name} in data source: ${task._1.dataSourceName}. " +
            "Please enable only one mode."
        )
      }

      if (isStepEnabledReferenceMode) {
        LOGGER.info(s"Reading reference data for step, data-source=${task._1.dataSourceName}, step-name=${s.name}")

        try {
          // Validate reference mode configuration
          DataSourceReader.validateReferenceMode(s, dataSourceConfig)

          // Read data from the data source
          val referenceDf = DataSourceReader.readDataFromSource(task._1.dataSourceName, s, dataSourceConfig)

          if (referenceDf.schema.isEmpty) {
            LOGGER.warn(s"Reference data source has empty schema, data-source=${task._1.dataSourceName}, step-name=${s.name}")
          }

          val recordCount = if (flagsConfig.enableCount && referenceDf.schema.nonEmpty) {
            referenceDf.count()
          } else {
            -1L  // Count disabled or empty schema
          }

          if (recordCount == 0) {
            LOGGER.warn(s"Reference data source contains no records. This may cause issues with foreign key relationships, " +
              s"data-source=${task._1.dataSourceName}, step-name=${s.name}")
          } else if (recordCount > 0) {
            LOGGER.info(s"Successfully loaded reference data, data-source=${task._1.dataSourceName}, step-name=${s.name}, num-records=$recordCount")
          }

          (dataSourceStepName, referenceDf)
        } catch {
          case ex: Exception =>
            LOGGER.error(s"Failed to read reference data, data-source=${task._1.dataSourceName}, step-name=${s.name}, error=${ex.getMessage}")
            throw new RuntimeException(s"Failed to read reference data for ${task._1.dataSourceName}.${s.name}: ${ex.getMessage}", ex)
        }
      } else if (isStepEnabledGenerateData) {
        val recordStepName = s"${task._2.name}_${s.name}"
        val stepRecords = trackRecordsPerStep(recordStepName)
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

        val genDf = dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName, startIndex, endIndex)
        val initialDf = getUniqueGeneratedRecords(uniqueFieldUtil, dataSourceStepName, genDf, s)
        if (!initialDf.storageLevel.useMemory) initialDf.cache()
        genDf.unpersist()

        val initialRecordCount = if (flagsConfig.enableCount) initialDf.count() else actualRecordsToGenerate
        val targetNumRecords = actualRecordsToGenerate

        LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${s.name}, " +
          s"target-num-records=$targetNumRecords, actual-num-records=$initialRecordCount, records-to-generate=$recordsToGenerate")

        // if record count doesn't match expected record count, generate more data
        def generateAdditionalRecords(currentDf: DataFrame, currentRecordCount: Long, currentBaseRecordCount: Long): (DataFrame, Long, Long) = {
          LOGGER.debug(s"Generating additional records for batch, batch=$batch, step-name=${s.name}, " +
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
            .generateDataForStep(s, task._1.dataSourceName, currentBaseRecordCount, newBaseEndIndex)
          val additionalDf = getUniqueGeneratedRecords(uniqueFieldUtil, dataSourceStepName, additionalGenDf, s)
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

          LOGGER.debug(s"Generated more records for step, batch=$batch, step-name=${s.name}, " +
            s"new-num-records=$additionalRecordCount, actual-num-records=$newRecordCount, current-df-count=${currentDf.count()}")
          (newDf, newRecordCount, newBaseRecordCount)
        }

        // Recursive function to generate additional records
        @tailrec
        def generateRecordsRecursively(currentDf: DataFrame, currentRecordCount: Long, currentBaseRecordCount: Long, retries: Int): (DataFrame, Long) = {
          if (targetNumRecords == currentRecordCount || retries >= maxRetries) {
            LOGGER.debug(s"Reached expected num records for batch or max retries, target-num-records=$targetNumRecords, actual-num-records=$currentRecordCount, num-retries=$retries, max-retries=$maxRetries")
            (currentDf, currentRecordCount)
          } else {
            LOGGER.debug(s"Record count does not reach expected num records for batch, generating more records until reached, " +
            s"target-num-records=$targetNumRecords, actual-num-records=$currentRecordCount, num-retries=$retries, max-retries=$maxRetries")
            val (newDf, newRecordCount, newBaseRecordCount) = generateAdditionalRecords(currentDf, currentRecordCount, currentBaseRecordCount)
            generateRecordsRecursively(newDf, newRecordCount, newBaseRecordCount, retries + 1)
          }
        }

        //if random amount of records, don't try to regenerate more records
        val (finalDf, finalRecordCount) = if (s.count.options.isEmpty && s.count.perField.forall(_.options.isEmpty)) {
          generateRecordsRecursively(initialDf, initialRecordCount, endIndex, 0)
        } else {
          LOGGER.debug("Random amount of records generated, not attempting to generate more records")
          (initialDf, initialRecordCount)
        }

        if (targetNumRecords != finalRecordCount && s.count.options.isEmpty && s.count.perField.forall(_.options.isEmpty)) {
          LOGGER.warn("Unable to reach expected number of records due to reaching max retries. " +
            s"Can be due to limited number of potential unique records, " +
            s"target-num-records=$targetNumRecords, actual-num-records=$finalRecordCount")
        }

        trackRecordsPerStep = trackRecordsPerStep ++ Map(recordStepName -> stepRecords.copy(currentNumRecords = stepRecords.currentNumRecords + finalRecordCount))
        (dataSourceStepName, finalDf)
      } else {
        LOGGER.debug(s"Step has both data generation and reference mode disabled, data-source=${task._1.dataSourceName}, step-name=${s.name}")
        (dataSourceStepName, sparkSession.emptyDataFrame)
      }
    }

    val dataSourceResults = (1 to numBatches).flatMap(batch => {
      val startTime = LocalDateTime.now()
      LOGGER.info(s"Starting batch, batch=$batch, num-batches=$numBatches")
      val generatedDataForeachTask = executableTasks.flatMap(task => {
        task._2.steps.filter(_.enabled).map(s => {
          LOGGER.debug(s"Generating data for step, task-name=${task._1.name}, step-name=${s.name}, data-source-name=${task._1.dataSourceName}")
          try {
            generateDataForStep(batch, task, s)
          } catch {
            case ex: Exception =>
              LOGGER.error(s"Failed to generate data for step, task-name=${task._1.name}, step-name=${s.name}, data-source-name=${task._1.dataSourceName}")
              throw ex
          }
        })
      })

      val sinkDf = plan.sinkOptions
        .map(_ => ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask, flagsConfig.enableForeignKeyV2, Some(executableTasks)))
        .getOrElse(generatedDataForeachTask)
      val sinkResults = pushDataToSinks(plan, executableTasks, sinkDf, batch, numBatches, startTime, optValidations)
      sinkDf.foreach(_._2.unpersist())
      sparkSession.sparkContext.getPersistentRDDs.foreach { case (_, rdd) => rdd.unpersist() }
      val endTime = LocalDateTime.now()
      val timeTakenMs = Duration.between(startTime, endTime).toMillis
      LOGGER.info(s"Finished batch, batch=$batch, num-batches=$numBatches, time-taken-ms=$timeTakenMs")
      sinkResults
    }).toList

    LOGGER.debug(s"Completed all batches, num-batches=$numBatches")
    
    // Finalize any pending consolidations for multi-batch scenarios
    sinkFactory.finalizePendingConsolidations()
    
    uniqueFieldUtil.cleanup()
    dataSourceResults
  }

  private def pushDataToSinks(
                               plan: Plan,
                               executableTasks: List[(TaskSummary, Task)],
                               sinkDf: List[(String, DataFrame)],
                               batchNum: Int,
                               numBatches: Int,
                               startTime: LocalDateTime,
                               optValidations: Option[List[ValidationConfiguration]]
                             ): List[DataSourceResult] = {
    val stepAndTaskByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), (s, task._2)))
    ).toMap
    val dataSourcesUsedInValidation = getDataSourcesUsedInValidation(optValidations)

    val nonEmptyDfs = if (flagsConfig.enableCount) sinkDf.filter(s => !s._2.isEmpty) else sinkDf
    
    // Filter out reference data sources - they should not be saved to sinks
    val nonReferenceDfs = nonEmptyDfs.filter { case (dataSourceStepName, _) =>
      val dataSourceName = dataSourceStepName.split("\\.").head
      val (step, _) = stepAndTaskByDataSourceName(dataSourceStepName)
      val isReferenceMode = step.options.get(ENABLE_REFERENCE_MODE).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_REFERENCE_MODE)
      
      if (isReferenceMode) {
        LOGGER.debug(s"Skipping save for reference data source, data-source=$dataSourceName, step-name=${step.name}")
        false
      } else {
        true
      }
    }
    
    nonReferenceDfs.map(df => {
      val dataSourceName = df._1.split("\\.").head
      val (step, task) = stepAndTaskByDataSourceName(df._1)
      val dataSourceConfig = connectionConfigsByName.getOrElse(dataSourceName, Map())

      // Inherit task-level transformation if step doesn't have one configured
      val stepWithTaskTransformation = if (step.transformation.isEmpty && task.transformation.isDefined) {
        step.copy(transformation = task.transformation)
      } else {
        step
      }

      val stepWithDataSourceConfig = stepWithTaskTransformation.copy(options = dataSourceConfig ++ stepWithTaskTransformation.options)
      val stepWithSaveMode = checkSaveMode(batchNum, stepWithDataSourceConfig)

      if (flagsConfig.enableRecordTracking) {
        recordTrackingProcessor.trackRecords(df._2, dataSourceName, plan.name, stepWithSaveMode)
      }
      if (dataSourcesUsedInValidation.contains(dataSourceName)) {
        validationRecordTrackingProcessor.trackRecords(df._2, dataSourceName, plan.name, stepWithSaveMode)
      }

      val isLastBatch = batchNum == numBatches
      val sinkResult = sinkFactory.pushToSink(df._2, dataSourceName, stepWithSaveMode, startTime, numBatches > 1, isLastBatch)
      DataSourceResult(dataSourceName, task, stepWithSaveMode, sinkResult, batchNum)
    })
  }

  private def getDataSourcesUsedInValidation(optValidations: Option[List[ValidationConfiguration]]): List[String] = {
    optValidations.map(validations => validations.flatMap(vc => {
      val baseDataSourcesWithValidation = vc.dataSources.filter(dsv => dsv._2.exists(_.validations.nonEmpty)).keys.toList
      val dataSourcesUsedAsUpstreamValidation = vc.dataSources.flatMap(_._2.flatMap(_.validations.map(_.validation)))
        .map {
          case UpstreamDataSourceValidation(_, upstreamDataSource, _, _, _) => Some(upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
          case _ => None
        }.filter(_.isDefined)
        .map(_.get)
      (baseDataSourcesWithValidation ++ dataSourcesUsedAsUpstreamValidation).distinct
    })).getOrElse(List())
  }

  private def getUniqueGeneratedRecords(
                                         uniqueFieldUtil: UniqueFieldsUtil,
                                         dataSourceStepName: String,
                                         genDf: DataFrame,
                                         step: Step
                                       ): DataFrame = {
    if (uniqueFieldUtil.uniqueFieldsDf.exists(u => u._1.getDataSourceName == dataSourceStepName)) {
      LOGGER.debug(s"Ensuring field values are unique since there are fields with isUnique or isPrimaryKey set to true " +
        s"or is defined within foreign keys, data-source-step-name=$dataSourceStepName")
      uniqueFieldUtil.getUniqueFieldsValues(dataSourceStepName, genDf, step)
    } else {
      genDf
    }
  }

  private def checkSaveMode(batchNum: Int, step: Step): Step = {
    val saveMode = step.options.get(SAVE_MODE)
    saveMode match {
      case Some(value) =>
        if (value.toLowerCase == SaveMode.Overwrite.name().toLowerCase && batchNum > 1) {
          step.copy(options = step.options ++ Map(SAVE_MODE -> SaveMode.Append.name()))
        } else step
      case None => step
    }
  }

  private def getDataFaker(plan: Plan): Faker with Serializable = {
    val optSeed = plan.sinkOptions.flatMap(_.seed)
    val optLocale = plan.sinkOptions.flatMap(_.locale)
    val trySeed = Try(optSeed.map(_.toInt).get)

    (optSeed, trySeed, optLocale) match {
      case (None, _, Some(locale)) =>
        LOGGER.info(s"Locale defined at plan level. All data will be generated with the set locale, locale=$locale")
        new Faker(Locale.forLanguageTag(locale)) with Serializable
      case (Some(_), Success(seed), Some(locale)) =>
        LOGGER.info(s"Seed and locale defined at plan level. All data will be generated with the set seed and locale, seed-value=$seed, locale=$locale")
        new Faker(Locale.forLanguageTag(locale), new Random(seed)) with Serializable
      case (Some(_), Failure(exception), _) => throw InvalidRandomSeedException(exception)
      case _ => new Faker() with Serializable
    }
  }
}

