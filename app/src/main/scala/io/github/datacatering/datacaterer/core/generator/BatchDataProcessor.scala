package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_ENABLE_REFERENCE_MODE, ENABLE_REFERENCE_MODE, SAVE_MODE}
import io.github.datacatering.datacaterer.api.model.{DataSourceResult, FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, Plan, Step, Task, TaskSummary, UpstreamDataSourceValidation, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.exception.InvalidRandomSeedException
import io.github.datacatering.datacaterer.core.foreignkey.ForeignKeyProcessor
import io.github.datacatering.datacaterer.core.foreignkey.model.ForeignKeyContext
import io.github.datacatering.datacaterer.core.generator.execution.{DurationBasedExecutionStrategy, ExecutionStrategy, ExecutionStrategyFactory, GenerationMode, PatternBasedExecutionStrategy}
import io.github.datacatering.datacaterer.core.generator.metrics.PerformanceMetrics
import io.github.datacatering.datacaterer.core.generator.track.RecordTrackingProcessor
import io.github.datacatering.datacaterer.core.sink.{PekkoStreamingSinkWriter, SinkFactory, SinkRouter, SinkStrategy}
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.getDataSourceName
import io.github.datacatering.datacaterer.core.util.{RecordCountUtil, StepRecordCount, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.Serializable
import java.time.{Duration, LocalDateTime}
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

class BatchDataProcessor(connectionConfigsByName: Map[String, Map[String, String]], foldersConfig: FoldersConfig,
                         metadataConfig: MetadataConfig, flagsConfig: FlagsConfig, generationConfig: GenerationConfig)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val sinkFactory = new SinkFactory(flagsConfig, metadataConfig, foldersConfig)
  private lazy val sinkRouter = new SinkRouter()
  private lazy val recordTrackingProcessor = new RecordTrackingProcessor(foldersConfig.recordTrackingFolderPath)
  private lazy val validationRecordTrackingProcessor = new RecordTrackingProcessor(foldersConfig.recordTrackingForValidationFolderPath)

  def splitAndProcess(plan: Plan, executableTasks: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]])
                     (implicit sparkSession: SparkSession): (List[DataSourceResult], Option[PerformanceMetrics]) = {
    val faker = getDataFaker(plan)
    val dataGeneratorFactory = new DataGeneratorFactory(faker, flagsConfig.enableFastGeneration)
    val uniqueFieldUtil = new UniqueFieldsUtil(plan, executableTasks, flagsConfig.enableUniqueCheckOnlyInBatch, generationConfig)
    
    // Create StepDataCoordinator for data generation
    val stepDataCoordinator = new StepDataCoordinator(dataGeneratorFactory, uniqueFieldUtil, connectionConfigsByName, flagsConfig)
    
    // Create execution strategy
    val executionStrategy = ExecutionStrategyFactory.create(plan, executableTasks, generationConfig)

    // Route to appropriate execution mode based on strategy
    executionStrategy.getGenerationMode match {
      case GenerationMode.Batched =>
        LOGGER.info("Using batched generation mode")
        executeBatchedGeneration(plan, executableTasks, optValidations, stepDataCoordinator, executionStrategy)
      
      case GenerationMode.AllUpfront =>
        LOGGER.info("Using all-upfront generation mode for streaming execution")
        executeAllUpfrontGeneration(plan, executableTasks, optValidations, stepDataCoordinator, executionStrategy)
      
      case GenerationMode.Progressive =>
        LOGGER.warn("Progressive generation mode not yet implemented, falling back to batched mode")
        executeBatchedGeneration(plan, executableTasks, optValidations, stepDataCoordinator, executionStrategy)
    }
  }

  /**
   * Execute data generation in batched mode (original behavior).
   * Generates data incrementally per batch and writes to sinks after each batch.
   */
  private def executeBatchedGeneration(
    plan: Plan,
    executableTasks: List[(TaskSummary, Task)],
    optValidations: Option[List[ValidationConfiguration]],
    stepDataCoordinator: StepDataCoordinator,
    executionStrategy: ExecutionStrategy
  )(implicit sparkSession: SparkSession): (List[DataSourceResult], Option[PerformanceMetrics]) = {
    val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(List())
    var (numBatches, trackRecordsPerStep) = RecordCountUtil.calculateNumBatches(foreignKeys, executableTasks, generationConfig)

    var currentBatch = 1
    var dataSourceResults = List[DataSourceResult]()

    while (executionStrategy.shouldContinue(currentBatch)) {
      val startTime = LocalDateTime.now()
      executionStrategy.onBatchStart(currentBatch)

      LOGGER.info(s"Starting batch, batch=$currentBatch")
      
      // Generate data for each task/step
      val (generatedDataForeachTask, updatedTrackRecords) = executableTasks.foldLeft((List[(String, DataFrame)](), trackRecordsPerStep)) {
        case ((accData, accTracking), task) =>
          val (taskData, updatedTaskTracking) = task._2.steps.filter(_.enabled).foldLeft((List[(String, DataFrame)](), accTracking)) {
            case ((stepAccData, stepAccTracking), step) =>
              LOGGER.debug(s"Generating data for step, task-name=${task._1.name}, step-name=${step.name}, data-source-name=${task._1.dataSourceName}")
              try {
                val recordStepName = s"${task._2.name}_${step.name}"
                val stepRecords = stepAccTracking.getOrElse(recordStepName, StepRecordCount(0, 0, 0))
                val (dataSourceStepName, df, updatedStepRecords) = stepDataCoordinator.generateForStep(currentBatch, task, step, stepRecords)
                val newStepAccData = stepAccData :+ (dataSourceStepName, df)
                val newStepAccTracking = stepAccTracking + (recordStepName -> updatedStepRecords)
                (newStepAccData, newStepAccTracking)
              } catch {
                case ex: Exception =>
                  LOGGER.error(s"Failed to generate data for step, task-name=${task._1.name}, step-name=${step.name}, data-source-name=${task._1.dataSourceName}")
                  throw ex
              }
          }
          (accData ++ taskData, updatedTaskTracking)
      }
      
      trackRecordsPerStep = updatedTrackRecords

      // Apply foreign key relationships
      val sinkDf = plan.sinkOptions
        .map { sinkOptions =>
          if (sinkOptions.foreignKeys.nonEmpty) {
            val fkProcessor = new ForeignKeyProcessor()
            val fkConfig = io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig()
            val fkContext = ForeignKeyContext(plan, generatedDataForeachTask.toMap, Some(executableTasks), fkConfig)
            val fkResult = fkProcessor.process(fkContext)
            fkResult.dataFrames
          } else {
            generatedDataForeachTask
          }
        }
        .getOrElse(generatedDataForeachTask)

      val totalRecordsGenerated = if (flagsConfig.enableCount) {
        sinkDf.map(_._2.count()).sum
      } else {
        0L
      }

      val sinkResults = pushDataToSinks(plan, executableTasks, sinkDf, currentBatch, numBatches, startTime, optValidations)
      dataSourceResults = dataSourceResults ++ sinkResults

      sinkDf.foreach(_._2.unpersist())
      sparkSession.sparkContext.getPersistentRDDs.foreach { case (_, rdd) => rdd.unpersist() }

      val endTime = LocalDateTime.now()
      val timeTakenMs = Duration.between(startTime, endTime).toMillis
      executionStrategy.onBatchEnd(currentBatch, totalRecordsGenerated)

      LOGGER.info(s"Finished batch, batch=$currentBatch, time-taken-ms=$timeTakenMs, records-generated=$totalRecordsGenerated")
      currentBatch += 1
    }

    LOGGER.info(s"Completed all batches, total-batches=${currentBatch - 1}")

    // Finalize any pending consolidations for multi-batch scenarios
    sinkFactory.finalizePendingConsolidations()

    (dataSourceResults, executionStrategy.getMetrics)
  }

  /**
   * Execute data generation with all-upfront mode.
   * Generates all data upfront, then streams to sinks with rate control.
   */
  private def executeAllUpfrontGeneration(
    plan: Plan,
    executableTasks: List[(TaskSummary, Task)],
    optValidations: Option[List[ValidationConfiguration]],
    stepDataCoordinator: StepDataCoordinator,
    executionStrategy: ExecutionStrategy
  )(implicit sparkSession: SparkSession): (List[DataSourceResult], Option[PerformanceMetrics]) = {
    val startTime = LocalDateTime.now()
    
    // Extract streaming config from duration-based strategy
    val (durationSeconds, rate) = executionStrategy match {
      case dbs: DurationBasedExecutionStrategy =>
        (dbs.getDurationSeconds, dbs.getTargetRate.getOrElse(1))
      case _ =>
        throw new IllegalStateException("AllUpfront generation mode requires DurationBasedExecutionStrategy with rate configured")
    }

    LOGGER.info(s"Starting all-upfront data generation for streaming: duration=${durationSeconds}s, rate=$rate/sec")

    // Generate all data upfront and save to temp storage for progressive loading
    val generatedDataWithPaths = executableTasks.flatMap { task =>
      task._2.steps.filter(_.enabled).map { step =>
        val dataSourceStepName = getDataSourceName(task._1, step)
        val isReferenceMode = step.options.get(ENABLE_REFERENCE_MODE).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_REFERENCE_MODE)

        if (isReferenceMode) {
          // Reference mode: read existing data
          LOGGER.info(s"Reading reference data for streaming, data-source=${task._1.dataSourceName}, step-name=${step.name}")
          val referenceDf = stepDataCoordinator.readReferenceData(task, step)
          (dataSourceStepName, referenceDf, None) // No temp path for reference data
        } else {
          // Generate data
          LOGGER.info(s"Generating data for streaming, data-source=${task._1.dataSourceName}, step-name=${step.name}")
          
          // Calculate total records to generate based on duration and rate
          val totalRecords: Long = (durationSeconds * rate).toLong
          LOGGER.info(s"Generating $totalRecords records for streaming (${durationSeconds}s @ $rate/sec)")
          
          val genDf = stepDataCoordinator.generateAllUpfront(task, step, totalRecords)
          
          // Save to temp storage to avoid keeping all data in memory
          val tempPath = s"${foldersConfig.generatedReportsFolderPath}/streaming-temp-${java.util.UUID.randomUUID()}"
          LOGGER.info(s"Saving pre-generated data to temp storage, path=$tempPath, records=$totalRecords")
          genDf.write.mode(SaveMode.Overwrite).parquet(tempPath)
          
          // Read back for use (will be progressively loaded during streaming)
          val savedDf = sparkSession.read.parquet(tempPath)
          
          (dataSourceStepName, savedDf, Some(tempPath))
        }
      }
    }

    // Apply foreign key relationships if configured
    val sinkDf = plan.sinkOptions
      .map { sinkOptions =>
        if (sinkOptions.foreignKeys.nonEmpty) {
          val fkProcessor = new ForeignKeyProcessor()
          val fkConfig = io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig()
          val fkContext = ForeignKeyContext(plan, generatedDataWithPaths.map(t => (t._1, t._2)).toMap, Some(executableTasks), fkConfig)
          val fkResult = fkProcessor.process(fkContext)
          fkResult.dataFrames
        } else {
          generatedDataWithPaths.map(t => (t._1, t._2))
        }
      }
      .getOrElse(generatedDataWithPaths.map(t => (t._1, t._2)))

    // Route to appropriate sink writers based on format and configuration
    val dataSourceResults = routeAndPushToSinks(sinkDf, plan, executableTasks, startTime, optValidations, executionStrategy)

    // Cleanup temp storage
    generatedDataWithPaths.foreach { case (_, df, optTempPath) =>
      df.unpersist()
      optTempPath.foreach { tempPath =>
        try {
          import org.apache.hadoop.fs.Path
          val fs = org.apache.hadoop.fs.FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
          fs.delete(new Path(tempPath), true)
          LOGGER.debug(s"Cleaned up temp storage, path=$tempPath")
        } catch {
          case ex: Exception => LOGGER.warn(s"Failed to cleanup temp storage, path=$tempPath, error=${ex.getMessage}")
        }
      }
    }

    val metrics = executionStrategy.getMetrics
    LOGGER.info(s"All-upfront generation completed, total-results=${dataSourceResults.size}")
    
    (dataSourceResults, metrics)
  }

  /**
   * Route data to appropriate sink writers based on format, generation mode, and configuration.
   */
  private def routeAndPushToSinks(
    generatedData: List[(String, DataFrame)],
    plan: Plan,
    executableTasks: List[(TaskSummary, Task)],
    startTime: LocalDateTime,
    optValidations: Option[List[ValidationConfiguration]],
    executionStrategy: ExecutionStrategy
  )(implicit sparkSession: SparkSession): List[DataSourceResult] = {
    val stepAndTaskByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), (s, task._2)))
    ).toMap
    
    val dataSourcesUsedInValidation = getDataSourcesUsedInValidation(optValidations)
    val pekkoStreamingWriter = new PekkoStreamingSinkWriter(foldersConfig)

    generatedData.flatMap { case (dataSourceStepName, df) =>
      val dataSourceName = dataSourceStepName.split("\\.").head
      val (step, task) = stepAndTaskByDataSourceName(dataSourceStepName)
      val dataSourceConfig = connectionConfigsByName.getOrElse(dataSourceName, Map())
      
      // Skip reference mode steps - they should not be saved to sinks
      val isReferenceMode = step.options.get(ENABLE_REFERENCE_MODE).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_REFERENCE_MODE)
      if (isReferenceMode) {
        LOGGER.debug(s"Skipping save for reference data source, data-source=$dataSourceName, step-name=${step.name}")
        None
      } else {
        val stepWithConfig = step.copy(options = dataSourceConfig ++ step.options)
        val format = stepWithConfig.options.getOrElse("format", throw new IllegalArgumentException(s"No format specified for $dataSourceName"))
        
        // Add rate information to options for router decision
        val optionsWithRate = executionStrategy match {
          case dbs: DurationBasedExecutionStrategy if dbs.getTargetRate.isDefined =>
            stepWithConfig.options + ("hasRateControl" -> "true")
          case _: PatternBasedExecutionStrategy =>
            stepWithConfig.options + ("hasRateControl" -> "true")
          case _ => stepWithConfig.options
        }
        
        // Determine sink strategy using router
        val sinkStrategy = sinkRouter.determineSinkStrategy(format, executionStrategy.getGenerationMode, optionsWithRate)
        
        LOGGER.info(s"Routing to sink, data-source=$dataSourceName, format=$format, strategy=$sinkStrategy")
        
        // Apply record tracking if enabled
        if (flagsConfig.enableRecordTracking) {
          recordTrackingProcessor.trackRecords(df, dataSourceName, plan.name, stepWithConfig)
        }
        if (dataSourcesUsedInValidation.contains(dataSourceName)) {
          validationRecordTrackingProcessor.trackRecords(df, dataSourceName, plan.name, stepWithConfig)
        }
        
        val sinkResult = sinkStrategy match {
          case SinkStrategy.BatchSink =>
            // Use standard batch writer
            sinkFactory.pushToSink(df, dataSourceName, stepWithConfig, startTime, isMultiBatch = false, isLastBatch = true)
          
          case SinkStrategy.StreamingSink =>
            // Use Pekko streaming writer with rate control
            val rate = executionStrategy match {
              case dbs: DurationBasedExecutionStrategy => dbs.getTargetRate.getOrElse(1)
              case _ => 1
            }
            pekkoStreamingWriter.saveWithRateControl(dataSourceName, df, format, dataSourceConfig, stepWithConfig, rate, startTime)
        }
        
        Some(DataSourceResult(dataSourceName, task, stepWithConfig, sinkResult, 1))
      }
    }
  }

  /**
   * Push data to sinks for batched generation mode.
   * This is the original pushDataToSinks method for batch-by-batch execution.
   */
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

