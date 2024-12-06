package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_ENABLE_GENERATE_DATA, ENABLE_DATA_GENERATION, SAVE_MODE}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, Plan, Step, Task, TaskSummary, UpstreamDataSourceValidation, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.exception.InvalidRandomSeedException
import io.github.datacatering.datacaterer.core.generator.track.RecordTrackingProcessor
import io.github.datacatering.datacaterer.core.model.DataSourceResult
import io.github.datacatering.datacaterer.core.sink.SinkFactory
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.getDataSourceName
import io.github.datacatering.datacaterer.core.util.PlanImplicits.PerColumnCountOps
import io.github.datacatering.datacaterer.core.util.RecordCountUtil.calculateNumBatches
import io.github.datacatering.datacaterer.core.util.{ForeignKeyUtil, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.Serializable
import java.time.LocalDateTime
import java.util.{Locale, Random}
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
    val dataGeneratorFactory = new DataGeneratorFactory(faker)
    val uniqueFieldUtil = new UniqueFieldsUtil(plan, executableTasks)
    val tasks = executableTasks.map(_._2)
    var (numBatches, trackRecordsPerStep) = calculateNumBatches(tasks, generationConfig)

    def generateDataForStep(batch: Int, task: (TaskSummary, Task), s: Step): (String, DataFrame) = {
      val isStepEnabledGenerateData = s.options.get(ENABLE_DATA_GENERATION).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_GENERATE_DATA)
      val dataSourceStepName = getDataSourceName(task._1, s)
      if (isStepEnabledGenerateData) {
        val recordStepName = s"${task._2.name}_${s.name}"
        val stepRecords = trackRecordsPerStep(recordStepName)
        val startIndex = stepRecords.currentNumRecords
        val endIndex = stepRecords.currentNumRecords + stepRecords.numRecordsPerBatch

        val genDf = dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName, startIndex, endIndex)
        var df = getUniqueGeneratedRecords(uniqueFieldUtil, dataSourceStepName, genDf, s)

        if (!df.storageLevel.useMemory) df.cache()
        var dfRecordCount = if (flagsConfig.enableCount) df.count() else stepRecords.numRecordsPerBatch
        var retries = 0
        val targetNumRecords = stepRecords.numRecordsPerBatch * s.count.perColumn.map(_.averageCountPerColumn).getOrElse(1L)
        LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${s.name}, " +
          s"target-num-records=$targetNumRecords, actual-num-records=$dfRecordCount")

        // if record count doesn't match expected record count, generate more data
        def generateAdditionalRecords(): Unit = {
          LOGGER.debug(s"Record count does not reach expected num records for batch, generating more records until reached, " +
            s"target-num-records=$targetNumRecords, actual-num-records=$dfRecordCount, num-retries=$retries, max-retries=$maxRetries")
          val additionalGenDf = dataGeneratorFactory
            .generateDataForStep(s, task._1.dataSourceName, stepRecords.currentNumRecords + dfRecordCount, endIndex)
          val additionalDf = getUniqueGeneratedRecords(uniqueFieldUtil, dataSourceStepName, additionalGenDf, s)
          df = df.union(additionalDf)
          dfRecordCount = df.count()
          LOGGER.debug(s"Generated more records for step, batch=$batch, step-name=${s.name}, " +
            s"new-num-records=${additionalDf.count()}, actual-num-records=$dfRecordCount")
        }

        //if random amount of records, don't try to regenerate more records
        if (s.count.generator.isEmpty && s.count.perColumn.forall(_.generator.isEmpty)) {
          while (targetNumRecords != dfRecordCount && retries < maxRetries) {
            retries += 1
            generateAdditionalRecords()
          }
          if (targetNumRecords != dfRecordCount && retries == maxRetries) {
            LOGGER.warn("Unable to reach expected number of records due to reaching max retries. " +
              s"Can be due to limited number of potential unique records, " +
              s"target-num-records=$targetNumRecords, actual-num-records=${dfRecordCount}")
          }
        } else {
          LOGGER.debug("Random amount of records generated, not attempting to generate more records")
        }

        trackRecordsPerStep = trackRecordsPerStep ++ Map(recordStepName -> stepRecords.copy(currentNumRecords = dfRecordCount + stepRecords.currentNumRecords))
        (dataSourceStepName, df)
      } else {
        LOGGER.debug(s"Step has data generation disabled, data-source=${task._1.dataSourceName}")
        (dataSourceStepName, sparkSession.emptyDataFrame)
      }
    }

    val dataSourceResults = (1 to numBatches).flatMap(batch => {
      val startTime = LocalDateTime.now()
      LOGGER.info(s"Starting batch, batch=$batch, num-batches=$numBatches")
      val generatedDataForeachTask = executableTasks.flatMap(task =>
        task._2.steps.filter(_.enabled).map(s => generateDataForStep(batch, task, s))
      ).toMap

      val sinkDf = plan.sinkOptions
        .map(_ => ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask))
        .getOrElse(generatedDataForeachTask.toList)
      val sinkResults = pushDataToSinks(plan, executableTasks, sinkDf, batch, startTime, optValidations)
      sinkDf.foreach(_._2.unpersist())
      LOGGER.info(s"Finished batch, batch=$batch, num-batches=$numBatches")
      sinkResults
    }).toList

    LOGGER.debug(s"Completed all batches, num-batches=$numBatches")
    dataSourceResults
  }

  private def pushDataToSinks(
                               plan: Plan,
                               executableTasks: List[(TaskSummary, Task)],
                               sinkDf: List[(String, DataFrame)],
                               batchNum: Int,
                               startTime: LocalDateTime,
                               optValidations: Option[List[ValidationConfiguration]]
                             ): List[DataSourceResult] = {
    val stepAndTaskByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), (s, task._2)))
    ).toMap
    val dataSourcesUsedInValidation = getDataSourcesUsedInValidation(optValidations)

    sinkDf.filter(s => !s._2.isEmpty).map(df => {
      val dataSourceName = df._1.split("\\.").head
      val (step, task) = stepAndTaskByDataSourceName(df._1)
      val dataSourceConfig = connectionConfigsByName.getOrElse(dataSourceName, Map())
      val stepWithDataSourceConfig = step.copy(options = dataSourceConfig ++ step.options)
      val stepWithSaveMode = checkSaveMode(batchNum, stepWithDataSourceConfig)

      if (flagsConfig.enableRecordTracking) {
        recordTrackingProcessor.trackRecords(df._2, dataSourceName, plan.name, stepWithSaveMode)
      }
      if (dataSourcesUsedInValidation.contains(dataSourceName)) {
        validationRecordTrackingProcessor.trackRecords(df._2, dataSourceName, plan.name, stepWithSaveMode)
      }

      val sinkResult = sinkFactory.pushToSink(df._2, dataSourceName, stepWithSaveMode, startTime)
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

