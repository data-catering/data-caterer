package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_ENABLE_GENERATE_DATA, ENABLE_DATA_GENERATION}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig, Plan, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.model.DataSourceResult
import io.github.datacatering.datacaterer.core.sink.SinkFactory
import io.github.datacatering.datacaterer.core.util.GeneratorUtil.getDataSourceName
import io.github.datacatering.datacaterer.core.util.PlanImplicits.StepOps
import io.github.datacatering.datacaterer.core.util.RecordCountUtil.calculateNumBatches
import io.github.datacatering.datacaterer.core.util.{ForeignKeyUtil, UniqueFieldsUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.Serializable
import java.time.LocalDateTime
import java.util.{Locale, Random}
import scala.util.{Failure, Success, Try}

class BatchDataProcessor(connectionConfigsByName: Map[String, Map[String, String]], foldersConfig: FoldersConfig,
                         metadataConfig: MetadataConfig, flagsConfig: FlagsConfig, generationConfig: GenerationConfig)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val sinkFactory = new SinkFactory(flagsConfig, metadataConfig)

  def splitAndProcess(plan: Plan, executableTasks: List[(TaskSummary, Task)])
                     (implicit sparkSession: SparkSession): List[DataSourceResult] = {
    val faker = getDataFaker(plan)
    val dataGeneratorFactory = new DataGeneratorFactory(faker)
    val uniqueFieldUtil = new UniqueFieldsUtil(executableTasks)
    val tasks = executableTasks.map(_._2)
    var (numBatches, trackRecordsPerStep) = calculateNumBatches(tasks, generationConfig)

    val dataSourceResults = (1 to numBatches).flatMap(batch => {
      val startTime = LocalDateTime.now()
      LOGGER.info(s"Starting batch, batch=$batch, num-batches=$numBatches")
      val generatedDataForeachTask = executableTasks.flatMap(task =>
        task._2.steps.filter(_.enabled).map(s => {
          val isStepGenerateData = s.options.get(ENABLE_DATA_GENERATION).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_GENERATE_DATA)
          val dataSourceStepName = getDataSourceName(task._1, s)
          if (isStepGenerateData) {
            val recordStepName = s"${task._2.name}_${s.name}"
            val stepRecords = trackRecordsPerStep(recordStepName)
            val startIndex = stepRecords.currentNumRecords
            val endIndex = stepRecords.currentNumRecords + stepRecords.numRecordsPerBatch

            val genDf = dataGeneratorFactory.generateDataForStep(s, task._1.dataSourceName, startIndex, endIndex)
            val df = getUniqueGeneratedRecords(uniqueFieldUtil, s, dataSourceStepName, genDf)

            if (!df.storageLevel.useMemory) df.cache()
            val dfRecordCount = if (flagsConfig.enableCount) df.count() else stepRecords.numRecordsPerBatch
            LOGGER.debug(s"Step record count for batch, batch=$batch, step-name=${s.name}, target-num-records=${stepRecords.numRecordsPerBatch}, actual-num-records=$dfRecordCount")
            trackRecordsPerStep = trackRecordsPerStep ++ Map(recordStepName -> stepRecords.copy(currentNumRecords = dfRecordCount))
            (dataSourceStepName, df)
          } else {
            LOGGER.info(s"Step has data generation disabled, data-source=${task._1.dataSourceName}")
            (dataSourceStepName, sparkSession.emptyDataFrame)
          }
        })
      ).toMap

      val sinkDf = plan.sinkOptions
        .map(_ => ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask))
        .getOrElse(generatedDataForeachTask.toList)
      val sinkResults = pushDataToSinks(executableTasks, sinkDf, batch, startTime)
      sinkDf.foreach(_._2.unpersist())
      LOGGER.info(s"Finished batch, batch=$batch, num-batches=$numBatches")
      sinkResults
    }).toList
    LOGGER.debug(s"Completed all batches, num-batches=$numBatches")
    dataSourceResults
  }

  def pushDataToSinks(executableTasks: List[(TaskSummary, Task)], sinkDf: List[(String, DataFrame)], batchNum: Int,
                      startTime: LocalDateTime): List[DataSourceResult] = {
    val stepAndTaskByDataSourceName = executableTasks.flatMap(task =>
      task._2.steps.map(s => (getDataSourceName(task._1, s), (s, task._2)))
    ).toMap

    sinkDf.filter(s => !s._2.isEmpty).map(df => {
      val dataSourceName = df._1.split("\\.").head
      val (step, task) = stepAndTaskByDataSourceName(df._1)
      val dataSourceConfig = connectionConfigsByName.getOrElse(dataSourceName, Map())
      val stepWithDataSourceConfig = step.copy(options = dataSourceConfig ++ step.options)
      val sinkResult = sinkFactory.pushToSink(df._2, dataSourceName, stepWithDataSourceConfig, flagsConfig, startTime)
      DataSourceResult(dataSourceName, task, stepWithDataSourceConfig, sinkResult, batchNum)
    })
  }

  private def getUniqueGeneratedRecords(uniqueFieldUtil: UniqueFieldsUtil, s: Step, dataSourceStepName: String, genDf: DataFrame): DataFrame = {
    if (s.gatherUniqueFields.nonEmpty || s.gatherPrimaryKeys.nonEmpty) {
      LOGGER.debug(s"Ensuring field values are unique since there are fields with isUnique or isPrimaryKey set to true, " +
        s"data-source-step-name=$dataSourceStepName")
      uniqueFieldUtil.getUniqueFieldsValues(dataSourceStepName, genDf)
    } else {
      genDf
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
      case (Some(_), Failure(exception), _) =>
        throw new RuntimeException("Failed to get seed value from plan sink options", exception)
      case _ => new Faker() with Serializable
    }
  }
}

