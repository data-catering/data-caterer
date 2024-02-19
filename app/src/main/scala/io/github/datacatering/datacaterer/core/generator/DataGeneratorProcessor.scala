package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, Task, TaskSummary, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.alert.AlertProcessor
import io.github.datacatering.datacaterer.core.generator.result.DataGenerationResultWriter
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.model.{DataSourceResult, PlanRunResults, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.util.PlanImplicits.TaskOps
import io.github.datacatering.datacaterer.core.validator.ValidationProcessor
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class DataGeneratorProcessor(dataCatererConfiguration: DataCatererConfiguration)(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val connectionConfigsByName = dataCatererConfiguration.connectionConfigByName
  private val foldersConfig = dataCatererConfiguration.foldersConfig
  private val metadataConfig = dataCatererConfiguration.metadataConfig
  private val flagsConfig = dataCatererConfiguration.flagsConfig
  private val generationConfig = dataCatererConfiguration.generationConfig
  private lazy val batchDataProcessor = new BatchDataProcessor(connectionConfigsByName, foldersConfig, metadataConfig, flagsConfig, generationConfig)
  private lazy val sparkRecordListener = new SparkRecordListener(flagsConfig.enableCount)
  sparkSession.sparkContext.addSparkListener(sparkRecordListener)

  def generateData(): PlanRunResults = {
    val plan = PlanParser.parsePlan(foldersConfig.planFilePath)
    val enabledPlannedTasks = plan.tasks.filter(_.enabled)
    val enabledTaskMap = enabledPlannedTasks.map(t => (t.name, t)).toMap
    val tasks = PlanParser.parseTasks(foldersConfig.taskFolderPath)
    val enabledTasks = tasks.filter(t => enabledTaskMap.contains(t.name)).toList

    generateData(plan.copy(tasks = enabledPlannedTasks), enabledTasks, None)
  }

  def generateData(plan: Plan, tasks: List[Task], optValidations: Option[List[ValidationConfiguration]]): PlanRunResults = {
    val tasksByName = tasks.map(t => (t.name, t)).toMap
    val summaryWithTask = plan.tasks.map(t => (t, tasksByName(t.name)))
    generateDataWithResult(plan, summaryWithTask, optValidations)
  }

  private def generateDataWithResult(plan: Plan, summaryWithTask: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]]): PlanRunResults = {
    if (flagsConfig.enableDeleteGeneratedRecords) {
      LOGGER.warn("Both enableGenerateData and enableDeleteGeneratedData are true. Please only enable one at a time. Will continue with generating data")
    }
    if (LOGGER.isDebugEnabled) {
      LOGGER.debug(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=($summaryWithTask)")
      summaryWithTask.foreach(t => LOGGER.debug(s"Enabled task details: ${t._2.toTaskDetailString}"))
    }
    val stepNames = summaryWithTask.map(t => s"task=${t._2.name}, num-steps=${t._2.steps.size}, steps=${t._2.steps.map(_.name).mkString(",")}").mkString("||")

    if (summaryWithTask.isEmpty) {
      LOGGER.warn("No tasks found or no tasks enabled. No data will be generated or validated")
      PlanRunResults(List(), List())
    } else {
      val generationResult = if (flagsConfig.enableGenerateData) {
        LOGGER.info(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=$stepNames")
        batchDataProcessor.splitAndProcess(plan, summaryWithTask)
      } else List()

      val validationResults = if (flagsConfig.enableValidation) {
        new ValidationProcessor(connectionConfigsByName, optValidations, dataCatererConfiguration.validationConfig, foldersConfig)
          .executeValidations
      } else List()

      applyPostPlanProcessors(plan, sparkRecordListener, generationResult, validationResults)
      PlanRunResults(generationResult, validationResults)
    }
  }

  private def applyPostPlanProcessors(plan: Plan, sparkRecordListener: SparkRecordListener,
                                      generationResult: List[DataSourceResult], validationResults: List[ValidationConfigResult]): Unit = {
    val postPlanProcessors = List(new DataGenerationResultWriter(dataCatererConfiguration), new AlertProcessor(dataCatererConfiguration))

    postPlanProcessors.foreach(postPlanProcessor => {
      if (postPlanProcessor.enabled) postPlanProcessor.apply(plan, sparkRecordListener, generationResult, validationResults)
    })
  }

}
