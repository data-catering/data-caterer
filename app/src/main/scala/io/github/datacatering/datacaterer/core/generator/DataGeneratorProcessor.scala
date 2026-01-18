package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{PLAN_STAGE_DELETE_DATA, PLAN_STAGE_GENERATE_DATA, PLAN_STAGE_POST_PLAN_PROCESSORS, PLAN_STAGE_VALIDATE_DATA}
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceResult, Plan, Task, TaskSummary, ValidationConfigResult, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.activity.PlanRunPostPlanProcessor
import io.github.datacatering.datacaterer.core.alert.AlertProcessor
import io.github.datacatering.datacaterer.core.generator.delete.DeleteRecordProcessor
import io.github.datacatering.datacaterer.core.generator.result.DataGenerationResultWriter
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.model.PlanRunResults
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
  private val streamingConfig = dataCatererConfiguration.streamingConfig
  private lazy val deleteRecordProcessor = new DeleteRecordProcessor(connectionConfigsByName, foldersConfig.recordTrackingFolderPath)
  private lazy val batchDataProcessor = new BatchDataProcessor(connectionConfigsByName, foldersConfig, metadataConfig, flagsConfig, generationConfig, streamingConfig)
  private lazy val sparkRecordListener = new SparkRecordListener(flagsConfig.enableCount)
  private lazy val planRunPostPlanProcessor = new PlanRunPostPlanProcessor(dataCatererConfiguration)
  sparkSession.sparkContext.addSparkListener(sparkRecordListener)

  def generateData(): PlanRunResults = {
    val (parsedPlan, enabledTasks, validations) = PlanParser.getPlanTasksFromYaml(dataCatererConfiguration)
    generateData(parsedPlan, enabledTasks, validations)
  }

  def generateData(plan: Plan, tasks: List[Task], optValidations: Option[List[ValidationConfiguration]]): PlanRunResults = {
    val tasksByName = tasks.map(t => (t.name, t)).toMap
    val summaryWithTask = plan.tasks.map(t => (t, tasksByName(t.name)))
    val result = generateDataWithResult(plan, summaryWithTask, optValidations)
    if (flagsConfig.enableDeleteGeneratedRecords) {
      runDeleteRecords(plan, tasks, summaryWithTask, result)
    }
    result
  }

  private def generateDataWithResult(plan: Plan, summaryWithTask: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]]): PlanRunResults = {
    if (flagsConfig.enableDeleteGeneratedRecords && flagsConfig.enableGenerateData) {
      LOGGER.warn("Both enableGenerateData and enableDeleteGeneratedData are true. Please only enable one at a time. Will continue with generating data")
    }
    if (LOGGER.isDebugEnabled) {
      LOGGER.debug(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=($summaryWithTask)")
      summaryWithTask.foreach(t => LOGGER.debug(s"Enabled task details: ${t._2.toTaskDetailString}"))
    }
    val numSteps = summaryWithTask.map(t =>
      t._2.steps.count(s => if (s.fields.nonEmpty) true else false)
    ).sum

    val (generationResult, optPerformanceMetrics) = if (flagsConfig.enableGenerateData && numSteps > 0) {
      val stepNames = summaryWithTask.map(t => s"task=${t._2.name}, num-steps=${t._2.steps.size}, steps=${t._2.steps.map(_.name).mkString(",")}").mkString("||")
      LOGGER.debug(s"Following tasks are enabled and will be executed: num-tasks=${summaryWithTask.size}, tasks=$stepNames")
      runDataGeneration(plan, summaryWithTask, optValidations)
    } else {
      LOGGER.warn(s"No data will be generated as it is either disabled or there are no tasks defined with a schema, " +
        s"enable-generate-data=${flagsConfig.enableGenerateData}, num-steps=$numSteps")
      (List(), None)
    }

    val validationResults = if (flagsConfig.enableValidation) {
      runDataValidation(optValidations, plan, generationResult, optPerformanceMetrics)
    } else {
      LOGGER.debug("Data validations disabled by flag configuration")
      List()
    }

    applyPostPlanProcessors(plan, sparkRecordListener, generationResult, validationResults)
    val optReportPath = if (flagsConfig.enableSaveReports) {
      plan.runId.map(id => s"${foldersConfig.generatedReportsFolderPath}/$id")
        .orElse(Some(foldersConfig.generatedReportsFolderPath))
    } else None
    PlanRunResults(generationResult, validationResults, optReportPath)
  }

  private def runDeleteRecords(plan: Plan, tasks: List[Task], summaryWithTask: List[(TaskSummary, Task)], planRunResults: PlanRunResults): Unit = {
    val stepsByName = tasks.flatMap(_.steps).filter(_.enabled).map(s => (s.name, s)).toMap
    try {
      deleteRecordProcessor.deleteGeneratedRecords(plan, stepsByName, summaryWithTask)
    } catch {
      case exception: Exception =>
        notifyResult(plan, planRunResults.generationResults, planRunResults.validationResults, PLAN_STAGE_DELETE_DATA, exception)
        throw exception
    }
  }

  private def runDataValidation(
                                 optValidations: Option[List[ValidationConfiguration]],
                                 plan: Plan,
                                 generationResults: List[DataSourceResult],
                                 optPerformanceMetrics: Option[io.github.datacatering.datacaterer.api.model.PerformanceMetrics]
                               ): List[ValidationConfigResult] = {
    try {
      new ValidationProcessor(connectionConfigsByName, optValidations, dataCatererConfiguration.validationConfig, foldersConfig, optPerformanceMetrics)
        .executeValidations
    } catch {
      case exception: Exception =>
        notifyResult(plan, generationResults, List(), PLAN_STAGE_VALIDATE_DATA, exception)
        throw exception
    }
  }

  private def runDataGeneration(plan: Plan, summaryWithTask: List[(TaskSummary, Task)], optValidations: Option[List[ValidationConfiguration]]): (List[DataSourceResult], Option[io.github.datacatering.datacaterer.api.model.PerformanceMetrics]) = {
    try {
      batchDataProcessor.splitAndProcess(plan, summaryWithTask, optValidations)
    } catch {
      case exception: Exception =>
        notifyResult(plan, List(), List(), PLAN_STAGE_GENERATE_DATA, exception)
        throw exception
    }
  }

  private def applyPostPlanProcessors(plan: Plan, sparkRecordListener: SparkRecordListener,
                                      generationResult: List[DataSourceResult], validationResult: List[ValidationConfigResult]): Unit = {
    val postPlanProcessors = List(
      new DataGenerationResultWriter(dataCatererConfiguration),
      new AlertProcessor(dataCatererConfiguration),
      planRunPostPlanProcessor
    )

    try {
      postPlanProcessors.foreach(postPlanProcessor => {
        if (postPlanProcessor.enabled) postPlanProcessor.apply(plan, sparkRecordListener, generationResult, validationResult)
      })
    } catch {
      case exception: Exception =>
        notifyResult(plan, generationResult, validationResult, PLAN_STAGE_POST_PLAN_PROCESSORS, exception)
        throw exception
    }
  }

  private def notifyResult(
                            plan: Plan,
                            generationResult: List[DataSourceResult],
                            validationResult: List[ValidationConfigResult],
                            stage: String,
                            exception: Exception
                          ): Unit = {
    planRunPostPlanProcessor.notifyPlanResult(plan, generationResult, validationResult, stage, Some(exception))
  }

}
