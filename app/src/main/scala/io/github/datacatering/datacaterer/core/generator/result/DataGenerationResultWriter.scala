package io.github.datacatering.datacaterer.core.generator.result

import com.fasterxml.jackson.annotation.JsonInclude.Include
import io.github.datacatering.datacaterer.api.model.Constants.SPECIFIC_DATA_SOURCE_OPTIONS
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceResult, DataSourceResultSummary, Field, Plan, Step, StepResultSummary, Task, TaskResultSummary, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.model.Constants.{REPORT_DATA_CATERING_SVG, REPORT_DATA_SOURCES_HTML, REPORT_FIELDS_HTML, REPORT_HOME_HTML, REPORT_MAIN_CSS, REPORT_RESULT_JSON, REPORT_TASK_HTML, REPORT_VALIDATIONS_HTML}
import io.github.datacatering.datacaterer.core.plan.PostPlanProcessor
import io.github.datacatering.datacaterer.core.util.FileUtil.writeStringToFile
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.xml.Node

class DataGenerationResultWriter(val dataCatererConfiguration: DataCatererConfiguration)
                                (implicit sparkSession: SparkSession) extends PostPlanProcessor {

  private lazy val LOGGER = Logger.getLogger(getClass.getName)
  private lazy val OBJECT_MAPPER = ObjectMapperUtil.jsonObjectMapper
  private val foldersConfig = dataCatererConfiguration.foldersConfig
  private val validationConfig = dataCatererConfiguration.validationConfig

  override val enabled: Boolean = dataCatererConfiguration.flagsConfig.enableSaveReports

  override def apply(plan: Plan, sparkRecordListener: SparkRecordListener, generationResult: List[DataSourceResult],
                     validationResults: List[ValidationConfigResult]): Unit = {
    writeResult(plan, generationResult, validationResults, sparkRecordListener)
  }

  def writeResult(
                   plan: Plan,
                   generationResult: List[DataSourceResult],
                   validationResults: List[ValidationConfigResult],
                   sparkRecordListener: SparkRecordListener
                 ): Unit = {
    OBJECT_MAPPER.setSerializationInclusion(Include.NON_ABSENT)
    val (stepSummary, taskSummary, dataSourceSummary) = getSummaries(generationResult)
    val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    fileSystem.setWriteChecksum(false)
    val reportFolder = plan.runId.map(id => s"${foldersConfig.generatedReportsFolderPath}/$id").getOrElse(foldersConfig.generatedReportsFolderPath)

    LOGGER.info(s"Writing data generation summary to HTML files, folder-path=$reportFolder")
    val htmlWriter = new ResultHtmlWriter()
    val fileWriter = writeToFile(fileSystem, reportFolder) _

    try {
      fileWriter(REPORT_HOME_HTML, htmlWriter.index(plan, stepSummary, taskSummary, dataSourceSummary,
        validationResults, dataCatererConfiguration.flagsConfig, sparkRecordListener))
      fileWriter(REPORT_TASK_HTML, htmlWriter.taskDetails(taskSummary))
      fileWriter(REPORT_FIELDS_HTML, htmlWriter.stepDetails(stepSummary))
      fileWriter(REPORT_DATA_SOURCES_HTML, htmlWriter.dataSourceDetails(stepSummary.flatMap(_.dataSourceResults)))
      fileWriter(REPORT_VALIDATIONS_HTML, htmlWriter.validations(validationResults, validationConfig))
      writeStringToFile(fileSystem, s"$reportFolder/$REPORT_RESULT_JSON", resultsAsJson(generationResult, validationResults))
      writeStringToFile(fileSystem, s"$reportFolder/$REPORT_DATA_CATERING_SVG", htmlWriter.dataCateringSvg)
      writeStringToFile(fileSystem, s"$reportFolder/$REPORT_MAIN_CSS", htmlWriter.mainCss)
    } catch {
      case ex: Exception =>
        LOGGER.error("Failed to write data generation summary to HTML files", ex)
    }
  }

  private def writeToFile(fileSystem: FileSystem, folderPath: String)(fileName: String, content: Node): Unit = {
    writeStringToFile(fileSystem, s"$folderPath/$fileName", content.toString())
  }

  private def getSummaries(generationResult: List[DataSourceResult]): (List[StepResultSummary], List[TaskResultSummary], List[DataSourceResultSummary]) = {
    val resultByStep = generationResult.groupBy(_.step).map(getResultSummary).toList
    val resultByTask = generationResult.groupBy(_.task).map(getResultSummary).toList
    val resultByDataSource = generationResult.groupBy(_.name).map(getResultSummary).toList
    (resultByStep, resultByTask, resultByDataSource)
  }

  private def getResultSummary(result: (Step, List[DataSourceResult])): StepResultSummary = {
    val (totalRecords, isSuccess, _, _) = summariseDataSourceResult(result._2)
    StepResultSummary(result._1, totalRecords, isSuccess, result._2)
  }

  private def getResultSummary(result: (Task, List[DataSourceResult])): TaskResultSummary = {
    val (totalRecords, isSuccess, _, _) = summariseDataSourceResult(result._2)
    val stepResults = result._1.steps.map(step => getResultSummary((step, result._2.filter(_.step == step))))
    TaskResultSummary(result._1, totalRecords, isSuccess, stepResults)
  }

  private def getResultSummary(result: (String, List[DataSourceResult])): DataSourceResultSummary = {
    val (totalRecords, isSuccess, _, _) = summariseDataSourceResult(result._2)
    DataSourceResultSummary(result._1, totalRecords, isSuccess, result._2)
  }

  private def summariseDataSourceResult(dataSourceResults: List[DataSourceResult]): (Long, Boolean, List[String], List[Field]) = {
    val totalRecords = dataSourceResults.map(_.sinkResult.count).sum
    val isSuccess = dataSourceResults.forall(_.sinkResult.isSuccess)
    val sample = dataSourceResults.flatMap(_.sinkResult.sample).take(dataCatererConfiguration.metadataConfig.numGeneratedSamples)
    val fieldMetadata = dataSourceResults.flatMap(_.sinkResult.generatedMetadata)
      .groupBy(_.name)
      .map(field => {
        val metadataList = field._2.map(_.options)
        //TODO combine the metadata from each batch together to show summary
        field._2.head
      }).toList
    (totalRecords, isSuccess, sample, fieldMetadata)
  }

  private def resultsAsJson(generationResult: List[DataSourceResult], validationResults: List[ValidationConfigResult]): String = {
    val resultMap = Map(
      "generation" -> getGenerationJsonSummary(generationResult),
      "validation" -> validationResults.map(_.jsonSummary)
    )
    OBJECT_MAPPER.writeValueAsString(resultMap)
  }

  private def getGenerationJsonSummary(generationResult: List[DataSourceResult]): List[Map[String, Any]] = {
    val generationPerSubDataSource = generationResult.groupBy(dsr => {
      val dataSpecificOptions = dsr.step.options.filter(opt => SPECIFIC_DATA_SOURCE_OPTIONS.contains(opt._1))
      (dsr.name, dataSpecificOptions)
    }).map(grpBySubDataSource => {
      val totalRecords = grpBySubDataSource._2.map(_.sinkResult.count).sum
      val isSuccess = grpBySubDataSource._2.forall(_.sinkResult.isSuccess)
      Map(
        "name" -> grpBySubDataSource._1._1,
        "options" -> grpBySubDataSource._1._2,
        "numRecords" -> totalRecords,
        "isSuccess" -> isSuccess
      )
    }).toList
    generationPerSubDataSource
  }

}

