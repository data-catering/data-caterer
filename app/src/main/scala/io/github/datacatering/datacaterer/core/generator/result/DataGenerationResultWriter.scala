package io.github.datacatering.datacaterer.core.generator.result

import com.fasterxml.jackson.annotation.JsonInclude.Include
import io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_GENERATED_REPORTS_FOLDER_PATH
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Field, Plan, Step, Task}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.model.Constants.{REPORT_DATA_SOURCES_HTML, REPORT_FIELDS_HTML, REPORT_HOME_HTML, REPORT_VALIDATIONS_HTML}
import io.github.datacatering.datacaterer.core.model.{DataSourceResult, DataSourceResultSummary, StepResultSummary, TaskResultSummary, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.plan.PostPlanProcessor
import io.github.datacatering.datacaterer.core.util.FileUtil.writeStringToFile
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}
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
      fileWriter("tasks.html", htmlWriter.taskDetails(taskSummary))
      fileWriter(REPORT_FIELDS_HTML, htmlWriter.stepDetails(stepSummary))
      fileWriter(REPORT_DATA_SOURCES_HTML, htmlWriter.dataSourceDetails(stepSummary.flatMap(_.dataSourceResults)))
      fileWriter(REPORT_VALIDATIONS_HTML, htmlWriter.validations(validationResults, validationConfig))

      copyHtmlResources(fileSystem, reportFolder)
    } catch {
      case ex: Exception =>
        LOGGER.error("Failed to write data generation summary to HTML files", ex)
    }
  }

  private def copyHtmlResources(fileSystem: FileSystem, folder: String): Unit = {
    val resources = List("main.css", "data_catering_transparent.svg")
    if (!foldersConfig.generatedReportsFolderPath.equalsIgnoreCase(DEFAULT_GENERATED_REPORTS_FOLDER_PATH)) {
      resources.foreach(resource => {
        val defaultResourcePath = new Path(s"file:///$DEFAULT_GENERATED_REPORTS_FOLDER_PATH/$resource")
        val tryLocalUri = Try(new Path(getClass.getResource(s"/report/$resource").toURI))
        val resourcePath = tryLocalUri match {
          case Failure(_) =>
            defaultResourcePath
          case Success(value) =>
            Try(value.getName) match {
              case Failure(_) => defaultResourcePath
              case Success(name) =>
                if (name.startsWith("jar:")) defaultResourcePath else value
            }
        }
        val destination = s"file:///$folder/$resource"
        fileSystem.copyFromLocalFile(resourcePath, new Path(destination))
      })
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
        val metadataList = field._2.map(_.generator.map(gen => gen.options).getOrElse(Map()))
        //TODO combine the metadata from each batch together to show summary
        field._2.head
      }).toList
    (totalRecords, isSuccess, sample, fieldMetadata)
  }

}

