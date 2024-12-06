package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_DATA_SOURCE_NAME
import io.github.datacatering.datacaterer.api.model.{Field, Step, Task}
import io.github.datacatering.datacaterer.core.util.ResultWriterUtil.getSuccessSymbol

import java.time.{Duration, LocalDateTime}

case class PlanRunResults(
                           generationResults: List[DataSourceResult] = List(),
                           validationResults: List[ValidationConfigResult] = List(),
                           optReportPath: Option[String] = None
                         ) {

  def summariseGenerationResults: (List[List[String]], Long) = {
    val timeTaken = generationResults.map(_.sinkResult.durationInSeconds).max
    val dataSourceSummary = generationResults.map(_.summarise)
    (dataSourceSummary, timeTaken)
  }

  def summariseValidationResults: (List[List[String]], Long) = {
    val timeTaken = validationResults.map(_.durationInSeconds).sum
    val validationSummary = validationResults.map(_.summarise)
    (validationSummary, timeTaken)
  }
}

case class DataSourceResultSummary(
                                    name: String,
                                    numRecords: Long,
                                    isSuccess: Boolean,
                                    dataSourceResults: List[DataSourceResult]
                                  )

case class DataSourceResult(
                             name: String = DEFAULT_DATA_SOURCE_NAME,
                             task: Task = Task(),
                             step: Step = Step(),
                             sinkResult: SinkResult = SinkResult(),
                             batchNum: Int = 0
                           ) {

  def summarise: List[String] = {
    val format = sinkResult.format
    val isSuccess = getSuccessSymbol(sinkResult.isSuccess)
    val numRecords = sinkResult.count.toString
    List(name, format, isSuccess, numRecords)
  }

  def jsonSummary: Map[String, Any] = {
    Map(
      "name" -> name,
      "options" -> step.options,
      "isSuccess" -> sinkResult.isSuccess,
      "numRecords" -> sinkResult.count
    )
  }
}

case class TaskResultSummary(
                              task: Task,
                              numRecords: Long,
                              isSuccess: Boolean,
                              stepResults: List[StepResultSummary]
                            )

case class StepResultSummary(
                              step: Step,
                              numRecords: Long,
                              isSuccess: Boolean,
                              dataSourceResults: List[DataSourceResult]
                            )

case class SinkResult(
                       name: String = DEFAULT_DATA_SOURCE_NAME,
                       format: String = "json",
                       saveMode: String = "append",
                       options: Map[String, String] = Map(),
                       count: Long = -1,
                       isSuccess: Boolean = true,
                       sample: Array[String] = Array(),
                       startTime: LocalDateTime = LocalDateTime.now(),
                       endTime: LocalDateTime = LocalDateTime.now(),
                       generatedMetadata: Array[Field] = Array(),
                       exception: Option[Throwable] = None
                     ) {

  def durationInSeconds: Long = Duration.between(startTime, endTime).toSeconds
}

case class RealTimeSinkResult(result: String = "")
