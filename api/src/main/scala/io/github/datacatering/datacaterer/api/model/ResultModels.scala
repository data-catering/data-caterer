package io.github.datacatering.datacaterer.api.model

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_DATA_SOURCE_NAME, PLAN_STAGE_FINISHED}
import io.github.datacatering.datacaterer.api.util.ConfigUtil.cleanseOptions
import io.github.datacatering.datacaterer.api.util.ResultWriterUtil.getSuccessSymbol

import java.time.{Duration, LocalDateTime}
import scala.math.BigDecimal.RoundingMode


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


case class ValidationConfigResult(
                                   name: String = "default_validation_result",
                                   description: String = "Validation result for data sources",
                                   dataSourceValidationResults: List[DataSourceValidationResult] = List(),
                                   startTime: LocalDateTime = LocalDateTime.now(),
                                   endTime: LocalDateTime = LocalDateTime.now()
                                 ) {
  def durationInSeconds: Long = Duration.between(startTime, endTime).toSeconds

  def summarise: List[String] = {
    val validationRes = dataSourceValidationResults.flatMap(_.validationResults)
    if (validationRes.nonEmpty) {
      val (numSuccess, successRate, isSuccess) = baseSummary(validationRes)
      val successRateVisual = s"$numSuccess/${validationRes.size} ($successRate%)"
      List(name, description, getSuccessSymbol(isSuccess), successRateVisual)
    } else List()
  }

  def jsonSummary: Map[String, Any] = {
    val validationRes = dataSourceValidationResults.flatMap(dsv =>
      dsv.validationResults.map(v => (dsv.dataSourceName, dsv.options, v))
    )
    if (validationRes.nonEmpty) {
      val (numSuccess, successRate, isSuccess) = baseSummary(validationRes.map(_._3))
      val errorMap = validationRes.filter(vr => !vr._3.isSuccess).map(res => {
        val validationDetails = res._3.validation.toOptions.map(v => (v.head, v.last)).toMap
        Map(
          "dataSourceName" -> res._1,
          "options" -> cleanseOptions(res._2),
          "validation" -> validationDetails,
          "numErrors" -> res._3.numErrors,
          "sampleErrorValues" -> res._3.sampleErrorValues.getOrElse(Array())
        )
      })
      val baseValidationMap = Map(
        "name" -> name,
        "description" -> description,
        "isSuccess" -> isSuccess,
        "numSuccess" -> numSuccess,
        "numValidations" -> validationRes.size,
        "successRate" -> successRate
      )
      if (errorMap.nonEmpty) {
        baseValidationMap ++ Map("errorValidations" -> errorMap)
      } else baseValidationMap
    } else Map()
  }

  private def baseSummary(validationRes: List[ValidationResult]): (Int, BigDecimal, Boolean) = {
    val validationSuccess = validationRes.map(_.isSuccess)
    val numSuccess = validationSuccess.count(x => x)
    val successRate = BigDecimal(numSuccess.toDouble / validationRes.size * 100).setScale(2, RoundingMode.HALF_UP)
    val isSuccess = validationSuccess.forall(x => x)
    (numSuccess, successRate, isSuccess)
  }
}

case class DataSourceValidationResult(
                                       dataSourceName: String = "default_data_source",
                                       options: Map[String, String] = Map(),
                                       validationResults: List[ValidationResult] = List()
                                     )

case class ValidationResult(
                             validation: Validation = ExpressionValidation(),
                             isSuccess: Boolean = true,
                             numErrors: Long = 0,
                             total: Long = 0,
                             sampleErrorValues: Option[Array[Map[String, Any]]] = None
                           )

object ValidationResult {
  def fromValidationWithBaseResult(validation: Validation, validationResult: ValidationResult): ValidationResult = {
    ValidationResult(validation, validationResult.isSuccess, validationResult.numErrors, validationResult.total, validationResult.sampleErrorValues)
  }
}

case class PlanResults(
                        plan: Plan,
                        generationResult: List[DataSourceResult],
                        validationResult: List[ValidationConfigResult],
                        generationSuccessful: Boolean = true,
                        validationSuccessful: Boolean = true,
                        stage: String = PLAN_STAGE_FINISHED,
                        exception: Option[String] = None
                      )

case class PlanRunSummary(
                           plan: Plan,
                           tasks: List[Task],
                           validations: List[ValidationConfiguration]
                         )
