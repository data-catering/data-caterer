package io.github.datacatering.datacaterer.api.model

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.util.ConfigUtil.cleanseAdditionalOptions
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
                             batchNum: Int = 0,
                             performanceMetrics: Option[PerformanceMetrics] = None
                           ) {

  def summarise: List[String] = {
    val format = sinkResult.format
    val isSuccess = getSuccessSymbol(sinkResult.isSuccess)
    val numRecords = sinkResult.count.toString
    List(name, format, isSuccess, numRecords)
  }

  def jsonSummary: Map[String, Any] = {
    Map(
      GENERATION_NAME -> name,
      GENERATION_FORMAT -> sinkResult.format,
      GENERATION_OPTIONS -> cleanseAdditionalOptions(step.options),
      GENERATION_IS_SUCCESS -> sinkResult.isSuccess,
      GENERATION_NUM_RECORDS -> sinkResult.count,
      GENERATION_TIME_TAKEN_SECONDS -> sinkResult.durationInSeconds,
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

  def jsonSummary(hasSampleErrors: Boolean = true): Map[String, Any] = {
    val validationRes = dataSourceValidationResults.flatMap(dsv =>
      dsv.validationResults.map(v => (dsv.dataSourceName, dsv.options, v))
    )
    if (validationRes.nonEmpty) {
      val (numSuccess, successRate, isSuccess) = baseSummary(validationRes.map(_._3))
      val errorMap = validationRes
        .filter(vr => !vr._3.isSuccess)
        .map(res => {
          val validationDetails = res._3.validation.toOptions.map(v => (v.head, v.last)).toMap
          val baseDetails = Map(
            VALIDATION_DATA_SOURCE_NAME -> res._1,
            VALIDATION_OPTIONS -> cleanseAdditionalOptions(res._2),
            VALIDATION_DETAILS -> validationDetails,
            VALIDATION_NUM_ERRORS -> res._3.numErrors,
            VALIDATION_ERROR_THRESHOLD -> res._3.validation.errorThreshold.getOrElse(0.0)
          )
          val sampleErrors = if (hasSampleErrors) {
            Map(VALIDATION_SAMPLE_ERRORS -> res._3.sampleErrorValues.getOrElse(Array()))
          } else Map()
          baseDetails ++ sampleErrors
        })

      val baseValidationMap = Map(
        VALIDATION_NAME -> name,
        VALIDATION_DESCRIPTION -> description,
        VALIDATION_IS_SUCCESS -> isSuccess,
        VALIDATION_NUM_SUCCESS -> numSuccess,
        VALIDATION_NUM_VALIDATIONS -> validationRes.size,
        VALIDATION_SUCCESS_RATE -> successRate
      )
      if (errorMap.nonEmpty) {
        baseValidationMap ++ Map(VALIDATION_ERROR_VALIDATIONS -> errorMap)
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
                        generationResult: List[Map[String, Any]],
                        validationResult: List[Map[String, Any]],
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
