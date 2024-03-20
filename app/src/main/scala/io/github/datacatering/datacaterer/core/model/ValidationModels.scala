package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, Validation}
import io.github.datacatering.datacaterer.core.util.ResultWriterUtil.getSuccessSymbol
import org.apache.spark.sql.DataFrame

import java.time.{Duration, LocalDateTime}
import scala.math.BigDecimal.RoundingMode

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
      val validationSuccess = validationRes.map(_.isSuccess)
      val numSuccess = validationSuccess.count(x => x)
      val successRate = BigDecimal(numSuccess.toDouble / validationRes.size * 100).setScale(2, RoundingMode.HALF_UP)
      val isSuccess = getSuccessSymbol(validationSuccess.forall(x => x))
      val successRateVisual = s"$numSuccess/${validationRes.size} ($successRate%)"

      List(name, description, isSuccess, successRateVisual)
    } else List()
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
                             sampleErrorValues: Option[DataFrame] = None
                           )

object ValidationResult {
  def fromValidationWithBaseResult(validation: Validation, validationResult: ValidationResult): ValidationResult = {
    ValidationResult(validation, validationResult.isSuccess, validationResult.numErrors, validationResult.total, validationResult.sampleErrorValues)
  }
}
