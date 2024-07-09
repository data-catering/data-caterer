package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, Validation}
import io.github.datacatering.datacaterer.core.util.ConfigUtil.cleanseOptions
import io.github.datacatering.datacaterer.core.util.ResultWriterUtil.getSuccessSymbol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.delta.implicits.stringEncoder

import java.time.{Duration, LocalDateTime}
import scala.collection.mutable
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
      val (numSuccess, successRate, isSuccess) = baseSummary(validationRes)
      val successRateVisual = s"$numSuccess/${validationRes.size} ($successRate%)"
      List(name, description, getSuccessSymbol(isSuccess), successRateVisual)
    } else List()
  }

  def jsonSummary(numErrorSamples: Int): Map[String, Any] = {
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
          "sampleErrorValues" -> getErrorSamplesAsMap(numErrorSamples, res._3)
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

  private def getErrorSamplesAsMap(numErrorSamples: Int, res: ValidationResult): Array[Map[String, Any]] = {
    def parseValueMap[T](valMap: (String, T)): Map[String, Any] = {
      valMap._2 match {
        case genericRow: GenericRowWithSchema =>
          val innerValueMap = genericRow.getValuesMap[T](genericRow.schema.fields.map(_.name))
          Map(valMap._1 -> innerValueMap.flatMap(parseValueMap[T]))
        case wrappedArray: mutable.WrappedArray[_] =>
          Map(valMap._1 -> wrappedArray.map {
            case genericRowWithSchema: GenericRowWithSchema =>
              val innerValueMap = genericRowWithSchema.getValuesMap[T](genericRowWithSchema.schema.fields.map(_.name))
              innerValueMap.flatMap(parseValueMap[T])
            case x => x
          })
        case _ =>
          Map(valMap)
      }
    }

    res.sampleErrorValues.get.take(numErrorSamples)
      .map(row => {
        val valuesMap = row.getValuesMap(res.sampleErrorValues.get.columns)
        valuesMap.flatMap(valMap => parseValueMap(valMap))
      })
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
                             sampleErrorValues: Option[DataFrame] = None
                           )

object ValidationResult {
  def fromValidationWithBaseResult(validation: Validation, validationResult: ValidationResult): ValidationResult = {
    ValidationResult(validation, validationResult.isSuccess, validationResult.numErrors, validationResult.total, validationResult.sampleErrorValues)
  }
}
