package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM, SPECIFIC_DATA_SOURCE_OPTIONS}
import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, Validation, ValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ColumnValidationBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataValidationAggregateFunctionException
import io.github.datacatering.datacaterer.core.util.ConfigUtil.cleanseOptions
import io.github.datacatering.datacaterer.core.util.ResultWriterUtil.getSuccessSymbol
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

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

trait ExternalDataValidation {
  val LOGGER: Logger = Logger.getLogger(getClass.getName)

  val name: String = "default_external_data_validation"
  val params: List[String]

  def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = List()

  def matchesParams(inputParams: Map[String, String]): Boolean = {
    val hasAllParams = params.exists(inputParams.contains)
    if (!hasAllParams) {
      LOGGER.debug(s"Failed to find all required parameters for data validation, name=$name, " +
        s"expected-params=${params.mkString(",")}, given-params=${inputParams.keys.mkString(",")}")
    }
    hasAllParams
  }

  def getAggregatedValidation(functionName: String, optFieldName: Option[String]): ColumnValidationBuilder = {
    val baseValidation = ValidationBuilder().groupBy()
    functionName.toLowerCase match {
      case AGGREGATION_MAX => baseValidation.max(optFieldName.get)
      case AGGREGATION_MIN => baseValidation.min(optFieldName.get)
      case AGGREGATION_SUM => baseValidation.sum(optFieldName.get)
      case AGGREGATION_STDDEV => baseValidation.stddev(optFieldName.get)
      case AGGREGATION_COUNT => baseValidation.count(optFieldName.get)
      case AGGREGATION_AVG => baseValidation.avg(optFieldName.get)
      case x => throw UnsupportedDataValidationAggregateFunctionException(x, optFieldName.getOrElse(""))
    }
  }
}

object ValidationResult {
  def fromValidationWithBaseResult(validation: Validation, validationResult: ValidationResult): ValidationResult = {
    ValidationResult(validation, validationResult.isSuccess, validationResult.numErrors, validationResult.total, validationResult.sampleErrorValues)
  }
}

object ValidationConfigurationHelper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * A few different scenarios:
   * - user defined validations, no generated validations
   * - user defined validations, generated validations
   * - user defined validation for 1 data source, generated validations for 2 data sources
   *
   * @param userValidationConf      User defined validation configuration
   * @param generatedValidationConf Generated validation configuration from metadata sources or from generated suggestions
   * @return Merged ValidationConfiguration
   */
  def merge(userValidationConf: List[ValidationConfiguration], generatedValidationConf: ValidationConfiguration): ValidationConfiguration = {
    val userDataSourceValidations = userValidationConf.flatMap(_.dataSources)
    val genDataSourceValidations = generatedValidationConf.dataSources

    val mergedUserDataSourceValidations = userDataSourceValidations.map(userDsValid => {
      val currentUserDsValid = userDsValid._2
      val combinedDataSourceValidations = genDataSourceValidations.get(userDsValid._1)
        .map(dsv2 => {
          dsv2.map(genV => {
            //need to match only on the data source specific options (i.e. table name, file path)
            val genDataSourceOpts = genV.options.filter(o => SPECIFIC_DATA_SOURCE_OPTIONS.contains(o._1))
            val optMatchingValidations = currentUserDsValid.find(userV => {
                val userDataSourceOpts = userV.options.filter(o => SPECIFIC_DATA_SOURCE_OPTIONS.contains(o._1))
                genDataSourceOpts.forall(genOpt => userDataSourceOpts.get(genOpt._1).contains(genOpt._2))
              })

            if (optMatchingValidations.isDefined) {
              LOGGER.debug(s"Found matching data source with manual validations, merging list of validations together, " +
                s"data-source-options=$genDataSourceOpts")
            } else {
              LOGGER.debug("No matching data source found with manual validations, only generated validations found, " +
                s"data-source-options=$genDataSourceOpts")
            }

            optMatchingValidations
              .map(matchUserDef => matchUserDef.copy(validations = genV.validations ++ matchUserDef.validations))
              .getOrElse(genV)
          })
        }).getOrElse(currentUserDsValid)
      (userDsValid._1, combinedDataSourceValidations)
    }).toMap

    //data source from generated not in user
    val genDsValidationNotInUser = genDataSourceValidations.filter(genDs => !userDataSourceValidations.exists(_._1 == genDs._1))
    val allValidations = mergedUserDataSourceValidations ++ genDsValidationNotInUser
    userValidationConf.head.copy(dataSources = allValidations)
  }
}

