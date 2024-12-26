package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM, SPECIFIC_DATA_SOURCE_OPTIONS}
import io.github.datacatering.datacaterer.api.model.ValidationConfiguration
import io.github.datacatering.datacaterer.api.{FieldValidationBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.exception.UnsupportedDataValidationAggregateFunctionException
import org.apache.log4j.Logger


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

  def getAggregatedValidation(functionName: String, optFieldName: Option[String]): FieldValidationBuilder = {
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

