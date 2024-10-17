package io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model._
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger

import java.io.File
import scala.util.{Failure, Success, Try}

object GreatExpectationsDataValidations {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private val greatExpectationsValidations = List(
    ColumnDistinctInSetValidation(), ColumnDistinctContainSetValidation(), ColumnDistinctEqualSetValidation(),
    ColumnMaxBetweenValidation(), ColumnMeanBetweenValidation(), ColumnMedianBetweenValidation(),
    ColumnMinBetweenValidation(), ColumnStddevBetweenValidation(), ColumnSumBetweenValidation(),
    ColumnLengthsBetweenValidation(), ColumnLengthEqualValidation(), ColumnValuesBetweenValidation(),
    ColumnValuesInSetValidation(), ColumnValuesInTypeListValidation(), ColumnValuesDecreasingValidation(),
    ColumnValuesIncreasingValidation(), ColumnValuesJsonParsableValidation(), ColumnValuesIsNullValidation(),
    ColumnValuesIsTypeValidation(), ColumnValuesUniqueValidation(), ColumnValuesMatchJsonSchemaValidation(),
    ColumnValuesLikePatternValidation(), ColumnValuesLikePatternListValidation(), ColumnValuesMatchRegexValidation(),
    ColumnValuesMatchRegexListValidation(), ColumnValuesMatchDateTimeFormatValidation(), ColumnValuesNotInSetValidation(),
    ColumnValuesNotNullValidation(), ColumnValuesNotLikePatternValidation(), ColumnValuesNotLikePatternListValidation(),
    ColumnValuesNotMatchRegexValidation(), ColumnValuesNotMatchRegexListValidation(), ColumnCompoundUniqueValidation(),
    ColumnMostCommonValueInSetValidation(), ColumnPairValueAGreaterThanBValidation(), ColumnPairValueAEqualToBValidation(),
    ColumnPairValuesInSetValidation(), ColumnUniqueValuesProportionBetweenValidation(), ColumnQuantileValuesBetweenValidation(),
    ColumnUniqueValueCountBetweenValidation(), ColumnValuesDateParseableValidation(), MultiColumnSumEqualValidation(),
    MultiColumnValuesUniqueValidation(), MultiColumnValuesUniqueWithinRecordValidation(), TableColumnExistsValidation(),
    TableColumnCountBetweenValidation(), TableColumnCountEqualValidation(), TableColumnMatchOrderedListValidation(),
    TableColumnMatchSetValidation(), TableCountBetweenValidation(), TableCountEqualValidation()
  )
  private val validationsMapByName = greatExpectationsValidations.map(v => v.name -> v).toMap

  def getDataValidations(expectationsFilePath: String): List[ValidationBuilder] = {
    //expectationsFolder represent the top level folder that keeps all expectations (data quality tests)
    //under the folder, are nested folders relating to the data source
    //within each data source folder exists json files
    //foreach json file, create list of validations
    val expectationsFile = new File(expectationsFilePath)
    val tryParseJson = Try(ObjectMapperUtil.jsonObjectMapper.readValue(expectationsFile, classOf[GreatExpectationsTestSuite]))

    tryParseJson match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to parse Great Expectations JSON file, folder=$expectationsFilePath, file=$expectationsFilePath", exception)
        List()
      case Success(value) =>
        LOGGER.debug(s"Successfully parsed Great Expectations JSON file, folder=$expectationsFilePath, file=$expectationsFilePath")
        createValidationsFromGreatExpectationsTestSuite(value)
    }
  }

  def createValidationsFromGreatExpectationsTestSuite(value: GreatExpectationsTestSuite): List[ValidationBuilder] = {
    value.expectations.flatMap(expectation => {
      val optValidationBuilder = validationsMapByName.get(expectation.expectationType)
      optValidationBuilder.map(validationBuilder => {
        val hasAllRequiredParams = validationBuilder.params.forall(expectation.kwargs.contains)
        if (hasAllRequiredParams) {
          LOGGER.debug(s"Create data validations for Great Expectations expectation, expectation-type=${expectation.expectationType}")
          validationBuilder.validationWithAdditionalArgs(expectation.kwargs)
        } else {
          LOGGER.warn(s"Not all required params found in Great Expectations expectation, unable to create data validation for expectation, " +
            s"expectation-type=${expectation.expectationType}, params=${expectation.kwargs.keys.mkString(",")}, " +
            s"required-params=${validationBuilder.params.mkString(",")}")
          List()
        }
      }).getOrElse(List())
    })
  }
}
