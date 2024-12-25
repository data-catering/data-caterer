package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.model.{FieldInSetDataQuality, FieldMatchRegexDataQuality, FieldMaxBetweenDataQuality, FieldMeanBetweenDataQuality, FieldMedianBetweenDataQuality, FieldMinBetweenDataQuality, FieldMissingCountDataQuality, FieldNotInSetDataQuality, FieldNotMatchRegexDataQuality, FieldNotNullDataQuality, FieldStdDevBetweenDataQuality, FieldSumBetweenDataQuality, FieldUniqueDataQuality, FieldValuesBetweenDataQuality, TableCustomSqlDataQuality, TableRowCountBetweenDataQuality, TableRowCountEqualDataQuality}
import org.apache.log4j.Logger
import org.openmetadata.client.model.TestCase

object OpenMetadataDataValidations {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getDataValidations(testCase: TestCase, testParams: Map[String, String], optFieldName: Option[String]): List[ValidationBuilder] = {
    val openMetadataDataQualityList = List(TableCustomSqlDataQuality(), TableRowCountEqualDataQuality(), TableRowCountBetweenDataQuality(),
      FieldMatchRegexDataQuality(), FieldNotMatchRegexDataQuality(), FieldNotNullDataQuality(), FieldNotInSetDataQuality(), FieldInSetDataQuality(),
      FieldUniqueDataQuality(), FieldMaxBetweenDataQuality(), FieldMeanBetweenDataQuality(), FieldMinBetweenDataQuality(), FieldSumBetweenDataQuality(),
      FieldMedianBetweenDataQuality(), FieldValuesBetweenDataQuality(), FieldMissingCountDataQuality(), FieldStdDevBetweenDataQuality()
    )

    val dataQualityParamMatch = openMetadataDataQualityList.filter(dq => dq.matchesParams(testParams))
    val validations = if (dataQualityParamMatch.size > 1) {
      //only known scenario where it will be greater than 1 match is when minValue and maxValue are defined
      //if optFieldName is defined, use field between, else use table count between
      if (optFieldName.isDefined) {
        dataQualityParamMatch.filter(_.isInstanceOf[FieldValuesBetweenDataQuality]).head.getValidation(testParams, optFieldName)
      } else {
        dataQualityParamMatch.filter(_.isInstanceOf[TableRowCountBetweenDataQuality]).head.getValidation(testParams, optFieldName)
      }
    } else if (dataQualityParamMatch.size == 1) {
      dataQualityParamMatch.head.getValidation(testParams, optFieldName)
    } else {
      LOGGER.warn(s"Unknown OpenMetadata parameters given, cannot map to corresponding data validation rule(s), parameters=$testParams")
      List()
    }

    if (testCase.getDescription != null) {
      validations.map(v => v.description(testCase.getDescription))
    } else validations
  }
}
