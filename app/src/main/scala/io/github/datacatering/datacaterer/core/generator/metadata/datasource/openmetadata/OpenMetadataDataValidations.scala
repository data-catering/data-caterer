package io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.openmetadata.model.{ColumnInSetDataQuality, ColumnMatchRegexDataQuality, ColumnMaxBetweenDataQuality, ColumnMeanBetweenDataQuality, ColumnMedianBetweenDataQuality, ColumnMinBetweenDataQuality, ColumnMissingCountDataQuality, ColumnNotInSetDataQuality, ColumnNotMatchRegexDataQuality, ColumnNotNullDataQuality, ColumnStdDevBetweenDataQuality, ColumnSumBetweenDataQuality, ColumnUniqueDataQuality, ColumnValuesBetweenDataQuality, TableCustomSqlDataQuality, TableRowCountBetweenDataQuality, TableRowCountEqualDataQuality}
import org.apache.log4j.Logger
import org.openmetadata.client.model.TestCase

object OpenMetadataDataValidations {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def getDataValidations(testCase: TestCase, testParams: Map[String, String], optColumnName: Option[String]): List[ValidationBuilder] = {
    val openMetadataDataQualityList = List(TableCustomSqlDataQuality(), TableRowCountEqualDataQuality(), TableRowCountBetweenDataQuality(),
      ColumnMatchRegexDataQuality(), ColumnNotMatchRegexDataQuality(), ColumnNotNullDataQuality(), ColumnNotInSetDataQuality(), ColumnInSetDataQuality(),
      ColumnUniqueDataQuality(), ColumnMaxBetweenDataQuality(), ColumnMeanBetweenDataQuality(), ColumnMinBetweenDataQuality(), ColumnSumBetweenDataQuality(),
      ColumnMedianBetweenDataQuality(), ColumnValuesBetweenDataQuality(), ColumnMissingCountDataQuality(), ColumnStdDevBetweenDataQuality()
    )

    val dataQualityParamMatch = openMetadataDataQualityList.filter(dq => dq.matchesParams(testParams))
    val validations = if (dataQualityParamMatch.size > 1) {
      //only known scenario where it will be greater than 1 match is when minValue and maxValue are defined
      //if optColumnName is defined, use column between, else use table count between
      if (optColumnName.isDefined) {
        dataQualityParamMatch.filter(_.isInstanceOf[ColumnValuesBetweenDataQuality]).head.getValidation(testParams, optColumnName)
      } else {
        dataQualityParamMatch.filter(_.isInstanceOf[TableRowCountBetweenDataQuality]).head.getValidation(testParams, optColumnName)
      }
    } else if (dataQualityParamMatch.size == 1) {
      dataQualityParamMatch.head.getValidation(testParams, optColumnName)
    } else {
      LOGGER.warn(s"Unknown OpenMetadata parameters given, cannot map to corresponding data validation rule(s), parameters=$testParams")
      List()
    }

    if (testCase.getDescription != null) {
      validations.map(v => v.description(testCase.getDescription))
    } else validations
  }
}
