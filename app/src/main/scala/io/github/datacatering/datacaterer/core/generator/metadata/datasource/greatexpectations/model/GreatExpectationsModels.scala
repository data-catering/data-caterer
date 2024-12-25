package io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.GreatExpectationsExpectationType._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.GreatExpectationsParam._
import io.github.datacatering.datacaterer.core.model.ExternalDataValidation
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanFieldName

@JsonIgnoreProperties(ignoreUnknown = true)
case class GreatExpectationsTestSuite(
                                       @JsonProperty("data_asset_type") dataAssetType: String,
                                       @JsonProperty("expectation_suite_name") expectationSuiteName: String,
                                       expectations: List[GreatExpectationsExpectation] = List(),
                                       @JsonProperty("ge_cloud_id") geCloudId: String = "",
                                       meta: Map[String, Any] = Map()
                                     )

@JsonIgnoreProperties(ignoreUnknown = true)
case class GreatExpectationsExpectation(
                                         @JsonProperty("expectation_type") expectationType: String,
                                         kwargs: Map[String, Any] = Map(),
                                         meta: Map[String, Any] = Map()
                                       )

trait GreatExpectationsDataValidation extends ExternalDataValidation {
  def validationWithAdditionalArgs(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val baseValidation = getValidation(inputParams).map(_.description(name))
    if (inputParams.contains(MOSTLY)) {
      baseValidation.map(_.errorThreshold(inputParams(MOSTLY).toString.toDouble))
    } else baseValidation
  }

  def convertListToString(list: List[Any]): String = {
    list.head match {
      case _: String => list.mkString("'", "','", "'")
      case _ => list.mkString(",")
    }
  }
}

case class ColumnDistinctInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_DISTINCT_VALUES_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    val field = inputParams(COLUMN).toString
    List(ValidationBuilder().field(field).distinctInSet(valueSet: _*))
  }
}

case class ColumnDistinctContainSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    val field = inputParams(COLUMN).toString
    List(ValidationBuilder().field(field).distinctContainsSet(valueSet: _*))
  }
}

case class ColumnDistinctEqualSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_DISTINCT_VALUES_TO_EQUAL_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    val field = inputParams(COLUMN).toString
    List(ValidationBuilder().field(field).distinctEqual(valueSet: _*))
  }
}

case class ColumnMaxBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MAX_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .maxBetween(inputParams(MIN_VALUE), inputParams(MAX_VALUE))
    )
  }
}

case class ColumnMeanBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MEAN_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .meanBetween(inputParams(MIN_VALUE), inputParams(MAX_VALUE))
    )
  }
}

case class ColumnMedianBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MEDIAN_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    val minValue = inputParams(MIN_VALUE).toString
    val maxValue = inputParams(MAX_VALUE).toString
    List(ValidationBuilder().field(field).medianBetween(minValue, maxValue))
  }
}

case class ColumnMinBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MIN_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .minBetween(inputParams(MIN_VALUE), inputParams(MAX_VALUE))
    )
  }
}

case class ColumnStddevBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_STDEV_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .stdDevBetween(inputParams(MIN_VALUE), inputParams(MAX_VALUE))
    )
  }
}

case class ColumnSumBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_SUM_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .sumBetween(inputParams(MIN_VALUE), inputParams(MAX_VALUE))
    )
  }
}

case class ColumnLengthsBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUE_LENGTHS_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .lengthBetween(inputParams(MIN_VALUE).toString.toInt, inputParams(MAX_VALUE).toString.toInt)
    )
  }
}

case class ColumnLengthEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUE_LENGTHS_TO_EQUAL
  override val params: List[String] = List(COLUMN, VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).lengthEqual(inputParams(VALUE).toString.toInt))
  }
}

case class ColumnValuesBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnValuesInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    List(ValidationBuilder().field(inputParams(COLUMN).toString).in(valueSet: _*))
  }
}

case class ColumnValuesInTypeListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_IN_TYPE_LIST
  override val params: List[String] = List(COLUMN, TYPE_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(TYPE_LIST).asInstanceOf[List[String]]
    List(ValidationBuilder().field(inputParams(COLUMN).toString).hasTypes(valueSet: _*))
  }
}

case class ColumnValuesDecreasingValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_DECREASING
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    val isStrictlyDecreasing = inputParams.get(STRICTLY).exists(_.toString.toLowerCase.toBoolean)
    List(ValidationBuilder().field(field).isDecreasing(isStrictlyDecreasing))
  }
}

case class ColumnValuesIncreasingValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_INCREASING
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    val isStrictlyIncreasing = inputParams.get(STRICTLY).exists(_.toString.toLowerCase.toBoolean)
    List(ValidationBuilder().field(field).isIncreasing(isStrictlyIncreasing))
  }
}

case class ColumnValuesJsonParsableValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_JSON_PARSEABLE
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).isJsonParsable())
  }
}

case class ColumnValuesIsNullValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_NULL
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).isNull())
  }
}

case class ColumnValuesIsTypeValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_OF_TYPE
  override val params: List[String] = List(COLUMN, TYPE_)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).hasType(inputParams(TYPE_).toString))
  }
}

case class ColumnValuesUniqueValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_UNIQUE
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().unique(inputParams(COLUMN).toString))
  }
}

case class ColumnValuesMatchJsonSchemaValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_JSON_SCHEMA
  override val params: List[String] = List(COLUMN, JSON_SCHEMA)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).matchJsonSchema(inputParams(JSON_SCHEMA).toString))
  }
}

case class ColumnValuesLikePatternValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN
  override val params: List[String] = List(COLUMN, LIKE_PATTERN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().expr(s"LIKE(${inputParams(COLUMN).toString}, '${inputParams(LIKE_PATTERN).toString}')"))
  }
}

case class ColumnValuesLikePatternListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN_LIST
  override val params: List[String] = List(COLUMN, LIKE_PATTERN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val mkStringValue = if (inputParams.get(MATCH_ON).contains("all")) " AND " else " OR "
    val cleanField = cleanFieldName(inputParams(COLUMN).toString)
    val checkAllPatterns = inputParams(LIKE_PATTERN_LIST).asInstanceOf[List[String]]
      .map(likePattern => s"LIKE($cleanField, '$likePattern')")
      .mkString(mkStringValue)
    List(ValidationBuilder().expr(checkAllPatterns))
  }
}

case class ColumnValuesNotLikePatternValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN
  override val params: List[String] = List(COLUMN, LIKE_PATTERN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val cleanField = cleanFieldName(inputParams(COLUMN).toString)
    List(ValidationBuilder().expr(s"NOT LIKE($cleanField, '${inputParams(LIKE_PATTERN).toString}')"))
  }
}

case class ColumnValuesNotLikePatternListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN_LIST
  override val params: List[String] = List(COLUMN, LIKE_PATTERN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val cleanField = cleanFieldName(inputParams(COLUMN).toString)
    val checkAllPatterns = inputParams(LIKE_PATTERN_LIST).asInstanceOf[List[String]]
      .map(likePattern => s"LIKE($cleanField, '$likePattern')")
      .mkString(" OR ")
    List(ValidationBuilder().expr(s"NOT ($checkAllPatterns)"))
  }
}

case class ColumnValuesMatchRegexValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_REGEX
  override val params: List[String] = List(COLUMN, REGEX)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).matches(inputParams(REGEX).toString))
  }
}

case class ColumnValuesMatchRegexListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST
  override val params: List[String] = List(COLUMN, REGEX_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .matchesList(
          inputParams(REGEX_LIST).asInstanceOf[List[String]],
          inputParams.get(MATCH_ON).contains("all")
        )
    )
  }
}

case class ColumnValuesNotMatchRegexValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX
  override val params: List[String] = List(COLUMN, REGEX)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).matches(inputParams(REGEX).toString, true))
  }
}

case class ColumnValuesNotMatchRegexListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX_LIST
  override val params: List[String] = List(COLUMN, REGEX_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .matchesList(inputParams(REGEX_LIST).asInstanceOf[List[String]], false, true)
    )
  }
}

case class ColumnValuesMatchDateTimeFormatValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_STRFTIME_FORMAT
  override val params: List[String] = List(COLUMN, STRFTIME_FORMAT)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().field(inputParams(COLUMN).toString).matchDateTimeFormat(inputParams(STRFTIME_FORMAT).toString))
  }
}

case class ColumnValuesNotInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    val field = inputParams(COLUMN).toString
    List(ValidationBuilder().field(field).in(valueSet, true))
  }
}

case class ColumnValuesNotNullValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_BE_NULL
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    List(ValidationBuilder().field(field).isNull(true))
  }
}

case class ColumnCompoundUniqueValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COMPOUND_COLUMNS_TO_BE_UNIQUE
  override val params: List[String] = List(COLUMN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val fields = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    List(ValidationBuilder().unique(fields: _*))
  }
}

case class ColumnMostCommonValueInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MOST_COMMON_VALUE_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .mostCommonValueInSet(inputParams(VALUE_SET).asInstanceOf[List[Any]])
    )
  }
}

case class ColumnPairValueAGreaterThanBValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B
  override val params: List[String] = List(COLUMN_A, COLUMN_B)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val cleanFieldA = cleanFieldName(inputParams(COLUMN_A).toString)
    val cleanFieldB = cleanFieldName(inputParams(COLUMN_B).toString)
    val isEqual = inputParams.get(OR_EQUAL).exists(_.toString.toBoolean)
    val greaterThanSign = if (isEqual) ">=" else ">"
    List(ValidationBuilder().expr(s"$cleanFieldA $greaterThanSign $cleanFieldB"))
  }
}

case class ColumnPairValueAEqualToBValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PAIR_VALUES_TO_BE_EQUAL
  override val params: List[String] = List(COLUMN_A, COLUMN_B)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val cleanFieldA = cleanFieldName(inputParams(COLUMN_A).toString)
    val cleanFieldB = cleanFieldName(inputParams(COLUMN_B).toString)
    List(ValidationBuilder().expr(s"$cleanFieldA == $cleanFieldB"))
  }
}

case class ColumnPairValuesInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PAIR_VALUES_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN_A, COLUMN_B, VALUE_PAIRS_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val cleanFieldA = cleanFieldName(inputParams(COLUMN_A).toString)
    val cleanFieldB = cleanFieldName(inputParams(COLUMN_B).toString)
    val valuePairsSet = inputParams(VALUE_PAIRS_SET).asInstanceOf[List[List[Any]]]
    val condition = valuePairsSet.map(pair => {
      val pair1Parsed = if (pair.head.isInstanceOf[String]) s"'${pair.head}'" else pair.head.toString
      val pair2Parsed = if (pair.last.isInstanceOf[String]) s"'${pair.last}'" else pair.last.toString
      s"($cleanFieldA == $pair1Parsed AND $cleanFieldB == $pair2Parsed)"
    }).mkString(" OR ")
    List(ValidationBuilder().expr(condition))
  }
}

case class ColumnUniqueValuesProportionBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PROPORTION_OF_UNIQUE_VALUES_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val minValue = inputParams.get(MIN_VALUE).map(_.toString.toDouble).getOrElse(0.0)
    val maxValue = inputParams.get(MAX_VALUE).map(_.toString.toDouble).getOrElse(1.0)
    List(
      ValidationBuilder()
        .field(inputParams(COLUMN).toString)
        .uniqueValuesProportionBetween(minValue, maxValue)
    )
  }
}

case class ColumnQuantileValuesBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_QUANTILE_VALUES_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, QUANTILE_RANGES)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    val quantileRanges = inputParams(QUANTILE_RANGES).asInstanceOf[Map[String, List[Any]]]
    val quantiles = quantileRanges(QUANTILES).map(_.toString.toDouble)
    val valueRanges = quantileRanges(VALUE_RANGES).map(range => range.asInstanceOf[List[Any]])
    val mapQuantiles = quantiles.zip(valueRanges)
      .map(q => q._1 -> (q._2.head.toString.toDouble, q._2.last.toString.toDouble))
      .toMap

    List(ValidationBuilder().field(field).quantileValuesBetween(mapQuantiles))
  }
}

case class ColumnUniqueValueCountBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_UNIQUE_VALUE_COUNT_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    val minValue = inputParams(MIN_VALUE).toString.toLong
    val maxValue = inputParams(MAX_VALUE).toString.toLong
    List(ValidationBuilder().groupBy(field).count().between(minValue, maxValue))
  }
}

case class ColumnValuesDateParseableValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_DATEUTIL_PARSEABLE
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val cleanField = cleanFieldName(inputParams(COLUMN).toString)
    List(ValidationBuilder().expr(s"CAST($cleanField AS DATE) IS NOT NULL"))
  }
}

case class MultiColumnSumEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_MULTICOLUMN_SUM_TO_EQUAL
  override val params: List[String] = List(COLUMN_LIST, SUM_TOTAL)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val fields = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    val fieldSumExpr = s"${fields.map(cleanFieldName).mkString(" + ")} AS _field_sum"
    List(
      ValidationBuilder().selectExpr(fieldSumExpr)
        .expr(s"_field_sum == ${inputParams(SUM_TOTAL)}")
    )
  }
}

case class MultiColumnValuesUniqueValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_MULTICOLUMNVALUES_TO_BE_UNIQUE
  override val params: List[String] = List(COLUMN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    ColumnCompoundUniqueValidation().getValidation(inputParams)
  }
}

case class MultiColumnValuesUniqueWithinRecordValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_SELECT_COLUMN_VALUES_TO_BE_UNIQUE_WITHIN_RECORD
  override val params: List[String] = List(COLUMN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val fields = inputParams(COLUMN_LIST).asInstanceOf[List[String]].map(cleanFieldName)
    List(ValidationBuilder().selectExpr(s"COLLECT_SET(CONCAT(${fields.mkString(",")})) AS _unique_multi_field")
      .expr(s"SIZE(_unique_multi_field) == ${fields.size}"))
  }
}

case class TableColumnExistsValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_TO_EXIST
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val field = inputParams(COLUMN).toString
    List(ValidationBuilder().fieldNames.matchSet(field))
  }
}

case class TableColumnCountBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMN_COUNT_TO_BE_BETWEEN
  override val params: List[String] = List(MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val minValue = inputParams(MIN_VALUE).toString.toInt
    val maxValue = inputParams(MAX_VALUE).toString.toInt
    List(ValidationBuilder().fieldNames.countBetween(minValue, maxValue))
  }
}

case class TableColumnCountEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMN_COUNT_TO_EQUAL
  override val params: List[String] = List(VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val value = inputParams(VALUE).toString.toInt
    List(ValidationBuilder().fieldNames.countEqual(value))
  }
}

case class TableColumnMatchOrderedListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMNS_TO_MATCH_ORDERED_LIST
  override val params: List[String] = List(COLUMN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val fields = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    List(ValidationBuilder().fieldNames.matchOrder(fields: _*))
  }
}

case class TableColumnMatchSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMNS_TO_MATCH_SET
  override val params: List[String] = List(COLUMN_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val fields = inputParams(COLUMN_SET).asInstanceOf[List[String]]
    List(ValidationBuilder().fieldNames.matchSet(fields: _*))
  }
}

case class TableCountBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_ROW_COUNT_TO_BE_BETWEEN
  override val params: List[String] = List(MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val minValue = inputParams(MIN_VALUE).toString.toLong
    val maxValue = inputParams(MAX_VALUE).toString.toLong
    List(ValidationBuilder().count().between(minValue, maxValue))
  }
}

case class TableCountEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_ROW_COUNT_TO_EQUAL
  override val params: List[String] = List(VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val value = inputParams(VALUE).toString.toLong
    List(ValidationBuilder().count().isEqual(value))
  }
}


object GreatExpectationsParam {
  lazy val VALUE = "value"
  lazy val VALUE_SET = "value_set"

  lazy val COLUMN = "column"
  lazy val COLUMN_A = "column_A"
  lazy val COLUMN_B = "column_B"
  lazy val COLUMN_SET = "column_set"
  lazy val COLUMN_LIST = "column_list"

  lazy val MIN_VALUE = "min_value"
  lazy val MAX_VALUE = "max_value"
  lazy val QUANTILE_RANGES = "quantile_ranges"
  lazy val QUANTILES = "quantiles"
  lazy val VALUE_RANGES = "value_ranges"
  lazy val VALUE_PAIRS_SET = "value_pairs_set"
  lazy val TYPE_ = "type_"
  lazy val TYPE_LIST = "type_list"
  lazy val SUM_TOTAL = "sum_total"
  lazy val OR_EQUAL = "or_equal"
  lazy val STRFTIME_FORMAT = "strftime_format"
  lazy val JSON_SCHEMA = "json_schema"

  lazy val LIKE_PATTERN = "like_pattern"
  lazy val LIKE_PATTERN_LIST = "like_pattern_list"
  lazy val REGEX = "regex"
  lazy val REGEX_LIST = "regex_list"
  lazy val MATCH_ON = "match_on"

  lazy val STRICTLY = "strictly"
  lazy val MOSTLY = "mostly"
}

object GreatExpectationsExpectationType {
  lazy val EXPECT_COLUMN_DISTINCT_VALUES_TO_BE_IN_SET = "expect_column_distinct_values_to_be_in_set"
  lazy val EXPECT_COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET = "expect_column_distinct_values_to_contain_set"
  lazy val EXPECT_COLUMN_DISTINCT_VALUES_TO_EQUAL_SET = "expect_column_distinct_values_to_equal_set"
  lazy val EXPECT_COLUMN_MAX_TO_BE_BETWEEN = "expect_column_max_to_be_between"
  lazy val EXPECT_COLUMN_MEAN_TO_BE_BETWEEN = "expect_column_mean_to_be_between"
  lazy val EXPECT_COLUMN_MEDIAN_TO_BE_BETWEEN = "expect_column_median_to_be_between"
  lazy val EXPECT_COLUMN_MIN_TO_BE_BETWEEN = "expect_column_min_to_be_between"
  lazy val EXPECT_COLUMN_STDEV_TO_BE_BETWEEN = "expect_column_stdev_to_be_between"
  lazy val EXPECT_COLUMN_SUM_TO_BE_BETWEEN = "expect_column_sum_to_be_between"
  lazy val EXPECT_COLUMN_VALUE_LENGTHS_TO_BE_BETWEEN = "expect_column_value_lengths_to_be_between"
  lazy val EXPECT_COLUMN_VALUE_LENGTHS_TO_EQUAL = "expect_column_value_lengths_to_equal"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_BETWEEN = "expect_column_values_to_be_between"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_IN_SET = "expect_column_values_to_be_in_set"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_IN_TYPE_LIST = "expect_column_values_to_be_in_type_list"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_DECREASING = "expect_column_values_to_be_decreasing"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_INCREASING = "expect_column_values_to_be_increasing"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_JSON_PARSEABLE = "expect_column_values_to_be_json_parseable"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_NULL = "expect_column_values_to_be_null"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_OF_TYPE = "expect_column_values_to_be_of_type"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_UNIQUE = "expect_column_values_to_be_unique"
  lazy val EXPECT_COLUMN_VALUES_TO_MATCH_JSON_SCHEMA = "expect_column_values_to_match_json_schema"
  lazy val EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN = "expect_column_values_to_match_like_pattern"
  lazy val EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN_LIST = "expect_column_values_to_match_like_pattern_list"
  lazy val EXPECT_COLUMN_VALUES_TO_MATCH_REGEX = "expect_column_values_to_match_regex"
  lazy val EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST = "expect_column_values_to_match_regex_list"
  lazy val EXPECT_COLUMN_VALUES_TO_MATCH_STRFTIME_FORMAT = "expect_column_values_to_match_strftime_format"
  lazy val EXPECT_COLUMN_VALUES_TO_NOT_BE_IN_SET = "expect_column_values_to_not_be_in_set"
  lazy val EXPECT_COLUMN_VALUES_TO_NOT_BE_NULL = "expect_column_values_to_not_be_null"
  lazy val EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN = "expect_column_values_to_not_match_like_pattern"
  lazy val EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN_LIST = "expect_column_values_to_not_match_like_pattern_list"
  lazy val EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX = "expect_column_values_to_not_match_regex"
  lazy val EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX_LIST = "expect_column_values_to_not_match_regex_list"

  lazy val EXPECT_COLUMN_MOST_COMMON_VALUE_TO_BE_IN_SET = "expect_column_most_common_value_to_be_in_set"
  lazy val EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B = "expect_column_pair_values_a_to_be_greater_than_b"
  lazy val EXPECT_COLUMN_PAIR_VALUES_TO_BE_EQUAL = "expect_column_pair_values_to_be_equal"
  lazy val EXPECT_COLUMN_PAIR_VALUES_TO_BE_IN_SET = "expect_column_pair_values_to_be_in_set"
  lazy val EXPECT_COLUMN_PROPORTION_OF_UNIQUE_VALUES_TO_BE_BETWEEN = "expect_column_proportion_of_unique_values_to_be_between"
  lazy val EXPECT_COLUMN_QUANTILE_VALUES_TO_BE_BETWEEN = "expect_column_quantile_values_to_be_between"
  lazy val EXPECT_COLUMN_UNIQUE_VALUE_COUNT_TO_BE_BETWEEN = "expect_column_unique_value_count_to_be_between"
  lazy val EXPECT_COLUMN_VALUES_TO_BE_DATEUTIL_PARSEABLE = "expect_column_values_to_be_dateutil_parseable"

  lazy val EXPECT_COMPOUND_COLUMNS_TO_BE_UNIQUE = "expect_compound_columns_to_be_unique"
  lazy val EXPECT_MULTICOLUMN_SUM_TO_EQUAL = "expect_multicolumn_sum_to_equal"
  lazy val EXPECT_MULTICOLUMNVALUES_TO_BE_UNIQUE = "expect_multicolumnvalues_to_be_unique"
  lazy val EXPECT_SELECT_COLUMN_VALUES_TO_BE_UNIQUE_WITHIN_RECORD = "expect_select_column_values_to_be_unique_within_record"

  lazy val EXPECT_COLUMN_TO_EXIST = "expect_column_to_exist"
  lazy val EXPECT_TABLE_COLUMN_COUNT_TO_BE_BETWEEN = "expect_table_column_count_to_be_between"
  lazy val EXPECT_TABLE_COLUMN_COUNT_TO_EQUAL = "expect_table_column_count_to_equal"
  lazy val EXPECT_TABLE_COLUMNS_TO_MATCH_ORDERED_LIST = "expect_table_columns_to_match_ordered_list"
  lazy val EXPECT_TABLE_COLUMNS_TO_MATCH_SET = "expect_table_columns_to_match_set"
  lazy val EXPECT_TABLE_ROW_COUNT_TO_BE_BETWEEN = "expect_table_row_count_to_be_between"
  lazy val EXPECT_TABLE_ROW_COUNT_TO_EQUAL = "expect_table_row_count_to_equal"
  lazy val EXPECT_TABLE_ROW_COUNT_TO_EQUAL_OTHER_TABLE = "expect_table_row_count_to_equal_other_table"
}
