package io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.GreatExpectationsExpectationType._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.GreatExpectationsParam._
import io.github.datacatering.datacaterer.core.model.ExternalDataValidation

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
    val valueSet = convertListToString(inputParams(VALUE_SET).asInstanceOf[List[Any]])
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().selectExpr(s"COLLECT_SET($column) AS ${column}_distinct")
      .expr(s"FORALL(${column}_distinct, x -> ARRAY_CONTAINS(ARRAY($valueSet), x))"))
  }
}

case class ColumnDistinctContainSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = convertListToString(inputParams(VALUE_SET).asInstanceOf[List[Any]])
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().selectExpr(s"COLLECT_SET($column) AS ${column}_distinct")
      .expr(s"FORALL(ARRAY($valueSet), x -> ARRAY_CONTAINS(${column}_distinct, x))"))
  }
}

case class ColumnDistinctEqualSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_DISTINCT_VALUES_TO_EQUAL_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    //TODO is it the same validation?
    ColumnDistinctContainSetValidation().getValidation(inputParams)
  }
}

case class ColumnMaxBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MAX_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(getAggregatedValidation(AGGREGATION_MAX, inputParams.get(COLUMN).map(_.toString))
      .between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnMeanBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MEAN_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(getAggregatedValidation(AGGREGATION_AVG, inputParams.get(COLUMN).map(_.toString))
      .between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnMedianBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MEDIAN_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val minValue = inputParams(MIN_VALUE).toString
    val maxValue = inputParams(MAX_VALUE).toString
    List(
      ValidationBuilder()
        .selectExpr(s"PERCENTILE($column, 0.5) AS ${column}_median")
        .expr(s"${column}_median BETWEEN $minValue AND $maxValue")
    )
  }
}

case class ColumnMinBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MIN_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(getAggregatedValidation(AGGREGATION_MIN, inputParams.get(COLUMN).map(_.toString))
      .between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnStddevBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_STDEV_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(getAggregatedValidation(AGGREGATION_STDDEV, inputParams.get(COLUMN).map(_.toString))
      .between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnSumBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_SUM_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(getAggregatedValidation(AGGREGATION_SUM, inputParams.get(COLUMN).map(_.toString))
      .between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnLengthsBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUE_LENGTHS_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(
      ValidationBuilder()
        .expr(s"LENGTH(${inputParams(COLUMN).toString}) BETWEEN ${inputParams(MIN_VALUE)} AND ${inputParams(MAX_VALUE)}")
    )
  }
}

case class ColumnLengthEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUE_LENGTHS_TO_EQUAL
  override val params: List[String] = List(COLUMN, VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(s"LENGTH(${inputParams(COLUMN).toString})").isEqual(inputParams(VALUE)))
  }
}

case class ColumnValuesBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(inputParams(COLUMN).toString).between(inputParams(MIN_VALUE), inputParams(MAX_VALUE)))
  }
}

case class ColumnValuesInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    List(ValidationBuilder().col(inputParams(COLUMN).toString).in(valueSet: _*))
  }
}

case class ColumnValuesInTypeListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_IN_TYPE_LIST
  override val params: List[String] = List(COLUMN, TYPE_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(TYPE_LIST).asInstanceOf[List[Any]]
    List(ValidationBuilder().col(s"TYPEOF(${inputParams(COLUMN).toString})").in(valueSet: _*))
  }
}

case class ColumnValuesDecreasingValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_DECREASING
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val isStrictlyDecreasing = inputParams.get(STRICTLY).exists(_.toString.toLowerCase.toBoolean)
    val lessSign = if (isStrictlyDecreasing) "<" else "<="
    List(ValidationBuilder()
      .selectExpr(s"$column $lessSign LAG($column) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_${column}_decreasing")
      .expr(s"is_${column}_decreasing"))
  }
}

case class ColumnValuesIncreasingValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_INCREASING
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val isStrictlyIncreasing = inputParams.get(STRICTLY).exists(_.toString.toLowerCase.toBoolean)
    val greaterSign = if (isStrictlyIncreasing) ">" else ">="
    List(ValidationBuilder()
      .selectExpr(s"$column $greaterSign LAG($column) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_${column}_increasing")
      .expr(s"is_${column}_increasing"))
  }
}

case class ColumnValuesJsonParsableValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_JSON_PARSEABLE
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().expr(s"TO_JSON(STRUCT(${inputParams(COLUMN).toString})) IS NOT NULL"))
  }
}

case class ColumnValuesIsNullValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_NULL
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(inputParams(COLUMN).toString).isNull)
  }
}

case class ColumnValuesIsTypeValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_OF_TYPE
  override val params: List[String] = List(COLUMN, TYPE_)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(inputParams(COLUMN).toString).hasType(inputParams(TYPE_).toString))
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
    List(ValidationBuilder().expr(s"FROM_JSON(${inputParams(COLUMN).toString}, '${inputParams(JSON_SCHEMA).toString}') IS NOT NULL"))
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
    val checkAllPatterns = inputParams(LIKE_PATTERN_LIST).asInstanceOf[List[String]]
      .map(likePattern => s"LIKE(${inputParams(COLUMN).toString}, '$likePattern')")
      .mkString(mkStringValue)
    List(ValidationBuilder().expr(checkAllPatterns))
  }
}

case class ColumnValuesNotLikePatternValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN
  override val params: List[String] = List(COLUMN, LIKE_PATTERN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().expr(s"NOT LIKE($column, '${inputParams(LIKE_PATTERN).toString}')"))
  }
}

case class ColumnValuesNotLikePatternListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN_LIST
  override val params: List[String] = List(COLUMN, LIKE_PATTERN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val checkAllPatterns = inputParams(LIKE_PATTERN_LIST).asInstanceOf[List[String]]
      .map(likePattern => s"LIKE(${inputParams(COLUMN).toString}, '$likePattern')")
      .mkString(" OR ")
    List(ValidationBuilder().expr(s"NOT ($checkAllPatterns)"))
  }
}

case class ColumnValuesMatchRegexValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_REGEX
  override val params: List[String] = List(COLUMN, REGEX)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(inputParams(COLUMN).toString).matches(inputParams(REGEX).toString))
  }
}

case class ColumnValuesMatchRegexListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST
  override val params: List[String] = List(COLUMN, REGEX_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val mkStringValue = if (inputParams.get(MATCH_ON).contains("all")) " AND " else " OR "
    val checkAllPatterns = inputParams(REGEX_LIST).asInstanceOf[List[String]]
      .map(regex => s"REGEXP(${inputParams(COLUMN).toString}, '$regex')")
      .mkString(mkStringValue)
    List(ValidationBuilder().expr(checkAllPatterns))
  }
}

case class ColumnValuesNotMatchRegexValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX
  override val params: List[String] = List(COLUMN, REGEX)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    List(ValidationBuilder().col(inputParams(COLUMN).toString).notMatches(inputParams(REGEX).toString))
  }
}

case class ColumnValuesNotMatchRegexListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX_LIST
  override val params: List[String] = List(COLUMN, REGEX_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val checkAllPatterns = inputParams(REGEX_LIST).asInstanceOf[List[String]]
      .map(regex => s"REGEXP(${inputParams(COLUMN).toString}, '$regex')")
      .mkString(" OR ")
    List(ValidationBuilder().expr(s"NOT ($checkAllPatterns)"))
  }
}

case class ColumnValuesMatchDateTimeFormatValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_MATCH_STRFTIME_FORMAT
  override val params: List[String] = List(COLUMN, STRFTIME_FORMAT)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val format = inputParams(STRFTIME_FORMAT).toString
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().expr(s"TRY_TO_TIMESTAMP($column, '$format') IS NOT NULL"))
  }
}

case class ColumnValuesNotInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val valueSet = inputParams(VALUE_SET).asInstanceOf[List[Any]]
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().col(column).notIn(valueSet: _*))
  }
}

case class ColumnValuesNotNullValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_NOT_BE_NULL
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().col(column).isNotNull)
  }
}

case class ColumnCompoundUniqueValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COMPOUND_COLUMNS_TO_BE_UNIQUE
  override val params: List[String] = List(COLUMN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columns = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    List(ValidationBuilder().unique(columns: _*))
  }
}

case class ColumnMostCommonValueInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_MOST_COMMON_VALUE_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN, VALUE_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val valueSet = convertListToString(inputParams(VALUE_SET).asInstanceOf[List[Any]])
    List(
      ValidationBuilder()
        .selectExpr(s"MODE($column) AS ${column}_mode")
        .expr(s"ARRAY_CONTAINS(ARRAY($valueSet), ${column}_mode)")
    )
  }
}

case class ColumnPairValueAGreaterThanBValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B
  override val params: List[String] = List(COLUMN_A, COLUMN_B)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columnA = inputParams(COLUMN_A).toString
    val columnB = inputParams(COLUMN_B).toString
    val isEqual = inputParams.get(OR_EQUAL).exists(_.toString.toBoolean)
    val greaterThanSign = if (isEqual) ">=" else ">"
    List(ValidationBuilder().expr(s"$columnA $greaterThanSign $columnB"))
  }
}

case class ColumnPairValueAEqualToBValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PAIR_VALUES_TO_BE_EQUAL
  override val params: List[String] = List(COLUMN_A, COLUMN_B)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columnA = inputParams(COLUMN_A).toString
    val columnB = inputParams(COLUMN_B).toString
    List(ValidationBuilder().expr(s"$columnA == $columnB"))
  }
}

case class ColumnPairValuesInSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PAIR_VALUES_TO_BE_IN_SET
  override val params: List[String] = List(COLUMN_A, COLUMN_B, VALUE_PAIRS_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columnA = inputParams(COLUMN_A).toString
    val columnB = inputParams(COLUMN_B).toString
    val valuePairsSet = inputParams(VALUE_PAIRS_SET).asInstanceOf[List[List[Any]]]
    val condition = valuePairsSet.map(pair => {
      val pair1Parsed = if (pair.head.isInstanceOf[String]) s"'${pair.head}'" else pair.head.toString
      val pair2Parsed = if (pair.last.isInstanceOf[String]) s"'${pair.last}'" else pair.last.toString
      s"($columnA == $pair1Parsed AND $columnB == $pair2Parsed)"
    }).mkString(" OR ")
    List(ValidationBuilder().expr(condition))
  }
}

case class ColumnUniqueValuesProportionBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_PROPORTION_OF_UNIQUE_VALUES_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val minValue = inputParams.get(MIN_VALUE).map(_.toString.toDouble).getOrElse(0.0)
    val maxValue = inputParams.get(MAX_VALUE).map(_.toString.toDouble).getOrElse(1.0)
    List(
      ValidationBuilder().selectExpr(s"COUNT(DISTINCT $column) / COUNT(1) AS ${column}_unique_proportion")
        .expr(s"${column}_unique_proportion BETWEEN $minValue AND $maxValue")
    )
  }
}

case class ColumnQuantileValuesBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_QUANTILE_VALUES_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, QUANTILE_RANGES)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val quantileRanges = inputParams(QUANTILE_RANGES).asInstanceOf[Map[String, List[Any]]]
    val quantiles = quantileRanges(QUANTILES).map(_.toString.toFloat)
    val valueRanges = quantileRanges(VALUE_RANGES).map(range => range.asInstanceOf[List[Any]])
    val minRanges = valueRanges.map(_.head)
    val maxRanges = valueRanges.map(_.last)

    val quantileExprs = quantiles.zipWithIndex.map(quantile => {
      val percentileColName = s"${column}_percentile_${quantile._2}"
      val selectExpr = s"PERCENTILE($column, ${quantile._1}) AS $percentileColName"
      val whereExpr = s"$percentileColName BETWEEN ${minRanges(quantile._2)} AND ${maxRanges(quantile._2)}"
      (selectExpr, whereExpr)
    })
    val selectExpr = quantileExprs.map(_._1)
    val whereExpr = quantileExprs.map(_._2).mkString(" AND ")
    List(ValidationBuilder().selectExpr(selectExpr: _*).expr(whereExpr))
  }
}

case class ColumnUniqueValueCountBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_UNIQUE_VALUE_COUNT_TO_BE_BETWEEN
  override val params: List[String] = List(COLUMN, MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    val minValue = inputParams(MIN_VALUE).toString.toLong
    val maxValue = inputParams(MAX_VALUE).toString.toLong
    List(ValidationBuilder().groupBy(column).count().between(minValue, maxValue))
  }
}

case class ColumnValuesDateParseableValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_VALUES_TO_BE_DATEUTIL_PARSEABLE
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().expr(s"CAST($column AS DATE) IS NOT NULL"))
  }
}

case class MultiColumnSumEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_MULTICOLUMN_SUM_TO_EQUAL
  override val params: List[String] = List(COLUMN_LIST, SUM_TOTAL)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columns = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    val columnSumExpr = s"${columns.mkString(" + ")} AS _column_sum"
    List(
      ValidationBuilder().selectExpr(columnSumExpr)
        .expr(s"_column_sum == ${inputParams(SUM_TOTAL)}")
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
    val columns = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    List(ValidationBuilder().selectExpr(s"COLLECT_SET(CONCAT(${columns.mkString(",")})) AS _unique_multi_column")
      .expr(s"SIZE(_unique_multi_column) == ${columns.size}"))
  }
}

case class TableColumnExistsValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_COLUMN_TO_EXIST
  override val params: List[String] = List(COLUMN)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val column = inputParams(COLUMN).toString
    List(ValidationBuilder().columnNames.matchSet(column))
  }
}

case class TableColumnCountBetweenValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMN_COUNT_TO_BE_BETWEEN
  override val params: List[String] = List(MIN_VALUE, MAX_VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val minValue = inputParams(MIN_VALUE).toString.toInt
    val maxValue = inputParams(MAX_VALUE).toString.toInt
    List(ValidationBuilder().columnNames.countBetween(minValue, maxValue))
  }
}

case class TableColumnCountEqualValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMN_COUNT_TO_EQUAL
  override val params: List[String] = List(VALUE)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val value = inputParams(VALUE).toString.toInt
    List(ValidationBuilder().columnNames.countEqual(value))
  }
}

case class TableColumnMatchOrderedListValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMNS_TO_MATCH_ORDERED_LIST
  override val params: List[String] = List(COLUMN_LIST)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columns = inputParams(COLUMN_LIST).asInstanceOf[List[String]]
    List(ValidationBuilder().columnNames.matchOrder(columns: _*))
  }
}

case class TableColumnMatchSetValidation() extends GreatExpectationsDataValidation {
  override val name: String = EXPECT_TABLE_COLUMNS_TO_MATCH_SET
  override val params: List[String] = List(COLUMN_SET)

  override def getValidation(inputParams: Map[String, Any]): List[ValidationBuilder] = {
    val columns = inputParams(COLUMN_SET).asInstanceOf[List[String]]
    List(ValidationBuilder().columnNames.matchSet(columns: _*))
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
