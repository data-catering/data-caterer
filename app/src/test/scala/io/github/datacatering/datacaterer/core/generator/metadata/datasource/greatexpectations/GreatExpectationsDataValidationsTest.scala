package io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM, VALIDATION_FIELD_NAME_COUNT_BETWEEN, VALIDATION_FIELD_NAME_COUNT_EQUAL, VALIDATION_FIELD_NAME_MATCH_ORDER, VALIDATION_FIELD_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, FieldNamesValidation, GroupByValidation}
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.GreatExpectationsExpectationType._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.GreatExpectationsParam._
import io.github.datacatering.datacaterer.core.generator.metadata.datasource.greatexpectations.model.{GreatExpectationsExpectation, GreatExpectationsTestSuite}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import io.github.datacatering.datacaterer.core.validator.ValidationHelper.getValidationType
import org.apache.spark.sql.Encoders

import java.sql.Timestamp
import java.time.Instant

class GreatExpectationsDataValidationsTest extends SparkSuite {

  private val df = sparkSession.createDataFrame(Seq(TaxiRecord(), TaxiRecord(), TaxiRecord(), TaxiRecord(), TaxiRecord()))
  df.cache()
  private val dfCount = df.count()


  test("Can create data validations based on Great Expectations JSON file") {
    val result = GreatExpectationsDataValidations.getDataValidations("src/test/resources/sample/validation/great-expectations/taxi-expectations.json")

    assertResult(11)(result.size)
    val fieldNameMatch = result.filter(_.validation.description.contains(EXPECT_TABLE_COLUMNS_TO_MATCH_ORDERED_LIST))
    assertResult(1)(fieldNameMatch.size)
    assert(fieldNameMatch.head.validation.isInstanceOf[FieldNamesValidation])
    assert(fieldNameMatch.head.validation.asInstanceOf[FieldNamesValidation].names sameElements Array("vendor_id", "pickup_datetime",
      "dropoff_datetime", "passenger_count", "trip_distance", "rate_code_id", "store_and_fwd_flag", "pickup_location_id",
      "dropoff_location_id", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
      "improvement_surcharge", "total_amount", "congestion_surcharge"))
  }

  test("Can run validations from Great Expectations against Spark dataframe") {
    implicit val encoder = Encoders.kryo[TaxiRecord]
    val validations = GreatExpectationsDataValidations.getDataValidations("src/test/resources/sample/validation/great-expectations/taxi-expectations.json")

    val results = validations.flatMap(validBuilder => {
      getValidationType(validBuilder.validation, "/tmp/record-tracking").validate(df, df.count())
    })

    assertResult(11)(results.size)
    assertResult(4)(results.count(_.isSuccess))
  }

  test("Can return back empty validations if expected parameters are not found for expectations") {
    val expectations = List(GreatExpectationsExpectation(
      EXPECT_COLUMN_DISTINCT_VALUES_TO_BE_IN_SET,
      Map(COLUMN -> "vendor_id")
    ))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assert(result.isEmpty)
  }

  test("Can convert EXPECT_COLUMN_DISTINCT_VALUES_TO_BE_IN_SET Great Expectations test to validation") {
    val result = checkExpectation(
      EXPECT_COLUMN_DISTINCT_VALUES_TO_BE_IN_SET,
      Map(COLUMN -> "vendor_id", VALUE_SET -> List("abc123", "xyz789")),
      "FORALL(vendor_id_distinct, x -> ARRAY_CONTAINS(ARRAY('abc123','xyz789'), x))"
    )
    assertResult("COLLECT_SET(`vendor_id`) AS vendor_id_distinct")(result.head.validation.asInstanceOf[ExpressionValidation].selectExpr.head)
  }

  test("Can convert EXPECT_COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET Great Expectations test to validation") {
    val result = checkExpectation(
      EXPECT_COLUMN_DISTINCT_VALUES_TO_CONTAIN_SET,
      Map(COLUMN -> "vendor_id", VALUE_SET -> List("abc123", "xyz789")),
      "FORALL(ARRAY('abc123','xyz789'), x -> ARRAY_CONTAINS(vendor_id_distinct, x))"
    )
    assertResult("COLLECT_SET(`vendor_id`) AS vendor_id_distinct")(result.head.validation.asInstanceOf[ExpressionValidation].selectExpr.head)
  }

  test("Can convert EXPECT_COLUMN_DISTINCT_VALUES_TO_EQUAL_SET Great Expectations test to validation") {
    val result = checkExpectation(
      EXPECT_COLUMN_DISTINCT_VALUES_TO_EQUAL_SET,
      Map(COLUMN -> "vendor_id", VALUE_SET -> List("abc123", "xyz789")),
      "ARRAY_SIZE(ARRAY_EXCEPT(ARRAY('abc123','xyz789'), vendor_id_distinct)) == 0"
    )
    assertResult("COLLECT_SET(`vendor_id`) AS vendor_id_distinct")(result.head.validation.asInstanceOf[ExpressionValidation].selectExpr.head)
  }

  test("Can convert EXPECT_COLUMN_MAX_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_MAX_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 10, MAX_VALUE -> 20),
      "max(`passenger_count`) BETWEEN 10 AND 20", AGGREGATION_MAX, "`passenger_count`", Seq()
    )
  }

  test("Can convert EXPECT_COLUMN_MEAN_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_MEAN_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 10, MAX_VALUE -> 20),
      "avg(`passenger_count`) BETWEEN 10 AND 20", AGGREGATION_AVG, "`passenger_count`", Seq()
    )
  }

  test("Can convert EXPECT_COLUMN_MEDIAN_TO_BE_BETWEEN Great Expectations test to validation") {
    val res = checkExpectation(
      EXPECT_COLUMN_MEDIAN_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 10, MAX_VALUE -> 20),
      "passenger_count_median BETWEEN 10 AND 20"
    )
    assertResult(List("PERCENTILE(`passenger_count`, 0.5) AS passenger_count_median"))(res.head.validation.asInstanceOf[ExpressionValidation].selectExpr)
  }

  test("Can convert EXPECT_COLUMN_MIN_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_MIN_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 10, MAX_VALUE -> 20),
      "min(`passenger_count`) BETWEEN 10 AND 20", AGGREGATION_MIN, "`passenger_count`", Seq()
    )
  }

  test("Can convert EXPECT_COLUMN_STDEV_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_STDEV_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 10, MAX_VALUE -> 20),
      "stddev(`passenger_count`) BETWEEN 10 AND 20", AGGREGATION_STDDEV, "`passenger_count`", Seq()
    )
  }

  test("Can convert EXPECT_COLUMN_SUM_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_SUM_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 10, MAX_VALUE -> 20),
      "sum(`passenger_count`) BETWEEN 10 AND 20", AGGREGATION_SUM, "`passenger_count`", Seq()
    )
  }

  test("Can convert EXPECT_COLUMN_VALUE_LENGTHS_TO_BE_BETWEEN Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUE_LENGTHS_TO_BE_BETWEEN,
      Map(COLUMN -> "payment_type", MIN_VALUE -> 1, MAX_VALUE -> 10),
      "LENGTH(`payment_type`) BETWEEN 1 AND 10"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUE_LENGTHS_TO_EQUAL Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUE_LENGTHS_TO_EQUAL,
      Map(COLUMN -> "payment_type", VALUE -> 5),
      "LENGTH(`payment_type`) == 5"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_BETWEEN Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", MIN_VALUE -> 5, MAX_VALUE -> 10),
      "`passenger_count` BETWEEN 5 AND 10"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_IN_SET Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_IN_SET,
      Map(COLUMN -> "passenger_count", VALUE_SET -> List(1, 2, 3)),
      "`passenger_count` IN (1,2,3)"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_IN_SET Great Expectations test with strings to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_IN_SET,
      Map(COLUMN -> "payment_type", VALUE_SET -> List("peter", "flook")),
      "`payment_type` IN ('peter','flook')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_IN_TYPE_LIST Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_IN_TYPE_LIST,
      Map(COLUMN -> "payment_type", TYPE_LIST -> List("string")),
      "TYPEOF(`payment_type`) IN ('string')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_DECREASING Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_DECREASING,
      Map(COLUMN -> "fare_amount"),
      "is_fare_amount_decreasing",
      "`fare_amount` <= LAG(`fare_amount`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_fare_amount_decreasing"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_DECREASING Great Expectations test with strictly true to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_DECREASING,
      Map(COLUMN -> "fare_amount", STRICTLY -> "true"),
      "is_fare_amount_decreasing",
      "`fare_amount` < LAG(`fare_amount`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_fare_amount_decreasing"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_INCREASING Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_INCREASING,
      Map(COLUMN -> "fare_amount"),
      "is_fare_amount_increasing",
      "`fare_amount` >= LAG(`fare_amount`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_fare_amount_increasing"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_INCREASING Great Expectations test with strictly true to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_INCREASING,
      Map(COLUMN -> "fare_amount", STRICTLY -> "true"),
      "is_fare_amount_increasing",
      "`fare_amount` > LAG(`fare_amount`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_fare_amount_increasing"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_JSON_PARSEABLE Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_JSON_PARSEABLE,
      Map(COLUMN -> "pickup_location_id"),
      "GET_JSON_OBJECT(`pickup_location_id`, '$') IS NOT NULL"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_NULL Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_NULL,
      Map(COLUMN -> "pickup_location_id"),
      "ISNULL(`pickup_location_id`)"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_OF_TYPE Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_OF_TYPE,
      Map(COLUMN -> "pickup_location_id", TYPE_ -> "string"),
      "TYPEOF(`pickup_location_id`) == 'string'"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_UNIQUE Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_UNIQUE,
      Map(COLUMN -> "vendor_id"),
      "count == 1", AGGREGATION_COUNT, "unique", Seq("vendor_id")
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_JSON_SCHEMA Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_JSON_SCHEMA,
      Map(COLUMN -> "pickup_location_id", JSON_SCHEMA -> "a INT, b DOUBLE"),
      "FROM_JSON(`pickup_location_id`, 'a INT, b DOUBLE') IS NOT NULL"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN,
      Map(COLUMN -> "pickup_location_id", LIKE_PATTERN -> "_%Spark"),
      "LIKE(pickup_location_id, '_%Spark')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN_LIST Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN_LIST,
      Map(COLUMN -> "pickup_location_id", LIKE_PATTERN_LIST -> List("_%Spark", "Spark_%")),
      "LIKE(`pickup_location_id`, '_%Spark') OR LIKE(`pickup_location_id`, 'Spark_%')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN_LIST Great Expectations test with match all to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_LIKE_PATTERN_LIST,
      Map(COLUMN -> "pickup_location_id", LIKE_PATTERN_LIST -> List("_%Spark", "Spark_%"), MATCH_ON -> "all"),
      "LIKE(`pickup_location_id`, '_%Spark') AND LIKE(`pickup_location_id`, 'Spark_%')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN,
      Map(COLUMN -> "payment_type", LIKE_PATTERN -> "_%Spark"),
      "NOT LIKE(`payment_type`, '_%Spark')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN_LIST Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_NOT_MATCH_LIKE_PATTERN_LIST,
      Map(COLUMN -> "payment_type", LIKE_PATTERN_LIST -> List("_%Spark", "Spark_%")),
      "NOT (LIKE(`payment_type`, '_%Spark') OR LIKE(`payment_type`, 'Spark_%'))"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_REGEX Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_REGEX,
      Map(COLUMN -> "pickup_location_id", REGEX -> ".*Spark"),
      "REGEXP(`pickup_location_id`, '.*Spark')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST,
      Map(COLUMN -> "pickup_location_id", REGEX_LIST -> List(".*Spark", "Spark.*")),
      "(REGEXP(`pickup_location_id`, '.*Spark') OR REGEXP(`pickup_location_id`, 'Spark.*'))"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST Great Expectations test with match all to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_REGEX_LIST,
      Map(COLUMN -> "pickup_location_id", REGEX_LIST -> List(".*Spark", "Spark.*"), MATCH_ON -> "all"),
      "(REGEXP(`pickup_location_id`, '.*Spark') AND REGEXP(`pickup_location_id`, 'Spark.*'))"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX,
      Map(COLUMN -> "pickup_location_id", REGEX -> ".*Spark"),
      "!REGEXP(`pickup_location_id`, '.*Spark')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX_LIST Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_NOT_MATCH_REGEX_LIST,
      Map(COLUMN -> "pickup_location_id", REGEX_LIST -> List(".*Spark", "Spark.*")),
      "NOT (REGEXP(`pickup_location_id`, '.*Spark') OR REGEXP(`pickup_location_id`, 'Spark.*'))"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_MATCH_STRFTIME_FORMAT Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_MATCH_STRFTIME_FORMAT,
      Map(COLUMN -> "pickup_datetime", STRFTIME_FORMAT -> "yyyy-MM-dd HH:mm"),
      "TRY_TO_TIMESTAMP(`pickup_datetime`, 'yyyy-MM-dd HH:mm') IS NOT NULL"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_NOT_BE_IN_SET Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_NOT_BE_IN_SET,
      Map(COLUMN -> "payment_type", VALUE_SET -> List("peter", "flook")),
      "NOT `payment_type` IN ('peter','flook')"
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_NOT_BE_NULL Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_NOT_BE_NULL,
      Map(COLUMN -> "payment_type"),
      "ISNOTNULL(`payment_type`)"
    )
  }

  test("Can convert EXPECT_COMPOUND_COLUMNS_TO_BE_UNIQUE Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COMPOUND_COLUMNS_TO_BE_UNIQUE,
      Map(COLUMN_LIST -> List("payment_type", "vendor_id")),
      "count == 1", AGGREGATION_COUNT, "unique", Seq("payment_type", "vendor_id")
    )
  }

  test("Can convert EXPECT_COLUMN_MOST_COMMON_VALUE_TO_BE_IN_SET Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_MOST_COMMON_VALUE_TO_BE_IN_SET,
      Map(COLUMN -> "payment_type", VALUE_SET -> List("peter", "flook")),
      "ARRAY_CONTAINS(ARRAY('peter','flook'), payment_type_mode)",
      "MODE(`payment_type`) AS payment_type_mode"
    )
  }

  test("Can convert EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B,
      Map(COLUMN_A -> "total_amount", COLUMN_B -> "passenger_count"),
      "`total_amount` > `passenger_count`"
    )
  }

  test("Can convert EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B Great Expectations test with or_equal to validation") {
    checkExpectation(
      EXPECT_COLUMN_PAIR_VALUES_A_TO_BE_GREATER_THAN_B,
      Map(COLUMN_A -> "total_amount", COLUMN_B -> "passenger_count", OR_EQUAL -> "true"),
      "`total_amount` >= `passenger_count`"
    )
  }

  test("Can convert EXPECT_COLUMN_PAIR_VALUES_TO_BE_EQUAL Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_PAIR_VALUES_TO_BE_EQUAL,
      Map(COLUMN_A -> "total_amount", COLUMN_B -> "passenger_count"),
      "`total_amount` == `passenger_count`"
    )
  }

  test("Can convert EXPECT_COLUMN_PAIR_VALUES_TO_BE_IN_SET Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_PAIR_VALUES_TO_BE_IN_SET,
      Map(COLUMN_A -> "payment_type", COLUMN_B -> "passenger_count", VALUE_PAIRS_SET -> List(List("peter", 10), List("flook", 20))),
      "(`payment_type` == 'peter' AND `passenger_count` == 10) OR (`payment_type` == 'flook' AND `passenger_count` == 20)"
    )
  }

  test("Can convert EXPECT_COLUMN_PROPORTION_OF_UNIQUE_VALUES_TO_BE_BETWEEN Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_PROPORTION_OF_UNIQUE_VALUES_TO_BE_BETWEEN,
      Map(COLUMN -> "payment_type", MIN_VALUE -> 0.4, MAX_VALUE -> 0.5),
      "payment_type_unique_proportion BETWEEN 0.4 AND 0.5",
      "COUNT(DISTINCT `payment_type`) / COUNT(1) AS payment_type_unique_proportion"
    )
  }

  test("Can convert EXPECT_COLUMN_QUANTILE_VALUES_TO_BE_BETWEEN Great Expectations test to validation") {
    val result = checkExpectation(
      EXPECT_COLUMN_QUANTILE_VALUES_TO_BE_BETWEEN,
      Map(COLUMN -> "passenger_count", QUANTILE_RANGES -> Map(QUANTILES -> List(0.1, 0.4), VALUE_RANGES -> List(List(10, 12), List(20, 30)))),
      "passenger_count_percentile_0 BETWEEN 10.0 AND 12.0 AND passenger_count_percentile_1 BETWEEN 20.0 AND 30.0"
    )
    assertResult("PERCENTILE(`passenger_count`, 0.1) AS passenger_count_percentile_0")(result.head.validation.asInstanceOf[ExpressionValidation].selectExpr.head)
    assertResult("PERCENTILE(`passenger_count`, 0.4) AS passenger_count_percentile_1")(result.head.validation.asInstanceOf[ExpressionValidation].selectExpr.last)
  }

  test("Can convert EXPECT_COLUMN_UNIQUE_VALUE_COUNT_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_COLUMN_UNIQUE_VALUE_COUNT_TO_BE_BETWEEN,
      Map(COLUMN -> "payment_type", MIN_VALUE -> 10, MAX_VALUE -> 12),
      "count BETWEEN 10 AND 12", AGGREGATION_COUNT, "", Seq("payment_type")
    )
  }

  test("Can convert EXPECT_COLUMN_VALUES_TO_BE_DATEUTIL_PARSEABLE Great Expectations test to validation") {
    checkExpectation(
      EXPECT_COLUMN_VALUES_TO_BE_DATEUTIL_PARSEABLE,
      Map(COLUMN -> "pickup_datetime"),
      "CAST(`pickup_datetime` AS DATE) IS NOT NULL"
    )
  }

  test("Can convert EXPECT_MULTICOLUMN_SUM_TO_EQUAL Great Expectations test to validation") {
    checkExpectation(
      EXPECT_MULTICOLUMN_SUM_TO_EQUAL,
      Map(COLUMN_LIST -> List("fare_amount", "tip_amount"), SUM_TOTAL -> 100),
      "_field_sum == 100",
      "`fare_amount` + `tip_amount` AS _field_sum"
    )
  }

  test("Can convert EXPECT_MULTICOLUMNVALUES_TO_BE_UNIQUE Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_MULTICOLUMNVALUES_TO_BE_UNIQUE,
      Map(COLUMN_LIST -> List("payment_type", "vendor_id")),
      "count == 1", AGGREGATION_COUNT, "unique", Seq("payment_type", "vendor_id")
    )
  }

  test("Can convert EXPECT_SELECT_COLUMN_VALUES_TO_BE_UNIQUE_WITHIN_RECORD Great Expectations test to validation") {
    checkExpectation(
      EXPECT_SELECT_COLUMN_VALUES_TO_BE_UNIQUE_WITHIN_RECORD,
      Map(COLUMN_LIST -> List("payment_type", "vendor_id")),
      "SIZE(_unique_multi_field) == 2",
      "COLLECT_SET(CONCAT(`payment_type`,`vendor_id`)) AS _unique_multi_field"
    )
  }

  test("Can convert EXPECT_COLUMN_TO_EXIST Great Expectations test to validation") {
    val expectations = List(GreatExpectationsExpectation(EXPECT_COLUMN_TO_EXIST, Map(COLUMN -> "payment_type")))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    assert(result.head.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_MATCH_SET)(result.head.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assert(result.head.validation.asInstanceOf[FieldNamesValidation].names sameElements Array("payment_type"))
  }

  test("Can convert EXPECT_TABLE_COLUMN_COUNT_TO_BE_BETWEEN Great Expectations test to validation") {
    val expectations = List(GreatExpectationsExpectation(EXPECT_TABLE_COLUMN_COUNT_TO_BE_BETWEEN, Map(MIN_VALUE -> 5, MAX_VALUE -> 6)))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    assert(result.head.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_COUNT_BETWEEN)(result.head.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assertResult(5)(result.head.validation.asInstanceOf[FieldNamesValidation].min)
    assertResult(6)(result.head.validation.asInstanceOf[FieldNamesValidation].max)
  }

  test("Can convert EXPECT_TABLE_COLUMN_COUNT_TO_EQUAL Great Expectations test to validation") {
    val expectations = List(GreatExpectationsExpectation(EXPECT_TABLE_COLUMN_COUNT_TO_EQUAL, Map(VALUE -> 5)))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    assert(result.head.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_COUNT_EQUAL)(result.head.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assertResult(5)(result.head.validation.asInstanceOf[FieldNamesValidation].count)
  }

  test("Can convert EXPECT_TABLE_COLUMNS_TO_MATCH_ORDERED_LIST Great Expectations test to validation") {
    val expectations = List(GreatExpectationsExpectation(EXPECT_TABLE_COLUMNS_TO_MATCH_ORDERED_LIST, Map(COLUMN_LIST -> List("payment_type", "vendor_id"))))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    assert(result.head.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_MATCH_ORDER)(result.head.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assert(result.head.validation.asInstanceOf[FieldNamesValidation].names sameElements Array("payment_type", "vendor_id"))
  }

  test("Can convert EXPECT_TABLE_COLUMNS_TO_MATCH_SET Great Expectations test to validation") {
    val expectations = List(GreatExpectationsExpectation(EXPECT_TABLE_COLUMNS_TO_MATCH_SET, Map(COLUMN_SET -> List("payment_type", "passenger_count"))))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    assert(result.head.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_MATCH_SET)(result.head.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assert(result.head.validation.asInstanceOf[FieldNamesValidation].names sameElements Array("payment_type", "passenger_count"))
  }

  test("Can convert EXPECT_TABLE_ROW_COUNT_TO_BE_BETWEEN Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_TABLE_ROW_COUNT_TO_BE_BETWEEN,
      Map(MIN_VALUE -> 10, MAX_VALUE -> 20),
      "count BETWEEN 10 AND 20", AGGREGATION_COUNT, "", Seq()
    )
  }

  test("Can convert EXPECT_TABLE_ROW_COUNT_TO_EQUAL Great Expectations test to validation") {
    checkAggExpectation(
      EXPECT_TABLE_ROW_COUNT_TO_EQUAL,
      Map(VALUE -> 10),
      "count == 10", AGGREGATION_COUNT, "", Seq()
    )
  }

  private def checkExpectation(name: String, args: Map[String, Any],
                               expectedWhereExpr: String, expectedSelectExpr: String = ""): List[ValidationBuilder] = {
    val expectations = List(GreatExpectationsExpectation(name, args))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    getValidationType(result.head.validation, "/tmp/record-tracking").validate(df, dfCount)
    assert(result.head.validation.isInstanceOf[ExpressionValidation])
    assertResult(expectedWhereExpr)(result.head.validation.asInstanceOf[ExpressionValidation].expr)
    if (expectedSelectExpr.nonEmpty) {
      assertResult(expectedSelectExpr)(result.head.validation.asInstanceOf[ExpressionValidation].selectExpr.head)
    }
    result
  }

  private def checkAggExpectation(name: String, args: Map[String, Any], expectedExpr: String, expectedAggType: String,
                                  expectedAggCol: String, expectedGroupByCols: Seq[String]): Unit = {
    val expectations = List(GreatExpectationsExpectation(name, args))
    val greatExpectationsTestSuite = GreatExpectationsTestSuite("assetType", "my_tests", expectations)
    val result = GreatExpectationsDataValidations.createValidationsFromGreatExpectationsTestSuite(greatExpectationsTestSuite)

    assertResult(1)(result.size)
    getValidationType(result.head.validation, "/tmp/record-tracking").validate(df, dfCount)
    assert(result.head.validation.isInstanceOf[GroupByValidation])
    val grpValidation = result.head.validation.asInstanceOf[GroupByValidation]
    assertResult(expectedExpr)(grpValidation.aggExpr)
    assertResult(expectedAggType)(grpValidation.aggType)
    assertResult(expectedAggCol)(grpValidation.aggField)
    assertResult(expectedGroupByCols)(grpValidation.groupByFields)
  }
}

case class TaxiRecord(
                       vendor_id: String = "123",
                       pickup_datetime: Timestamp = Timestamp.from(Instant.now()),
                       dropoff_datetime: Timestamp = Timestamp.from(Instant.now()),
                       passenger_count: Int = 2,
                       trip_distance: String = "10",
                       rate_code_id: String = "CD1",
                       store_and_fwd_flag: String = "yes",
                       pickup_location_id: String = "id123",
                       dropoff_location_id: String = "id987",
                       payment_type: String = "card",
                       fare_amount: Double = 10.1,
                       extra: String = "none",
                       mta_tax: Double = 1.1,
                       tip_amount: Double = 2.5,
                       tolls_amount: Double = 0.0,
                       improvement_surcharge: Double = 0.0,
                       total_amount: Double = 13.7,
                       congestion_surcharge: Double = 0.0
                     )
