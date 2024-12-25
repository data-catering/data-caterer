package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD, DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES, PATH, VALIDATION_FIELD_NAME_COUNT_BETWEEN, VALIDATION_FIELD_NAME_COUNT_EQUAL, VALIDATION_FIELD_NAME_MATCH_ORDER, VALIDATION_FIELD_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.model.{FieldNamesValidation, ConditionType, DataExistsWaitCondition, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation, WebhookWaitCondition}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class ValidationConfigurationBuilderTest extends AnyFunSuite {

  test("Can create simple validation for data source") {
    val result = ValidationConfigurationBuilder()
      .name("my validations")
      .description("check account data")
      .addValidations(
        "account_json",
        Map("path" -> "/my/data/path"),
        ValidationBuilder().expr("amount < 100"),
        ValidationBuilder().expr("name == 'Peter'")
      ).validationConfiguration

    assertResult("my validations")(result.name)
    assertResult("check account data")(result.description)
    assertResult(1)(result.dataSources.size)
    assertResult("account_json")(result.dataSources.head._1)
    val headDsValid = result.dataSources.head._2.head
    assertResult(Map("path" -> "/my/data/path"))(headDsValid.options)
    assertResult(PauseWaitCondition())(headDsValid.waitCondition)
    assertResult(2)(headDsValid.validations.size)
    assert(headDsValid.validations.map(_.validation).contains(ExpressionValidation("amount < 100")))
    assert(headDsValid.validations.map(_.validation).contains(ExpressionValidation("name == 'Peter'")))
  }

  test("Can create field specific validation") {
    val result = ValidationBuilder().field("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` > 10")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field equal to validation") {
    val result = ValidationBuilder().field("my_col").isEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` == 10")(result.validation.asInstanceOf[ExpressionValidation].expr)

    val resultStr = ValidationBuilder().field("my_col").isEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` == 'created'")(resultStr.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field equal to another field validation") {
    val result = ValidationBuilder().field("my_col").isEqualField("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` == other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not equal to validation") {
    val result = ValidationBuilder().field("my_col").isEqual(10, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` != 10")(result.validation.asInstanceOf[ExpressionValidation].expr)

    val resultStr = ValidationBuilder().field("my_col").isEqual("created", true)

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` != 'created'")(resultStr.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not equal to another field validation") {
    val result = ValidationBuilder().field("my_col").isEqualField("other_col", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` != other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is null validation") {
    val result = ValidationBuilder().field("my_col").isNull()

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("ISNULL(`my_col`)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is not null validation") {
    val result = ValidationBuilder().field("my_col").isNull(true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("ISNOTNULL(`my_col`)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field contains validation") {
    val result = ValidationBuilder().field("my_col").contains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("CONTAINS(`my_col`, 'apple')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not contains validation") {
    val result = ValidationBuilder().field("my_col").contains("apple", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!CONTAINS(`my_col`, 'apple')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field less than validation") {
    val result = ValidationBuilder().field("my_col").lessThan(Date.valueOf("2023-01-01"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` < DATE('2023-01-01')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field less than other field validation") {
    val result = ValidationBuilder().field("my_col").lessThanField("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` < other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field less than or equal validation") {
    val result = ValidationBuilder().field("my_col").lessThan(Timestamp.valueOf("2023-01-01 00:00:00.0"), false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` <= TIMESTAMP('2023-01-01 00:00:00.0')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field less than or equal other field validation") {
    val result = ValidationBuilder().field("my_col").lessThanField("other_col", false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` <= other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater than validation") {
    val result = ValidationBuilder().field("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` > 10")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater than other field validation") {
    val result = ValidationBuilder().field("my_col").greaterThanField("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` > other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater than or equal validation") {
    val result = ValidationBuilder().field("my_col").greaterThan(10, false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` >= 10")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater than or equal other field validation") {
    val result = ValidationBuilder().field("my_col").greaterThanField("other_col", false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` >= other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field between validation") {
    val result = ValidationBuilder().field("my_col").between(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` BETWEEN 10 AND 20")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field between other col validation") {
    val result = ValidationBuilder().field("my_col").betweenFields("other_col", "another_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` BETWEEN other_col AND another_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not between validation") {
    val result = ValidationBuilder().field("my_col").between(10, 20, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` NOT BETWEEN 10 AND 20")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not between other col validation") {
    val result = ValidationBuilder().field("my_col").betweenFields("other_col", "another_col", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` NOT BETWEEN other_col AND another_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field in validation") {
    val result = ValidationBuilder().field("my_col").in("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` IN ('open','closed')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not in validation") {
    val result = ValidationBuilder().field("my_col").in(List("open", "closed"), true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("NOT `my_col` IN ('open','closed')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field matches validation") {
    val result = ValidationBuilder().field("my_col").matches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("REGEXP(`my_col`, 'ACC[0-9]{8}')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not matches validation") {
    val result = ValidationBuilder().field("my_col").matches("ACC[0-9]{8}", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!REGEXP(`my_col`, 'ACC[0-9]{8}')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field starts with validation") {
    val result = ValidationBuilder().field("my_col").startsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("STARTSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not starts with validation") {
    val result = ValidationBuilder().field("my_col").startsWith("ACC", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!STARTSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field ends with validation") {
    val result = ValidationBuilder().field("my_col").endsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("ENDSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not ends with validation") {
    val result = ValidationBuilder().field("my_col").endsWith("ACC", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!ENDSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field size validation") {
    val result = ValidationBuilder().field("my_col").size(2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) == 2")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not size validation") {
    val result = ValidationBuilder().field("my_col").size(5, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) != 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field less than size validation") {
    val result = ValidationBuilder().field("my_col").lessThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) < 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field less than or equal size validation") {
    val result = ValidationBuilder().field("my_col").lessThanSize(5, false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) <= 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater than size validation") {
    val result = ValidationBuilder().field("my_col").greaterThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) > 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater than or equal size validation") {
    val result = ValidationBuilder().field("my_col").greaterThanSize(5, false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) >= 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field greater luhn check validation") {
    val result = ValidationBuilder().field("my_col").luhnCheck()

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("LUHN_CHECK(`my_col`)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field type validation") {
    val result = ValidationBuilder().field("my_col").hasType("double")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TYPEOF(`my_col`) == 'double'")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not type validation") {
    val result = ValidationBuilder().field("my_col").hasType("double", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TYPEOF(`my_col`) != 'double'")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field types validation") {
    val result = ValidationBuilder().field("my_col").hasTypes("double", "string")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TYPEOF(`my_col`) IN ('double','string')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not types validation") {
    val result = ValidationBuilder().field("my_col").hasTypes(List("double", "string"), true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TYPEOF(`my_col`) NOT IN ('double','string')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field distinct in set validation") {
    val result = ValidationBuilder().field("my_col").distinctInSet("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COLLECT_SET(`my_col`) AS my_col_distinct"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("FORALL(my_col_distinct, x -> ARRAY_CONTAINS(ARRAY('open','closed'), x))")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not distinct in set validation") {
    val result = ValidationBuilder().field("my_col").distinctInSet(List("open", "closed"), true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COLLECT_SET(`my_col`) AS my_col_distinct"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("!FORALL(my_col_distinct, x -> ARRAY_CONTAINS(ARRAY('open','closed'), x))")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field distinct contains set validation") {
    val result = ValidationBuilder().field("my_col").distinctContainsSet("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COLLECT_SET(`my_col`) AS my_col_distinct"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("FORALL(ARRAY('open','closed'), x -> ARRAY_CONTAINS(my_col_distinct, x))")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field distinct not contains set validation") {
    val result = ValidationBuilder().field("my_col").distinctContainsSet(List("open", "closed"), true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COLLECT_SET(`my_col`) AS my_col_distinct"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("!FORALL(ARRAY('open','closed'), x -> ARRAY_CONTAINS(my_col_distinct, x))")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field distinct equals validation") {
    val result = ValidationBuilder().field("my_col").distinctEqual("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COLLECT_SET(`my_col`) AS my_col_distinct"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("ARRAY_SIZE(ARRAY_EXCEPT(ARRAY('open','closed'), my_col_distinct)) == 0")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field distinct not equals validation") {
    val result = ValidationBuilder().field("my_col").distinctEqual(List("open", "closed"), true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COLLECT_SET(`my_col`) AS my_col_distinct"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("ARRAY_SIZE(ARRAY_EXCEPT(ARRAY('open','closed'), my_col_distinct)) != 0")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field max between validation") {
    val result = ValidationBuilder().field("my_col").maxBetween(1, 2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq())(validation.groupByFields)
    assertResult("`my_col`")(validation.aggField)
    assertResult("max")(validation.aggType)
    assertResult("max(`my_col`) BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field max not between validation") {
    val result = ValidationBuilder().field("my_col").maxBetween(1, 2, true)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult("max(`my_col`) NOT BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field mean between validation") {
    val result = ValidationBuilder().field("my_col").meanBetween(1, 2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq())(validation.groupByFields)
    assertResult("`my_col`")(validation.aggField)
    assertResult("avg")(validation.aggType)
    assertResult("avg(`my_col`) BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field mean not between validation") {
    val result = ValidationBuilder().field("my_col").meanBetween(1, 2, true)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult("avg(`my_col`) NOT BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field median between validation") {
    val result = ValidationBuilder().field("my_col").medianBetween(1, 2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    val validation = result.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("PERCENTILE(`my_col`, 0.5) AS my_col_median"))(validation.selectExpr)
    assertResult("my_col_median BETWEEN 1 AND 2")(validation.expr)
  }

  test("Can create field median not between validation") {
    val result = ValidationBuilder().field("my_col").medianBetween(1, 2, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    val validation = result.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("PERCENTILE(`my_col`, 0.5) AS my_col_median"))(validation.selectExpr)
    assertResult("my_col_median NOT BETWEEN 1 AND 2")(validation.expr)
  }

  test("Can create field min between validation") {
    val result = ValidationBuilder().field("my_col").minBetween(1, 2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq())(validation.groupByFields)
    assertResult("`my_col`")(validation.aggField)
    assertResult("min")(validation.aggType)
    assertResult("min(`my_col`) BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field min not between validation") {
    val result = ValidationBuilder().field("my_col").minBetween(1, 2, true)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult("min(`my_col`) NOT BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field stddev between validation") {
    val result = ValidationBuilder().field("my_col").stdDevBetween(1, 2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq())(validation.groupByFields)
    assertResult("`my_col`")(validation.aggField)
    assertResult("stddev")(validation.aggType)
    assertResult("stddev(`my_col`) BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field stddev not between validation") {
    val result = ValidationBuilder().field("my_col").stdDevBetween(1, 2, true)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult("stddev(`my_col`) NOT BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field sum between validation") {
    val result = ValidationBuilder().field("my_col").sumBetween(1, 2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq())(validation.groupByFields)
    assertResult("`my_col`")(validation.aggField)
    assertResult("sum")(validation.aggType)
    assertResult("sum(`my_col`) BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field sum not between validation") {
    val result = ValidationBuilder().field("my_col").sumBetween(1, 2, true)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult("sum(`my_col`) NOT BETWEEN 1 AND 2")(validation.aggExpr)
  }

  test("Can create field length between validation") {
    val result = ValidationBuilder().field("my_col").lengthBetween(1, 2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("LENGTH(`my_col`) BETWEEN 1 AND 2")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field length not between validation") {
    val result = ValidationBuilder().field("my_col").lengthBetween(1, 2, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("LENGTH(`my_col`) NOT BETWEEN 1 AND 2")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field length equal validation") {
    val result = ValidationBuilder().field("my_col").lengthEqual(1)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("LENGTH(`my_col`) == 1")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field length not equal validation") {
    val result = ValidationBuilder().field("my_col").lengthEqual(1, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("LENGTH(`my_col`) != 1")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is decreasing validation") {
    val result = ValidationBuilder().field("my_col").isDecreasing()

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("`my_col` < LAG(`my_col`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_my_col_decreasing"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("is_my_col_decreasing")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is not strictly decreasing validation") {
    val result = ValidationBuilder().field("my_col").isDecreasing(false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("`my_col` <= LAG(`my_col`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_my_col_decreasing"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("is_my_col_decreasing")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is increasing validation") {
    val result = ValidationBuilder().field("my_col").isIncreasing()

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("`my_col` > LAG(`my_col`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_my_col_increasing"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("is_my_col_increasing")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is not strictly increasing validation") {
    val result = ValidationBuilder().field("my_col").isIncreasing(false)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("`my_col` >= LAG(`my_col`) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_my_col_increasing"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("is_my_col_increasing")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is JSON parsable validation") {
    val result = ValidationBuilder().field("my_col").isJsonParsable()

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("GET_JSON_OBJECT(`my_col`, '$') IS NOT NULL")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field is not JSON parsable validation") {
    val result = ValidationBuilder().field("my_col").isJsonParsable(true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("GET_JSON_OBJECT(`my_col`, '$') IS NULL")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field matches JSON schema validation") {
    val result = ValidationBuilder().field("my_col").matchJsonSchema("schema")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("FROM_JSON(`my_col`, 'schema') IS NOT NULL")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not matches JSON schema validation") {
    val result = ValidationBuilder().field("my_col").matchJsonSchema("schema", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("FROM_JSON(`my_col`, 'schema') IS NULL")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field matches date time format validation") {
    val result = ValidationBuilder().field("my_col").matchDateTimeFormat("format")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TRY_TO_TIMESTAMP(`my_col`, 'format') IS NOT NULL")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field not matches date time format validation") {
    val result = ValidationBuilder().field("my_col").matchDateTimeFormat("format", true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TRY_TO_TIMESTAMP(`my_col`, 'format') IS NULL")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field most common value in set validation") {
    val result = ValidationBuilder().field("my_col").mostCommonValueInSet(List("open"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("MODE(`my_col`) AS my_col_mode"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("ARRAY_CONTAINS(ARRAY('open'), my_col_mode)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field most common value not in set validation") {
    val result = ValidationBuilder().field("my_col").mostCommonValueInSet(List("open"), true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("MODE(`my_col`) AS my_col_mode"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("!ARRAY_CONTAINS(ARRAY('open'), my_col_mode)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field unique values proportion between validation") {
    val result = ValidationBuilder().field("my_col").uniqueValuesProportionBetween(0.1, 0.2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COUNT(DISTINCT `my_col`) / COUNT(1) AS my_col_unique_proportion"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("my_col_unique_proportion BETWEEN 0.1 AND 0.2")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field unique values proportion not between validation") {
    val result = ValidationBuilder().field("my_col").uniqueValuesProportionBetween(0.1, 0.2, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("COUNT(DISTINCT `my_col`) / COUNT(1) AS my_col_unique_proportion"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("my_col_unique_proportion NOT BETWEEN 0.1 AND 0.2")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field quantile values between validation") {
    val quantileRanges = Map(
      0.1 -> (1.0, 5.0),
      0.5 -> (10.0, 15.0)
    )
    val result = ValidationBuilder().field("my_col").quantileValuesBetween(quantileRanges)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("PERCENTILE(`my_col`, 0.1) AS my_col_percentile_0", "PERCENTILE(`my_col`, 0.5) AS my_col_percentile_1"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("my_col_percentile_0 BETWEEN 1.0 AND 5.0 AND my_col_percentile_1 BETWEEN 10.0 AND 15.0")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field quantile values not between validation") {
    val quantileRanges = Map(
      0.1 -> (1.0, 5.0),
      0.5 -> (10.0, 15.0)
    )
    val result = ValidationBuilder().field("my_col").quantileValuesBetween(quantileRanges, true)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult(List("PERCENTILE(`my_col`, 0.1) AS my_col_percentile_0", "PERCENTILE(`my_col`, 0.5) AS my_col_percentile_1"))(result.validation.asInstanceOf[ExpressionValidation].selectExpr)
    assertResult("my_col_percentile_0 NOT BETWEEN 1.0 AND 5.0 AND my_col_percentile_1 NOT BETWEEN 10.0 AND 15.0")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create field generic expression validation") {
    val result = ValidationBuilder().field("my_col").expr("my_col * 2 < other_col / 4")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("my_col * 2 < other_col / 4")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create group by field validation") {
    val result = ValidationBuilder()
      .description("my_description")
      .errorThreshold(0.5)
      .groupBy("account_id", "year")
      .sum("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id", "year"))(validation.groupByFields)
    assertResult("amount")(validation.aggField)
    assertResult("sum")(validation.aggType)
    assertResult("sum(amount) < 100")(validation.aggExpr)
    assert(validation.description.contains("my_description"))
    assert(validation.errorThreshold.contains(0.5))
  }

  test("Can create dataset count validation") {
    val result = ValidationBuilder().count().lessThan(10)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByFields.isEmpty)
    assert(validation.aggField.isEmpty)
    assertResult("count")(validation.aggType)
    assertResult("count < 10")(validation.aggExpr)
  }

  test("Can create group by then get count field validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .count("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByFields)
    assertResult("amount")(validation.aggField)
    assertResult("count")(validation.aggType)
    assertResult("count(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get max field validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .max("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByFields)
    assertResult("amount")(validation.aggField)
    assertResult("max")(validation.aggType)
    assertResult("max(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get min field validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .min("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByFields)
    assertResult("amount")(validation.aggField)
    assertResult("min")(validation.aggType)
    assertResult("min(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get average field validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .avg("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByFields)
    assertResult("amount")(validation.aggField)
    assertResult("avg")(validation.aggType)
    assertResult("avg(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get stddev field validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .stddev("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByFields)
    assertResult("amount")(validation.aggField)
    assertResult("stddev")(validation.aggType)
    assertResult("stddev(amount) < 100")(validation.aggExpr)
  }

  test("Can create unique field validation") {
    val result = ValidationBuilder().unique("account_id").description("my_description").errorThreshold(0.2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByFields)
    assertResult("unique")(validation.aggField)
    assertResult("count")(validation.aggType)
    assertResult("count == 1")(validation.aggExpr)
    assert(validation.description.contains("my_description"))
    assert(validation.errorThreshold.contains(0.2))
  }

  test("Can create unique field validation with multiple fields") {
    val result = ValidationBuilder().unique("account_id", "year", "name")

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id", "year", "name"))(validation.groupByFields)
    assertResult("unique")(validation.aggField)
    assertResult("count")(validation.aggType)
    assertResult("count == 1")(validation.aggExpr)
  }

  test("Can create validation based on data from another data source") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinFields("account_id")
      .validations(ValidationBuilder().field("amount").lessThanField("other_data_source_balance", false))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult("other_data_source")(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(validation.joinType)
    assertResult(List("account_id"))(validation.joinFields)
    assert(validation.validations.head.validation.isInstanceOf[ExpressionValidation])
    assertResult("`amount` <= other_data_source_balance")(validation.validations.head.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create validation based on data from another data source as an anti-join") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinFields("account_id")
      .joinType("anti-join")
      .validations(ValidationBuilder().count().isEqual(0))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult("other_data_source")(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
    assertResult("anti-join")(validation.joinType)
    assertResult(List("account_id"))(validation.joinFields)
    assert(validation.validations.head.validation.isInstanceOf[GroupByValidation])
    assertResult("count == 0")(validation.validations.head.validation.asInstanceOf[GroupByValidation].aggExpr)
  }

  test("Can create validation based on data from another data source with expression for join logic") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinExpr("account_id == CONCAT('ACC', other_data_source_account_number)")
      .validations(ValidationBuilder().count().isEqual(0))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult("other_data_source")(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(validation.joinType)
    assertResult(List("expr:account_id == CONCAT('ACC', other_data_source_account_number)"))(validation.joinFields)
    assert(validation.validations.head.validation.isInstanceOf[GroupByValidation])
    assertResult("count == 0")(validation.validations.head.validation.asInstanceOf[GroupByValidation].aggExpr)
  }

  test("Can create field count validation") {
    val result = ValidationBuilder().fieldNames.countEqual(5)

    assert(result.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_COUNT_EQUAL)(result.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assertResult(5)(result.validation.asInstanceOf[FieldNamesValidation].count)
  }

  test("Can create field count between validation") {
    val result = ValidationBuilder().fieldNames.countBetween(5, 10)

    assert(result.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_COUNT_BETWEEN)(result.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assertResult(5)(result.validation.asInstanceOf[FieldNamesValidation].min)
    assertResult(10)(result.validation.asInstanceOf[FieldNamesValidation].max)
  }

  test("Can create field names match ordered list of names") {
    val result = ValidationBuilder().fieldNames.matchOrder("account_id", "year")

    assert(result.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_MATCH_ORDER)(result.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assert(result.validation.asInstanceOf[FieldNamesValidation].names sameElements Array("account_id", "year"))
  }

  test("Can create field names exist in set of names") {
    val result = ValidationBuilder().fieldNames.matchSet("account_id", "year")

    assert(result.validation.isInstanceOf[FieldNamesValidation])
    assertResult(VALIDATION_FIELD_NAME_MATCH_SET)(result.validation.asInstanceOf[FieldNamesValidation].fieldNameType)
    assert(result.validation.asInstanceOf[FieldNamesValidation].names sameElements Array("account_id", "year"))
  }

  test("Can create validation pause wait condition") {
    val result = WaitConditionBuilder().pause(10).waitCondition

    assert(result.isInstanceOf[PauseWaitCondition])
    assertResult(10)(result.asInstanceOf[PauseWaitCondition].pauseInSeconds)
  }

  test("Can create validation file exists wait condition") {
    val result = WaitConditionBuilder().file("/my/file/path").waitCondition

    assert(result.isInstanceOf[FileExistsWaitCondition])
    assertResult("/my/file/path")(result.asInstanceOf[FileExistsWaitCondition].path)
  }

  test("Can create validation data exists wait condition") {
    val result = WaitConditionBuilder().dataExists("my_json", Map(PATH -> "/my/json"), "created_date > '2023-01-01'").waitCondition

    assert(result.isInstanceOf[DataExistsWaitCondition])
    val waitCondition = result.asInstanceOf[DataExistsWaitCondition]
    assertResult("my_json")(waitCondition.dataSourceName)
    assert(waitCondition.options.nonEmpty)
    assertResult(Map(PATH -> "/my/json"))(waitCondition.options)
    assertResult("created_date > '2023-01-01'")(waitCondition.expr)
  }

  test("Can create validation webhook wait condition") {
    val result = WaitConditionBuilder().webhook("localhost:8080/ready").waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assertResult("localhost:8080/ready")(waitCondition.url)
    assertResult(DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD)(waitCondition.method)
    assertResult(DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME)(waitCondition.dataSourceName)
    assertResult(DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES)(waitCondition.statusCodes)
  }

  test("Can create validation webhook wait condition with PUT method and 202 status code") {
    val result = WaitConditionBuilder().webhook("localhost:8080/ready", "PUT", 202).waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assertResult("localhost:8080/ready")(waitCondition.url)
    assertResult("PUT")(waitCondition.method)
    assertResult(DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME)(waitCondition.dataSourceName)
    assertResult(List(202))(waitCondition.statusCodes)
  }

  test("Can create validation webhook wait condition using pre-defined HTTP data source name") {
    val result = WaitConditionBuilder().webhook("my_http", "localhost:8080/ready").waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assertResult("localhost:8080/ready")(waitCondition.url)
    assertResult(DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD)(waitCondition.method)
    assertResult("my_http")(waitCondition.dataSourceName)
    assertResult(DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES)(waitCondition.statusCodes)
  }

  test("Can create validation webhook wait condition using pre-defined HTTP data source name, with different method and status code") {
    val result = WaitConditionBuilder().webhook("my_http", "localhost:8080/ready", "PUT", 202).waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assertResult("localhost:8080/ready")(waitCondition.url)
    assertResult("PUT")(waitCondition.method)
    assertResult("my_http")(waitCondition.dataSourceName)
    assertResult(List(202))(waitCondition.statusCodes)
  }

  test("Can create field pre-filter condition for validation") {
    val result = PreFilterBuilder().filter(ValidationBuilder().field("balance").greaterThan(100))

    assertResult(1)(result.validationPreFilterBuilders.size)
    assert(result.validationPreFilterBuilders.head.isLeft)
    assert(result.validationPreFilterBuilders.head.left.exists(_.validation.isInstanceOf[ExpressionValidation]))
  }

  test("Can create field pre-filter condition for validation with OR condition") {
    val result = PreFilterBuilder()
      .filter(ValidationBuilder().field("balance").greaterThan(100))
      .or(ValidationBuilder().field("amount").greaterThan(10))

    assertResult(3)(result.validationPreFilterBuilders.size)
    assert(result.validationPreFilterBuilders.head.isLeft)
    assert(result.validationPreFilterBuilders.head.left.exists(_.validation.isInstanceOf[ExpressionValidation]))
    assert(result.validationPreFilterBuilders(1).isRight)
    assert(result.validationPreFilterBuilders(1).right.exists(_ == ConditionType.OR))
    assert(result.validationPreFilterBuilders.last.isLeft)
    assert(result.validationPreFilterBuilders.last.left.exists(_.validation.isInstanceOf[ExpressionValidation]))
  }

  test("Can create pre-filter conditions for validation") {
    val result = ValidationBuilder()
      .preFilter(PreFilterBuilder().filter(ValidationBuilder().field("category").in("utilities")))
      .field("amount").greaterThan(100)

    assert(result.optCombinationPreFilterBuilder.isDefined)
    assert(result.optCombinationPreFilterBuilder.get.validate())
  }
}
