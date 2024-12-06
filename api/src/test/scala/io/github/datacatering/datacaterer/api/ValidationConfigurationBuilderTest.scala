package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD, DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES, PATH, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.model.{ColumnNamesValidation, ConditionType, DataExistsWaitCondition, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation, WebhookWaitCondition}
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

  test("Can create column specific validation") {
    val result = ValidationBuilder().col("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` > 10")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column equal to validation") {
    val result = ValidationBuilder().col("my_col").isEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` == 10")(result.validation.asInstanceOf[ExpressionValidation].expr)

    val resultStr = ValidationBuilder().col("my_col").isEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` == 'created'")(resultStr.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column equal to another column validation") {
    val result = ValidationBuilder().col("my_col").isEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` == other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not equal to validation") {
    val result = ValidationBuilder().col("my_col").isNotEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` != 10")(result.validation.asInstanceOf[ExpressionValidation].expr)

    val resultStr = ValidationBuilder().col("my_col").isNotEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` != 'created'")(resultStr.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not equal to another column validation") {
    val result = ValidationBuilder().col("my_col").isNotEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` != other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column is null validation") {
    val result = ValidationBuilder().col("my_col").isNull

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("ISNULL(`my_col`)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column is not null validation") {
    val result = ValidationBuilder().col("my_col").isNotNull

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("ISNOTNULL(`my_col`)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column contains validation") {
    val result = ValidationBuilder().col("my_col").contains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("CONTAINS(`my_col`, 'apple')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not contains validation") {
    val result = ValidationBuilder().col("my_col").notContains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!CONTAINS(`my_col`, 'apple')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column less than validation") {
    val result = ValidationBuilder().col("my_col").lessThan(Date.valueOf("2023-01-01"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` < DATE('2023-01-01')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column less than other column validation") {
    val result = ValidationBuilder().col("my_col").lessThanCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` < other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column less than or equal validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqual(Timestamp.valueOf("2023-01-01 00:00:00.0"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` <= TIMESTAMP('2023-01-01 00:00:00.0')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column less than or equal other column validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` <= other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater than validation") {
    val result = ValidationBuilder().col("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` > 10")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater than other column validation") {
    val result = ValidationBuilder().col("my_col").greaterThanCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` > other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater than or equal validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` >= 10")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater than or equal other column validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` >= other_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column between validation") {
    val result = ValidationBuilder().col("my_col").between(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` BETWEEN 10 AND 20")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column between other col validation") {
    val result = ValidationBuilder().col("my_col").betweenCol("other_col", "another_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` BETWEEN other_col AND another_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not between validation") {
    val result = ValidationBuilder().col("my_col").notBetween(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` NOT BETWEEN 10 AND 20")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not between other col validation") {
    val result = ValidationBuilder().col("my_col").notBetweenCol("other_col", "another_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` NOT BETWEEN other_col AND another_col")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column in validation") {
    val result = ValidationBuilder().col("my_col").in("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("`my_col` IN ('open','closed')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not in validation") {
    val result = ValidationBuilder().col("my_col").notIn("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("NOT `my_col` IN ('open','closed')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column matches validation") {
    val result = ValidationBuilder().col("my_col").matches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("REGEXP(`my_col`, 'ACC[0-9]{8}')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not matches validation") {
    val result = ValidationBuilder().col("my_col").notMatches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!REGEXP(`my_col`, 'ACC[0-9]{8}')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column starts with validation") {
    val result = ValidationBuilder().col("my_col").startsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("STARTSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not starts with validation") {
    val result = ValidationBuilder().col("my_col").notStartsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!STARTSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column ends with validation") {
    val result = ValidationBuilder().col("my_col").endsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("ENDSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not ends with validation") {
    val result = ValidationBuilder().col("my_col").notEndsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("!ENDSWITH(`my_col`, 'ACC')")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column size validation") {
    val result = ValidationBuilder().col("my_col").size(2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) == 2")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column not size validation") {
    val result = ValidationBuilder().col("my_col").notSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) != 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column less than size validation") {
    val result = ValidationBuilder().col("my_col").lessThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) < 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column less than or equal size validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqualSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) <= 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater than size validation") {
    val result = ValidationBuilder().col("my_col").greaterThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) > 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater than or equal size validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqualSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("SIZE(`my_col`) >= 5")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column greater luhn check validation") {
    val result = ValidationBuilder().col("my_col").luhnCheck

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("LUHN_CHECK(`my_col`)")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column type validation") {
    val result = ValidationBuilder().col("my_col").hasType("double")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("TYPEOF(`my_col`) == 'double'")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create column generic expression validation") {
    val result = ValidationBuilder().col("my_col").expr("my_col * 2 < other_col / 4")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assertResult("my_col * 2 < other_col / 4")(result.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create group by column validation") {
    val result = ValidationBuilder()
      .description("my_description")
      .errorThreshold(0.5)
      .groupBy("account_id", "year")
      .sum("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id", "year"))(validation.groupByCols)
    assertResult("amount")(validation.aggCol)
    assertResult("sum")(validation.aggType)
    assertResult("sum(amount) < 100")(validation.aggExpr)
    assert(validation.description.contains("my_description"))
    assert(validation.errorThreshold.contains(0.5))
  }

  test("Can create dataset count validation") {
    val result = ValidationBuilder().count().lessThan(10)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols.isEmpty)
    assert(validation.aggCol.isEmpty)
    assertResult("count")(validation.aggType)
    assertResult("count < 10")(validation.aggExpr)
  }

  test("Can create group by then get count column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .count("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByCols)
    assertResult("amount")(validation.aggCol)
    assertResult("count")(validation.aggType)
    assertResult("count(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get max column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .max("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByCols)
    assertResult("amount")(validation.aggCol)
    assertResult("max")(validation.aggType)
    assertResult("max(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get min column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .min("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByCols)
    assertResult("amount")(validation.aggCol)
    assertResult("min")(validation.aggType)
    assertResult("min(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get average column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .avg("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByCols)
    assertResult("amount")(validation.aggCol)
    assertResult("avg")(validation.aggType)
    assertResult("avg(amount) < 100")(validation.aggExpr)
  }

  test("Can create group by then get stddev column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .stddev("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByCols)
    assertResult("amount")(validation.aggCol)
    assertResult("stddev")(validation.aggType)
    assertResult("stddev(amount) < 100")(validation.aggExpr)
  }

  test("Can create unique column validation") {
    val result = ValidationBuilder().unique("account_id").description("my_description").errorThreshold(0.2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(validation.groupByCols)
    assertResult("unique")(validation.aggCol)
    assertResult("count")(validation.aggType)
    assertResult("count == 1")(validation.aggExpr)
    assert(validation.description.contains("my_description"))
    assert(validation.errorThreshold.contains(0.2))
  }

  test("Can create unique column validation with multiple columns") {
    val result = ValidationBuilder().unique("account_id", "year", "name")

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id", "year", "name"))(validation.groupByCols)
    assertResult("unique")(validation.aggCol)
    assertResult("count")(validation.aggType)
    assertResult("count == 1")(validation.aggExpr)
  }

  test("Can create validation based on data from another data source") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinColumns("account_id")
      .withValidation(ValidationBuilder().col("amount").lessThanOrEqualCol("other_data_source_balance"))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult("other_data_source")(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(validation.joinType)
    assertResult(List("account_id"))(validation.joinColumns)
    assert(validation.validation.validation.isInstanceOf[ExpressionValidation])
    assertResult("`amount` <= other_data_source_balance")(validation.validation.validation.asInstanceOf[ExpressionValidation].expr)
  }

  test("Can create validation based on data from another data source as an anti-join") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinColumns("account_id")
      .joinType("anti-join")
      .withValidation(ValidationBuilder().count().isEqual(0))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult("other_data_source")(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
    assertResult("anti-join")(validation.joinType)
    assertResult(List("account_id"))(validation.joinColumns)
    assert(validation.validation.validation.isInstanceOf[GroupByValidation])
    assertResult("count == 0")(validation.validation.validation.asInstanceOf[GroupByValidation].aggExpr)
  }

  test("Can create validation based on data from another data source with expression for join logic") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinExpr("account_id == CONCAT('ACC', other_data_source_account_number)")
      .withValidation(ValidationBuilder().count().isEqual(0))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult("other_data_source")(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(validation.joinType)
    assertResult(List("expr:account_id == CONCAT('ACC', other_data_source_account_number)"))(validation.joinColumns)
    assert(validation.validation.validation.isInstanceOf[GroupByValidation])
    assertResult("count == 0")(validation.validation.validation.asInstanceOf[GroupByValidation].aggExpr)
  }

  test("Can create column count validation") {
    val result = ValidationBuilder().columnNames.countEqual(5)

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assertResult(VALIDATION_COLUMN_NAME_COUNT_EQUAL)(result.validation.asInstanceOf[ColumnNamesValidation].columnNameType)
    assertResult(5)(result.validation.asInstanceOf[ColumnNamesValidation].count)
  }

  test("Can create column count between validation") {
    val result = ValidationBuilder().columnNames.countBetween(5, 10)

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assertResult(VALIDATION_COLUMN_NAME_COUNT_BETWEEN)(result.validation.asInstanceOf[ColumnNamesValidation].columnNameType)
    assertResult(5)(result.validation.asInstanceOf[ColumnNamesValidation].minCount)
    assertResult(10)(result.validation.asInstanceOf[ColumnNamesValidation].maxCount)
  }

  test("Can create column names match ordered list of names") {
    val result = ValidationBuilder().columnNames.matchOrder("account_id", "year")

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assertResult(VALIDATION_COLUMN_NAME_MATCH_ORDER)(result.validation.asInstanceOf[ColumnNamesValidation].columnNameType)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].names sameElements Array("account_id", "year"))
  }

  test("Can create column names exist in set of names") {
    val result = ValidationBuilder().columnNames.matchSet("account_id", "year")

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assertResult(VALIDATION_COLUMN_NAME_MATCH_SET)(result.validation.asInstanceOf[ColumnNamesValidation].columnNameType)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].names sameElements Array("account_id", "year"))
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

  test("Can create column pre-filter condition for validation") {
    val result = PreFilterBuilder().filter(ValidationBuilder().col("balance").greaterThan(100))

    assertResult(1)(result.validationPreFilterBuilders.size)
    assert(result.validationPreFilterBuilders.head.isLeft)
    assert(result.validationPreFilterBuilders.head.left.exists(_.validation.isInstanceOf[ExpressionValidation]))
  }

  test("Can create column pre-filter condition for validation with OR condition") {
    val result = PreFilterBuilder()
      .filter(ValidationBuilder().col("balance").greaterThan(100))
      .or(ValidationBuilder().col("amount").greaterThan(10))

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
      .preFilter(PreFilterBuilder().filter(ValidationBuilder().col("category").in("utilities")))
      .col("amount").greaterThan(100)

    assert(result.optCombinationPreFilterBuilder.isDefined)
    assert(result.optCombinationPreFilterBuilder.get.validate())
  }
}
