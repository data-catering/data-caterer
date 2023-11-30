package com.github.pflooky.datacaterer.api

import com.github.pflooky.datacaterer.api.model.Constants.{DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD, DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES, PATH, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET}
import com.github.pflooky.datacaterer.api.model.{ColumnNamesValidation, DataExistsWaitCondition, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation, WebhookWaitCondition}
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

    assert(result.name == "my validations")
    assert(result.description == "check account data")
    assert(result.dataSources.size == 1)
    assert(result.dataSources.head._1 == "account_json")
    val headDsValid = result.dataSources.head._2.head
    assert(headDsValid.options == Map("path" -> "/my/data/path"))
    assert(headDsValid.waitCondition == PauseWaitCondition())
    assert(headDsValid.validations.size == 2)
    assert(headDsValid.validations.map(_.validation).contains(ExpressionValidation("amount < 100")))
    assert(headDsValid.validations.map(_.validation).contains(ExpressionValidation("name == 'Peter'")))
  }

  test("Can create column specific validation") {
    val result = ValidationBuilder().col("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col > 10")
  }

  test("Can create column equal to validation") {
    val result = ValidationBuilder().col("my_col").isEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col == 10")

    val resultStr = ValidationBuilder().col("my_col").isEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assert(resultStr.validation.asInstanceOf[ExpressionValidation].expr == "my_col == 'created'")
  }

  test("Can create column equal to another column validation") {
    val result = ValidationBuilder().col("my_col").isEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col == other_col")
  }

  test("Can create column not equal to validation") {
    val result = ValidationBuilder().col("my_col").isNotEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col != 10")

    val resultStr = ValidationBuilder().col("my_col").isNotEqual("created")

    assert(resultStr.validation.isInstanceOf[ExpressionValidation])
    assert(resultStr.validation.asInstanceOf[ExpressionValidation].expr == "my_col != 'created'")
  }

  test("Can create column not equal to another column validation") {
    val result = ValidationBuilder().col("my_col").isNotEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col != other_col")
  }

  test("Can create column is null validation") {
    val result = ValidationBuilder().col("my_col").isNull

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "ISNULL(my_col)")
  }

  test("Can create column is not null validation") {
    val result = ValidationBuilder().col("my_col").isNotNull

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "ISNOTNULL(my_col)")
  }

  test("Can create column contains validation") {
    val result = ValidationBuilder().col("my_col").contains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "CONTAINS(my_col, 'apple')")
  }

  test("Can create column not contains validation") {
    val result = ValidationBuilder().col("my_col").notContains("apple")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!CONTAINS(my_col, 'apple')")
  }

  test("Can create column less than validation") {
    val result = ValidationBuilder().col("my_col").lessThan(Date.valueOf("2023-01-01"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col < DATE('2023-01-01')")
  }

  test("Can create column less than other column validation") {
    val result = ValidationBuilder().col("my_col").lessThanCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col < other_col")
  }

  test("Can create column less than or equal validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqual(Timestamp.valueOf("2023-01-01 00:00:00.0"))

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col <= TIMESTAMP('2023-01-01 00:00:00.0')")
  }

  test("Can create column less than or equal other column validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col <= other_col")
  }

  test("Can create column greater than validation") {
    val result = ValidationBuilder().col("my_col").greaterThan(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col > 10")
  }

  test("Can create column greater than other column validation") {
    val result = ValidationBuilder().col("my_col").greaterThanCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col > other_col")
  }

  test("Can create column greater than or equal validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqual(10)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col >= 10")
  }

  test("Can create column greater than or equal other column validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqualCol("other_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col >= other_col")
  }

  test("Can create column between validation") {
    val result = ValidationBuilder().col("my_col").between(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col BETWEEN 10 AND 20")
  }

  test("Can create column between other col validation") {
    val result = ValidationBuilder().col("my_col").betweenCol("other_col", "another_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col BETWEEN other_col AND another_col")
  }

  test("Can create column not between validation") {
    val result = ValidationBuilder().col("my_col").notBetween(10, 20)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col NOT BETWEEN 10 AND 20")
  }

  test("Can create column not between other col validation") {
    val result = ValidationBuilder().col("my_col").notBetweenCol("other_col", "another_col")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col NOT BETWEEN other_col AND another_col")
  }

  test("Can create column in validation") {
    val result = ValidationBuilder().col("my_col").in("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col IN ('open','closed')")
  }

  test("Can create column not in validation") {
    val result = ValidationBuilder().col("my_col").notIn("open", "closed")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "NOT my_col IN ('open','closed')")
  }

  test("Can create column matches validation") {
    val result = ValidationBuilder().col("my_col").matches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "REGEXP(my_col, 'ACC[0-9]{8}')")
  }

  test("Can create column not matches validation") {
    val result = ValidationBuilder().col("my_col").notMatches("ACC[0-9]{8}")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!REGEXP(my_col, 'ACC[0-9]{8}')")
  }

  test("Can create column starts with validation") {
    val result = ValidationBuilder().col("my_col").startsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "STARTSWITH(my_col, 'ACC')")
  }

  test("Can create column not starts with validation") {
    val result = ValidationBuilder().col("my_col").notStartsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!STARTSWITH(my_col, 'ACC')")
  }

  test("Can create column ends with validation") {
    val result = ValidationBuilder().col("my_col").endsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "ENDSWITH(my_col, 'ACC')")
  }

  test("Can create column not ends with validation") {
    val result = ValidationBuilder().col("my_col").notEndsWith("ACC")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "!ENDSWITH(my_col, 'ACC')")
  }

  test("Can create column size validation") {
    val result = ValidationBuilder().col("my_col").size(2)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) == 2")
  }

  test("Can create column not size validation") {
    val result = ValidationBuilder().col("my_col").notSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) != 5")
  }

  test("Can create column less than size validation") {
    val result = ValidationBuilder().col("my_col").lessThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) < 5")
  }

  test("Can create column less than or equal size validation") {
    val result = ValidationBuilder().col("my_col").lessThanOrEqualSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) <= 5")
  }

  test("Can create column greater than size validation") {
    val result = ValidationBuilder().col("my_col").greaterThanSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) > 5")
  }

  test("Can create column greater than or equal size validation") {
    val result = ValidationBuilder().col("my_col").greaterThanOrEqualSize(5)

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "SIZE(my_col) >= 5")
  }

  test("Can create column greater luhn check validation") {
    val result = ValidationBuilder().col("my_col").luhnCheck

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "LUHN_CHECK(my_col)")
  }

  test("Can create column type validation") {
    val result = ValidationBuilder().col("my_col").hasType("double")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "TYPEOF(my_col) == 'double'")
  }

  test("Can create column generic expression validation") {
    val result = ValidationBuilder().col("my_col").expr("my_col * 2 < other_col / 4")

    assert(result.validation.isInstanceOf[ExpressionValidation])
    assert(result.validation.asInstanceOf[ExpressionValidation].expr == "my_col * 2 < other_col / 4")
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
    assert(validation.groupByCols == Seq("account_id", "year"))
    assert(validation.aggCol == "amount")
    assert(validation.aggType == "sum")
    assert(validation.expr == "sum(amount) < 100")
    assert(validation.description.contains("my_description"))
    assert(validation.errorThreshold.contains(0.5))
  }

  test("Can create dataset count validation") {
    val result = ValidationBuilder().count().lessThan(10)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols.isEmpty)
    assert(validation.aggCol.isEmpty)
    assert(validation.aggType == "count")
    assert(validation.expr == "count < 10")
  }

  test("Can create group by then get count column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .count("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id"))
    assert(validation.aggCol == "amount")
    assert(validation.aggType == "count")
    assert(validation.expr == "count(amount) < 100")
  }

  test("Can create group by then get max column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .max("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id"))
    assert(validation.aggCol == "amount")
    assert(validation.aggType == "max")
    assert(validation.expr == "max(amount) < 100")
  }

  test("Can create group by then get min column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .min("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id"))
    assert(validation.aggCol == "amount")
    assert(validation.aggType == "min")
    assert(validation.expr == "min(amount) < 100")
  }

  test("Can create group by then get average column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .avg("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id"))
    assert(validation.aggCol == "amount")
    assert(validation.aggType == "avg")
    assert(validation.expr == "avg(amount) < 100")
  }

  test("Can create group by then get stddev column validation") {
    val result = ValidationBuilder()
      .groupBy("account_id")
      .stddev("amount")
      .lessThan(100)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id"))
    assert(validation.aggCol == "amount")
    assert(validation.aggType == "stddev")
    assert(validation.expr == "stddev(amount) < 100")
  }

  test("Can create unique column validation") {
    val result = ValidationBuilder().unique("account_id").description("my_description").errorThreshold(0.2)

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id"))
    assert(validation.aggCol == "unique")
    assert(validation.aggType == "count")
    assert(validation.expr == "count == 1")
    assert(validation.description.contains("my_description"))
    assert(validation.errorThreshold.contains(0.2))
  }

  test("Can create unique column validation with multiple columns") {
    val result = ValidationBuilder().unique("account_id", "year", "name")

    assert(result.validation.isInstanceOf[GroupByValidation])
    val validation = result.validation.asInstanceOf[GroupByValidation]
    assert(validation.groupByCols == Seq("account_id", "year", "name"))
    assert(validation.aggCol == "unique")
    assert(validation.aggType == "count")
    assert(validation.expr == "count == 1")
  }

  test("Can create validation based on data from another data source") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinColumns("account_id")
      .withValidation(ValidationBuilder().col("amount").lessThanOrEqualCol("other_data_source_balance"))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assert(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName == "other_data_source")
    assert(validation.joinType == DEFAULT_VALIDATION_JOIN_TYPE)
    assert(validation.joinCols == List("account_id"))
    assert(validation.validationBuilder.validation.isInstanceOf[ExpressionValidation])
    assert(validation.validationBuilder.validation.asInstanceOf[ExpressionValidation].expr == "amount <= other_data_source_balance")
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
    assert(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName == "other_data_source")
    assert(validation.joinType == "anti-join")
    assert(validation.joinCols == List("account_id"))
    assert(validation.validationBuilder.validation.isInstanceOf[GroupByValidation])
    assert(validation.validationBuilder.validation.asInstanceOf[GroupByValidation].expr == "count == 0")
  }

  test("Can create validation based on data from another data source with expression for join logic") {
    val upstreamDataSource = ConnectionConfigWithTaskBuilder().file("other_data_source", "json")
    val result = ValidationBuilder()
      .upstreamData(upstreamDataSource)
      .joinExpr("account_id == CONCAT('ACC', other_data_source_account_number)")
      .withValidation(ValidationBuilder().count().isEqual(0))

    assert(result.validation.isInstanceOf[UpstreamDataSourceValidation])
    val validation = result.validation.asInstanceOf[UpstreamDataSourceValidation]
    assert(validation.upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName == "other_data_source")
    assert(validation.joinType == DEFAULT_VALIDATION_JOIN_TYPE)
    assert(validation.joinCols == List("expr:account_id == CONCAT('ACC', other_data_source_account_number)"))
    assert(validation.validationBuilder.validation.isInstanceOf[GroupByValidation])
    assert(validation.validationBuilder.validation.asInstanceOf[GroupByValidation].expr == "count == 0")
  }

  test("Can create column count validation") {
    val result = ValidationBuilder().columnNames().countEqual(5)

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assert(result.validation.asInstanceOf[ColumnNamesValidation].`type` == VALIDATION_COLUMN_NAME_COUNT_EQUAL)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].count == 5)
  }

  test("Can create column count between validation") {
    val result = ValidationBuilder().columnNames().countBetween(5, 10)

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assert(result.validation.asInstanceOf[ColumnNamesValidation].`type` == VALIDATION_COLUMN_NAME_COUNT_BETWEEN)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].minCount == 5)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].maxCount == 10)
  }

  test("Can create column names match ordered list of names") {
    val result = ValidationBuilder().columnNames().matchOrder("account_id", "year")

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assert(result.validation.asInstanceOf[ColumnNamesValidation].`type` == VALIDATION_COLUMN_NAME_MATCH_ORDER)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].names sameElements Array("account_id", "year"))
  }

  test("Can create column names exist in set of names") {
    val result = ValidationBuilder().columnNames().matchSet("account_id", "year")

    assert(result.validation.isInstanceOf[ColumnNamesValidation])
    assert(result.validation.asInstanceOf[ColumnNamesValidation].`type` == VALIDATION_COLUMN_NAME_MATCH_SET)
    assert(result.validation.asInstanceOf[ColumnNamesValidation].names sameElements Array("account_id", "year"))
  }

  test("Can create validation pause wait condition") {
    val result = WaitConditionBuilder().pause(10).waitCondition

    assert(result.isInstanceOf[PauseWaitCondition])
    assert(result.asInstanceOf[PauseWaitCondition].pauseInSeconds == 10)
  }

  test("Can create validation file exists wait condition") {
    val result = WaitConditionBuilder().file("/my/file/path").waitCondition

    assert(result.isInstanceOf[FileExistsWaitCondition])
    assert(result.asInstanceOf[FileExistsWaitCondition].path == "/my/file/path")
  }

  test("Can create validation data exists wait condition") {
    val result = WaitConditionBuilder().dataExists("my_json", Map(PATH -> "/my/json"), "created_date > '2023-01-01'").waitCondition

    assert(result.isInstanceOf[DataExistsWaitCondition])
    val waitCondition = result.asInstanceOf[DataExistsWaitCondition]
    assert(waitCondition.dataSourceName == "my_json")
    assert(waitCondition.options.nonEmpty)
    assert(waitCondition.options == Map(PATH -> "/my/json"))
    assert(waitCondition.expr == "created_date > '2023-01-01'")
  }

  test("Can create validation webhook wait condition") {
    val result = WaitConditionBuilder().webhook("localhost:8080/ready").waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assert(waitCondition.url == "localhost:8080/ready")
    assert(waitCondition.method == DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD)
    assert(waitCondition.dataSourceName == DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME)
    assert(waitCondition.statusCodes == DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES)
  }

  test("Can create validation webhook wait condition with PUT method and 202 status code") {
    val result = WaitConditionBuilder().webhook("localhost:8080/ready", "PUT", 202).waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assert(waitCondition.url == "localhost:8080/ready")
    assert(waitCondition.method == "PUT")
    assert(waitCondition.dataSourceName == DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME)
    assert(waitCondition.statusCodes == List(202))
  }

  test("Can create validation webhook wait condition using pre-defined HTTP data source name") {
    val result = WaitConditionBuilder().webhook("my_http", "localhost:8080/ready").waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assert(waitCondition.url == "localhost:8080/ready")
    assert(waitCondition.method == DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD)
    assert(waitCondition.dataSourceName == "my_http")
    assert(waitCondition.statusCodes == DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES)
  }

  test("Can create validation webhook wait condition using pre-defined HTTP data source name, with different method and status code") {
    val result = WaitConditionBuilder().webhook("my_http", "localhost:8080/ready", "PUT", 202).waitCondition

    assert(result.isInstanceOf[WebhookWaitCondition])
    val waitCondition = result.asInstanceOf[WebhookWaitCondition]
    assert(waitCondition.url == "localhost:8080/ready")
    assert(waitCondition.method == "PUT")
    assert(waitCondition.dataSourceName == "my_http")
    assert(waitCondition.statusCodes == List(202))
  }
}
