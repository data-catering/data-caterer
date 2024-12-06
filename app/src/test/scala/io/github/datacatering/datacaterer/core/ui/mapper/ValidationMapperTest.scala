package io.github.datacatering.datacaterer.core.ui.mapper;

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.connection.FileBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_VALIDATION_JOIN_TYPE, VALIDATION_AVERAGE, VALIDATION_COLUMN, VALIDATION_COLUMN_NAMES, VALIDATION_COLUMN_NAMES_COUNT_BETWEEN, VALIDATION_COLUMN_NAMES_COUNT_EQUAL, VALIDATION_COLUMN_NAMES_MATCH_ORDER, VALIDATION_COLUMN_NAMES_MATCH_SET, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET, VALIDATION_COUNT, VALIDATION_EQUAL, VALIDATION_FIELD, VALIDATION_GROUP_BY, VALIDATION_GROUP_BY_COLUMNS, VALIDATION_MAX, VALIDATION_MIN, VALIDATION_STANDARD_DEVIATION, VALIDATION_SUM, VALIDATION_UPSTREAM, VALIDATION_UPSTREAM_JOIN_COLUMNS, VALIDATION_UPSTREAM_JOIN_EXPR, VALIDATION_UPSTREAM_JOIN_TYPE, VALIDATION_UPSTREAM_TASK_NAME}
import io.github.datacatering.datacaterer.api.model.{ColumnNamesValidation, ExpressionValidation, GroupByValidation, UpstreamDataSourceValidation}
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, ValidationItemRequest, ValidationItemRequests, ValidationRequest}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ValidationMapperTest extends AnyFunSuite {

  test("Can convert UI validation mapping for basic column validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "account_id", VALIDATION_EQUAL -> "abc123", "description" -> "valid desc", "errorThreshold" -> "2"))))
    ))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val exprValid = res.head.validation.asInstanceOf[ExpressionValidation]
    assertResult("`account_id` == abc123")(exprValid.expr)
    assertResult(Some("valid desc"))(exprValid.description)
    assertResult(Some(2.0))(exprValid.errorThreshold)
    assertResult(1)(exprValid.selectExpr.size)
    assertResult("*")(exprValid.selectExpr.head)
  }

  test("Can convert UI validation mapping for column name validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_COUNT_EQUAL -> "5"))))
    ))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_COUNT_EQUAL)(valid.columnNameType)
    assertResult(5)(valid.count)
  }

  test("Can convert UI validation mapping for column name validation count between") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_COUNT_BETWEEN -> "blah", VALIDATION_MIN -> "1", VALIDATION_MAX -> "2"))))
    ))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_COUNT_BETWEEN)(valid.columnNameType)
    assertResult(1)(valid.minCount)
    assertResult(2)(valid.maxCount)
  }

  test("Can convert UI validation mapping for column name validation match order") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_MATCH_ORDER -> "account_id,year"))))
    ))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_MATCH_ORDER)(valid.columnNameType)
    assertResult(Array("account_id", "year"))(valid.names)
  }

  test("Can convert UI validation mapping for column name validation match set") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_MATCH_SET -> "account_id,year"))))
    ))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_MATCH_SET)(valid.columnNameType)
    assertResult(Array("account_id", "year"))(valid.names)
  }

  test("Can convert UI validation mapping, when unknown option, default to column name count equals 1") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map("unknown" -> "hello"))))
    ))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_COUNT_EQUAL)(valid.columnNameType)
    assertResult(1)(valid.count)
  }

  test("Can convert UI validation mapping with min group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_MIN, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_MIN)(valid.aggType)
    assertResult("min(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with max group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_MAX, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_MAX)(valid.aggType)
    assertResult("max(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with count group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_COUNT, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_COUNT)(valid.aggType)
    assertResult("count(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with sum group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_SUM, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_SUM)(valid.aggType)
    assertResult("sum(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with average group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_AVERAGE, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult("avg")(valid.aggType)
    assertResult("avg(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with standard deviation group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_STANDARD_DEVIATION, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = ValidationMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult("stddev")(valid.aggType)
    assertResult("stddev(amount) == 10")(valid.aggExpr)
  }

  test("Throw error when given unknown aggregation type") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> "unknown", "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    assertThrows[IllegalArgumentException](ValidationMapper.validationMapping(dataSourceRequest))
  }

  test("Throw error when no aggType or aggCol is given") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val dataSourceRequest1 = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> "max", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val dataSourceRequest2 = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map(VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    assertThrows[RuntimeException](ValidationMapper.validationMapping(dataSourceRequest))
    assertThrows[RuntimeException](ValidationMapper.validationMapping(dataSourceRequest1))
    assertThrows[RuntimeException](ValidationMapper.validationMapping(dataSourceRequest2))
  }

  test("Can convert UI upstream validation mapping") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_TYPE -> "outer",
        VALIDATION_UPSTREAM_JOIN_COLUMNS -> "account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = ValidationMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("account_id"))(upstreamValidation.joinColumns)
    assertResult("outer")(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("`year` == 2020")(exprValid.expr)
  }

  test("Can convert UI upstream validation mapping with join expression") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_TYPE -> "outer",
        VALIDATION_UPSTREAM_JOIN_EXPR -> "account_id == task-2_account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = ValidationMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("expr:account_id == task-2_account_id"))(upstreamValidation.joinColumns)
    assertResult("outer")(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("`year` == 2020")(exprValid.expr)
  }

  test("Can convert UI upstream validation mapping with join columns only") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_COLUMNS -> "account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = ValidationMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("account_id"))(upstreamValidation.joinColumns)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("`year` == 2020")(exprValid.expr)
  }

  test("Can convert UI upstream validation mapping with join expression only") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_EXPR -> "account_id == task-2_account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = ValidationMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("expr:account_id == task-2_account_id"))(upstreamValidation.joinColumns)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("`year` == 2020")(exprValid.expr)
  }
}