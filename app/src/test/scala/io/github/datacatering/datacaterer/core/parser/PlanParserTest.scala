package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.Constants.YAML_REAL_TIME_BODY_FIELD
import io.github.datacatering.datacaterer.api.model.{Count, Field, ForeignKeyRelation, Step, Task}
import io.github.datacatering.datacaterer.core.util.SparkSuite

class PlanParserTest extends SparkSuite {

  test("Can parse plan in YAML file") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/account-create-plan-test.yaml")

    assert(result.name.nonEmpty)
    assert(result.description.nonEmpty)
    assertResult(4)(result.tasks.size)
    assertResult(1)(result.validations.size)
    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult(ForeignKeyRelation("solace", "jms_account", List("account_id")))(result.sinkOptions.get.foreignKeys.head.source)
    assertResult(List(ForeignKeyRelation("json", "file_account", List("account_id"))))(result.sinkOptions.get.foreignKeys.head.generate)
  }

  test("Can parse task in YAML file") {
    val result = PlanParser.parseTasks("src/test/resources/sample/task")

    assertResult(18)(result.length)
  }

  test("Can parse plan in YAML file with foreign key") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/large-plan.yaml")

    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult(ForeignKeyRelation("json", "file_account", List("account_id")))(result.sinkOptions.get.foreignKeys.head.source)
    assertResult(1)(result.sinkOptions.get.foreignKeys.head.generate.size)
    assertResult(ForeignKeyRelation("csv", "transaction", List("account_id")))(result.sinkOptions.get.foreignKeys.head.generate.head)
  }

  test("Can convert task into specific fields from YAML task") {
    val task = Task("task-name", List(Step(
      "my-step",
      "json",
      Count(),
      Map(),
      List(
        Field("account_id_uuid", Some("string"), Map("uuid" -> "")),
        Field("account_id_uuid_inc", Some("string"), Map("uuid" -> "", "incremental" -> "1")),
        Field("account_id_inc", Some("int"), Map("incremental" -> "5")),
        Field(YAML_REAL_TIME_BODY_FIELD, Some("struct"), fields = List(
          Field("other_acc_id", Some("string"), Map("uuid" -> "", "incremental" -> "10"))
        )),
      )
    )))

    val result = PlanParser.convertToSpecificFields(task)

    assertResult(1)(result.steps.size)
    assertResult(5)(result.steps.head.fields.size)
    assertResult(Field("account_id_uuid", Some("string"), Map("sql" -> "UUID()", "uuid" -> "")))(result.steps.head.fields.head)
    assertResult(Field("account_id_uuid_inc", Some("string"), Map("sql" ->
      """CONCAT(
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 1, 8), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 9, 4), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 13, 4), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 17, 4), '-',
        |SUBSTR(MD5(CAST(1 + __index_inc AS STRING)), 21, 12)
        |)""".stripMargin, "uuid" -> "", "incremental" -> "1")))(result.steps.head.fields(1))
    assertResult(Field("account_id_inc", Some("integer"), Map("incremental" -> "5")))(result.steps.head.fields(2))
    assertResult(Field("value", Some("string"), Map("sql" -> "TO_JSON(body)")))(result.steps.head.fields(3))
    assertResult(Field("body", Some("string"), fields = List(Field(
      "other_acc_id", Some("string"), Map("sql" ->
        """CONCAT(
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 1, 8), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 9, 4), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 13, 4), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 17, 4), '-',
          |SUBSTR(MD5(CAST(10 + __index_inc AS STRING)), 21, 12)
          |)""".stripMargin, "uuid" -> "", "incremental" -> "10")
    ))))(result.steps.head.fields(4))
  }
}
