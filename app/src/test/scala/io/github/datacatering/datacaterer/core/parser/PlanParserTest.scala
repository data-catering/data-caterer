package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation
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

    assertResult(16)(result.length)
  }

  test("Can parse plan in YAML file with foreign key") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/large-plan.yaml")

    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult(ForeignKeyRelation("json", "file_account", List("account_id")))(result.sinkOptions.get.foreignKeys.head.source)
    assertResult(1)(result.sinkOptions.get.foreignKeys.head.generate.size)
    assertResult(ForeignKeyRelation("csv", "transaction", List("account_id")))(result.sinkOptions.get.foreignKeys.head.generate.head)
  }

}
