package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanParserTest extends SparkSuite {

  test("Can parse plan in YAML file") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/account-create-plan-test.yaml")

    assert(result.name.nonEmpty)
    assert(result.description.nonEmpty)
    assertResult(4)(result.tasks.size)
    assertResult(1)(result.validations.size)
    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult("solace.jms_account.account_id")(result.sinkOptions.get.foreignKeys.head._1)
    assertResult(List("json.file_account.account_id"))(result.sinkOptions.get.foreignKeys.head._2)
  }

  test("Can parse task in YAML file") {
    val result = PlanParser.parseTasks("src/test/resources/sample/task")

    assertResult(13)(result.length)
  }

  test("Can parse plan in YAML file with foreign key") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/large-plan.yaml")

    assert(result.sinkOptions.isDefined)
    assertResult(1)(result.sinkOptions.get.foreignKeys.size)
    assertResult("json.file_account.account_id")(result.sinkOptions.get.foreignKeys.head._1)
    assertResult(1)(result.sinkOptions.get.foreignKeys.head._2.size)
    assertResult("csv.transactions.account_id")(result.sinkOptions.get.foreignKeys.head._2.head)
  }

}
