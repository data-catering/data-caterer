package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanParserTest extends SparkSuite {

  test("Can parse plan in YAML file") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/account-create-plan.yaml")

    assert(result.name.nonEmpty)
    assert(result.description.nonEmpty)
    assert(result.tasks.size == 4)
    assert(result.validations.size == 1)
    assert(result.sinkOptions.isDefined)
    assert(result.sinkOptions.get.foreignKeys.size == 1)
    assert(result.sinkOptions.get.foreignKeys.head._1 == "solace.jms_account.account_id")
    assert(result.sinkOptions.get.foreignKeys.head._2 == List("json.file_account.account_id"))
  }

  test("Can parse task in YAML file") {
    val result = PlanParser.parseTasks("src/test/resources/sample/task")

    assert(result.length == 12)
  }

  test("Can parse plan in YAML file with foreign key") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/large-plan.yaml")

    assert(result.sinkOptions.isDefined)
    assert(result.sinkOptions.get.foreignKeys.size == 1)
    assert(result.sinkOptions.get.foreignKeys.head._1 == "json.file_account.account_id")
    assert(result.sinkOptions.get.foreignKeys.head._2.size == 1)
    assert(result.sinkOptions.get.foreignKeys.head._2.head == "csv.transactions.account_id")
  }

}
