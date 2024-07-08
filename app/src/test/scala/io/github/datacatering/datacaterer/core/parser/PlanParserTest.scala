package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanParserTest extends SparkSuite {

  test("Can parse plan in YAML file") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/account-create-plan-test.yaml")

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

    assert(result.length == 13)
  }

  test("Can parse plan in YAML file with foreign key") {
    val result = PlanParser.parsePlan("src/test/resources/sample/plan/large-plan.yaml")

    assert(result.sinkOptions.isDefined)
    assert(result.sinkOptions.get.foreignKeys.size == 1)
    assert(result.sinkOptions.get.foreignKeys.head._1 == "json.file_account.account_id")
    assert(result.sinkOptions.get.foreignKeys.head._2.size == 1)
    assert(result.sinkOptions.get.foreignKeys.head._2.head == "csv.transactions.account_id")
  }

  test("Can parse validations in YAML file") {
    val connectionConfig = Map(
      "my_first_json" -> Map("path" -> "/tmp/json_1"),
      "my_third_json" -> Map("path" -> "/tmp/json_2"),
    )
    val result = PlanParser.parseValidations("src/test/resources/sample/validation/all", connectionConfig)

    assertResult(1)(result.size)
    assertResult(1)(result.head.dataSources.size)
    assertResult("json")(result.head.dataSources.head._1)
    val baseVal = result.head.dataSources.head._2.head
    assertResult(Map("path" -> "app/src/test/resources/sample/json/txn-gen"))(baseVal.options)
    assert(baseVal.waitCondition.isInstanceOf[PauseWaitCondition])
    assertResult(1)(baseVal.waitCondition.asInstanceOf[PauseWaitCondition].pauseInSeconds)
    assertResult(16)(baseVal.validations.size)

    def validateExpr(expectedExpr: String, optThreshold: Option[Double] = None, optPreFilter: Option[String] = None) = {
      assert(baseVal.validations.exists(v => {
        v.validation match {
          case ExpressionValidation(expr, _) =>
            expr == expectedExpr &&
              optThreshold.forall(t => v.validation.errorThreshold.get == t) &&
              optPreFilter.forall(f => v.validation.preFilterExpr.get == f)
          case _ => false
        }
      }))
    }

    validateExpr("amount < 100")
    validateExpr("year == 2021", Some(0.1))
    validateExpr("regexp_like(name, 'Peter .*')", Some(200))
    validateExpr("amount > 50", None, Some("name == 'peter'"))

    assert(baseVal.validations.exists(v => {
      v.validation match {
        case GroupByValidation(groupByCols, _, aggType, whereExpr) =>
          groupByCols == Seq("account_id") && aggType == "count" && whereExpr == "count == 1"
        case _ => false
      }
    }))

    assert(baseVal.validations.exists(v => {
      v.validation match {
        case UpstreamDataSourceValidation(validation, upstreamDataSource, _, joinColumns, joinType) =>
          upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName == "my_first_json" &&
            joinColumns == List("account_id") && joinType == "outer" &&
            validation.validation.isInstanceOf[ExpressionValidation] &&
            validation.validation.asInstanceOf[ExpressionValidation].expr == "my_first_json_customer_details.name == name"
        case _ => false
      }
    }))

    assert(baseVal.validations.exists(v => {
      v.validation match {
        case UpstreamDataSourceValidation(validation, upstreamDataSource, _, joinColumns, joinType) =>
          upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName == "my_first_json" &&
            joinColumns == List("account_id") && joinType == "anti" &&
            validation.validation.isInstanceOf[GroupByValidation] &&
            validation.validation.asInstanceOf[GroupByValidation].aggExpr == "count == 0" &&
            validation.validation.asInstanceOf[GroupByValidation].aggType == "count"
        case _ => false
      }
    }))
  }

}
