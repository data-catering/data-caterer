package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{FORMAT, JDBC_TABLE, PATH, URL, VALIDATION_IDENTIFIER}
import io.github.datacatering.datacaterer.api.model.ExpressionValidation
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanRunTest extends AnyFunSuite {

  test("Can create plan with each type of connection") {
    val result = new PlanRun {
      val mySchema = field.name("account_id")
      val myCsv = csv("my_csv", "/my/csv").fields(mySchema)
      val myJson = json("my_json", "/my/json").fields(mySchema)
      val myParquet = parquet("my_parquet", "/my/parquet").fields(mySchema)
      val myOrc = orc("my_orc", "/my/orc").fields(mySchema)
      val myPostgres = postgres("my_postgres").table("account").fields(mySchema)
      val myMySql = mysql("my_mysql").table("transaction").fields(mySchema)
      val myCassandra = cassandra("my_cassandra").table("account", "accounts").fields(mySchema)
      val mySolace = solace("my_solace").destination("solace_topic").fields(mySchema)
      val myKafka = kafka("my_kafka").topic("kafka_topic").fields(mySchema)
      val myHttp = http("my_http").fields(mySchema)

      execute(myCsv, myJson, myParquet, myOrc, myPostgres, myMySql, myCassandra, mySolace, myKafka, myHttp)
    }

    val dsNames = List("my_csv", "my_json", "my_parquet", "my_orc", "my_postgres", "my_mysql", "my_cassandra", "my_solace", "my_kafka", "my_http")
    assertResult(10)(result._plan.tasks.size)
    assertResult(dsNames)(result._plan.tasks.map(_.dataSourceName))
    assertResult(10)(result._configuration.connectionConfigByName.size)
    assert(result._configuration.connectionConfigByName.keys.forall(dsNames.contains))
    assertResult(10)(result._tasks.size)
  }

  test("Can create plan using same connection details from another step") {
    val result = new PlanRun {
      val myPostgresAccount = postgres("my_postgres", "my_postgres_url")
        .table("account.accounts")
        .fields(field.name("account_id"))
      val myPostgresTransaction = postgres(myPostgresAccount)
        .table("account.transactions")
        .fields(field.name("txn_id"))

      execute(myPostgresAccount, myPostgresTransaction)
    }

    assertResult(2)(result._plan.tasks.size)
    assert(result._plan.tasks.map(_.dataSourceName).forall(_ == "my_postgres"))
    assertResult(1)(result._configuration.connectionConfigByName.size)
    assert(result._configuration.connectionConfigByName.contains("my_postgres"))
    assert(result._configuration.connectionConfigByName("my_postgres").contains(URL))
    assert(result._configuration.connectionConfigByName("my_postgres").get(URL).contains("my_postgres_url"))
    assertResult(2)(result._tasks.size)
    val steps = result._tasks.flatMap(_.steps)
    val resAccount = steps.filter(s => s.options.get(JDBC_TABLE).contains("account.accounts")).head
    assertResult(1)(resAccount.fields.size)
    assertResult("account_id")(resAccount.fields.head.name)
    val resTxn = steps.filter(s => s.options.get(JDBC_TABLE).contains("account.transactions")).head
    assertResult(1)(resTxn.fields.size)
    assertResult("txn_id")(resTxn.fields.head.name)
    assert(result._validations.isEmpty)
  }

  test("Can create plan with validations for one data source") {
    val result = new PlanRun {
      val myCsv = csv("my_csv", "/my/data/path")
        .fields(field.name("account_id"))
        .validations(validation.expr("account_id != ''"))

      execute(myCsv)
    }

    assertResult(1)(result._validations.size)
    assertResult(1)(result._validations.head.dataSources.size)
    assertResult(1)(result._validations.head.dataSources.head._2.size)
    val dsValidation = result._validations.head.dataSources.head
    assertResult("my_csv")(dsValidation._1)
    assert(dsValidation._2.head.options.nonEmpty)
    assertResult(Map(
      FORMAT -> "csv",
      PATH -> "/my/data/path",
      VALIDATION_IDENTIFIER -> result._tasks.head.steps.head.name
    ))(dsValidation._2.head.options)
    assertResult(1)(dsValidation._2.head.validations.size)
    assert(dsValidation._2.head.validations.head.validation.isInstanceOf[ExpressionValidation])
    val expressionValidation = dsValidation._2.head.validations.head.validation.asInstanceOf[ExpressionValidation]
    assertResult("account_id != ''")(expressionValidation.expr)
  }

  test("Can create plan with multiple validations for one data source") {
    val result = new PlanRun {
      val myPostgresAccount = postgres("my_postgres")
        .table("account.accounts")
        .validations(validation.expr("account_id != ''"))
      val myPostgresTransaction = postgres("my_postgres")
        .table("account", "transactions")
        .validations(validation.expr("txn_id IS NOT NULL"))

      execute(myPostgresAccount, myPostgresTransaction)
    }

    assertResult(1)(result._validations.size)
    assertResult(1)(result._validations.head.dataSources.size)
    val dsValidation = result._validations.head.dataSources.head
    assertResult("my_postgres")(dsValidation._1)
    val accountValid = dsValidation._2.filter(_.options.get(JDBC_TABLE).contains("account.accounts")).head
    assertResult(1)(accountValid.validations.size)
    assert(accountValid.validations.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "account_id != ''"))
    val txnValid = dsValidation._2.filter(_.options.get(JDBC_TABLE).contains("account.transactions")).head
    assertResult(1)(txnValid.validations.size)
    assert(txnValid.validations.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "txn_id IS NOT NULL"))
  }

  test("Can create plan with validations only defined") {
    val result = new PlanRun {
      val myCsv = csv("my_csv", "/my/csv")
        .validations(validation.expr("account_id != 'acc123'"))

      execute(myCsv)
    }

    assertResult(1)(result._tasks.size)
    assertResult(1)(result._validations.size)
    assert(result._validations.head.dataSources.contains("my_csv"))
    val validRes = result._validations.head.dataSources("my_csv").head
    assertResult(1)(validRes.validations.size)
    assertResult("account_id != 'acc123'")(validRes.validations.head.validation.asInstanceOf[ExpressionValidation].expr)
    assert(validRes.options.nonEmpty)
    assertResult(Map(
      FORMAT -> "csv",
      PATH -> "/my/csv",
      VALIDATION_IDENTIFIER -> result._tasks.head.steps.head.name
    ))(validRes.options)
  }


}