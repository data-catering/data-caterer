package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{CSV, FORMAT, JDBC_TABLE, PATH, URL}
import io.github.datacatering.datacaterer.api.model.ExpressionValidation
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PlanRunTest extends AnyFunSuite {

  test("Can create plan with each type of connection") {
    val result = new PlanRun {
      val mySchema = schema.addFields(field.name("account_id"))
      val myCsv = csv("my_csv", "/my/csv").schema(mySchema)
      val myJson = json("my_json", "/my/json").schema(mySchema)
      val myParquet = parquet("my_parquet", "/my/parquet").schema(mySchema)
      val myOrc = orc("my_orc", "/my/orc").schema(mySchema)
      val myPostgres = postgres("my_postgres").table("account").schema(mySchema)
      val myMySql = mysql("my_mysql").table("transaction").schema(mySchema)
      val myCassandra = cassandra("my_cassandra").table("account", "accounts").schema(mySchema)
      val mySolace = solace("my_solace").destination("solace_topic").schema(mySchema)
      val myKafka = kafka("my_kafka").topic("kafka_topic").schema(mySchema)
      val myHttp = http("my_http").schema(mySchema)

      execute(myCsv, myJson, myParquet, myOrc, myPostgres, myMySql, myCassandra, mySolace, myKafka, myHttp)
    }

    val dsNames = List("my_csv", "my_json", "my_parquet", "my_orc", "my_postgres", "my_mysql", "my_cassandra", "my_solace", "my_kafka", "my_http")
    assert(result._plan.tasks.size == 10)
    assert(result._plan.tasks.map(_.dataSourceName) == dsNames)
    assert(result._configuration.connectionConfigByName.size == 10)
    assert(result._configuration.connectionConfigByName.keys.forall(dsNames.contains))
    assert(result._tasks.size == 10)
  }

  test("Can create plan using same connection details from another step") {
    val result = new PlanRun {
      val myPostgresAccount = postgres("my_postgres", "my_postgres_url")
        .table("account.accounts")
        .schema(field.name("account_id"))
      val myPostgresTransaction = postgres(myPostgresAccount)
        .table("account.transactions")
        .schema(field.name("txn_id"))

      execute(myPostgresAccount, myPostgresTransaction)
    }

    assert(result._plan.tasks.size == 2)
    assert(result._plan.tasks.map(_.dataSourceName).forall(_ == "my_postgres"))
    assert(result._configuration.connectionConfigByName.size == 1)
    assert(result._configuration.connectionConfigByName.contains("my_postgres"))
    assert(result._configuration.connectionConfigByName("my_postgres").contains(URL))
    assert(result._configuration.connectionConfigByName("my_postgres").get(URL).contains("my_postgres_url"))
    assert(result._tasks.size == 2)
    val steps = result._tasks.flatMap(_.steps)
    val resAccount = steps.filter(s => s.options.get(JDBC_TABLE).contains("account.accounts")).head
    assert(resAccount.schema.fields.isDefined)
    assert(resAccount.schema.fields.get.size == 1)
    assert(resAccount.schema.fields.get.head.name == "account_id")
    val resTxn = steps.filter(s => s.options.get(JDBC_TABLE).contains("account.transactions")).head
    assert(resTxn.schema.fields.isDefined)
    assert(resTxn.schema.fields.get.size == 1)
    assert(resTxn.schema.fields.get.head.name == "txn_id")
    assert(result._validations.isEmpty)
  }

  test("Can create plan with validations for one data source") {
    val result = new PlanRun {
      val myCsv = csv("my_csv", "/my/data/path")
        .schema(field.name("account_id"))
        .validations(validation.expr("account_id != ''"))

      execute(myCsv)
    }

    assert(result._validations.size == 1)
    assert(result._validations.head.dataSources.size == 1)
    assert(result._validations.head.dataSources.head._2.size == 1)
    val dsValidation = result._validations.head.dataSources.head
    assert(dsValidation._1 == "my_csv")
    assert(dsValidation._2.head.options.nonEmpty)
    assert(dsValidation._2.head.options == Map(FORMAT -> "csv", PATH -> "/my/data/path"))
    assert(dsValidation._2.head.validations.size == 1)
    assert(dsValidation._2.head.validations.head.validation.isInstanceOf[ExpressionValidation])
    val expressionValidation = dsValidation._2.head.validations.head.validation.asInstanceOf[ExpressionValidation]
    assert(expressionValidation.expr == "account_id != ''")
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

    assert(result._validations.size == 1)
    assert(result._validations.head.dataSources.size == 1)
    val dsValidation = result._validations.head.dataSources.head
    assert(dsValidation._1 == "my_postgres")
    val accountValid = dsValidation._2.filter(_.options.get(JDBC_TABLE).contains("account.accounts")).head
    assert(accountValid.validations.size == 1)
    assert(accountValid.validations.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "account_id != ''"))
    val txnValid = dsValidation._2.filter(_.options.get(JDBC_TABLE).contains("account.transactions")).head
    assert(txnValid.validations.size == 1)
    assert(txnValid.validations.exists(v => v.validation.asInstanceOf[ExpressionValidation].expr == "txn_id IS NOT NULL"))
  }

  test("Can create plan with validations only defined") {
    val result = new PlanRun {
      val myCsv = csv("my_csv", "/my/csv")
        .validations(validation.expr("account_id != 'acc123'"))

      execute(myCsv)
    }

    assert(result._tasks.size == 1)
    assert(result._validations.size == 1)
    assert(result._validations.head.dataSources.contains("my_csv"))
    val validRes = result._validations.head.dataSources("my_csv").head
    assert(validRes.validations.size == 1)
    assert(validRes.validations.head.validation.asInstanceOf[ExpressionValidation].expr == "account_id != 'acc123'")
    assert(validRes.options.nonEmpty)
    assert(validRes.options == Map(FORMAT -> "csv", PATH -> "/my/csv"))
  }


}