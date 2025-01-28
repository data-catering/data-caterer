package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.PlanRun
import io.github.datacatering.datacaterer.api.model.Constants.FOREIGN_KEY_DELIMITER
import io.github.datacatering.datacaterer.api.model.{ForeignKey, ForeignKeyRelation, Plan, SinkOptions, TaskSummary}
import io.github.datacatering.datacaterer.core.exception.MissingDataSourceFromForeignKeyException
import io.github.datacatering.datacaterer.core.model.{ForeignKeyRelationship, ForeignKeyWithGenerateAndDelete}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date
import java.time.LocalDate

@RunWith(classOf[JUnitRunner])
class ForeignKeyUtilTest extends SparkSuite {

  test("When no foreign keys defined, return back same dataframes") {
    val sinkOptions = SinkOptions(None, None, List())
    val plan = Plan("no foreign keys", "simple plan", List(), Some(sinkOptions))
    val dfMap = List("name" -> sparkSession.emptyDataFrame)

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)

    assertResult(result)(dfMap)
  }

  test("Can get insert order") {
    val foreignKeys = List(
      "orders" -> List("customers"),
      "order_items" -> List("orders", "products"),
      "reviews" -> List("products", "customers")
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)

    result should contain theSameElementsInOrderAs List("order_items", "reviews", "orders", "products", "customers")
  }

  test("Can get insert order with multiple foreign keys") {
    val foreignKeys = List(
      "products" -> List("customers", "prices", "orders"),
      "customers" -> List("addresses")
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)

    result should contain theSameElementsInOrderAs List("products", "customers", "prices", "orders", "addresses")
  }

  test("Can get insert order when multiple generations are defined") {
    val foreignKeys = List(
      "products" -> List("customers", "prices", "orders"),
    )
    val result = ForeignKeyUtil.getInsertOrder(foreignKeys)

    result should contain theSameElementsInOrderAs List("products", "customers", "prices", "orders")
  }

  test("Can link foreign keys between data sets") {
    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("postgres", "account", List("account_id")),
        List(ForeignKeyRelation("postgres", "transaction", List("account_id"))), List()))
    )
    val plan = Plan("foreign keys", "simple plan", List(), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand2", "id124", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id125", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = List(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    val resTxnRows = txn.collect()
    resTxnRows.foreach(r => {
      r.getString(0) == "acc1" || r.getString(0) == "acc2" || r.getString(0) == "acc3"
    })
  }

  test("Can link foreign keys between data sets with multiple fields") {
    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("postgres", "account", List("account_id", "name")),
        List(ForeignKeyRelation("postgres", "transaction", List("account_id", "name"))), List()))
    )
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "postgres")), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand1", "id124", Date.valueOf(LocalDate.now()), 12.0),
      Transaction("some_acc9", "rand2", "id125", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id126", Date.valueOf(LocalDate.now()), 85.1),
    )
    val dfMap = List(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    val resTxnRows = txn.collect()
    val acc1 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc1"))
    assert(acc1.isDefined)
    assert(acc1.get.getString(1).equalsIgnoreCase("peter"))
    val acc2 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc2"))
    assert(acc2.isDefined)
    assert(acc2.get.getString(1).equalsIgnoreCase("john"))
    val acc3 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc3.isDefined)
    assert(acc3.get.getString(1).equalsIgnoreCase("jack"))
    val acc1Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc1"))
    val acc2Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc2"))
    val acc3Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc1Count == 2 || acc2Count == 2 || acc3Count == 2)
  }

  test("Can link foreign keys between data sets with multiple records per field") {
    val sinkOptions = SinkOptions(None, None,
      List(ForeignKey(ForeignKeyRelation("postgres", "account", List("account_id")),
        List(ForeignKeyRelation("postgres", "transaction", List("account_id"))), List()))
    )
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "postgres")), Some(sinkOptions))
    val accountsList = List(
      Account("acc1", "peter", Date.valueOf(LocalDate.now())),
      Account("acc2", "john", Date.valueOf(LocalDate.now())),
      Account("acc3", "jack", Date.valueOf(LocalDate.now()))
    )
    val transactionList = List(
      Transaction("some_acc9", "rand1", "id123", Date.valueOf(LocalDate.now()), 10.0),
      Transaction("some_acc9", "rand1", "id124", Date.valueOf(LocalDate.now()), 12.0),
      Transaction("some_acc9", "rand2", "id125", Date.valueOf(LocalDate.now()), 23.9),
      Transaction("some_acc10", "rand3", "id126", Date.valueOf(LocalDate.now()), 85.1),
      Transaction("some_acc10", "rand3", "id127", Date.valueOf(LocalDate.now()), 72.1),
      Transaction("some_acc11", "rand3", "id128", Date.valueOf(LocalDate.now()), 5.9)
    )
    val dfMap = List(
      "postgres.account" -> sparkSession.createDataFrame(accountsList),
      "postgres.transaction" -> sparkSession.createDataFrame(transactionList)
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, dfMap)
    val txn = result.filter(f => f._1.equalsIgnoreCase("postgres.transaction")).head._2
    txn.show(false)
    val resTxnRows = txn.collect()
    val acc1 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc1"))
    assert(acc1.isDefined)
    val acc2 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc2"))
    assert(acc2.isDefined)
    val acc3 = resTxnRows.find(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc3.isDefined)
    val acc1Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc1"))
    val acc2Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc2"))
    val acc3Count = resTxnRows.count(_.getString(0).equalsIgnoreCase("acc3"))
    assert(acc1Count == 3 || acc2Count == 3 || acc3Count == 3)
    assert(acc1Count == 2 || acc2Count == 2 || acc3Count == 2)
    assert(acc1Count == 1 || acc2Count == 1 || acc3Count == 1)
  }

  test("Can get delete order based on foreign keys defined") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder ==
      List(
        s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
        s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
        s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id"
      )
    )
  }

  test("Can get delete order based on nested foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    val expected = List(
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id"
    )
    assertResult(expected)(deleteOrder)

    val foreignKeys1 = List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder1 = ForeignKeyUtil.getDeleteOrder(foreignKeys1)
    assertResult(expected)(deleteOrder1)

    val foreignKeys2 = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id"),
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id" -> List(s"postgres${FOREIGN_KEY_DELIMITER}customer${FOREIGN_KEY_DELIMITER}account_id"),
    )
    val deleteOrder2 = ForeignKeyUtil.getDeleteOrder(foreignKeys2)
    val expected2 = List(s"postgres${FOREIGN_KEY_DELIMITER}customer${FOREIGN_KEY_DELIMITER}account_id") ++ expected
    assertResult(expected2)(deleteOrder2)
  }

  test("Can generate correct values when per field count is defined over multiple fields that are also defined as foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id")
    )
  }

  test("Can generate correct values when primary keys are defined over multiple fields that are also defined as foreign keys") {
    val foreignKeys = List(
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id" ->
        List(s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id", s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id")
    )
    val deleteOrder = ForeignKeyUtil.getDeleteOrder(foreignKeys)
    assert(deleteOrder == List(
      s"postgres${FOREIGN_KEY_DELIMITER}balances${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}transactions${FOREIGN_KEY_DELIMITER}account_id",
      s"postgres${FOREIGN_KEY_DELIMITER}accounts${FOREIGN_KEY_DELIMITER}account_id")
    )
  }

  test("Can update foreign keys with updated names from metadata") {
    implicit val encoder = Encoders.kryo[ForeignKeyRelationship]
    val generatedForeignKeys = List(sparkSession.createDataset(Seq(ForeignKeyRelationship(
      ForeignKeyRelation("my_postgres", "public.account", List("account_id")),
      ForeignKeyRelation("my_postgres", "public.orders", List("customer_id")),
    ))))
    val optPlanRun = Some(new ForeignKeyPlanRun())
    val stepNameMapping = Map(
      s"my_csv${FOREIGN_KEY_DELIMITER}random_step" -> s"my_csv${FOREIGN_KEY_DELIMITER}public.accounts"
    )

    val result = ForeignKeyUtil.getAllForeignKeyRelationships(generatedForeignKeys, optPlanRun, stepNameMapping)

    assertResult(3)(result.size)
    assert(result.contains(
      ForeignKey(
        ForeignKeyRelation("my_csv", "public.accounts", List("id")),
        List(ForeignKeyRelation("my_postgres", "public.accounts", List("account_id"))),
        List()
      )
    ))
    assert(result.contains(
      ForeignKey(
        ForeignKeyRelation("my_json", "json_step", List("id")),
        List(ForeignKeyRelation("my_postgres", "public.orders", List("customer_id"))),
        List()
      )
    ))
    assert(result.contains(
      ForeignKey(
        ForeignKeyRelation("my_postgres", "public.account", List("account_id")),
        List(ForeignKeyRelation("my_postgres", "public.orders", List("customer_id"))),
        List()
      )
    ))
  }

  test("Can link foreign keys with nested field names") {
    val nestedStruct = StructType(Array(StructField("account_id", StringType)))
    val nestedInArray = ArrayType(nestedStruct)
    val fields = Array(StructField("my_json", nestedStruct), StructField("my_array", nestedInArray))

    assert(ForeignKeyUtil.hasDfContainField("my_array.account_id", fields))
    assert(ForeignKeyUtil.hasDfContainField("my_json.account_id", fields))
    assert(!ForeignKeyUtil.hasDfContainField("my_json.name", fields))
    assert(!ForeignKeyUtil.hasDfContainField("my_array.name", fields))
  }

  test("getDataFramesWithForeignKeys should return back list of dataframes in correct order when foreign keys are defined") {
    val sinkOptions = SinkOptions(None, None,
      List(
        ForeignKey(
          ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
          List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
          List()
        )
      ))
    val plan = Plan("foreign keys", "simple plan", List(
      TaskSummary("my_task", "sourceDf"),
      TaskSummary("my_target_task", "targetDf"),
      TaskSummary("my_other_task", "otherDf")
    ), Some(sinkOptions))
    val generatedDataForeachTask = List(
      ("otherDf.otherDataSource", sparkSession.createDataFrame(Seq((1, "f"), (2, "g"))).toDF("id", "value")),
      ("sourceDf.sourceDataSource", sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value")),
      ("targetDf.targetDataSource", sparkSession.createDataFrame(Seq((1, "x"), (2, "y"))).toDF("id", "value")),
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    val resultDfNames = result.map(_._1)
    val expectedDfNamesOrder = List("sourceDf.sourceDataSource", "targetDf.targetDataSource", "otherDf.otherDataSource")

    resultDfNames should contain theSameElementsInOrderAs expectedDfNamesOrder
  }

  test("getDataFramesWithForeignKeys should return back list of dataframes in correct order when multiple generations are defined") {
    val sinkOptions = SinkOptions(None, None,
      List(
        ForeignKey(
          ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
          List(
            ForeignKeyRelation("targetDf1", "targetDataSource1", List("value")),
            ForeignKeyRelation("targetDf2", "targetDataSource2", List("value")),
            ForeignKeyRelation("targetDf3", "targetDataSource3", List("value")),
          ),
          List()
        )
      ))
    val plan = Plan("foreign keys", "simple plan", List(
      TaskSummary("my_task", "sourceDf"),
      TaskSummary("my_target3_task", "targetDf3"),
      TaskSummary("my_target1_task", "targetDf1"),
      TaskSummary("my_target2_task", "targetDf2"),
    ), Some(sinkOptions))
    val generatedDataForeachTask = List(
      ("targetDf3.targetDataSource3", sparkSession.createDataFrame(Seq((1, "x"), (2, "y"))).toDF("id", "value")),
      ("targetDf1.targetDataSource1", sparkSession.createDataFrame(Seq((1, "c"), (2, "d"))).toDF("id", "value")),
      ("sourceDf.sourceDataSource", sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value")),
      ("targetDf2.targetDataSource2", sparkSession.createDataFrame(Seq((1, "f"), (2, "g"))).toDF("id", "value")),
    )

    val result = ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    val resultDfNames = result.map(_._1)
    val expectedDfNamesOrder = List("sourceDf.sourceDataSource", "targetDf1.targetDataSource1", "targetDf2.targetDataSource2", "targetDf3.targetDataSource3")

    resultDfNames should contain theSameElementsInOrderAs expectedDfNamesOrder
  }

  test("getDataFramesWithForeignKeys should throw MissingDataSourceFromForeignKeyException if source dataframe is missing") {
    val sinkOptions = SinkOptions(None, None,
      List(
        ForeignKey(
          ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
          List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
          List()
        )
      ))
    val plan = Plan("foreign keys", "simple plan", List(TaskSummary("my_task", "sourceDf"), TaskSummary("my_target_task", "targetDf")), Some(sinkOptions))
    val generatedDataForeachTask = List(
      ("targetDf.targetDataSource", sparkSession.createDataFrame(Seq((1, "x"), (2, "y"))).toDF("id", "value"))
    )

    assertThrows[MissingDataSourceFromForeignKeyException] {
      ForeignKeyUtil.getDataFramesWithForeignKeys(plan, generatedDataForeachTask)
    }
  }

  test("isValidForeignKeyRelation should return true for valid foreign key relation") {
    val generatedDataForeachTask = Map(
      "sourceDf.sourceDataSource" -> sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value"),
      "targetDf.targetDataSource" -> sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value"),
    )
    val enabledSources = List("sourceDf", "targetDf")
    val fkr = ForeignKeyWithGenerateAndDelete(
      ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
      List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
      List()
    )

    val result = ForeignKeyUtil.isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr)
    result shouldBe true
  }

  test("isValidForeignKeyRelation should return false if main foreign key source is not enabled") {
    val generatedDataForeachTask = Map(
      "sourceDf.sourceDataSource" -> sparkSession.createDataFrame(Seq((1, "a"), (2, "b"))).toDF("id", "value")
    )
    val enabledSources = List("targetDataSource")
    val fkr = ForeignKeyWithGenerateAndDelete(
      ForeignKeyRelation("sourceDf", "sourceDataSource", List("value")),
      List(ForeignKeyRelation("targetDf", "targetDataSource", List("value"))),
      List()
    )

    val result = ForeignKeyUtil.isValidForeignKeyRelation(generatedDataForeachTask, enabledSources, fkr)
    result shouldBe false
  }

  class ForeignKeyPlanRun extends PlanRun {
    val myPlan = plan.addForeignKeyRelationship(
      foreignField("my_csv", "random_step", "id"),
      foreignField("my_postgres", "public.accounts", "account_id")
    ).addForeignKeyRelationship(
      foreignField("my_json", "json_step", "id"),
      foreignField("my_postgres", "public.orders", "customer_id")
    )

    execute(plan = myPlan)
  }
}

case class Account(account_id: String = "acc123", name: String = "peter", open_date: Date = Date.valueOf("2023-01-31"), age: Int = 10, debitCredit: String = "D")

case class Transaction(account_id: String, name: String, transaction_id: String, created_date: Date, amount: Double, links: List[String] = List())
