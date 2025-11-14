package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.{IS_PRIMARY_KEY, IS_UNIQUE}
import io.github.datacatering.datacaterer.api.model.{Count, Field, PerFieldCount, Plan, Step, Task, TaskSummary}
import org.apache.spark.util.sketch.BloomFilter

import java.sql.Date

class UniqueFieldsUtilTest extends SparkSuite {

  private val FIELDS = List(
    Field("account_id", Some("string"), Map(IS_UNIQUE -> "true")),
    Field("name", Some("string"), Map(IS_UNIQUE -> "true")),
    Field("open_date", Some("date")),
    Field("age", Some("int")),
  )

  test("Can identify the unique fields and create a data frame with unique values for field") {
    val tasks = List((
      TaskSummary("gen data", "postgresAccount"),
      Task("account_postgres", List(
        Step("accounts", "postgres", Count(), Map(), FIELDS)
      ))
    ))
    val uniqueFieldUtil = new UniqueFieldsUtil(Plan(), tasks)

    val uniqueFields = uniqueFieldUtil.uniqueFieldsDf
    assertResult(2)(uniqueFields.size)
    assertResult(2)(uniqueFieldUtil.uniqueFieldsDf.size)
    val col = uniqueFields.filter(_._1.fields == List("account_id")).head
    assertResult("postgresAccount")(col._1.dataSource)
    assertResult("accounts")(col._1.step)

    val generatedData = sparkSession.createDataFrame(Seq(
      Account("acc1", "peter"),
      Account("acc1", "john"),
      Account("acc2", "jack"),
      Account("acc3", "bob")
    ))
    val result = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData, Step())

    val data = result.select("account_id").collect().map(_.getString(0))
    val expectedUniqueAccounts = Array("acc1", "acc2", "acc3")
    assertResult(3)(data.length)
    data.foreach(a => assert(expectedUniqueAccounts.contains(a)))
    assertResult(2)(uniqueFieldUtil.uniqueFieldsDf.size)

    val generatedData2 = sparkSession.createDataFrame(Seq(
      Account("acc1", "dog"), Account("acc3", "bob"), Account("acc4", "cat"), Account("acc5", "peter")
    ))
    val result2 = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData2, Step())

    result2.cache()
    val data2 = result2.select("account_id", "name").collect()

    assertResult(1)(data2.length)
    assertResult("acc4")(data2.head.getString(0))
    assertResult("cat")(data2.head.getString(1))
  }

  test("Can filter out duplicate records from previous generated data") {
    val generatedData = sparkSession.createDataFrame(Seq(
      Account("acc1", "peter"),
      Account("acc2", "john"),
      Account("acc3", "jack"),
      Account("acc4", "bob"),
      Account("acc5", "alice")
    ))

    val bloomFilter = BloomFilter.create(1e7.toLong, 0.001) // 10M entries, 0.1% error rate
    bloomFilter.putString("acc4|bob")
    bloomFilter.putString("acc5|alice")
    val bloomFilterBC = sparkSession.sparkContext.broadcast(bloomFilter)
    val prevGen = UniqueFields("my-data-source", "step1", List("account_id", "name")) -> bloomFilterBC
    val uniqueDF = new UniqueFieldsUtil(Plan(), List()).filterUniqueRecords(generatedData, prevGen)

    val res = uniqueDF.collect()
    assertResult(3)(res.length)
    assert(res.exists(r => r.getString(0) == "acc1"))
    assert(res.exists(r => r.getString(0) == "acc2"))
    assert(res.exists(r => r.getString(0) == "acc3"))
  }

  test("Can identify the unique fields and create a data frame with unique values with per field count defined") {
    val step = Step(
      "accounts",
      "postgres",
      Count(perField = Some(PerFieldCount(List("account_id", "name"), Some(2)))),
      Map(),
      FIELDS
    )
    val tasks = List((
      TaskSummary("gen data", "postgresAccount"),
      Task("account_postgres", List(step))
    ))
    val uniqueFieldUtil = new UniqueFieldsUtil(Plan(), tasks)

    val firstDate = Date.valueOf("2020-01-01")
    val secDate = Date.valueOf("2020-01-02")
    val generatedData = sparkSession.createDataFrame(Seq(
      Account("acc1", open_date = firstDate), Account("acc1", open_date = secDate),
      Account("acc2", "jack", firstDate), Account("acc2", "jack", secDate),
      Account("acc3", "susie", firstDate), Account("acc3", "susie", secDate), Account("acc3", "susie", Date.valueOf("2020-01-03")),
      Account("acc3", "blah", firstDate)
    ))
    val result = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData, step)

    val data = result.select("account_id").collect().map(_.getString(0))
    val expectedUniqueAccounts = Array("acc1", "acc2", "acc3")
    assertResult(3)(data.length)
    data.foreach(a => assert(expectedUniqueAccounts.contains(a)))
    assertResult(2)(uniqueFieldUtil.uniqueFieldsDf.size)
  }

  test("Can identify the primary key fields, create a data frame with per field count defined, create unique values for the primary key fields and don't track globally unique values") {
    val step = Step(
      "accounts",
      "postgres",
      Count(perField = Some(PerFieldCount(List("account_id"), Some(2)))),
      Map(),
      List(
        Field("account_id", Some("string"), Map(IS_PRIMARY_KEY -> "true")),
        Field("name", Some("string"), Map(IS_PRIMARY_KEY -> "true")),
        Field("open_date", Some("date")),
        Field("age", Some("int")),
      )
    )
    val tasks = List((
      TaskSummary("gen data", "postgresAccount"),
      Task("account_postgres", List(step))
    ))
    val uniqueFieldUtil = new UniqueFieldsUtil(Plan(), tasks, true)

    val firstDate = Date.valueOf("2020-01-01")
    val secDate = Date.valueOf("2020-01-02")
    val generatedData = sparkSession.createDataFrame(Seq(
      Account("acc1", open_date = firstDate), Account("acc1", open_date = secDate),
      Account("acc2", "jack", firstDate), Account("acc2", "john", secDate),
      Account("acc3", "susie", firstDate), Account("acc3", "susie", secDate), Account("acc3", "susie", Date.valueOf("2020-01-03")),
      Account("acc3", "blah", firstDate)
    ))
    val result = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData, step)

    val accountIdData = result.select("account_id").collect().map(_.getString(0))
    val expectedUniqueAccounts = Array("acc1", "acc2", "acc3")
    assertResult(5)(accountIdData.length)
    accountIdData.foreach(a => assert(expectedUniqueAccounts.contains(a)))
    val nameData = result.select("name").collect().map(_.getString(0))
    val expectedUniqueNames = Array("peter", "jack", "john", "susie", "blah")
    assertResult(5)(nameData.length)
    nameData.foreach(a => assert(expectedUniqueNames.contains(a)))
    assertResult(1)(uniqueFieldUtil.uniqueFieldsDf.size)
  }
}
