package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.IS_UNIQUE
import io.github.datacatering.datacaterer.api.model.{Count, Field, PerFieldCount, Plan, Step, Task, TaskSummary}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Date

@RunWith(classOf[JUnitRunner])
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
    assert(uniqueFieldUtil.uniqueFieldsDf.head._2.isEmpty)
    val col = uniqueFields.filter(_._1.fields == List("account_id")).head
    assertResult("postgresAccount")(col._1.dataSource)
    assertResult("accounts")(col._1.step)

    val generatedData = sparkSession.createDataFrame(Seq(
      Account("acc1", "peter"), Account("acc1", "john"), Account("acc2", "jack"), Account("acc3", "bob")
    ))
    val result = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData, Step())

    val data = result.select("account_id").collect().map(_.getString(0))
    val expectedUniqueAccounts = Array("acc1", "acc2", "acc3")
    assertResult(3)(data.length)
    data.foreach(a => assert(expectedUniqueAccounts.contains(a)))
    assertResult(2)(uniqueFieldUtil.uniqueFieldsDf.size)
    assertResult(3)(uniqueFieldUtil.uniqueFieldsDf.head._2.count())
    val currentUniqueAcc = uniqueFieldUtil.uniqueFieldsDf.filter(_._1.fields == List("account_id")).head._2.collect().map(_.getString(0))
    currentUniqueAcc.foreach(a => assert(expectedUniqueAccounts.contains(a)))

    val generatedData2 = sparkSession.createDataFrame(Seq(
      Account("acc1", "dog"), Account("acc3", "bob"), Account("acc4", "cat"), Account("acc5", "peter")
    ))
    val result2 = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData2, Step())

    val data2 = result2.select("account_id", "name").collect()
    val expectedUniqueNames = Array("peter", "jack", "bob", "cat")
    val expectedUniqueAccounts2 = Array("acc1", "acc2", "acc3", "acc4")

    assertResult(1)(data2.length)
    assertResult("acc4")(data2.head.getString(0))
    assertResult("cat")(data2.head.getString(1))

    val currentUniqueAcc2 = uniqueFieldUtil.uniqueFieldsDf.filter(_._1.fields == List("account_id")).head._2.collect().map(_.getString(0))
    currentUniqueAcc2.foreach(a => assert(expectedUniqueAccounts2.contains(a)))
    val currentUniqueName = uniqueFieldUtil.uniqueFieldsDf.filter(_._1.fields == List("name")).head._2.collect().map(_.getString(0))
    currentUniqueName.foreach(a => assert(expectedUniqueNames.contains(a)))
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
      Account("acc3", "susie", firstDate), Account("acc3", "susie", secDate), Account("acc3", "susie", Date.valueOf("2020-01-03"))
    ))
    val result = uniqueFieldUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData, step)

    val data = result.select("account_id").collect().map(_.getString(0))
    val expectedUniqueAccounts = Array("acc1", "acc2")
    assertResult(4)(data.length)
    data.foreach(a => assert(expectedUniqueAccounts.contains(a)))
    assertResult(2)(uniqueFieldUtil.uniqueFieldsDf.size)
    assertResult(4)(uniqueFieldUtil.uniqueFieldsDf.head._2.count())
    assertResult(4)(uniqueFieldUtil.uniqueFieldsDf.last._2.count())
    val currentUniqueAcc = uniqueFieldUtil.uniqueFieldsDf.filter(_._1.fields == List("account_id")).head._2.collect().map(_.getString(0))
    currentUniqueAcc.foreach(a => assert(expectedUniqueAccounts.contains(a)))
  }
}
