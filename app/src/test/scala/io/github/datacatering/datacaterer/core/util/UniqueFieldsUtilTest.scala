package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.Constants.IS_UNIQUE
import io.github.datacatering.datacaterer.api.model.{Count, Field, Generator, Plan, Schema, Step, Task, TaskSummary}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UniqueFieldsUtilTest extends SparkSuite {

  test("Can identify the unique columns and create a data frame with unique values for column") {
    val tasks = List((
      TaskSummary("gen data", "postgresAccount"),
      Task("account_postgres", List(
        Step("accounts", "postgres", Count(), Map(), Schema(fields = Some(List(
          Field("account_id", Some("string"), generator = Some(Generator("random", Map(IS_UNIQUE -> "true")))),
          Field("name", Some("string"), generator = Some(Generator("random", Map(IS_UNIQUE -> "true")))),
          Field("open_date", Some("date"), generator = Some(Generator())),
          Field("age", Some("int"), generator = Some(Generator())),
        ))))
      ))
    ))
    val uniqueColumnUtil = new UniqueFieldsUtil(Plan(), tasks)

    val uniqueColumns = uniqueColumnUtil.uniqueFieldsDf
    assert(uniqueColumns.size == 2)
    assert(uniqueColumnUtil.uniqueFieldsDf.size == 2)
    assert(uniqueColumnUtil.uniqueFieldsDf.head._2.isEmpty)
    val col = uniqueColumns.filter(_._1.columns == List("account_id")).head
    assert(col._1.dataSource == "postgresAccount")
    assert(col._1.step == "accounts")

    val generatedData = sparkSession.createDataFrame(Seq(
      Account("acc1", "peter"), Account("acc1", "john"), Account("acc2", "jack"), Account("acc3", "bob")
    ))
    val result = uniqueColumnUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData)

    val data = result.select("account_id").collect().map(_.getString(0))
    val expectedUniqueAccounts = Array("acc1", "acc2", "acc3")
    assert(data.length == 3)
    data.foreach(a => assert(expectedUniqueAccounts.contains(a)))
    assert(uniqueColumnUtil.uniqueFieldsDf.size == 2)
    assert(uniqueColumnUtil.uniqueFieldsDf.head._2.count() == 3)
    val currentUniqueAcc = uniqueColumnUtil.uniqueFieldsDf.filter(_._1.columns == List("account_id")).head._2.collect().map(_.getString(0))
    currentUniqueAcc.foreach(a => assert(expectedUniqueAccounts.contains(a)))

    val generatedData2 = sparkSession.createDataFrame(Seq(
      Account("acc1", "dog"), Account("acc3", "bob"), Account("acc4", "cat"), Account("acc5", "peter")
    ))
    val result2 = uniqueColumnUtil.getUniqueFieldsValues("postgresAccount.accounts", generatedData2)

    val data2 = result2.select("account_id", "name").collect()
    val expectedUniqueNames = Array("peter", "jack", "bob", "cat")
    val expectedUniqueAccounts2 = Array("acc1", "acc2", "acc3", "acc4")

    assert(data2.length == 1)
    assert(data2.head.getString(0) == "acc4")
    assert(data2.head.getString(1) == "cat")

    val currentUniqueAcc2 = uniqueColumnUtil.uniqueFieldsDf.filter(_._1.columns == List("account_id")).head._2.collect().map(_.getString(0))
    currentUniqueAcc2.foreach(a => assert(expectedUniqueAccounts2.contains(a)))
    val currentUniqueName = uniqueColumnUtil.uniqueFieldsDf.filter(_._1.columns == List("name")).head._2.collect().map(_.getString(0))
    currentUniqueName.foreach(a => assert(expectedUniqueNames.contains(a)))
  }
}
