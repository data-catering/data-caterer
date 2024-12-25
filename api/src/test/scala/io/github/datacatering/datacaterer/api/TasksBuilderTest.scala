package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.{ArrayType, Count, DateType, Field, IntegerType, StringType}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TasksBuilderTest extends AnyFunSuite {

  test("Can create a task summary when given a task") {
    val result = TaskSummaryBuilder()
      .task(TaskBuilder().name("my task"))
      .enabled(false)
      .dataSource("account_json")
      .taskSummary

    assertResult("my task")(result.name)
    assertResult("account_json")(result.dataSourceName)
    assert(!result.enabled)
  }

  test("Can create a step with details") {
    val result = StepBuilder()
      .name("my step")
      .`type`("csv")
      .enabled(false)
      .fields()
      .count(CountBuilder())
      .option("dbtable" -> "account.history")
      .options(Map("stringtype" -> "undefined"))
      .step

    assertResult("my step")(result.name)
    assertResult("csv")(result.`type`)
    assert(!result.enabled)
    assert(result.fields.isEmpty)
    assertResult(Count())(result.count)
    assert(result.options == Map(
      "dbtable" -> "account.history",
      "stringtype" -> "undefined"
    ))
  }

  test("Can create simple count") {
    val result = CountBuilder().records(20).count

    assert(result.records.contains(20))
    assert(result.perField.isEmpty)
    assert(result.options.isEmpty)
  }

  test("Can create per field count") {
    val result = CountBuilder()
      .perField(PerFieldCountBuilder()
        .records(20, "account_id")
      )
      .count

    assert(result.records.contains(1000))
    assert(result.perField.isDefined)
    assert(result.perField.get.count.contains(20))
    assertResult(List("account_id"))(result.perField.get.fieldNames)
    assert(result.perField.get.options.isEmpty)
    assert(result.options.isEmpty)
  }

  test("Can create records per field from count builder") {
    val result = CountBuilder()
      .recordsPerField(20, "account_id")
      .count

    assert(result.records.contains(1000))
    assert(result.perField.isDefined)
    assert(result.perField.get.count.contains(20))
    assertResult(List("account_id"))(result.perField.get.fieldNames)
    assert(result.perField.get.options.isEmpty)
    assert(result.options.isEmpty)
  }

  test("Can create generated records per field from count builder") {
    val result = CountBuilder()
      .recordsPerFieldGenerator(GeneratorBuilder(), "account_id")
      .count

    assert(result.records.contains(1000))
    assert(result.perField.isDefined)
    assert(result.perField.get.count.contains(10))
    assertResult(List("account_id"))(result.perField.get.fieldNames)
    assert(result.perField.get.options.isEmpty)
    assert(result.options.isEmpty)
  }

  test("Can create generated records per field with total records from count builder") {
    val result = CountBuilder()
      .recordsPerFieldGenerator(100, GeneratorBuilder(), "account_id")
      .count

    assert(result.records.contains(100))
    assert(result.perField.isDefined)
    assert(result.perField.get.count.contains(10))
    assertResult(List("account_id"))(result.perField.get.fieldNames)
    assert(result.perField.get.options.isEmpty)
    assert(result.options.isEmpty)
  }

  test("Can create per field count with generator") {
    val result = CountBuilder()
      .perField(PerFieldCountBuilder()
        .generator(GeneratorBuilder().min(5), "account_id")
      ).count

    assert(result.records.contains(1000))
    assert(result.perField.isDefined)
    assert(result.perField.get.count.contains(10))
    assertResult(List("account_id"))(result.perField.get.fieldNames)
    assert(result.perField.get.options.nonEmpty)
    assertResult("5")(result.perField.get.options("min"))
    assert(result.options.isEmpty)
  }

  test("Can create field") {
    val result = FieldBuilder()
      .name("account_id")
      .`type`(StringType)
      .nullable(false)
      .options(Map("hello" -> "world"))
      .field

    assertResult("account_id")(result.name)
    assert(result.`type`.contains("string"))
    assert(!result.nullable)
    assertResult(Map("hello" -> "world"))(result.options)
  }

  test("Can create field generated from sql expression") {
    val result = FieldBuilder()
      .name("account_id")
      .sql("SUBSTRING(account, 1, 5)")
      .field

    assertResult("account_id")(result.name)
    assert(result.`type`.contains("string"))
    assert(result.options.nonEmpty)
    assertResult("SUBSTRING(account, 1, 5)")(result.options("sql"))
  }

  test("Can create field generated from one of list of doubles") {
    val result = FieldBuilder().name("account_id").oneOf(123.1, 789.2).field

    assertResult("account_id")(result.name)
    assert(result.`type`.contains("double"))
    assert(result.options.nonEmpty)
    assertResult(List(123.1, 789.2))(result.options("oneOf"))
  }

  test("Can create field generated from one of list of strings") {
    val result = FieldBuilder().name("status").oneOf("open", "closed").field

    assertResult("status")(result.name)
    assert(result.`type`.contains("string"))
    assertResult(List("open", "closed"))(result.options("oneOf"))
  }

  test("Can create field generated from one of list of long") {
    val result = FieldBuilder().name("amount").oneOf(100L, 200L).field

    assertResult("amount")(result.name)
    assert(result.`type`.contains("long"))
    assertResult(List(100L, 200L))(result.options("oneOf"))
  }

  test("Can create field generated from one of list of int") {
    val result = FieldBuilder().name("amount").oneOf(100, 200).field

    assertResult("amount")(result.name)
    assert(result.`type`.contains("integer"))
    assertResult(List(100, 200))(result.options("oneOf"))
  }

  test("Can create field generated from one of list of boolean") {
    val result = FieldBuilder().name("is_open").oneOf(true, false).field

    assertResult("is_open")(result.name)
    assert(result.`type`.contains("boolean"))
    assertResult(List(true, false))(result.options("oneOf"))
  }

  test("Can create field with nested schema") {
    val result = FieldBuilder()
      .name("txn_list")
      .`type`(new ArrayType(DateType))
      .fields(FieldBuilder().name("date").`type`(DateType))
      .field

    assertResult("txn_list")(result.name)
    assert(result.`type`.contains("array<date>"))
  }

  test("Can create field with metadata") {
    val result = FieldBuilder()
      .name("account_id")
      .regex("acc[0-9]{3}")
      .seed(1)
      .min(2)
      .max(10)
      .minLength(3)
      .maxLength(4)
      .avgLength(3)
      .arrayMinLength(2)
      .arrayMaxLength(2)
      .expression("hello")
      .nullable(false)
      .static("acc123")
      .arrayType("boolean")
      .numericPrecision(10)
      .numericScale(1)
      .enableEdgeCases(true)
      .edgeCaseProbability(0.5)
      .enableNull(true)
      .nullProbability(0.1)
      .unique(true)
      .omit(false)
      .primaryKey(true)
      .primaryKeyPosition(1)
      .clusteringPosition(1)
      .standardDeviation(0.1)
      .mean(5.1)
      .options(Map("customMetadata" -> "yes"))
      .option("data" -> "big")
      .field

    assertResult("account_id")(result.name)
    assert(result.`type`.contains("string"))
    assert(!result.nullable)
    val gen = result.options
    assertResult("acc[0-9]{3}")(gen("regex"))
    assertResult("1")(gen("seed"))
    assertResult("2")(gen("min"))
    assertResult("10")(gen("max"))
    assertResult("3")(gen("minLen"))
    assertResult("4")(gen("maxLen"))
    assertResult("3")(gen("avgLen"))
    assertResult("2")(gen("arrayMinLen"))
    assertResult("2")(gen("arrayMaxLen"))
    assertResult("hello")(gen("expression"))
    assertResult("acc123")(gen("static"))
    assertResult("boolean")(gen("arrayType"))
    assertResult("10")(gen("precision"))
    assertResult("1")(gen("scale"))
    assertResult("true")(gen("enableEdgeCase"))
    assertResult("0.5")(gen("edgeCaseProb"))
    assertResult("true")(gen("enableNull"))
    assertResult("0.1")(gen("nullProb"))
    assertResult("true")(gen("isUnique"))
    assertResult("false")(gen("omit"))
    assertResult("true")(gen("isPrimaryKey"))
    assertResult("1")(gen("primaryKeyPos"))
    assertResult("1")(gen("clusteringPos"))
    assertResult("yes")(gen("customMetadata"))
    assertResult("big")(gen("data"))
    assertResult("0.1")(gen("stddev"))
    assertResult("5.1")(gen("mean"))
  }

}
