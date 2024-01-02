package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.{ArrayType, Count, DateType, Field, Generator, IntegerType, StringType}
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

    assert(result.name == "my task")
    assert(result.dataSourceName == "account_json")
    assert(!result.enabled)
  }

  test("Can create a step with details") {
    val result = StepBuilder()
      .name("my step")
      .`type`("csv")
      .enabled(false)
      .schema(SchemaBuilder())
      .count(CountBuilder())
      .option("dbtable" -> "account.history")
      .options(Map("stringtype" -> "undefined"))
      .step

    assert(result.name == "my step")
    assert(result.`type` == "csv")
    assert(!result.enabled)
    assert(result.schema.fields.isEmpty)
    assert(result.count == Count())
    assert(result.options == Map(
      "dbtable" -> "account.history",
      "stringtype" -> "undefined"
    ))
  }

  test("Can create simple count") {
    val result = CountBuilder().records(20).count

    assert(result.records.contains(20))
    assert(result.perColumn.isEmpty)
    assert(result.generator.isEmpty)
  }

  test("Can create per column count") {
    val result = CountBuilder()
      .perColumn(PerColumnCountBuilder()
        .records(20, "account_id")
      )
      .count

    assert(result.records.contains(1000))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(20))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isEmpty)
    assert(result.generator.isEmpty)
  }

  test("Can create records per column from count builder") {
    val result = CountBuilder()
      .recordsPerColumn(20, "account_id")
      .count

    assert(result.records.contains(1000))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(20))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isEmpty)
    assert(result.generator.isEmpty)
  }

  test("Can create generated records per column from count builder") {
    val result = CountBuilder()
      .recordsPerColumnGenerator(GeneratorBuilder(), "account_id")
      .count

    assert(result.records.contains(1000))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(10))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isDefined)
    assert(result.generator.isEmpty)
  }

  test("Can create generated records per column with total records from count builder") {
    val result = CountBuilder()
      .recordsPerColumnGenerator(100, GeneratorBuilder(), "account_id")
      .count

    assert(result.records.contains(100))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(10))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isDefined)
    assert(result.generator.isEmpty)
  }

  test("Can create per column count with generator") {
    val result = CountBuilder()
      .perColumn(PerColumnCountBuilder()
        .generator(
          GeneratorBuilder().min(5),
          "account_id"
        )
      ).count

    assert(result.records.contains(1000))
    assert(result.perColumn.isDefined)
    assert(result.perColumn.get.count.contains(10))
    assert(result.perColumn.get.columnNames == List("account_id"))
    assert(result.perColumn.get.generator.isDefined)
    assert(result.perColumn.get.generator.get.`type` == "random")
    assert(result.perColumn.get.generator.get.options("min") == "5")
    assert(result.generator.isEmpty)
  }

  test("Can create schema with add fields") {
    val result = SchemaBuilder()
      .addField("account_id")
      .addField("year", IntegerType)
      .addFields(FieldBuilder().name("name"))
      .schema

    assert(result.fields.isDefined)
    assert(result.fields.get.size == 3)
    assert(result.fields.get.contains(Field("account_id", Some("string"))))
    assert(result.fields.get.contains(Field("year", Some("integer"))))
    assert(result.fields.get.contains(Field("name", Some("string"))))
  }

  test("Can create field") {
    val result = FieldBuilder()
      .name("account_id")
      .`type`(StringType)
      .nullable(false)
      .generator(GeneratorBuilder())
      .field

    assert(result.name == "account_id")
    assert(result.`type`.contains("string"))
    assert(!result.nullable)
    assert(result.generator.isDefined)
    assert(result.generator.contains(Generator()))
  }

  test("Can create field generated from sql expression") {
    val result = FieldBuilder()
      .name("account_id")
      .sql("SUBSTRING(account, 1, 5)")
      .field

    assert(result.name == "account_id")
    assert(result.`type`.contains("string"))
    assert(result.generator.isDefined)
    assert(result.generator.get.`type` == "sql")
    assert(result.generator.get.options("sql") == "SUBSTRING(account, 1, 5)")
  }

  test("Can create field generated from one of list of doubles") {
    val result = FieldBuilder().name("account_id").oneOf(123.1, 789.2).field

    assert(result.name == "account_id")
    assert(result.`type`.contains("double"))
    assert(result.generator.isDefined)
    assert(result.generator.get.options("oneOf") == List(123.1, 789.2))
  }

  test("Can create field generated from one of list of strings") {
    val result = FieldBuilder().name("status").oneOf("open", "closed").field

    assert(result.name == "status")
    assert(result.`type`.contains("string"))
    assert(result.generator.get.options("oneOf") == List("open", "closed"))
  }

  test("Can create field generated from one of list of long") {
    val result = FieldBuilder().name("amount").oneOf(100L, 200L).field

    assert(result.name == "amount")
    assert(result.`type`.contains("long"))
    assert(result.generator.get.options("oneOf") == List(100L, 200L))
  }

  test("Can create field generated from one of list of int") {
    val result = FieldBuilder().name("amount").oneOf(100, 200).field

    assert(result.name == "amount")
    assert(result.`type`.contains("integer"))
    assert(result.generator.get.options("oneOf") == List(100, 200))
  }

  test("Can create field generated from one of list of boolean") {
    val result = FieldBuilder().name("is_open").oneOf(true, false).field

    assert(result.name == "is_open")
    assert(result.`type`.contains("boolean"))
    assert(result.generator.get.options("oneOf") == List(true, false))
  }

  test("Can create field with nested schema") {
    val result = FieldBuilder()
      .name("txn_list")
      .`type`(new ArrayType(DateType))
      .schema(SchemaBuilder().addFields(
        FieldBuilder().name("date").`type`(DateType)
      ))
      .field

    assert(result.name == "txn_list")
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

    assert(result.name == "account_id")
    assert(result.`type`.contains("string"))
    assert(!result.nullable)
    assert(result.generator.get.`type` == "regex")
    val gen = result.generator.get.options
    assert(gen("regex") == "acc[0-9]{3}")
    assert(gen("seed") == "1")
    assert(gen("min") == "2")
    assert(gen("max") == "10")
    assert(gen("minLen") == "3")
    assert(gen("maxLen") == "4")
    assert(gen("avgLen") == "3")
    assert(gen("arrayMinLen") == "2")
    assert(gen("arrayMaxLen") == "2")
    assert(gen("expression") == "hello")
    assert(gen("static") == "acc123")
    assert(gen("arrayType") == "boolean")
    assert(gen("precision") == "10")
    assert(gen("scale") == "1")
    assert(gen("enableEdgeCase") == "true")
    assert(gen("edgeCaseProb") == "0.5")
    assert(gen("enableNull") == "true")
    assert(gen("nullProb") == "0.1")
    assert(gen("isUnique") == "true")
    assert(gen("omit") == "false")
    assert(gen("isPrimaryKey") == "true")
    assert(gen("primaryKeyPos") == "1")
    assert(gen("clusteringPos") == "1")
    assert(gen("customMetadata") == "yes")
    assert(gen("data") == "big")
    assert(gen("stddev") == "0.1")
    assert(gen("mean") == "5.1")
  }

}
