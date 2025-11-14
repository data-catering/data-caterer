package io.github.datacatering.datacaterer.core.foreignkey.util

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class NestedFieldUtilTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("NestedFieldUtilTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("hasDfContainField: Top-level field exists") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("id", schema.fields)

    result shouldBe true
  }

  test("hasDfContainField: Top-level field does not exist") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("missing", schema.fields)

    result shouldBe false
  }

  test("hasDfContainField: Nested field exists") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("address", StructType(List(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false)
      )), nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("address.city", schema.fields)

    result shouldBe true
  }

  test("hasDfContainField: Nested field does not exist") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("address", StructType(List(
        StructField("city", StringType, nullable = false)
      )), nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("address.missing", schema.fields)

    result shouldBe false
  }

  test("hasDfContainField: Deeply nested field exists") {
    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("profile", StructType(List(
          StructField("name", StringType, nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("user.profile.name", schema.fields)

    result shouldBe true
  }

  test("hasDfContainField: Parent field missing") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("address.city", schema.fields)

    result shouldBe false
  }

  test("hasDfContainField: Array of structs") {
    val schema = StructType(List(
      StructField("items", ArrayType(StructType(List(
        StructField("name", StringType, nullable = false)
      ))), nullable = false)
    ))

    val result = NestedFieldUtil.hasDfContainField("items.name", schema.fields)

    result shouldBe true
  }

  test("updateNestedField: Top-level field update") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "test"))),
      schema
    )

    val updated = NestedFieldUtil.updateNestedField(df, "name", lit("updated"))
    val result = updated.collect()(0).getString(1)

    result shouldBe "updated"
  }

  test("updateNestedField: Simple nested field update (2 levels)") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("address", StructType(List(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false)
      )), nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, Row("NYC", "USA")))),
      schema
    )

    val updated = NestedFieldUtil.updateNestedField(df, "address.city", lit("SF"))
    val result = updated.collect()(0).getStruct(1).getString(0)

    result shouldBe "SF"
  }

  test("updateNestedField: Deep nested field update (3 levels)") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("user", StructType(List(
        StructField("profile", StructType(List(
          StructField("name", StringType, nullable = false),
          StructField("age", IntegerType, nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, Row(Row("John", 30))))),
      schema
    )

    val updated = NestedFieldUtil.updateNestedField(df, "user.profile.name", lit("Jane"))
    val result = updated.collect()(0).getStruct(1).getStruct(0).getString(0)

    result shouldBe "Jane"
  }

  test("updateNestedField: Very deep nesting (4 levels)") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("level1", StructType(List(
        StructField("level2", StructType(List(
          StructField("level3", StructType(List(
            StructField("value", StringType, nullable = false)
          )), nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, Row(Row(Row("original")))))),
      schema
    )

    val updated = NestedFieldUtil.updateNestedField(df, "level1.level2.level3.value", lit("updated"))
    val result = updated.collect()(0).getStruct(1).getStruct(0).getStruct(0).getString(0)

    result shouldBe "updated"
  }

  test("updateNestedField: Preserves other fields in struct") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("address", StructType(List(
        StructField("city", StringType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("zip", StringType, nullable = false)
      )), nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, Row("NYC", "USA", "10001")))),
      schema
    )

    val updated = NestedFieldUtil.updateNestedField(df, "address.city", lit("SF"))
    val resultRow = updated.collect()(0).getStruct(1)

    resultRow.getString(0) shouldBe "SF"  // city updated
    resultRow.getString(1) shouldBe "USA"  // country preserved
    resultRow.getString(2) shouldBe "10001"  // zip preserved
  }

  test("getNestedFieldType: Top-level field") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))

    val result = NestedFieldUtil.getNestedFieldType(schema, "id")

    result shouldBe IntegerType
  }

  test("getNestedFieldType: Nested field") {
    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = false)
      )), nullable = false)
    ))

    val result = NestedFieldUtil.getNestedFieldType(schema, "user.name")

    result shouldBe StringType
  }

  test("getNestedFieldType: Deeply nested field") {
    val schema = StructType(List(
      StructField("level1", StructType(List(
        StructField("level2", StructType(List(
          StructField("level3", IntegerType, nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))

    val result = NestedFieldUtil.getNestedFieldType(schema, "level1.level2.level3")

    result shouldBe IntegerType
  }

  test("getNestedFieldType: Struct type") {
    val innerSchema = StructType(List(
      StructField("city", StringType, nullable = false),
      StructField("country", StringType, nullable = false)
    ))
    val schema = StructType(List(
      StructField("address", innerSchema, nullable = false)
    ))

    val result = NestedFieldUtil.getNestedFieldType(schema, "address")

    result shouldBe innerSchema
  }

  test("getNestedFieldType: Array of structs") {
    val elementSchema = StructType(List(
      StructField("name", StringType, nullable = false)
    ))
    val schema = StructType(List(
      StructField("items", ArrayType(elementSchema), nullable = false)
    ))

    val result = NestedFieldUtil.getNestedFieldType(schema, "items.name")

    result shouldBe StringType
  }

  test("getNestedFieldType: Invalid field path throws exception") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false)
    ))

    val exception = intercept[IllegalArgumentException] {
      NestedFieldUtil.getNestedFieldType(schema, "missing")
    }

    exception.getMessage should include("missing")
    exception.getMessage should include("does not exist")
  }

  test("getNestedFieldType: Empty path throws exception") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false)
    ))

    val exception = intercept[IllegalArgumentException] {
      NestedFieldUtil.getNestedFieldType(schema, "")
    }

    // Empty string splits into Array(""), which Spark treats as a field lookup
    exception.getMessage should include("does not exist")
  }

  test("getNestedFieldType: Cannot traverse non-struct type") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false)
    ))

    val exception = intercept[IllegalArgumentException] {
      NestedFieldUtil.getNestedFieldType(schema, "id.invalid")
    }

    exception.getMessage should include("Cannot traverse non-struct type")
  }
}
