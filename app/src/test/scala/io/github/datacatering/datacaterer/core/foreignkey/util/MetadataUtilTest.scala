package io.github.datacatering.datacaterer.core.foreignkey.util

import io.github.datacatering.datacaterer.api.model.Constants.OMIT
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MetadataUtilTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MetadataUtilTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("getMetadata: Top-level field with metadata") {
    val metadata = new MetadataBuilder()
      .putString("key1", "value1")
      .putLong("key2", 42L)
      .build()

    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false, metadata)
    ))

    val result = MetadataUtil.getMetadata("id", schema.fields)

    result.getString("key1") shouldBe "value1"
    result.getLong("key2") shouldBe 42L
  }

  test("getMetadata: Top-level field without metadata") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false)
    ))

    val result = MetadataUtil.getMetadata("id", schema.fields)

    result shouldBe Metadata.empty
  }

  test("getMetadata: Field does not exist returns empty metadata") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false)
    ))

    val result = MetadataUtil.getMetadata("missing", schema.fields)

    result shouldBe Metadata.empty
  }

  test("getMetadata: Nested field with metadata") {
    val nestedMetadata = new MetadataBuilder()
      .putString("nested_key", "nested_value")
      .build()

    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("id", IntegerType, nullable = false, nestedMetadata)
      )), nullable = false)
    ))

    val result = MetadataUtil.getMetadata("user.id", schema.fields)

    result.getString("nested_key") shouldBe "nested_value"
  }

  test("getMetadata: Deeply nested field with metadata") {
    val deepMetadata = new MetadataBuilder()
      .putString("deep_key", "deep_value")
      .build()

    val schema = StructType(List(
      StructField("level1", StructType(List(
        StructField("level2", StructType(List(
          StructField("level3", IntegerType, nullable = false, deepMetadata)
        )), nullable = false)
      )), nullable = false)
    ))

    val result = MetadataUtil.getMetadata("level1.level2.level3", schema.fields)

    result.getString("deep_key") shouldBe "deep_value"
  }

  test("getMetadata: Array of structs with metadata") {
    val elementMetadata = new MetadataBuilder()
      .putString("array_key", "array_value")
      .build()

    val schema = StructType(List(
      StructField("items", ArrayType(StructType(List(
        StructField("name", StringType, nullable = false, elementMetadata)
      ))), nullable = false)
    ))

    val result = MetadataUtil.getMetadata("items.name", schema.fields)

    result.getString("array_key") shouldBe "array_value"
  }

  test("getMetadata: Nested field that doesn't exist") {
    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("id", IntegerType, nullable = false)
      )), nullable = false)
    ))

    val result = MetadataUtil.getMetadata("user.missing", schema.fields)

    result shouldBe Metadata.empty
  }

  test("withMetadata: Apply metadata to existing column") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "test"))),
      schema
    )

    val newMetadata = new MetadataBuilder()
      .putString("custom_key", "custom_value")
      .build()

    val result = MetadataUtil.withMetadata(df, "name", newMetadata)

    val nameField = result.schema("name")
    nameField.metadata.getString("custom_key") shouldBe "custom_value"
  }

  test("withMetadata: Preserves data type and nullability") {
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "test"))),
      schema
    )

    val newMetadata = new MetadataBuilder().putString("key", "value").build()
    val result = MetadataUtil.withMetadata(df, "name", newMetadata)

    val nameField = result.schema("name")
    nameField.dataType shouldBe StringType
    nameField.nullable shouldBe true
  }

  test("withMetadata: Preserves other columns unchanged") {
    val idMetadata = new MetadataBuilder().putString("id_key", "id_value").build()
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false, idMetadata),
      StructField("name", StringType, nullable = false)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "test"))),
      schema
    )

    val nameMetadata = new MetadataBuilder().putString("name_key", "name_value").build()
    val result = MetadataUtil.withMetadata(df, "name", nameMetadata)

    // Check that id field metadata is preserved
    val idField = result.schema("id")
    idField.metadata.getString("id_key") shouldBe "id_value"

    // Check that name field has new metadata
    val nameField = result.schema("name")
    nameField.metadata.getString("name_key") shouldBe "name_value"
  }

  test("withMetadata: Overwrites existing metadata on target column") {
    val oldMetadata = new MetadataBuilder().putString("old_key", "old_value").build()
    val schema = StructType(List(
      StructField("name", StringType, nullable = false, oldMetadata)
    ))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("test"))),
      schema
    )

    val newMetadata = new MetadataBuilder().putString("new_key", "new_value").build()
    val result = MetadataUtil.withMetadata(df, "name", newMetadata)

    val nameField = result.schema("name")
    nameField.metadata.getString("new_key") shouldBe "new_value"
    nameField.metadata.contains("old_key") shouldBe false
  }

  test("combineMetadata: Simple case - single source and target column") {
    val sourceMetadata = new MetadataBuilder()
      .putString("source_key", "source_value")
      .build()
    val targetMetadata = new MetadataBuilder()
      .putString("target_key", "target_value")
      .build()

    val sourceSchema = StructType(List(
      StructField("id", IntegerType, nullable = false, sourceMetadata)
    ))
    val targetSchema = StructType(List(
      StructField("account_id", IntegerType, nullable = false, targetMetadata)
    ))

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      sourceSchema
    )
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )

    val resultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )

    val result = MetadataUtil.combineMetadata(
      sourceDf,
      List("id"),
      targetDf,
      List("account_id"),
      resultDf
    )

    val accountIdField = result.schema("account_id")
    // Should have both source and target metadata
    accountIdField.metadata.getString("source_key") shouldBe "source_value"
    accountIdField.metadata.getString("target_key") shouldBe "target_value"
  }

  test("combineMetadata: Removes OMIT marker from source metadata") {
    val sourceMetadata = new MetadataBuilder()
      .putString("source_key", "source_value")
      .putString(OMIT, "true")
      .build()
    val targetMetadata = new MetadataBuilder()
      .putString("target_key", "target_value")
      .build()

    val sourceSchema = StructType(List(
      StructField("id", IntegerType, nullable = false, sourceMetadata)
    ))
    val targetSchema = StructType(List(
      StructField("account_id", IntegerType, nullable = false, targetMetadata)
    ))

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      sourceSchema
    )
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )
    val resultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )

    val result = MetadataUtil.combineMetadata(
      sourceDf,
      List("id"),
      targetDf,
      List("account_id"),
      resultDf
    )

    val accountIdField = result.schema("account_id")
    accountIdField.metadata.getString("source_key") shouldBe "source_value"
    accountIdField.metadata.getString("target_key") shouldBe "target_value"
    accountIdField.metadata.contains(OMIT) shouldBe false
  }

  test("combineMetadata: Multiple columns") {
    val sourceMetadata1 = new MetadataBuilder().putString("s1", "v1").build()
    val sourceMetadata2 = new MetadataBuilder().putString("s2", "v2").build()
    val targetMetadata1 = new MetadataBuilder().putString("t1", "v1").build()
    val targetMetadata2 = new MetadataBuilder().putString("t2", "v2").build()

    val sourceSchema = StructType(List(
      StructField("id1", IntegerType, nullable = false, sourceMetadata1),
      StructField("id2", IntegerType, nullable = false, sourceMetadata2)
    ))
    val targetSchema = StructType(List(
      StructField("account_id1", IntegerType, nullable = false, targetMetadata1),
      StructField("account_id2", IntegerType, nullable = false, targetMetadata2)
    ))

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 2))),
      sourceSchema
    )
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 2))),
      targetSchema
    )
    val resultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, 2))),
      targetSchema
    )

    val result = MetadataUtil.combineMetadata(
      sourceDf,
      List("id1", "id2"),
      targetDf,
      List("account_id1", "account_id2"),
      resultDf
    )

    // Check first column
    val field1 = result.schema("account_id1")
    field1.metadata.getString("s1") shouldBe "v1"
    field1.metadata.getString("t1") shouldBe "v1"

    // Check second column
    val field2 = result.schema("account_id2")
    field2.metadata.getString("s2") shouldBe "v2"
    field2.metadata.getString("t2") shouldBe "v2"
  }

  test("combineMetadata: Source metadata takes precedence when keys conflict") {
    val sourceMetadata = new MetadataBuilder()
      .putString("shared_key", "source_value")
      .build()
    val targetMetadata = new MetadataBuilder()
      .putString("shared_key", "target_value")
      .build()

    val sourceSchema = StructType(List(
      StructField("id", IntegerType, nullable = false, sourceMetadata)
    ))
    val targetSchema = StructType(List(
      StructField("account_id", IntegerType, nullable = false, targetMetadata)
    ))

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      sourceSchema
    )
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )
    val resultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )

    val result = MetadataUtil.combineMetadata(
      sourceDf,
      List("id"),
      targetDf,
      List("account_id"),
      resultDf
    )

    val accountIdField = result.schema("account_id")
    // Source metadata wins in case of conflict (added last via withMetadata)
    accountIdField.metadata.getString("shared_key") shouldBe "source_value"
  }

  test("combineMetadata: Empty metadata lists") {
    val sourceSchema = StructType(List(
      StructField("id", IntegerType, nullable = false)
    ))
    val targetSchema = StructType(List(
      StructField("account_id", IntegerType, nullable = false)
    ))

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      sourceSchema
    )
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )
    val resultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )

    val result = MetadataUtil.combineMetadata(
      sourceDf,
      List("id"),
      targetDf,
      List("account_id"),
      resultDf
    )

    val accountIdField = result.schema("account_id")
    // Should have empty metadata
    accountIdField.metadata shouldBe Metadata.empty
  }

  test("combineMetadata: Nested field metadata") {
    val nestedMetadata = new MetadataBuilder()
      .putString("nested_key", "nested_value")
      .build()

    val sourceSchema = StructType(List(
      StructField("user", StructType(List(
        StructField("id", IntegerType, nullable = false, nestedMetadata)
      )), nullable = false)
    ))
    val targetSchema = StructType(List(
      StructField("account_id", IntegerType, nullable = false)
    ))

    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(Row(1)))),
      sourceSchema
    )
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )
    val resultDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1))),
      targetSchema
    )

    val result = MetadataUtil.combineMetadata(
      sourceDf,
      List("user.id"),
      targetDf,
      List("account_id"),
      resultDf
    )

    val accountIdField = result.schema("account_id")
    accountIdField.metadata.getString("nested_key") shouldBe "nested_value"
  }
}
