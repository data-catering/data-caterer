package io.github.datacatering.datacaterer.core.foreignkey.validator

import io.github.datacatering.datacaterer.api.model.ForeignKeyRelation
import io.github.datacatering.datacaterer.core.exception.MissingDataSourceFromForeignKeyException
import io.github.datacatering.datacaterer.core.model.ForeignKeyWithGenerateAndDelete
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ForeignKeyValidatorTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("ForeignKeyValidatorTest")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("isValidForeignKeyRelation: Valid FK relationship returns true") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1", "test"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe true
  }

  test("isValidForeignKeyRelation: Missing source data source throws exception") {
    val generatedDataMap = Map.empty[String, DataFrame]
    val enabledSources = List("accounts", "transactions")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"))),
      deleteLinks = List()
    )

    val exception = intercept[MissingDataSourceFromForeignKeyException] {
      ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)
    }

    exception.getMessage should include("accounts.accounts_table")
  }

  test("isValidForeignKeyRelation: Disabled main source returns false") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("transactions")  // accounts is NOT enabled

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe false
  }

  test("isValidForeignKeyRelation: Disabled sub source returns false") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts")  // transactions is NOT enabled

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe false
  }

  test("isValidForeignKeyRelation: Missing source field returns false") {
    val schema = StructType(List(
      StructField("name", StringType, nullable = false)  // "id" field is missing
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("test"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),  // References "id"
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe false
  }

  test("isValidForeignKeyRelation: Multiple generation links - all enabled") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions", "profiles")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),
      generationLinks = List(
        ForeignKeyRelation("transactions", "transactions_table", List("account_id")),
        ForeignKeyRelation("profiles", "profiles_table", List("account_id"))
      ),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe true
  }

  test("isValidForeignKeyRelation: Multiple generation links - one disabled") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions")  // profiles is NOT enabled

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id")),
      generationLinks = List(
        ForeignKeyRelation("transactions", "transactions_table", List("account_id")),
        ForeignKeyRelation("profiles", "profiles_table", List("account_id"))
      ),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe false
  }

  test("isValidForeignKeyRelation: Composite key - all fields exist") {
    val schema = StructType(List(
      StructField("id1", StringType, nullable = false),
      StructField("id2", StringType, nullable = false)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1", "A"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id1", "id2")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id1", "account_id2"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe true
  }

  test("isValidForeignKeyRelation: Composite key - missing one field") {
    val schema = StructType(List(
      StructField("id1", StringType, nullable = false)  // id2 is missing
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1"))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("id1", "id2")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id1", "account_id2"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe false
  }

  test("isValidForeignKeyRelation: Nested field - valid") {
    val schema = StructType(List(
      StructField("user", StructType(List(
        StructField("id", StringType, nullable = false)
      )), nullable = false)
    ))
    val sourceDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(org.apache.spark.sql.Row("1")))),
      schema
    )

    val generatedDataMap = Map("accounts.accounts_table" -> sourceDf)
    val enabledSources = List("accounts", "transactions")

    val fkr = ForeignKeyWithGenerateAndDelete(
      source = ForeignKeyRelation("accounts", "accounts_table", List("user.id")),
      generationLinks = List(ForeignKeyRelation("transactions", "transactions_table", List("account_id"))),
      deleteLinks = List()
    )

    val result = ForeignKeyValidator.isValidForeignKeyRelation(generatedDataMap, enabledSources, fkr)

    result shouldBe true
  }

  test("targetContainsAllFields: All fields exist") {
    val schema = StructType(List(
      StructField("account_id", StringType, nullable = false),
      StructField("amount", IntegerType, nullable = false)
    ))
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1", 100))),
      schema
    )

    val result = ForeignKeyValidator.targetContainsAllFields(List("account_id"), targetDf)

    result shouldBe true
  }

  test("targetContainsAllFields: Missing field") {
    val schema = StructType(List(
      StructField("amount", IntegerType, nullable = false)
    ))
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(100))),
      schema
    )

    val result = ForeignKeyValidator.targetContainsAllFields(List("account_id"), targetDf)

    result shouldBe false
  }

  test("targetContainsAllFields: Multiple fields - all exist") {
    val schema = StructType(List(
      StructField("account_id1", StringType, nullable = false),
      StructField("account_id2", StringType, nullable = false),
      StructField("amount", IntegerType, nullable = false)
    ))
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1", "A", 100))),
      schema
    )

    val result = ForeignKeyValidator.targetContainsAllFields(List("account_id1", "account_id2"), targetDf)

    result shouldBe true
  }

  test("targetContainsAllFields: Multiple fields - one missing") {
    val schema = StructType(List(
      StructField("account_id1", StringType, nullable = false),
      StructField("amount", IntegerType, nullable = false)
    ))
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1", 100))),
      schema
    )

    val result = ForeignKeyValidator.targetContainsAllFields(List("account_id1", "account_id2"), targetDf)

    result shouldBe false
  }

  test("targetContainsAllFields: Nested field - exists") {
    val schema = StructType(List(
      StructField("account", StructType(List(
        StructField("id", StringType, nullable = false)
      )), nullable = false),
      StructField("amount", IntegerType, nullable = false)
    ))
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row(org.apache.spark.sql.Row("1"), 100))),
      schema
    )

    val result = ForeignKeyValidator.targetContainsAllFields(List("account.id"), targetDf)

    result shouldBe true
  }

  test("targetContainsAllFields: Empty field list") {
    val schema = StructType(List(
      StructField("id", StringType, nullable = false)
    ))
    val targetDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(org.apache.spark.sql.Row("1"))),
      schema
    )

    val result = ForeignKeyValidator.targetContainsAllFields(List(), targetDf)

    result shouldBe true  // Empty list means all fields exist (vacuously true)
  }

  test("validateFieldMapping: Equal counts - valid") {
    ForeignKeyValidator.validateFieldMapping(
      List("id", "name"),
      List("account_id", "account_name")
    )
    // Should not throw exception
  }

  test("validateFieldMapping: Equal counts - single field") {
    ForeignKeyValidator.validateFieldMapping(
      List("id"),
      List("account_id")
    )
    // Should not throw exception
  }

  test("validateFieldMapping: Equal counts - empty lists") {
    ForeignKeyValidator.validateFieldMapping(
      List(),
      List()
    )
    // Should not throw exception
  }

  test("validateFieldMapping: Unequal counts - source > target") {
    val exception = intercept[IllegalArgumentException] {
      ForeignKeyValidator.validateFieldMapping(
        List("id", "name", "extra"),
        List("account_id", "account_name")
      )
    }

    exception.getMessage should include("Source and target field counts must match")
    exception.getMessage should include("source=3")
    exception.getMessage should include("target=2")
  }

  test("validateFieldMapping: Unequal counts - target > source") {
    val exception = intercept[IllegalArgumentException] {
      ForeignKeyValidator.validateFieldMapping(
        List("id"),
        List("account_id", "account_name")
      )
    }

    exception.getMessage should include("Source and target field counts must match")
    exception.getMessage should include("source=1")
    exception.getMessage should include("target=2")
  }

  test("validateFieldMapping: One empty list") {
    val exception = intercept[IllegalArgumentException] {
      ForeignKeyValidator.validateFieldMapping(
        List("id"),
        List()
      )
    }

    exception.getMessage should include("Source and target field counts must match")
  }
}
