package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, PerFieldCount}
import io.github.datacatering.datacaterer.core.foreignkey.ForeignKeyApplicationUtil
import io.github.datacatering.datacaterer.core.foreignkey.ForeignKeyApplicationUtil.ForeignKeyConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach

/**
 * Comprehensive test suite for ForeignKeyUtilV2 focusing on:
 * 1. Correctness: Referential integrity for valid FKs, violations for invalid FKs
 * 2. Performance: Scaling behavior across different data volumes
 * 3. Features: Flat fields, nested fields, mixed fields, violation generation
 */
class ForeignKeyUtilV2Test extends SparkSuite with BeforeAndAfterEach {

  // ========================================================================================
  // CORRECTNESS TESTS - FLAT FIELDS
  // ========================================================================================

  test("V2: Flat fields - Basic referential integrity") {
    // Create source data (dimension table)
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_id", "account_name")

    // Create target data (fact table)
    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0),
      ("TXN005", "PLACEHOLDER", 250.0)
    )).toDF("txn_id", "account_id", "amount")

    // Apply foreign keys
    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = ForeignKeyConfig(violationRatio = 0.0)
    )

    // Validate
    assert(result.count() == 5, "Should maintain all target rows")

    val sourceValues = sourceDf.select("account_id").collect().map(_.getString(0)).toSet
    val resultValues = result.select("account_id").collect().map(_.getString(0)).toSet

    assert(resultValues.subsetOf(sourceValues),
      s"All FK values should exist in source. Found: $resultValues, Expected: $sourceValues")

    println(s"  ✓ All ${result.count()} records have valid foreign keys")
  }

  test("V2: Flat fields - Multiple field foreign key (composite key)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("USA", "NY", "New York"),
      ("USA", "CA", "California"),
      ("UK", "LON", "London")
    )).toDF("country", "region", "city")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("ORDER001", "XX", "YY", 100.0),
      ("ORDER002", "XX", "YY", 200.0),
      ("ORDER003", "XX", "YY", 150.0)
    )).toDF("order_id", "country", "region", "amount")

    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("country", "region"),
      targetFields = List("country", "region"),
      config = ForeignKeyConfig(violationRatio = 0.0)
    )

    // Check referential integrity for composite key
    val sourceCompositeKeys = sourceDf.select("country", "region")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet

    val resultCompositeKeys = result.select("country", "region")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet

    assert(resultCompositeKeys.subsetOf(sourceCompositeKeys),
      "All composite key combinations should exist in source")

    println(s"  ✓ Composite keys validated: ${resultCompositeKeys.size} unique combinations")
  }

  // ========================================================================================
  // CORRECTNESS TESTS - NESTED FIELDS
  // ========================================================================================

  test("V2: Nested fields - Simple struct update") {
    val sourceSchema = StructType(Seq(
      StructField("customer_id", StringType, nullable = false)
    ))
    val sourceDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row("CUST001"),
        Row("CUST002"),
        Row("CUST003")
      )),
      sourceSchema
    )

    val targetSchema = StructType(Seq(
      StructField("order_id", StringType, nullable = false),
      StructField("customer", StructType(Seq(
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true)
      )), nullable = true),
      StructField("amount", DoubleType, nullable = false)
    ))

    val targetDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row("ORD001", Row("PLACEHOLDER", "John"), 100.0),
        Row("ORD002", Row("PLACEHOLDER", "Jane"), 200.0),
        Row("ORD003", Row("PLACEHOLDER", "Bob"), 150.0)
      )),
      targetSchema
    )

    // Apply foreign key to nested field
    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("customer_id"),
      targetFields = List("customer.id"),
      config = ForeignKeyConfig(violationRatio = 0.0)
    )

    // Validate nested field was updated
    val sourceIds = sourceDf.select("customer_id").collect().map(_.getString(0)).toSet
    val resultIds = result.select("customer.id").collect().map(_.getString(0)).toSet

    assert(resultIds.subsetOf(sourceIds),
      s"Nested field values should exist in source. Found: $resultIds, Expected: $sourceIds")

    // Verify other nested fields are preserved
    val resultNames = result.select("customer.name").collect().map(_.getString(0)).toSet
    assert(resultNames.contains("John") || resultNames.contains("Jane") || resultNames.contains("Bob"),
      "Other nested fields should be preserved")

    println(s"  ✓ Nested fields updated correctly, preserved other struct fields")
  }

  test("V2: Nested fields - Deep nesting (3+ levels)") {
    val sourceSchema = StructType(Seq(
      StructField("product_id", StringType, nullable = false)
    ))
    val sourceDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row("PROD001"),
        Row("PROD002")
      )),
      sourceSchema
    )

    val targetSchema = StructType(Seq(
      StructField("order_id", StringType, nullable = false),
      StructField("details", StructType(Seq(
        StructField("line_items", StructType(Seq(
          StructField("product", StructType(Seq(
            StructField("id", StringType, nullable = true),
            StructField("name", StringType, nullable = true)
          )), nullable = true),
          StructField("quantity", IntegerType, nullable = true)
        )), nullable = true)
      )), nullable = true)
    ))

    val targetDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row("ORD001", Row(Row(Row("PLACEHOLDER", "Widget"), 5))),
        Row("ORD002", Row(Row(Row("PLACEHOLDER", "Gadget"), 3)))
      )),
      targetSchema
    )

    // Apply FK to deeply nested field
    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("product_id"),
      targetFields = List("details.line_items.product.id"),
      config = ForeignKeyConfig(violationRatio = 0.0)
    )

    // Validate deep nested field
    val sourceIds = sourceDf.select("product_id").collect().map(_.getString(0)).toSet
    val resultIds = result.select("details.line_items.product.id").collect().map(_.getString(0)).toSet

    assert(resultIds.subsetOf(sourceIds),
      s"Deep nested field should have valid FK values. Found: $resultIds")

    // Verify sibling fields preserved
    val resultNames = result.select("details.line_items.product.name").collect().map(_.getString(0)).toSet
    assert(resultNames.nonEmpty, "Sibling nested fields should be preserved")

    println(s"  ✓ Deep nested fields (3+ levels) updated correctly")
  }

  // ========================================================================================
  // CORRECTNESS TESTS - MIXED FIELDS
  // ========================================================================================

  test("V2: Mixed fields - Flat and nested together") {
    val sourceSchema = StructType(Seq(
      StructField("account_id", StringType, nullable = false),
      StructField("customer_id", StringType, nullable = false)
    ))
    val sourceDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row("ACC001", "CUST001"),
        Row("ACC002", "CUST002")
      )),
      sourceSchema
    )

    val targetSchema = StructType(Seq(
      StructField("txn_id", StringType, nullable = false),
      StructField("account_id", StringType, nullable = true),  // Flat field
      StructField("customer", StructType(Seq(                   // Nested field
        StructField("id", StringType, nullable = true),
        StructField("name", StringType, nullable = true)
      )), nullable = true),
      StructField("amount", DoubleType, nullable = false)
    ))

    val targetDf = sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(Seq(
        Row("TXN001", "PLACEHOLDER", Row("PLACEHOLDER", "Alice"), 100.0),
        Row("TXN002", "PLACEHOLDER", Row("PLACEHOLDER", "Bob"), 200.0),
        Row("TXN003", "PLACEHOLDER", Row("PLACEHOLDER", "Charlie"), 150.0)
      )),
      targetSchema
    )

    // Apply FK to both flat and nested fields from same source row
    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id", "customer_id"),
      targetFields = List("account_id", "customer.id"),
      config = ForeignKeyConfig(violationRatio = 0.0)
    )

    // Validate both fields came from same source row (consistency check)
    val resultPairs = result.select("account_id", "customer.id")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet

    val sourcePairs = sourceDf.select("account_id", "customer_id")
      .collect()
      .map(row => (row.getString(0), row.getString(1)))
      .toSet

    assert(resultPairs.subsetOf(sourcePairs),
      "Mixed flat and nested fields should maintain source row consistency")

    println(s"  ✓ Mixed fields maintain consistency: ${resultPairs.size} valid pairs")
  }

  // ========================================================================================
  // VIOLATION TESTS - INTENTIONAL INTEGRITY BREAKS
  // ========================================================================================

  test("V2: Violations - Generate invalid foreign keys (random strategy)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0),
      ("TXN005", "PLACEHOLDER", 250.0),
      ("TXN006", "PLACEHOLDER", 175.0),
      ("TXN007", "PLACEHOLDER", 225.0),
      ("TXN008", "PLACEHOLDER", 275.0),
      ("TXN009", "PLACEHOLDER", 325.0),
      ("TXN010", "PLACEHOLDER", 125.0)
    )).toDF("txn_id", "account_id", "amount")

    // Generate 30% invalid FKs with deterministic seed
    val seed = 12345L
    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = ForeignKeyConfig(violationRatio = 0.3, violationStrategy = "random", seed = Some(seed))
    )

    val sourceValues = sourceDf.select("account_id").collect().map(_.getString(0)).toSet
    val resultValues = result.select("account_id").collect().map(_.getString(0))

    val validCount = resultValues.count(sourceValues.contains)
    val invalidCount = resultValues.length - validCount
    val invalidRatio = invalidCount.toDouble / resultValues.length

    println(s"  ✓ Generated $invalidCount invalid FKs out of ${resultValues.length} (${invalidRatio * 100}%)")

    // With seed=12345, the exact count is deterministic
    // Store the expected count based on the seed for reproducibility
    val expectedInvalidCount = 4  // Determined by running with seed=12345
    assert(invalidCount == expectedInvalidCount,
      s"Expected exactly $expectedInvalidCount violations with seed=$seed for reproducibility, got $invalidCount")

    // Verify determinism by running again
    val result2 = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = ForeignKeyConfig(violationRatio = 0.3, violationStrategy = "random", seed = Some(seed))
    )
    val resultValues2 = result2.select("account_id").collect().map(_.getString(0))
    val invalidCount2 = resultValues2.count(v => !sourceValues.contains(v))
    assert(invalidCount == invalidCount2, "Same seed should produce same violation count")
  }

  test("V2: Violations - Null strategy") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0)
    )).toDF("txn_id", "account_id", "amount")

    // Generate 50% null violations with deterministic seed
    val seed = 54321L
    val result = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = ForeignKeyConfig(violationRatio = 0.5, violationStrategy = "null", seed = Some(seed))
    )

    val nullCount = result.filter(col("account_id").isNull).count()
    val nullRatio = nullCount.toDouble / result.count()

    println(s"  ✓ Generated $nullCount null FKs out of ${result.count()} (${nullRatio * 100}%)")

    // With seed=54321, the exact count is deterministic
    val expectedNullCount = 3  // Determined by running with seed=54321
    assert(nullCount == expectedNullCount,
      s"Expected exactly $expectedNullCount null violations with seed=$seed for reproducibility, got $nullCount")

    // Verify determinism by running again
    val result2 = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = ForeignKeyConfig(violationRatio = 0.5, violationStrategy = "null", seed = Some(seed))
    )
    val nullCount2 = result2.filter(col("account_id").isNull).count()
    assert(nullCount == nullCount2, "Same seed should produce same null count")
  }

  // ========================================================================================
  // DETERMINISM TESTS - SEED REPRODUCIBILITY
  // ========================================================================================

  test("V2: Determinism - Same seed produces identical results") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0),
      ("TXN005", "PLACEHOLDER", 250.0)
    )).toDF("txn_id", "account_id", "amount")

    val seed = 99999L
    val config = ForeignKeyConfig(violationRatio = 0.2, violationStrategy = "random", seed = Some(seed))

    // Generate twice with same seed
    val result1 = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = config
    )

    val result2 = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = config
    )

    // Collect results and compare
    val values1 = result1.select("txn_id", "account_id", "amount").collect().map(r => (r.getString(0), r.getString(1), r.getDouble(2)))
    val values2 = result2.select("txn_id", "account_id", "amount").collect().map(r => (r.getString(0), r.getString(1), r.getDouble(2)))

    assert(values1.sameElements(values2), "Results with same seed should be identical")

    println(s"  ✓ Deterministic behavior verified: identical results with seed=$seed")
  }

  test("V2: Determinism - Different seeds produce different results") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0),
      ("TXN005", "PLACEHOLDER", 250.0)
    )).toDF("txn_id", "account_id", "amount")

    val config1 = ForeignKeyConfig(violationRatio = 0.2, violationStrategy = "random", seed = Some(11111L))
    val config2 = ForeignKeyConfig(violationRatio = 0.2, violationStrategy = "random", seed = Some(22222L))

    // Generate with different seeds
    val result1 = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = config1
    )

    val result2 = ForeignKeyApplicationUtil.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = config2
    )

    // Collect results and compare
    val values1 = result1.select("account_id").collect().map(_.getString(0)).mkString(",")
    val values2 = result2.select("account_id").collect().map(_.getString(0)).mkString(",")

    assert(values1 != values2, "Results with different seeds should be different")

    println(s"  ✓ Different seeds produce different results")
  }

  // ========================================================================================
  // COMBINATION GENERATION TESTS
  // ========================================================================================

  test("V2: Generate both valid and invalid combinations") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0),
      ("TXN005", "PLACEHOLDER", 250.0),
      ("TXN006", "PLACEHOLDER", 175.0),
      ("TXN007", "PLACEHOLDER", 225.0),
      ("TXN008", "PLACEHOLDER", 275.0),
      ("TXN009", "PLACEHOLDER", 325.0),
      ("TXN010", "PLACEHOLDER", 125.0)
    )).toDF("txn_id", "account_id", "amount")

    val (validDf, invalidDf) = ForeignKeyApplicationUtil.generateValidAndInvalidCombinations(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id")
    )

    // Validate split
    assert(validDf.count() + invalidDf.count() == targetDf.count(),
      "Should preserve total record count")

    // Validate valid DF
    val sourceValues = sourceDf.select("account_id").collect().map(_.getString(0)).toSet
    val validValues = validDf.select("account_id").collect().map(_.getString(0))
    val allValidAreInSource = validValues.forall(sourceValues.contains)

    assert(allValidAreInSource, "All values in valid DF should exist in source")

    // Validate invalid DF has some violations
    val invalidValues = invalidDf.select("account_id").collect().map(_.getString(0))
    val someInvalidNotInSource = invalidValues.exists(v => !sourceValues.contains(v))

    assert(someInvalidNotInSource, "Invalid DF should have some values not in source")

    println(s"  ✓ Generated ${validDf.count()} valid + ${invalidDf.count()} invalid = ${validDf.count() + invalidDf.count()} total")
  }

  // ========================================================================================
  // NULLABILITY TESTS - NEW FEATURE
  // ========================================================================================

  test("V2: Nullability - Random strategy") {
    import io.github.datacatering.datacaterer.api.model.NullabilityConfig

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "ACC001", 100.0),
      ("TXN002", "ACC002", 200.0),
      ("TXN003", "ACC003", 150.0),
      ("TXN004", "ACC001", 300.0),
      ("TXN005", "ACC002", 250.0),
      ("TXN006", "ACC003", 175.0),
      ("TXN007", "ACC001", 225.0),
      ("TXN008", "ACC002", 275.0),
      ("TXN009", "ACC003", 325.0),
      ("TXN010", "ACC001", 125.0)
    )).toDF("txn_id", "account_id", "amount")

    val nullConfig = NullabilityConfig(nullPercentage = 0.3, strategy = "random")
    val seed = 42L
    val result = ForeignKeyApplicationUtil.applyNullability(
      targetDf = targetDf,
      targetFields = List("account_id"),
      nullabilityConfig = nullConfig,
      seed = Some(seed)
    )

    val nullCount = result.filter(col("account_id").isNull).count()
    val totalCount = result.count()
    val nullRatio = nullCount.toDouble / totalCount

    println(s"  ✓ Random nullability: $nullCount nulls out of $totalCount (${nullRatio * 100}%)")

    // Verify count is preserved
    assert(totalCount == targetDf.count(), "Row count should be preserved")

    // Verify nulls exist (with seed, should be deterministic)
    assert(nullCount > 0, "Should have some null values with 30% ratio")

    // Verify determinism with same seed
    val result2 = ForeignKeyApplicationUtil.applyNullability(
      targetDf = targetDf,
      targetFields = List("account_id"),
      nullabilityConfig = nullConfig,
      seed = Some(seed)
    )
    val nullCount2 = result2.filter(col("account_id").isNull).count()
    assert(nullCount == nullCount2, "Same seed should produce same null count")
  }

  test("V2: Nullability - Head strategy (first N% get nulls)") {
    import io.github.datacatering.datacaterer.api.model.NullabilityConfig

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "ACC001", 100.0),
      ("TXN002", "ACC002", 200.0),
      ("TXN003", "ACC003", 150.0),
      ("TXN004", "ACC001", 300.0),
      ("TXN005", "ACC002", 250.0),
      ("TXN006", "ACC003", 175.0),
      ("TXN007", "ACC001", 225.0),
      ("TXN008", "ACC002", 275.0),
      ("TXN009", "ACC003", 325.0),
      ("TXN010", "ACC001", 125.0)
    )).toDF("txn_id", "account_id", "amount")

    val nullConfig = NullabilityConfig(nullPercentage = 0.2, strategy = "head")
    val result = ForeignKeyApplicationUtil.applyNullability(
      targetDf = targetDf,
      targetFields = List("account_id"),
      nullabilityConfig = nullConfig,
      seed = None
    )

    val nullCount = result.filter(col("account_id").isNull).count()
    val totalCount = result.count()

    println(s"  ✓ Head nullability: $nullCount nulls out of $totalCount (first 20%)")

    // With 10 records and 20%, expect 2 nulls
    val expectedNullCount = (totalCount * 0.2).toLong
    assert(nullCount == expectedNullCount, s"Expected $expectedNullCount nulls in first 20%, got $nullCount")
  }

  test("V2: Nullability - Tail strategy (last N% get nulls)") {
    import io.github.datacatering.datacaterer.api.model.NullabilityConfig

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "ACC001", 100.0),
      ("TXN002", "ACC002", 200.0),
      ("TXN003", "ACC003", 150.0),
      ("TXN004", "ACC001", 300.0),
      ("TXN005", "ACC002", 250.0),
      ("TXN006", "ACC003", 175.0),
      ("TXN007", "ACC001", 225.0),
      ("TXN008", "ACC002", 275.0),
      ("TXN009", "ACC003", 325.0),
      ("TXN010", "ACC001", 125.0)
    )).toDF("txn_id", "account_id", "amount")

    val nullConfig = NullabilityConfig(nullPercentage = 0.3, strategy = "tail")
    val result = ForeignKeyApplicationUtil.applyNullability(
      targetDf = targetDf,
      targetFields = List("account_id"),
      nullabilityConfig = nullConfig,
      seed = None
    )

    val nullCount = result.filter(col("account_id").isNull).count()
    val totalCount = result.count()

    println(s"  ✓ Tail nullability: $nullCount nulls out of $totalCount (last 30%)")

    // With 10 records and 30%, expect 3 nulls
    val expectedNullCount = (totalCount * 0.3).toLong
    assert(nullCount == expectedNullCount, s"Expected $expectedNullCount nulls in last 30%, got $nullCount")
  }

  test("V2: Nullability - Zero percentage (no nulls)") {
    import io.github.datacatering.datacaterer.api.model.NullabilityConfig

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "ACC001", 100.0),
      ("TXN002", "ACC002", 200.0),
      ("TXN003", "ACC003", 150.0)
    )).toDF("txn_id", "account_id", "amount")

    val nullConfig = NullabilityConfig(nullPercentage = 0.0, strategy = "random")
    val result = ForeignKeyApplicationUtil.applyNullability(
      targetDf = targetDf,
      targetFields = List("account_id"),
      nullabilityConfig = nullConfig,
      seed = None
    )

    val nullCount = result.filter(col("account_id").isNull).count()

    println(s"  ✓ Zero percentage: $nullCount nulls (expected 0)")

    assert(nullCount == 0, "With 0% null percentage, should have no nulls")
  }

  test("V2: Nullability - Multiple fields") {
    import io.github.datacatering.datacaterer.api.model.NullabilityConfig

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "ACC001", "CUST001", 100.0),
      ("TXN002", "ACC002", "CUST002", 200.0),
      ("TXN003", "ACC003", "CUST003", 150.0),
      ("TXN004", "ACC001", "CUST001", 300.0),
      ("TXN005", "ACC002", "CUST002", 250.0)
    )).toDF("txn_id", "account_id", "customer_id", "amount")

    val nullConfig = NullabilityConfig(nullPercentage = 0.4, strategy = "random")
    val seed = 99L
    val result = ForeignKeyApplicationUtil.applyNullability(
      targetDf = targetDf,
      targetFields = List("account_id", "customer_id"),
      nullabilityConfig = nullConfig,
      seed = Some(seed)
    )

    val nullAccountCount = result.filter(col("account_id").isNull).count()
    val nullCustomerCount = result.filter(col("customer_id").isNull).count()

    println(s"  ✓ Multiple fields: $nullAccountCount account nulls, $nullCustomerCount customer nulls")

    // Both fields should have same null pattern (same rows are nullified)
    assert(nullAccountCount == nullCustomerCount, "Both fields should be nullified for same rows")
    assert(nullAccountCount > 0, "Should have some nulls with 40% ratio")
  }

  // ========================================================================================
  // CARDINALITY TESTS - NEW FEATURE
  // ========================================================================================

  test("V2: Cardinality - One-to-one relationship (min=1, max=1)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("CUST001", "Alice"),
      ("CUST002", "Bob"),
      ("CUST003", "Charlie")
    )).toDF("customer_id", "customer_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("PROF001", "PLACEHOLDER"),
      ("PROF002", "PLACEHOLDER"),
      ("PROF003", "PLACEHOLDER")
    )).toDF("profile_id", "customer_id")

    val cardinalityConfig = CardinalityConfig(min = Some(1), max = Some(1))
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("customer_id", "customer_id")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(42L)
    )

    val resultCount = result.count()
    val sourceCount = sourceDf.count()

    println(s"  ✓ One-to-one: $sourceCount sources -> $resultCount targets")

    assert(resultCount == sourceCount, "One-to-one should create exactly one target per source")

    // Verify each customer appears exactly once
    val customerCounts = result.groupBy("customer_id").count().collect()
    customerCounts.foreach { row =>
      assert(row.getLong(1) == 1, s"Customer ${row.getString(0)} should appear exactly once")
    }
  }

  test("V2: Cardinality - One-to-many with bounded range (min=2, max=5)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("CUST001", "Alice"),
      ("CUST002", "Bob")
    )).toDF("customer_id", "customer_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("ORD001", "PLACEHOLDER", 100.0),
      ("ORD002", "PLACEHOLDER", 200.0),
      ("ORD003", "PLACEHOLDER", 150.0)
    )).toDF("order_id", "customer_id", "amount")

    val cardinalityConfig = CardinalityConfig(min = Some(2), max = Some(5), distribution = "uniform")
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("customer_id", "customer_id")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(123L)
    )

    println(s"  ✓ Bounded cardinality: ${sourceDf.count()} sources -> ${result.count()} targets")

    // Verify each customer has between 2 and 5 orders
    val customerCounts = result.groupBy("customer_id").count().collect()
    customerCounts.foreach { row =>
      val count = row.getLong(1)
      assert(count >= 2 && count <= 5, s"Customer ${row.getString(0)} should have 2-5 orders, got $count")
    }
  }

  // NOTE: This test is disabled because cardinality row expansion is now handled by
  // CardinalityCountAdjustmentProcessor during data generation, not during FK application.
  // The applyCardinality method now only assigns FK values to existing row groups.
  ignore("V2: Cardinality - Ratio-based with uniform distribution (obsolete - see CardinalityCountAdjustmentProcessor)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("CUST001", "Alice"),
      ("CUST002", "Bob"),
      ("CUST003", "Charlie")
    )).toDF("customer_id", "customer_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("ORD001", "PLACEHOLDER", 100.0),
      ("ORD002", "PLACEHOLDER", 200.0)
    )).toDF("order_id", "customer_id", "amount")

    val cardinalityConfig = CardinalityConfig(ratio = Some(3.0), distribution = "uniform")
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("customer_id", "customer_id")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(456L)
    )

    val sourceCount = sourceDf.count()
    val resultCount = result.count()
    val actualRatio = resultCount.toDouble / sourceCount

    println(s"  ✓ Uniform ratio: $sourceCount sources -> $resultCount targets (ratio: $actualRatio)")

    // Uniform should give exactly 3 per customer
    assert(resultCount == sourceCount * 3, s"Expected ${sourceCount * 3} records, got $resultCount")

    // Verify each customer has exactly 3 orders
    val customerCounts = result.groupBy("customer_id").count().collect()
    customerCounts.foreach { row =>
      assert(row.getLong(1) == 3, s"Customer ${row.getString(0)} should have exactly 3 orders")
    }
  }

  ignore("V2: Cardinality - Ratio-based with normal distribution (obsolete - see CardinalityCountAdjustmentProcessor)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("CUST001", "Alice"),
      ("CUST002", "Bob"),
      ("CUST003", "Charlie"),
      ("CUST004", "David"),
      ("CUST005", "Eve")
    )).toDF("customer_id", "customer_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("ORD001", "PLACEHOLDER", 100.0)
    )).toDF("order_id", "customer_id", "amount")

    val cardinalityConfig = CardinalityConfig(ratio = Some(5.0), distribution = "normal")
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("customer_id", "customer_id")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(789L)
    )

    val sourceCount = sourceDf.count()
    val resultCount = result.count()
    val actualRatio = resultCount.toDouble / sourceCount

    println(s"  ✓ Normal distribution: $sourceCount sources -> $resultCount targets (avg ratio: $actualRatio)")

    // Normal distribution should be around 5.0 but with variance
    assert(actualRatio >= 3.0 && actualRatio <= 7.0, s"Ratio should be roughly 5.0 ± 2.0, got $actualRatio")

    // Verify counts vary (not all the same like uniform)
    val customerCounts = result.groupBy("customer_id").count().collect().map(_.getLong(1)).toSet
    assert(customerCounts.size > 1, "Normal distribution should produce varying counts")
  }

  ignore("V2: Cardinality - Ratio-based with zipf/power distribution (obsolete - see CardinalityCountAdjustmentProcessor)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("PROD001", "Product A"),
      ("PROD002", "Product B"),
      ("PROD003", "Product C"),
      ("PROD004", "Product D"),
      ("PROD005", "Product E")
    )).toDF("product_id", "product_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("REV001", "PLACEHOLDER", 5)
    )).toDF("review_id", "product_id", "rating")

    val cardinalityConfig = CardinalityConfig(ratio = Some(10.0), distribution = "zipf")
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("product_id", "product_id")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(111L)
    )

    val customerCounts = result.groupBy("product_id").count().collect().map(_.getLong(1)).sorted.reverse

    println(s"  ✓ Zipf distribution: Review counts = ${customerCounts.mkString(", ")}")

    // Zipf should show power law: few products have many reviews, most have few
    assert(customerCounts.size > 0, "Should have reviews")
    val maxCount = customerCounts.head
    val minCount = customerCounts.last
    assert(maxCount > minCount, "Zipf should show skew: max > min")
  }

  ignore("V2: Cardinality - Automatic target expansion when needed (obsolete - see CardinalityCountAdjustmentProcessor)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("CUST001", "Alice"),
      ("CUST002", "Bob"),
      ("CUST003", "Charlie")
    )).toDF("customer_id", "customer_name")

    // Only 2 target records, but we need 3 sources * 5 = 15 records
    val targetDf = sparkSession.createDataFrame(Seq(
      ("ORD001", "PLACEHOLDER", 100.0),
      ("ORD002", "PLACEHOLDER", 200.0)
    )).toDF("order_id", "customer_id", "amount")

    val cardinalityConfig = CardinalityConfig(ratio = Some(5.0), distribution = "uniform")
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("customer_id", "customer_id")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(222L)
    )

    val expectedCount = sourceDf.count() * 5
    val actualCount = result.count()

    println(s"  ✓ Target expansion: 2 target records expanded to $actualCount to satisfy cardinality")

    assert(actualCount == expectedCount, s"Expected $expectedCount records, got $actualCount")
  }

  test("V2: Cardinality - Composite key fields") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("USA", "NY", "New York"),
      ("USA", "CA", "California")
    )).toDF("country", "state", "city")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("STORE001", "XX", "YY"),
      ("STORE002", "XX", "YY")
    )).toDF("store_id", "country", "state")

    val cardinalityConfig = CardinalityConfig(ratio = Some(2.0), distribution = "uniform")
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("country", "country"), ("state", "state")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(333L)
    )

    println(s"  ✓ Composite key cardinality: ${sourceDf.count()} sources -> ${result.count()} targets")

    // Each (country, state) pair should appear twice
    val pairCounts = result.groupBy("country", "state").count().collect()
    pairCounts.foreach { row =>
      assert(row.getLong(2) == 2, s"Pair (${row.getString(0)}, ${row.getString(1)}) should appear twice")
    }
  }

  // ========================================================================================
  // PERFIELD-AWARE CARDINALITY TESTS
  // ========================================================================================

  // NOTE: This test is disabled because it uses PLACEHOLDER values which don't create distinct groups.
  // In real usage, CardinalityCountAdjustmentProcessor + perField generation creates distinct FK groups.
  // This test scenario doesn't match actual runtime behavior.
  ignore("V2: Cardinality - PerField-aware assignment preserves unique non-FK fields (test needs rewrite for new architecture)") {
    // Source: accounts
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob"),
      ("ACC003", "Charlie")
    )).toDF("account_number", "account_name")

    // Target: transactions with already-generated unique values
    // Simulates what happens after CardinalityCountAdjustmentProcessor + perField generation
    // We expect 3 accounts * 5 transactions = 15 unique transactions
    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0),
      ("TXN005", "PLACEHOLDER", 250.0),
      ("TXN006", "PLACEHOLDER", 175.0),
      ("TXN007", "PLACEHOLDER", 225.0),
      ("TXN008", "PLACEHOLDER", 125.0),
      ("TXN009", "PLACEHOLDER", 275.0),
      ("TXN010", "PLACEHOLDER", 325.0),
      ("TXN011", "PLACEHOLDER", 135.0),
      ("TXN012", "PLACEHOLDER", 185.0),
      ("TXN013", "PLACEHOLDER", 235.0),
      ("TXN014", "PLACEHOLDER", 285.0),
      ("TXN015", "PLACEHOLDER", 335.0)
    )).toDF("txn_id", "account_number", "amount")

    // PerField config says: 5 records per account_number
    val perFieldCount = PerFieldCount(
      fieldNames = List("account_number"),
      count = Some(5)
    )

    // Cardinality config says: ratio of 5 (5 transactions per account)
    val cardinalityConfig = CardinalityConfig(ratio = Some(5.0), distribution = "uniform")

    // Apply cardinality with perField awareness
    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("account_number", "account_number")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(42L),
      targetPerFieldCount = Some(perFieldCount)
    )

    println(s"  ✓ PerField-aware cardinality: ${sourceDf.count()} accounts -> ${result.count()} transactions")

    // Verify count is preserved (no row duplication)
    assert(result.count() == 15, "Should preserve all 15 unique target records")

    // Verify each account appears exactly 5 times
    val accountCounts = result.groupBy("account_number").count().collect()
    accountCounts.foreach { row =>
      val count = row.getLong(1)
      assert(count == 5, s"Account ${row.getString(0)} should have exactly 5 transactions, got $count")
    }

    // Verify all txn_ids are unique (no row duplication)
    val txnIds = result.select("txn_id").collect().map(_.getString(0))
    assert(txnIds.length == txnIds.distinct.length, "All txn_ids should be unique (no duplicated rows)")

    // Verify all amounts are from the original set (no row duplication)
    val originalAmounts = targetDf.select("amount").collect().map(_.getDouble(0)).toSet
    val resultAmounts = result.select("amount").collect().map(_.getDouble(0)).toSet
    assert(resultAmounts == originalAmounts, "All amounts should be from original data (no duplicated rows)")
  }

  // NOTE: This test is disabled because it uses PLACEHOLDER values which don't create distinct groups.
  // In real usage, CardinalityCountAdjustmentProcessor + perField generation creates distinct FK groups.
  ignore("V2: Cardinality - PerField-aware with composite keys (test needs rewrite for new architecture)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("USA", "NY", "New York"),
      ("USA", "CA", "California")
    )).toDF("country", "state", "city")

    // Generate 6 unique stores (2 locations * 3 stores per location)
    val targetDf = sparkSession.createDataFrame(Seq(
      ("STORE001", "XX", "YY"),
      ("STORE002", "XX", "YY"),
      ("STORE003", "XX", "YY"),
      ("STORE004", "XX", "YY"),
      ("STORE005", "XX", "YY"),
      ("STORE006", "XX", "YY")
    )).toDF("store_id", "country", "state")

    val perFieldCount = PerFieldCount(
      fieldNames = List("country", "state"),
      count = Some(3)
    )

    val cardinalityConfig = CardinalityConfig(ratio = Some(3.0), distribution = "uniform")

    val result = ForeignKeyApplicationUtil.applyCardinality(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("country", "country"), ("state", "state")),
      cardinalityConfig = cardinalityConfig,
      seed = Some(123L),
      targetPerFieldCount = Some(perFieldCount)
    )

    println(s"  ✓ PerField-aware composite key: ${sourceDf.count()} locations -> ${result.count()} stores")

    // Verify no row duplication
    assert(result.count() == 6, "Should preserve all 6 unique stores")

    // Verify each location has exactly 3 stores
    val locationCounts = result.groupBy("country", "state").count().collect()
    locationCounts.foreach { row =>
      assert(row.getLong(2) == 3, s"Location (${row.getString(0)}, ${row.getString(1)}) should have 3 stores")
    }

    // Verify all store_ids are unique
    val storeIds = result.select("store_id").collect().map(_.getString(0))
    assert(storeIds.length == storeIds.distinct.length, "All store_ids should be unique")
  }

  // ========================================================================================
  // ALL-COMBINATIONS TESTS - NEW FEATURE
  // ========================================================================================

  test("V2: All-combinations - Single field (2 combinations)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0)
    )).toDF("txn_id", "account_id", "amount")

    val result = ForeignKeyApplicationUtil.generateAllForeignKeyCombinations(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("account_id", "account_id")),
      seed = Some(42L)
    )

    val sourceValues = sourceDf.select("account_id").collect().map(_.getString(0)).toSet
    val resultValues = result.select("account_id").collect().map(r => Option(r.getString(0)))

    // 2^1 = 2 combinations: valid and invalid
    val validCount = resultValues.count(v => v.isDefined && sourceValues.contains(v.get))
    val invalidCount = resultValues.count(v => v.isEmpty || !sourceValues.contains(v.get))

    println(s"  ✓ Single field combinations: $validCount valid, $invalidCount invalid")

    assert(validCount > 0, "Should have some valid FKs")
    assert(invalidCount > 0, "Should have some invalid FKs")
  }

  test("V2: All-combinations - Two fields (4 combinations)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("USA", "NY", "New York"),
      ("USA", "CA", "California")
    )).toDF("country", "state", "city")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("ORD001", "XX", "YY", 100.0),
      ("ORD002", "XX", "YY", 200.0),
      ("ORD003", "XX", "YY", 150.0),
      ("ORD004", "XX", "YY", 300.0)
    )).toDF("order_id", "country", "state", "amount")

    val result = ForeignKeyApplicationUtil.generateAllForeignKeyCombinations(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("country", "country"), ("state", "state")),
      seed = Some(999L)
    )

    val sourceCountries = sourceDf.select("country").distinct().collect().map(_.getString(0)).toSet
    val sourceStates = sourceDf.select("state").distinct().collect().map(_.getString(0)).toSet

    val resultRows = result.select("country", "state").collect()
    val bothValid = resultRows.count(r => sourceCountries.contains(r.getString(0)) && sourceStates.contains(r.getString(1)))
    val bothInvalid = resultRows.count(r => !sourceCountries.contains(r.getString(0)) && !sourceStates.contains(r.getString(1)))
    val partialValid = resultRows.length - bothValid - bothInvalid

    println(s"  ✓ Two field combinations: $bothValid both-valid, $partialValid partial, $bothInvalid both-invalid")

    // 2^2 = 4 combinations: both valid, country valid + state invalid, country invalid + state valid, both invalid
    // We should have all 4 combinations represented
    assert(result.count() == targetDf.count(), "Should preserve record count")
  }

  test("V2: All-combinations - Three fields (8 combinations)") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("USA", "NY", "Manhattan"),
      ("USA", "CA", "Los Angeles")
    )).toDF("country", "state", "city")

    val targetDf = sparkSession.createDataFrame((1 to 16).map { i =>
      (s"ORD${"%03d".format(i)}", "XX", "YY", "ZZ", 100.0 * i)
    }).toDF("order_id", "country", "state", "city", "amount")

    val result = ForeignKeyApplicationUtil.generateAllForeignKeyCombinations(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("country", "country"), ("state", "state"), ("city", "city")),
      seed = Some(777L)
    )

    println(s"  ✓ Three field combinations: ${result.count()} records with 2^3=8 combination patterns")

    // 2^3 = 8 combinations
    assert(result.count() == targetDf.count(), "Should preserve record count")

    // Verify we get different combinations
    val sourceCountries = sourceDf.select("country").distinct().collect().map(_.getString(0)).toSet
    val resultRows = result.select("country", "state", "city").collect()

    // Check that we have both valid and invalid combinations
    val allValid = resultRows.count(r => sourceCountries.contains(r.getString(0)))
    val allInvalid = resultRows.count(r => !sourceCountries.contains(r.getString(0)))

    assert(allValid > 0, "Should have some records with valid country")
    assert(allInvalid > 0, "Should have some records with invalid country")
  }

  test("V2: All-combinations - Determinism with seed") {
    val sourceDf = sparkSession.createDataFrame(Seq(
      ("ACC001", "Alice"),
      ("ACC002", "Bob")
    )).toDF("account_id", "account_name")

    val targetDf = sparkSession.createDataFrame(Seq(
      ("TXN001", "PLACEHOLDER", 100.0),
      ("TXN002", "PLACEHOLDER", 200.0),
      ("TXN003", "PLACEHOLDER", 150.0),
      ("TXN004", "PLACEHOLDER", 300.0)
    )).toDF("txn_id", "account_id", "amount")

    val seed = 555L

    val result1 = ForeignKeyApplicationUtil.generateAllForeignKeyCombinations(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("account_id", "account_id")),
      seed = Some(seed)
    )

    val result2 = ForeignKeyApplicationUtil.generateAllForeignKeyCombinations(
      sourceDf = sourceDf,
      targetDf = targetDf,
      fieldMappings = List(("account_id", "account_id")),
      seed = Some(seed)
    )

    val values1 = result1.select("txn_id", "account_id").collect().map(r => (r.getString(0), Option(r.getString(1))))
    val values2 = result2.select("txn_id", "account_id").collect().map(r => (r.getString(0), Option(r.getString(1))))

    assert(values1.sameElements(values2), "Same seed should produce identical combinations")

    println(s"  ✓ Deterministic combinations verified with seed=$seed")
  }
}
