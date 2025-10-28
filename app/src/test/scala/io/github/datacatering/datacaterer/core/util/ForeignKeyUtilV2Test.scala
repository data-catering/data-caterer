package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.core.util.ForeignKeyUtilV2.ForeignKeyConfig
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
    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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

    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result2 = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result2 = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result1 = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = config
    )

    val result2 = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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
    val result1 = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
      sourceDf = sourceDf,
      targetDf = targetDf,
      sourceFields = List("account_id"),
      targetFields = List("account_id"),
      config = config1
    )

    val result2 = ForeignKeyUtilV2.applyForeignKeysToTargetDf(
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

    val (validDf, invalidDf) = ForeignKeyUtilV2.generateValidAndInvalidCombinations(
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
}
