package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.types._

/**
 * Tests for verifying that DataGenerator produces deterministic results when a seed is provided.
 * The changes to use hash-based approach instead of rand(seed) ensure consistent behavior
 * across different Spark environments and partition layouts.
 */
class DataGeneratorDeterminismTest extends SparkSuite {

  test("generateSqlExpressionWrapper with seed produces deterministic SQL for null injection") {
    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ENABLED_NULL, "true")
      .putString(PROBABILITY_OF_NULL, "0.3")
      .build()

    val generator = new RandomDataGenerator.RandomStringDataGenerator(
      StructField("test_field", StringType, nullable = true, metadata)
    )

    // Generate SQL expression multiple times - should be identical
    val expressions = (1 to 5).map(_ => generator.generateSqlExpressionWrapper)

    // All expressions should be identical
    expressions.foreach { expr =>
      assert(expr == expressions.head,
        s"Expected deterministic SQL expression but got different: $expr vs ${expressions.head}")
    }

    // Verify the expression contains xxhash64 for deterministic behavior
    assert(expressions.head.contains("xxhash64") || expressions.head.contains("XXHASH64"),
      s"Expected hash-based expression with seed, but got: ${expressions.head}")
  }

  test("generateSqlExpressionWrapper with seed produces deterministic probability logic for edge cases") {
    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ENABLED_EDGE_CASE, "true")
      .putString(PROBABILITY_OF_EDGE_CASE, "0.5")
      .build()

    val generator = new RandomDataGenerator.RandomIntDataGenerator(
      StructField("test_field", IntegerType, nullable = false, metadata)
    )

    val expression = generator.generateSqlExpressionWrapper

    // Verify the expression contains xxhash64 for deterministic probability selection
    // Note: The edge case value itself may vary because it's selected via random.nextInt,
    // but the probability logic (whether to use edge case) is deterministic via xxhash64
    assert(expression.contains("xxhash64") || expression.contains("XXHASH64"),
      s"Expected hash-based expression with seed for probability logic, but got: $expression")

    // Verify it's a CASE WHEN expression with the hash-based probability check
    assert(expression.contains("CASE WHEN"),
      s"Expected CASE WHEN expression for edge case logic, but got: $expression")
  }

  test("generateSqlExpressionWrapper without seed uses rand()") {
    val metadata = new MetadataBuilder()
      .putString(ENABLED_NULL, "true")
      .putString(PROBABILITY_OF_NULL, "0.3")
      .build()

    val generator = new RandomDataGenerator.RandomStringDataGenerator(
      StructField("test_field", StringType, nullable = true, metadata)
    )

    val expression = generator.generateSqlExpressionWrapper

    // Without seed, should use RAND() not hash-based approach
    assert(expression.toUpperCase.contains("RAND()"),
      s"Expected RAND() in expression without seed, but got: $expression")
    assert(!expression.contains("xxhash64") && !expression.contains("XXHASH64"),
      s"Expected no hash-based expression without seed, but got: $expression")
  }

  test("SQL expression with seed produces deterministic results when executed") {
    import sparkSession.implicits._

    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ENABLED_NULL, "true")
      .putString(PROBABILITY_OF_NULL, "0.3")
      .build()

    val generator = new RandomDataGenerator.RandomStringDataGenerator(
      StructField("test_field", StringType, nullable = true, metadata)
    )

    val sqlExpression = generator.generateSqlExpressionWrapper

    // Create a test DataFrame
    val testDf = (1 to 10).map(i => (i, s"value_$i")).toDF("id", "original_value")

    // Execute the SQL expression multiple times
    val results = (1 to 3).map { _ =>
      testDf.selectExpr("id", s"$sqlExpression as generated_value")
        .collect()
        .map(r => (r.getInt(0), Option(r.getString(1))))
        .sortBy(_._1)
        .toList
    }

    // All executions should produce identical results
    results.foreach { result =>
      assert(result == results.head,
        s"Expected deterministic execution results but got different")
    }
  }

  test("SQL expression with seed produces exact expected null pattern") {
    import sparkSession.implicits._

    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ENABLED_NULL, "true")
      .putString(PROBABILITY_OF_NULL, "0.3")
      .build()

    val generator = new RandomDataGenerator.RandomStringDataGenerator(
      StructField("test_field", StringType, nullable = true, metadata)
    )

    val sqlExpression = generator.generateSqlExpressionWrapper

    // Create a test DataFrame
    val testDf = (1 to 10).map(i => (i, s"value_$i")).toDF("id", "original_value")

    // Execute the SQL expression
    val result = testDf.selectExpr("id", s"$sqlExpression as generated_value")
      .collect()
      .map(r => (r.getInt(0), Option(r.getString(1)).isEmpty))
      .sortBy(_._1)

    val nullIds = result.filter(_._2).map(_._1).toList

    // With seed=42 and 30% null probability, these exact IDs should be null
    // This verifies the hash-based approach is deterministic across runs
    val expectedNullIds = List(1, 5, 7)
    assert(nullIds == expectedNullIds,
      s"Expected exactly $expectedNullIds to be null with seed=42, but got $nullIds")
  }

  test("different seeds produce different null patterns") {
    import sparkSession.implicits._

    def createGenerator(seed: Long) = {
      val metadata = new MetadataBuilder()
        .putString(RANDOM_SEED, seed.toString)
        .putString(ENABLED_NULL, "true")
        .putString(PROBABILITY_OF_NULL, "0.3")
        .build()

      new RandomDataGenerator.RandomStringDataGenerator(
        StructField("test_field", StringType, nullable = true, metadata)
      )
    }

    val testDf = (1 to 10).map(i => (i, s"value_$i")).toDF("id", "original_value")

    val generator1 = createGenerator(42L)
    val generator2 = createGenerator(12345L)

    val result1 = testDf.selectExpr("id", s"${generator1.generateSqlExpressionWrapper} as generated_value")
      .collect()
      .map(r => (r.getInt(0), Option(r.getString(1)).isEmpty))
      .filter(_._2).map(_._1).toSet

    val result2 = testDf.selectExpr("id", s"${generator2.generateSqlExpressionWrapper} as generated_value")
      .collect()
      .map(r => (r.getInt(0), Option(r.getString(1)).isEmpty))
      .filter(_._2).map(_._1).toSet

    // Different seeds should produce different null patterns (with high probability)
    assert(result1 != result2 || result1.isEmpty,
      s"Different seeds should likely produce different null patterns: seed1=$result1, seed2=$result2")
  }

  test("generateSqlExpressionWrapper with both nulls and edge cases uses hash-based probability selection") {
    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ENABLED_NULL, "true")
      .putString(PROBABILITY_OF_NULL, "0.2")
      .putString(ENABLED_EDGE_CASE, "true")
      .putString(PROBABILITY_OF_EDGE_CASE, "0.3")
      .build()

    val generator = new RandomDataGenerator.RandomIntDataGenerator(
      StructField("test_field", IntegerType, nullable = true, metadata)
    )

    val expression = generator.generateSqlExpressionWrapper

    // With seed, should use hash-based approach for the probability selection (null/edge case decision)
    assert(expression.contains("xxhash64") || expression.contains("XXHASH64"),
      s"Expected hash-based expression for probability selection, but got: $expression")

    // The baseSqlExpression (actual value generation) may still use RAND(seed) for value generation
    // This is expected - we only made the null/edge case probability selection deterministic
    // The hash-based approach is used in the CASE WHEN condition, not the value generation
    assert(expression.contains("CASE WHEN"),
      s"Expected CASE WHEN structure for null/edge case logic, but got: $expression")

    // Verify the probability check uses xxhash64
    assert(expression.contains("xxhash64(monotonically_increasing_id()"),
      s"Expected xxhash64 with monotonically_increasing_id for probability check, but got: $expression")
  }

  test("static value bypasses random generation entirely") {
    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(STATIC, "fixed_value")
      .putString(ENABLED_NULL, "true")
      .putString(PROBABILITY_OF_NULL, "0.5")
      .build()

    val generator = new RandomDataGenerator.RandomStringDataGenerator(
      StructField("test_field", StringType, nullable = true, metadata)
    )

    val expression = generator.generateSqlExpressionWrapper

    // Static value should bypass all random logic
    assert(expression == "'fixed_value'",
      s"Expected static value expression, but got: $expression")
  }
}
