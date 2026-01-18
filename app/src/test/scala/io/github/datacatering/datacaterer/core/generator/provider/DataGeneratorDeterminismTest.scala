package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.generator.provider.FastDataGenerator.FastRegexDataGenerator
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.types._

/**
 * Tests for verifying that DataGenerator produces deterministic results when a seed is provided.
 * The changes to use hash-based approach instead of rand(seed) ensure consistent behavior
 * across different Spark environments and partition layouts.
 */
class DataGeneratorDeterminismTest extends SparkSuite {
  private def collectGeneratedValues(sqlExpression: String, rowCount: Int, valueExpr: Option[String] = None): List[(Int, String)] = {
    import sparkSession.implicits._
    val expr = valueExpr.getOrElse(s"CAST($sqlExpression AS STRING)")
    (1 to rowCount).map(i => i).toDF("id")
      .selectExpr("id", s"$expr as generated_value")
      .collect()
      .map(r => (r.getInt(0), r.getString(1)))
      .sortBy(_._1)
      .toList
  }

  private def assertDeterministicAndVaried(
                                            generator: DataGenerator[_],
                                            rowCount: Int = 50,
                                            valueExpr: Option[String] = None
                                          ): Unit = {
    val sqlExpression = generator.generateSqlExpressionWrapper
    val results1 = collectGeneratedValues(sqlExpression, rowCount, valueExpr)
    val results2 = collectGeneratedValues(sqlExpression, rowCount, valueExpr)

    assert(results1 == results2,
      s"Expected deterministic results for ${generator.getClass.getSimpleName} but got different results")
    val distinctValues = results1.map(_._2).distinct
    assert(distinctValues.size > 1,
      s"Expected varied values for ${generator.getClass.getSimpleName} but got only: ${distinctValues.take(3)}")
  }

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

  test("seeded one-of SQL produces expected deterministic values") {
    import sparkSession.implicits._

    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ONE_OF_GENERATOR, "RED,BLUE,GREEN")
      .build()

    val generator = new OneOfDataGenerator.RandomOneOfDataGenerator(
      StructField("color", StringType, nullable = false, metadata),
      oneOfValues = Array("RED", "BLUE", "GREEN")
    )

    val sqlExpression = generator.generateSqlExpression
    val testDf = (1 to 8).map(i => (i, s"row_$i")).toDF("id", "label")

    val result = testDf.selectExpr("id", s"$sqlExpression as generated_value")
      .collect()
      .map(r => (r.getInt(0), r.getString(1)))
      .sortBy(_._1)
      .toList

    val expected = List(
      (1, "BLUE"),
      (2, "BLUE"),
      (3, "GREEN"),
      (4, "RED"),
      (5, "GREEN"),
      (6, "BLUE"),
      (7, "GREEN"),
      (8, "RED")
    )

    assert(result == expected,
      s"Expected deterministic seeded one-of values, but got $result")
  }

  test("seeded fast regex SQL produces expected deterministic values") {
    import sparkSession.implicits._

    val metadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(REGEX_GENERATOR, "ACC[0-9]{4}")
      .build()

    val generator = new FastRegexDataGenerator(
      StructField("account_id", StringType, nullable = false, metadata)
    )

    val sqlExpression = generator.generateSqlExpression
    val testDf = (1 to 5).map(i => (i, s"row_$i")).toDF("id", "label")

    val result = testDf.selectExpr("id", s"$sqlExpression as generated_value")
      .collect()
      .map(r => (r.getInt(0), r.getString(1)))
      .sortBy(_._1)
      .toList

    val expected = List(
      (1, "ACC6191"),
      (2, "ACC5096"),
      (3, "ACC8325"),
      (4, "ACC2632"),
      (5, "ACC6702")
    )

    assert(result == expected,
      s"Expected deterministic seeded regex values, but got $result")
  }

  test("seeded SQL generators are deterministic and varied within a run") {
    val seedMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .build()

    val stringMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(MINIMUM_LENGTH, "3")
      .putString(MAXIMUM_LENGTH, "8")
      .build()

    val numericMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(MINIMUM, "10")
      .putString(MAXIMUM, "1000")
      .build()

    val decimalMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(MINIMUM, "1.5")
      .putString(MAXIMUM, "999.5")
      .build()

    val arrayMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ARRAY_ONE_OF, "'RED','BLUE','GREEN'")
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 4)
      .build()

    val weightedArrayMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(ARRAY_WEIGHTED_ONE_OF, "'HIGH':0.2,'MEDIUM':0.5,'LOW':0.3")
      .putLong(ARRAY_MINIMUM_LENGTH, 1)
      .putLong(ARRAY_MAXIMUM_LENGTH, 4)
      .build()

    val mapMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putLong(MAP_MINIMUM_SIZE, 1)
      .putLong(MAP_MAXIMUM_SIZE, 3)
      .build()

    val dateMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(MINIMUM, "2020-01-01")
      .putString(MAXIMUM, "2020-01-31")
      .build()

    val timestampMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(MINIMUM, "2020-01-01 00:00:00")
      .putString(MAXIMUM, "2020-01-02 00:00:00")
      .build()

    val binaryMetadata = new MetadataBuilder()
      .putString(RANDOM_SEED, "42")
      .putString(MINIMUM_LENGTH, "4")
      .putString(MAXIMUM_LENGTH, "8")
      .build()

    val generators = List(
      new RandomDataGenerator.RandomStringDataGenerator(StructField("seeded_string", StringType, nullable = false, stringMetadata)),
      new RandomDataGenerator.RandomIntDataGenerator(StructField("seeded_int", IntegerType, nullable = false, numericMetadata)),
      new RandomDataGenerator.RandomLongDataGenerator(StructField("seeded_long", LongType, nullable = false, numericMetadata)),
      new RandomDataGenerator.RandomDecimalDataGenerator(StructField("seeded_decimal", DecimalType(22, 2), nullable = false, decimalMetadata)),
      new RandomDataGenerator.RandomDoubleDataGenerator(StructField("seeded_double", DoubleType, nullable = false, numericMetadata)),
      new RandomDataGenerator.RandomFloatDataGenerator(StructField("seeded_float", FloatType, nullable = false, numericMetadata)),
      new RandomDataGenerator.RandomBooleanDataGenerator(StructField("seeded_bool", BooleanType, nullable = false, seedMetadata)),
      new RandomDataGenerator.RandomDateDataGenerator(StructField("seeded_date", DateType, nullable = false, dateMetadata)),
      new RandomDataGenerator.RandomTimestampDataGenerator(StructField("seeded_ts", TimestampType, nullable = false, timestampMetadata)),
      new RandomDataGenerator.RandomArrayDataGenerator[String](StructField("seeded_array", ArrayType(StringType), nullable = false, arrayMetadata), StringType),
      new RandomDataGenerator.RandomArrayDataGenerator[String](StructField("seeded_weighted_array", ArrayType(StringType), nullable = false, weightedArrayMetadata), StringType)
    )

    generators.foreach(assertDeterministicAndVaried(_))

    val mapGenerator = new RandomDataGenerator.RandomMapDataGenerator[String, String](
      StructField("seeded_map", MapType(StringType, StringType), nullable = false, mapMetadata),
      StringType,
      StringType
    )
    val mapExpr = s"TO_JSON(MAP_FROM_ENTRIES(SORT_ARRAY(MAP_ENTRIES(${mapGenerator.generateSqlExpressionWrapper}))))"
    val previousPolicy = sparkSession.conf.get("spark.sql.mapKeyDedupPolicy", "EXCEPTION")
    sparkSession.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
    try {
      assertDeterministicAndVaried(mapGenerator, valueExpr = Some(mapExpr))
    } finally {
      sparkSession.conf.set("spark.sql.mapKeyDedupPolicy", previousPolicy)
    }

    val binaryGenerator = new RandomDataGenerator.RandomBinaryDataGenerator(
      StructField("seeded_binary", BinaryType, nullable = false, binaryMetadata)
    )
    assertDeterministicAndVaried(binaryGenerator, valueExpr = Some(s"BASE64(${binaryGenerator.generateSqlExpressionWrapper})"))

    val byteGenerator = new RandomDataGenerator.RandomByteDataGenerator(
      StructField("seeded_byte", ByteType, nullable = false, seedMetadata)
    )
    assertDeterministicAndVaried(byteGenerator, valueExpr = Some(s"BASE64(${byteGenerator.generateSqlExpressionWrapper})"))
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
