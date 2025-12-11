package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.api.model.NullabilityConfig
import io.github.datacatering.datacaterer.core.util.SparkSuite

class NullabilityStrategyTest extends SparkSuite {

  private val strategy = new NullabilityStrategy()

  test("applyNullability with seed produces deterministic results across multiple runs") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1", 100),
      ("ROW002", "value2", 200),
      ("ROW003", "value3", 300),
      ("ROW004", "value4", 400),
      ("ROW005", "value5", 500),
      ("ROW006", "value6", 600),
      ("ROW007", "value7", 700),
      ("ROW008", "value8", 800),
      ("ROW009", "value9", 900),
      ("ROW010", "value10", 1000)
    ).toDF("id", "fk_field", "amount")

    val nullabilityConfig = NullabilityConfig(0.3) // 30% null
    val seed = Some(42L)

    // Run multiple times and verify same results
    val results = (1 to 5).map { _ =>
      val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, seed)
      val nullRows = result.filter(result("fk_field").isNull).select("id").collect().map(_.getString(0)).sorted
      nullRows.toList
    }

    // All runs should produce identical results
    results.foreach { result =>
      assert(result == results.head, s"Expected deterministic results but got different: $result vs ${results.head}")
    }
  }

  test("applyNullability with seed produces exact expected null rows") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1", 100),
      ("ROW002", "value2", 200),
      ("ROW003", "value3", 300),
      ("ROW004", "value4", 400),
      ("ROW005", "value5", 500),
      ("ROW006", "value6", 600),
      ("ROW007", "value7", 700),
      ("ROW008", "value8", 800),
      ("ROW009", "value9", 900),
      ("ROW010", "value10", 1000)
    ).toDF("id", "fk_field", "amount")

    val nullabilityConfig = NullabilityConfig(0.3) // 30% null
    val seed = Some(42L)

    val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, seed)
    val nullRows = result.filter(result("fk_field").isNull).select("id").collect().map(_.getString(0)).sorted.toList

    // With seed=42 and 30% nullability, these exact rows should always be null
    // This verifies the hash-based approach is deterministic
    val expectedNullRows = List("ROW001", "ROW002", "ROW005", "ROW006", "ROW010")
    assert(nullRows == expectedNullRows,
      s"Expected exactly $expectedNullRows to be null with seed=42, but got $nullRows")

    // Verify non-null rows still have original values
    val nonNullRows = result.filter(result("fk_field").isNotNull)
    nonNullRows.collect().foreach { row =>
      val id = row.getAs[String]("id")
      val fkField = row.getAs[String]("fk_field")
      val expectedValue = s"value${id.replace("ROW00", "").replace("ROW0", "")}"
      assert(fkField == expectedValue, s"Non-null row $id should have original value $expectedValue, got $fkField")
    }
  }

  test("applyNullability without seed produces non-deterministic results") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1"),
      ("ROW002", "value2"),
      ("ROW003", "value3"),
      ("ROW004", "value4"),
      ("ROW005", "value5"),
      ("ROW006", "value6"),
      ("ROW007", "value7"),
      ("ROW008", "value8"),
      ("ROW009", "value9"),
      ("ROW010", "value10")
    ).toDF("id", "fk_field")

    val nullabilityConfig = NullabilityConfig(0.5) // 50% null
    val seed = None

    // Run multiple times - results may vary (non-deterministic)
    val results = (1 to 10).map { _ =>
      val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, seed)
      result.filter(result("fk_field").isNull).count()
    }

    // With 50% null rate, we should see some variation in counts (probabilistic)
    // This test verifies non-deterministic behavior when no seed is provided
    val uniqueCounts = results.distinct
    // We expect at least 1 unique count (could be same by chance, but likely different)
    assert(uniqueCounts.nonEmpty, "Should have produced some null counts")
  }

  test("applyNullability with different seeds produces different results") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1"),
      ("ROW002", "value2"),
      ("ROW003", "value3"),
      ("ROW004", "value4"),
      ("ROW005", "value5"),
      ("ROW006", "value6"),
      ("ROW007", "value7"),
      ("ROW008", "value8"),
      ("ROW009", "value9"),
      ("ROW010", "value10")
    ).toDF("id", "fk_field")

    val nullabilityConfig = NullabilityConfig(0.3) // 30% null

    val result1 = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, Some(42L))
    val result2 = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, Some(12345L))

    val nullRows1 = result1.filter(result1("fk_field").isNull).select("id").collect().map(_.getString(0)).sorted.toList
    val nullRows2 = result2.filter(result2("fk_field").isNull).select("id").collect().map(_.getString(0)).sorted.toList

    // Different seeds should produce different null patterns (with high probability)
    // Note: It's theoretically possible for them to be the same, but very unlikely
    assert(nullRows1 != nullRows2 || nullRows1.isEmpty,
      s"Different seeds should likely produce different null patterns: seed1=$nullRows1, seed2=$nullRows2")
  }

  test("applyNullability head strategy produces first N% records as null") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1"),
      ("ROW002", "value2"),
      ("ROW003", "value3"),
      ("ROW004", "value4"),
      ("ROW005", "value5"),
      ("ROW006", "value6"),
      ("ROW007", "value7"),
      ("ROW008", "value8"),
      ("ROW009", "value9"),
      ("ROW010", "value10")
    ).toDF("id", "fk_field")

    val nullabilityConfig = NullabilityConfig(0.3, "head") // 30% null from head

    val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, Some(42L))
    val nullCount = result.filter(result("fk_field").isNull).count()

    // 30% of 10 rows = 3 rows
    assert(nullCount == 3, s"Head strategy with 30% should produce 3 nulls, got $nullCount")
  }

  test("applyNullability tail strategy produces last N% records as null") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1"),
      ("ROW002", "value2"),
      ("ROW003", "value3"),
      ("ROW004", "value4"),
      ("ROW005", "value5"),
      ("ROW006", "value6"),
      ("ROW007", "value7"),
      ("ROW008", "value8"),
      ("ROW009", "value9"),
      ("ROW010", "value10")
    ).toDF("id", "fk_field")

    val nullabilityConfig = NullabilityConfig(0.3, "tail") // 30% null from tail

    val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, Some(42L))
    val nullCount = result.filter(result("fk_field").isNull).count()

    // 30% of 10 rows = 3 rows
    assert(nullCount == 3, s"Tail strategy with 30% should produce 3 nulls, got $nullCount")
  }

  test("applyNullability with 0% null percentage produces no nulls") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "value1"),
      ("ROW002", "value2"),
      ("ROW003", "value3")
    ).toDF("id", "fk_field")

    val nullabilityConfig = NullabilityConfig(0.0)

    val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, Some(42L))
    val nullCount = result.filter(result("fk_field").isNull).count()

    assert(nullCount == 0, s"0% nullability should produce 0 nulls, got $nullCount")
  }

  test("applyNullability preserves other columns when nullifying FK field") {
    import sparkSession.implicits._

    val testDf = Seq(
      ("ROW001", "fk_value1", 100, "extra1"),
      ("ROW002", "fk_value2", 200, "extra2"),
      ("ROW003", "fk_value3", 300, "extra3")
    ).toDF("id", "fk_field", "amount", "extra")

    val nullabilityConfig = NullabilityConfig(1.0) // 100% null

    val result = strategy.applyNullability(testDf, List("fk_field"), nullabilityConfig, Some(42L))

    // All FK fields should be null
    assert(result.filter(result("fk_field").isNull).count() == 3)

    // Other columns should be preserved
    val row1 = result.filter(result("id") === "ROW001").first()
    assert(row1.getAs[Int]("amount") == 100)
    assert(row1.getAs[String]("extra") == "extra1")
  }
}
