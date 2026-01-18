package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import io.github.datacatering.datacaterer.core.util.SparkSuite

class GenerationModeStrategyTest extends SparkSuite {

  test("factory method allExist creates strategy with all-exist mode") {
    val strategy = GenerationModeStrategy.allExist()
    assert(strategy.name == "GenerationModeStrategy(all-exist)")
  }

  test("factory method partial creates strategy with partial mode") {
    val strategy = GenerationModeStrategy.partial()
    assert(strategy.name == "GenerationModeStrategy(partial)")
  }

  test("factory method allCombinations creates strategy with all-combinations mode") {
    val strategy = GenerationModeStrategy.allCombinations()
    assert(strategy.name == "GenerationModeStrategy(all-combinations)")
  }

  test("forMode factory method creates correct strategy for all-exist") {
    val strategy = GenerationModeStrategy.forMode("all-exist")
    assert(strategy.name == "GenerationModeStrategy(all-exist)")
  }

  test("forMode factory method creates correct strategy for partial") {
    val strategy = GenerationModeStrategy.forMode("partial")
    assert(strategy.name == "GenerationModeStrategy(partial)")
  }

  test("forMode factory method creates correct strategy for all-combinations") {
    val strategy = GenerationModeStrategy.forMode("all-combinations")
    assert(strategy.name == "GenerationModeStrategy(all-combinations)")
  }

  test("forMode factory method defaults to all-exist for unknown mode") {
    val strategy = GenerationModeStrategy.forMode("unknown-mode")
    assert(strategy.name == "GenerationModeStrategy(all-exist)")
  }

  test("forMode is case insensitive") {
    val strategy1 = GenerationModeStrategy.forMode("ALL-EXIST")
    val strategy2 = GenerationModeStrategy.forMode("All-Exist")
    val strategy3 = GenerationModeStrategy.forMode("ALL-COMBINATIONS")

    assert(strategy1.name == "GenerationModeStrategy(all-exist)")
    assert(strategy2.name == "GenerationModeStrategy(all-exist)")
    assert(strategy3.name == "GenerationModeStrategy(all-combinations)")
  }

  test("isApplicable returns true for all relations") {
    val strategy = GenerationModeStrategy.allExist()
    val config = ForeignKeyConfig()
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("id"),
      targetDataFrameName = "target",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    assert(strategy.isApplicable(relation) == true)
  }

  test("all-exist mode assigns valid foreign keys to all records") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allExist()

    val sourceDf = Seq(
      ("SRC001", "Alice"),
      ("SRC002", "Bob"),
      ("SRC003", "Charlie")
    ).toDF("source_id", "name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200),
      ("TGT003", "PLACEHOLDER", 300),
      ("TGT004", "PLACEHOLDER", 400),
      ("TGT005", "PLACEHOLDER", 500)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("source_id"),
      targetDataFrameName = "target",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    // All records should have valid FKs
    val validSourceIds = Set("SRC001", "SRC002", "SRC003")
    result.collect().foreach { row =>
      val fkValue = row.getAs[String]("source_id")
      assert(validSourceIds.contains(fkValue), s"Invalid FK: $fkValue")
    }

    assert(result.count() == 5)
  }

  test("partial mode assigns valid foreign keys (nullability handled separately)") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.partial()

    val sourceDf = Seq(
      ("SRC001", "Alice"),
      ("SRC002", "Bob")
    ).toDF("source_id", "name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200),
      ("TGT003", "PLACEHOLDER", 300)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("source_id"),
      targetDataFrameName = "target",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    // Partial mode first applies valid FKs, nullability is handled by NullabilityStrategy
    val validSourceIds = Set("SRC001", "SRC002")
    result.collect().foreach { row =>
      val fkValue = row.getAs[String]("source_id")
      assert(validSourceIds.contains(fkValue), s"Invalid FK: $fkValue")
    }

    assert(result.count() == 3)
  }

  test("all-combinations mode generates all FK patterns for single field") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allCombinations()

    val sourceDf = Seq(
      ("SRC001", "Alice"),
      ("SRC002", "Bob")
    ).toDF("source_id", "name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("source_id"),
      targetDataFrameName = "target",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    // For 1 field: 2^1 = 2 combinations (valid, invalid)
    assert(result.count() == 2)

    val fkValues = result.select("source_id").collect().map(_.getString(0))

    // Should have at least one invalid FK (starts with "INVALID_")
    val hasInvalid = fkValues.exists(_.startsWith("INVALID_"))
    assert(hasInvalid, "Should generate invalid FK combinations")

    // Should have at least one valid FK
    val validSourceIds = Set("SRC001", "SRC002")
    val hasValid = fkValues.exists(validSourceIds.contains)
    assert(hasValid, "Should generate valid FK combinations")
  }

  test("all-combinations mode generates all FK patterns for multiple fields") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allCombinations()

    val sourceDf = Seq(
      ("SRC001", "ORD001", "Alice"),
      ("SRC002", "ORD002", "Bob")
    ).toDF("source_id", "order_id", "name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER_SRC", "PLACEHOLDER_ORD", 100),
      ("TGT002", "PLACEHOLDER_SRC", "PLACEHOLDER_ORD", 200),
      ("TGT003", "PLACEHOLDER_SRC", "PLACEHOLDER_ORD", 300),
      ("TGT004", "PLACEHOLDER_SRC", "PLACEHOLDER_ORD", 400)
    ).toDF("target_id", "source_id", "order_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("source_id", "order_id"),
      targetDataFrameName = "target",
      targetFields = List("source_id", "order_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    // For 2 fields: 2^2 = 4 combinations
    // (valid, valid), (valid, invalid), (invalid, valid), (invalid, invalid)
    assert(result.count() == 4)

    val rows = result.select("source_id", "order_id").collect()

    // Check that we have different combinations
    val combinations = rows.map(r => (
      r.getString(0).startsWith("INVALID_"),
      r.getString(1).startsWith("INVALID_")
    )).toSet

    assert(combinations.size > 1, "Should have variety of combinations")
  }

  test("all-combinations mode with seed produces deterministic results") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allCombinations()

    val sourceDf = Seq(
      ("SRC001", "Alice")
    ).toDF("source_id", "name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("source_id"),
      targetDataFrameName = "target",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    // Run multiple times with same seed
    val results = (1 to 3).map { _ =>
      strategy.apply(sourceDf, targetDf, relation)
        .select("target_id", "source_id")
        .collect()
        .map(r => (r.getString(0), r.getString(1)))
        .sortBy(_._1)
        .toList
    }

    // All runs should produce identical results
    results.foreach { result =>
      assert(result == results.head)
    }
  }

  test("all-combinations mode generates invalid values for integer fields") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allCombinations()

    val sourceDf = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("user_id", "name")

    val targetDf = Seq(
      ("TGT001", 999, 100),
      ("TGT002", 999, 200)
    ).toDF("target_id", "user_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("user_id"),
      targetDataFrameName = "target",
      targetFields = List("user_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    assert(result.count() == 2)

    val userIds = result.select("user_id").collect().map(_.getInt(0))

    // Should have mix of valid (1, 2) and invalid values
    val hasInvalid = userIds.exists(id => id != 1 && id != 2)

    // At least one should be invalid due to combinations
    assert(hasInvalid == true)
  }

  test("all-combinations mode handles empty target dataframe") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allCombinations()

    val sourceDf = Seq(
      ("SRC001", "Alice")
    ).toDF("source_id", "name")

    val targetDf = Seq.empty[(String, String, Int)]
      .toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List("source_id"),
      targetDataFrameName = "target",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    assert(result.count() == 0)
  }

  test("all-combinations mode handles zero field mappings") {
    import sparkSession.implicits._

    val strategy = GenerationModeStrategy.allCombinations()

    val sourceDf = Seq(
      ("SRC001", "Alice")
    ).toDF("source_id", "name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source",
      sourceFields = List(),
      targetDataFrameName = "target",
      targetFields = List(),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    // Should return target unchanged when no fields to process
    assert(result.count() == 1)
  }

  test("name method returns strategy name with mode") {
    val allExist = GenerationModeStrategy.allExist()
    val partial = GenerationModeStrategy.partial()
    val allCombinations = GenerationModeStrategy.allCombinations()

    assert(allExist.name.contains("all-exist"))
    assert(partial.name.contains("partial"))
    assert(allCombinations.name.contains("all-combinations"))
  }
}
