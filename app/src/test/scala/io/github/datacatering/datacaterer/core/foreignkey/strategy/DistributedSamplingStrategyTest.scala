package io.github.datacatering.datacaterer.core.foreignkey.strategy

import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import io.github.datacatering.datacaterer.core.foreignkey.model.EnhancedForeignKeyRelation
import io.github.datacatering.datacaterer.core.util.SparkSuite

class DistributedSamplingStrategyTest extends SparkSuite {

  private val strategy = new DistributedSamplingStrategy()

  test("apply with seed produces deterministic FK assignments across multiple runs") {
    import sparkSession.implicits._

    val sourceDf = Seq(
      ("SRC001", "Source A"),
      ("SRC002", "Source B"),
      ("SRC003", "Source C")
    ).toDF("source_id", "source_name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200),
      ("TGT003", "PLACEHOLDER", 300),
      ("TGT004", "PLACEHOLDER", 400),
      ("TGT005", "PLACEHOLDER", 500),
      ("TGT006", "PLACEHOLDER", 600),
      ("TGT007", "PLACEHOLDER", 700),
      ("TGT008", "PLACEHOLDER", 800),
      ("TGT009", "PLACEHOLDER", 900)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source.table",
      sourceFields = List("source_id"),
      targetDataFrameName = "target.table",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    // Run multiple times and verify same results
    val results = (1 to 5).map { _ =>
      val result = strategy.apply(sourceDf, targetDf, relation)
      result.select("target_id", "source_id").collect().map(r => (r.getString(0), r.getString(1))).sortBy(_._1).toList
    }

    // All runs should produce identical results
    results.foreach { result =>
      assert(result == results.head, s"Expected deterministic FK assignments but got different results")
    }
  }

  test("apply with seed produces exact expected FK assignments") {
    import sparkSession.implicits._

    val sourceDf = Seq(
      ("SRC001", "Source A"),
      ("SRC002", "Source B"),
      ("SRC003", "Source C")
    ).toDF("source_id", "source_name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200),
      ("TGT003", "PLACEHOLDER", 300),
      ("TGT004", "PLACEHOLDER", 400),
      ("TGT005", "PLACEHOLDER", 500),
      ("TGT006", "PLACEHOLDER", 600)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source.table",
      sourceFields = List("source_id"),
      targetDataFrameName = "target.table",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)
    val fkAssignments = result.select("target_id", "source_id").collect()
      .map(r => (r.getString(0), r.getString(1)))
      .sortBy(_._1)
      .toList

    // With seed=42, these exact FK assignments should always be produced
    // This verifies the hash-based approach is deterministic
    val expectedAssignments = List(
      ("TGT001", "SRC001"),
      ("TGT002", "SRC001"),
      ("TGT003", "SRC001"),
      ("TGT004", "SRC003"),
      ("TGT005", "SRC002"),
      ("TGT006", "SRC003")
    )

    assert(fkAssignments == expectedAssignments,
      s"Expected exactly $expectedAssignments with seed=42, but got $fkAssignments")
  }

  test("apply without seed produces varying FK assignments") {
    import sparkSession.implicits._

    val sourceDf = Seq(
      ("SRC001", "Source A"),
      ("SRC002", "Source B"),
      ("SRC003", "Source C")
    ).toDF("source_id", "source_name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200),
      ("TGT003", "PLACEHOLDER", 300),
      ("TGT004", "PLACEHOLDER", 400),
      ("TGT005", "PLACEHOLDER", 500),
      ("TGT006", "PLACEHOLDER", 600)
    ).toDF("target_id", "source_id", "amount")

    val config = ForeignKeyConfig(seed = None)
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source.table",
      sourceFields = List("source_id"),
      targetDataFrameName = "target.table",
      targetFields = List("source_id"),
      config = config,
      targetPerFieldCount = None
    )

    // Run multiple times - results may vary
    val results = (1 to 10).map { _ =>
      val result = strategy.apply(sourceDf, targetDf, relation)
      result.select("target_id", "source_id").collect().map(r => (r.getString(0), r.getString(1))).toList
    }

    // All assigned FKs should be valid
    val validSourceIds = Set("SRC001", "SRC002", "SRC003")
    results.flatten.foreach { case (_, sourceId) =>
      assert(validSourceIds.contains(sourceId), s"Invalid source_id: $sourceId")
    }
  }

  test("apply with different seeds produces different FK assignments") {
    import sparkSession.implicits._

    val sourceDf = Seq(
      ("SRC001", "Source A"),
      ("SRC002", "Source B"),
      ("SRC003", "Source C")
    ).toDF("source_id", "source_name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100),
      ("TGT002", "PLACEHOLDER", 200),
      ("TGT003", "PLACEHOLDER", 300),
      ("TGT004", "PLACEHOLDER", 400),
      ("TGT005", "PLACEHOLDER", 500),
      ("TGT006", "PLACEHOLDER", 600)
    ).toDF("target_id", "source_id", "amount")

    val config1 = ForeignKeyConfig(seed = Some(42L))
    val config2 = ForeignKeyConfig(seed = Some(12345L))

    val relation1 = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source.table",
      sourceFields = List("source_id"),
      targetDataFrameName = "target.table",
      targetFields = List("source_id"),
      config = config1,
      targetPerFieldCount = None
    )

    val relation2 = relation1.copy(config = config2)

    val result1 = strategy.apply(sourceDf, targetDf, relation1)
    val result2 = strategy.apply(sourceDf, targetDf, relation2)

    val fkAssignments1 = result1.select("target_id", "source_id").collect()
      .map(r => (r.getString(0), r.getString(1)))
      .sortBy(_._1)
      .toList

    val fkAssignments2 = result2.select("target_id", "source_id").collect()
      .map(r => (r.getString(0), r.getString(1)))
      .sortBy(_._1)
      .toList

    // Different seeds should produce different assignments (with high probability)
    assert(fkAssignments1 != fkAssignments2,
      s"Different seeds should likely produce different FK assignments: seed1=$fkAssignments1, seed2=$fkAssignments2")
  }

  test("apply preserves other columns when assigning FKs") {
    import sparkSession.implicits._

    val sourceDf = Seq(
      ("SRC001", "Source A"),
      ("SRC002", "Source B")
    ).toDF("source_id", "source_name")

    val targetDf = Seq(
      ("TGT001", "PLACEHOLDER", 100, "extra1"),
      ("TGT002", "PLACEHOLDER", 200, "extra2"),
      ("TGT003", "PLACEHOLDER", 300, "extra3")
    ).toDF("target_id", "fk_field", "amount", "extra")

    val config = ForeignKeyConfig(seed = Some(42L))
    val relation = EnhancedForeignKeyRelation(
      sourceDataFrameName = "source.table",
      sourceFields = List("source_id"),
      targetDataFrameName = "target.table",
      targetFields = List("fk_field"),
      config = config,
      targetPerFieldCount = None
    )

    val result = strategy.apply(sourceDf, targetDf, relation)

    // Verify other columns are preserved
    val row1 = result.filter(result("target_id") === "TGT001").first()
    assert(row1.getAs[Int]("amount") == 100)
    assert(row1.getAs[String]("extra") == "extra1")

    val row2 = result.filter(result("target_id") === "TGT002").first()
    assert(row2.getAs[Int]("amount") == 200)
    assert(row2.getAs[String]("extra") == "extra2")

    // Verify FK field has valid source value
    val validSourceIds = Set("SRC001", "SRC002")
    result.collect().foreach { row =>
      val fkValue = row.getAs[String]("fk_field")
      assert(validSourceIds.contains(fkValue), s"Invalid FK value: $fkValue")
    }
  }
}
