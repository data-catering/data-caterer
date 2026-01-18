package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.{Count, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker

/**
 * Integration tests for Sequential Helper methods that verify sequential data generation patterns.
 */
class SequentialHelpersIntegrationTest extends SparkSuite {

  private val dataGeneratorFactory = new DataGeneratorFactory(new Faker() with Serializable, enableFastGeneration = false)

  // ========== sequence() Tests ==========

  test("sequence() generates sequential values with default parameters") {
    val fields = List(
      FieldBuilder().name("id").sequence()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(10L)(df.count())
    val ids = df.select("id").collect().map(_.getString(0))

    // Should generate: "1", "2", "3", ..., "10"
    ids.zipWithIndex.foreach { case (id, idx) =>
      assertResult((idx + 1).toString)(id)
    }
  }

  test("sequence() with custom start value") {
    val fields = List(
      FieldBuilder().name("order_id").sequence(start = 1000L)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val ids = df.select("order_id").collect().map(_.getString(0))

    // Should generate: "1000", "1001", "1002", ..., "1009"
    ids.zipWithIndex.foreach { case (id, idx) =>
      assertResult((1000 + idx).toString)(id)
    }
  }

  test("sequence() with custom step") {
    val fields = List(
      FieldBuilder().name("even_numbers").sequence(start = 0L, step = 2L)
    ).map(_.field)

    val testStep = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(testStep, "parquet", 0, 10)
    df.cache()

    val numbers = df.select("even_numbers").collect().map(_.getString(0))

    // Should generate: "0", "2", "4", ..., "18"
    numbers.zipWithIndex.foreach { case (num, idx) =>
      assertResult((idx * 2).toString)(num)
    }
  }

  test("sequence() with prefix") {
    val fields = List(
      FieldBuilder().name("order_id").sequence(prefix = "ORD-")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val ids = df.select("order_id").collect().map(_.getString(0))

    // Should generate: "ORD-1", "ORD-2", ..., "ORD-10"
    ids.zipWithIndex.foreach { case (id, idx) =>
      assertResult(s"ORD-${idx + 1}")(id)
    }
  }

  test("sequence() with suffix") {
    val fields = List(
      FieldBuilder().name("item_code").sequence(suffix = "-ITEM")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val codes = df.select("item_code").collect().map(_.getString(0))

    // Should generate: "1-ITEM", "2-ITEM", ..., "10-ITEM"
    codes.zipWithIndex.foreach { case (code, idx) =>
      assertResult(s"${idx + 1}-ITEM")(code)
    }
  }

  test("sequence() with padding") {
    val fields = List(
      FieldBuilder().name("padded_id").sequence(start = 1L, padding = 5)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val ids = df.select("padded_id").collect().map(_.getString(0))

    // Should generate: "00001", "00002", ..., "00010"
    ids.zipWithIndex.foreach { case (id, idx) =>
      val expected = f"${idx + 1}%05d"
      assertResult(expected)(id)
    }
  }

  test("sequence() with all parameters combined") {
    val fields = List(
      FieldBuilder().name("invoice_id").sequence(
        start = 2000L,
        step = 10L,
        prefix = "INV-",
        padding = 8,
        suffix = "-2024"
      )
    ).map(_.field)

    val testStep = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(testStep, "parquet", 0, 10)
    df.cache()

    val ids = df.select("invoice_id").collect().map(_.getString(0))

    // Should generate: "INV-00002000-2024", "INV-00002010-2024", ...
    ids.zipWithIndex.foreach { case (id, idx) =>
      val num = 2000L + (idx * 10L)
      val expected = f"INV-$num%08d-2024"
      assertResult(expected)(id)
    }
  }

  // ========== dailyBatchSequence() Tests ==========

  test("dailyBatchSequence() generates daily batch IDs with default format") {
    val fields = List(
      FieldBuilder().name("batch_id").dailyBatchSequence()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(10L)(df.count())
    val batchIds = df.select("batch_id").collect().map(_.getString(0))

    batchIds.zipWithIndex.foreach { case (batchId, idx) =>
      // Should start with "BATCH-" and end with "-001", "-002", etc.
      assert(batchId.startsWith("BATCH-"), s"Batch ID $batchId should start with BATCH-")
      assert(batchId.endsWith(f"-${idx}%03d"), 
        s"Batch ID $batchId should end with ${f"-${idx}%03d"}")
      
      // Should contain date in yyyyMMdd format (8 digits after BATCH-)
      val datePart = batchId.substring(6, 14)
      assert(datePart.matches("\\d{8}"), 
        s"Date part $datePart should be 8 digits in yyyyMMdd format")
    }
  }

  test("dailyBatchSequence() with custom prefix") {
    val fields = List(
      FieldBuilder().name("job_id").dailyBatchSequence(prefix = "JOB-")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(5L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 5)
    df.cache()

    val jobIds = df.select("job_id").collect().map(_.getString(0))

    jobIds.foreach { jobId =>
      assert(jobId.startsWith("JOB-"), s"Job ID $jobId should start with JOB-")
    }
  }

  test("dailyBatchSequence() with custom date format") {
    val fields = List(
      FieldBuilder().name("batch_id").dailyBatchSequence(dateFormat = "yyyy-MM-dd")
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(5L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 5)
    df.cache()

    val batchIds = df.select("batch_id").collect().map(_.getString(0))

    batchIds.foreach { batchId =>
      // Date format should be yyyy-MM-dd (10 characters)
      val datePart = batchId.substring(6, 16) // After "BATCH-"
      assert(datePart.matches("\\d{4}-\\d{2}-\\d{2}"), 
        s"Date part $datePart should match yyyy-MM-dd format")
    }
  }

  // ========== semanticVersion() Tests ==========

  test("semanticVersion() generates version numbers with default parameters") {
    val fields = List(
      FieldBuilder().name("version").semanticVersion()
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    assertResult(10L)(df.count())
    val versions = df.select("version").collect().map(_.getString(0))

    // Should generate: "1.0.0", "1.0.1", "1.0.2", ..., "1.0.9"
    versions.zipWithIndex.foreach { case (version, idx) =>
      assertResult(s"1.0.$idx")(version)
    }
  }

  test("semanticVersion() with custom major and minor versions") {
    val fields = List(
      FieldBuilder().name("version").semanticVersion(major = 2, minor = 5)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val versions = df.select("version").collect().map(_.getString(0))

    // Should generate: "2.5.0", "2.5.1", ..., "2.5.9"
    versions.zipWithIndex.foreach { case (version, idx) =>
      assertResult(s"2.5.$idx")(version)
    }
  }

  test("semanticVersion() with fixed patch number") {
    val fields = List(
      FieldBuilder().name("version").semanticVersion(major = 3, minor = 1, patchIncrement = false)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 10)
    df.cache()

    val versions = df.select("version").collect().map(_.getString(0))

    // All should be "3.1.0"
    versions.foreach { version =>
      assertResult("3.1.0")(version)
    }
  }

  // ========== Combined Tests ==========

  test("Multiple sequential helpers work together in single dataset") {
    val fields = List(
      FieldBuilder().name("id").sequence(start = 1L, padding = 5),
      FieldBuilder().name("order_number").sequence(prefix = "ORD-", start = 1000L),
      FieldBuilder().name("batch_id").dailyBatchSequence(),
      FieldBuilder().name("version").semanticVersion(major = 1, minor = 2)
    ).map(_.field)

    val step = Step("test", "parquet", Count(records = Some(20L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, 20)
    df.cache()

    assertResult(20L)(df.count())
    assertResult(Array("id", "order_number", "batch_id", "version"))(df.columns)

    val rows = df.collect()
    rows.zipWithIndex.foreach { case (row, idx) =>
      val id = row.getString(0)
      val orderNumber = row.getString(1)
      val batchId = row.getString(2)
      val version = row.getString(3)

      // ID should be padded
      assertResult(f"${idx + 1}%05d")(id)

      // Order number should have prefix
      assertResult(s"ORD-${1000 + idx}")(orderNumber)

      // Batch ID should have correct format
      assert(batchId.startsWith("BATCH-"))

      // Version should increment patch
      assertResult(s"1.2.$idx")(version)
    }
  }

  test("Sequential helpers produce deterministic results") {
    val fields = List(
      FieldBuilder().name("seq_id").sequence(start = 100L, step = 5L, prefix = "SEQ-")
    ).map(_.field)

    val step1 = Step("test1", "parquet", Count(records = Some(20L)), Map("path" -> "test"), fields)
    val df1 = dataGeneratorFactory.generateDataForStep(step1, "parquet", 0, 20)

    val step2 = Step("test2", "parquet", Count(records = Some(20L)), Map("path" -> "test"), fields)
    val df2 = dataGeneratorFactory.generateDataForStep(step2, "parquet", 0, 20)

    val ids1 = df1.select("seq_id").collect().map(_.getString(0))
    val ids2 = df2.select("seq_id").collect().map(_.getString(0))

    // Sequential generation should be deterministic (same values each time)
    ids1.zip(ids2).foreach { case (id1, id2) =>
      assertResult(id1)(id2)
    }
  }

  test("Sequential helpers maintain order across large datasets") {
    val fields = List(
      FieldBuilder().name("sequence_num").sequence(start = 1L, padding = 10)
    ).map(_.field)

    val numRecords = 1000L
    val step = Step("test", "parquet", Count(records = Some(numRecords)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(step, "parquet", 0, numRecords)
    df.cache()

    assertResult(numRecords)(df.count())
    val seqNums = df.select("sequence_num").collect().map(_.getString(0))

    // Verify all are unique
    assertResult(numRecords.toInt)(seqNums.distinct.length)

    // Verify sequential order
    seqNums.zipWithIndex.foreach { case (seqNum, idx) =>
      val expected = f"${idx + 1}%010d"
      assertResult(expected)(seqNum)
    }
  }

  test("Sequential helpers work with very large start values") {
    val fields = List(
      FieldBuilder().name("big_id").sequence(start = 999999999L, step = 1L)
    ).map(_.field)

    val testStep = Step("test", "parquet", Count(records = Some(10L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(testStep, "parquet", 0, 10)
    df.cache()

    val ids = df.select("big_id").collect().map(_.getString(0).toLong)

    ids.zipWithIndex.foreach { case (id, idx) =>
      assertResult(999999999L + idx)(id)
    }
  }

  test("Sequential helpers work with negative start values") {
    val fields = List(
      FieldBuilder().name("temp_offset").sequence(start = -10L, step = 1L)
    ).map(_.field)

    val testStep = Step("test", "parquet", Count(records = Some(20L)), Map("path" -> "test"), fields)
    val df = dataGeneratorFactory.generateDataForStep(testStep, "parquet", 0, 20)
    df.cache()

    val temps = df.select("temp_offset").collect().map(_.getString(0).toLong)

    // Should generate: -10, -9, -8, ..., 9
    temps.zipWithIndex.foreach { case (temp, idx) =>
      assertResult(-10L + idx)(temp)
    }
  }
}
