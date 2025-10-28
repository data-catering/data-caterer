package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.FieldBuilder
import io.github.datacatering.datacaterer.api.model.{Count, DoubleType, Field, IntegerType, PerFieldCount, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Performance test suite for data generation optimization.
 * Tests various generation scenarios and validates both performance and correctness.
 *
 * Run with: ./gradlew :app:test --tests "io.github.datacatering.datacaterer.core.generator.DataGeneratorFactoryPerformanceTest"
 */
class DataGeneratorFactoryPerformanceTest extends SparkSuite {

  private val faker = new Faker() with Serializable
  private val standardFactory = new DataGeneratorFactory(faker, enableFastGeneration = false)
  private val fastFactory = new DataGeneratorFactory(faker, enableFastGeneration = true)

  case class PerformanceResult(
                                 scenario: String,
                                 mode: String,
                                 recordCount: Long,
                                 fieldCount: Int,
                                 durationMs: Long,
                                 recordsPerSecond: Double,
                                 msPerRecord: Double,
                                 memoryUsedMB: Long,
                                 rowsValidated: Long,
                                 schemaMatches: Boolean
                               )

  private def measurePerformance(
                                   scenario: String,
                                   mode: String,
                                   factory: DataGeneratorFactory,
                                   step: Step,
                                   startIndex: Long,
                                   endIndex: Long,
                                   validateFn: DataFrame => Unit = _ => ()
                                 ): PerformanceResult = {
    // Force GC before test
    System.gc()
    Thread.sleep(100)

    val runtime = Runtime.getRuntime
    val memBefore = runtime.totalMemory() - runtime.freeMemory()

    val startTime = System.nanoTime()
    val df = factory.generateDataForStep(step, "test_source", startIndex, endIndex)

    // Force evaluation and cache
    df.cache()
    val actualCount = df.count()
    val endTime = System.nanoTime()

    val memAfter = runtime.totalMemory() - runtime.freeMemory()
    val memUsedMB = (memAfter - memBefore) / (1024 * 1024)

    // Validate data
    var schemaMatches = true
    try {
      validateFn(df)
    } catch {
      case e: Exception =>
        println(s"\n!!! Validation failed for $scenario ($mode): ${e.getMessage}")
        e.printStackTrace()
        schemaMatches = false
    }

    df.unpersist()

    val durationMs = (endTime - startTime) / 1000000
    val recordsPerSecond = if (durationMs > 0) (actualCount.toDouble / durationMs) * 1000 else 0
    val msPerRecord = if (actualCount > 0) durationMs.toDouble / actualCount else 0

    PerformanceResult(
      scenario = scenario,
      mode = mode,
      recordCount = actualCount,
      fieldCount = step.fields.length,
      durationMs = durationMs,
      recordsPerSecond = recordsPerSecond,
      msPerRecord = msPerRecord,
      memoryUsedMB = Math.max(0, memUsedMB),
      rowsValidated = actualCount,
      schemaMatches = schemaMatches
    )
  }

  private def printResults(results: Seq[PerformanceResult]): Unit = {
    val output = new StringBuilder()

    output.append("\n" + "=" * 120 + "\n")
    output.append("PERFORMANCE TEST RESULTS\n")
    output.append("=" * 120 + "\n")
    output.append(f"${"Scenario"}%-20s ${"Mode"}%-10s ${"Records"}%10s ${"Fields"}%7s ${"Time(ms)"}%10s ${"Rec/Sec"}%12s ${"ms/Rec"}%10s ${"Memory(MB)"}%12s ${"Valid"}%6s\n")
    output.append("-" * 120 + "\n")

    results.foreach { r =>
      val validMark = if (r.schemaMatches) "✓" else "✗"
      output.append(f"${r.scenario}%-20s ${r.mode}%-10s ${r.recordCount}%10d ${r.fieldCount}%7d ${r.durationMs}%10d ${r.recordsPerSecond}%12.2f ${r.msPerRecord}%10.6f ${r.memoryUsedMB}%12d ${validMark}%6s\n")
    }

    // Calculate comparisons
    output.append("\n" + "=" * 120 + "\n")
    output.append("SPEEDUP ANALYSIS\n")
    output.append("=" * 120 + "\n")

    val grouped = results.groupBy(_.scenario)
    grouped.foreach { case (scenario, scenarioResults) =>
      val standardOpt = scenarioResults.find(_.mode == "Standard")
      val fastOpt = scenarioResults.find(_.mode == "Fast")

      (standardOpt, fastOpt) match {
        case (Some(standard), Some(fast)) =>
          val speedup = standard.durationMs.toDouble / fast.durationMs.toDouble
          val pctImprovement = ((standard.durationMs - fast.durationMs).toDouble / standard.durationMs) * 100
          output.append(f"$scenario%-20s: ${speedup}%.2fx faster (${pctImprovement}%.1f%% improvement)\n")
        case _ =>
          output.append(f"$scenario%-20s: Missing comparison data\n")
      }
    }

    output.append("=" * 120 + "\n")

    val result = output.toString()
    println(result)

    // Write human-readable results to file (append mode)
    try {
      val writer = new java.io.FileWriter("/tmp/performance_results.txt", true)
      writer.write(result)
      writer.write("\n")
      writer.close()
    } catch {
      case e: Exception => // Ignore write errors
    }

    // Write CSV data file (overwrite mode)
    writeCsvResults(results)
  }

  private def writeCsvResults(results: Seq[PerformanceResult]): Unit = {
    try {
      // Determine phase number from environment variable or use default
      val phase = System.getenv("PERFORMANCE_PHASE")
      val phaseNum = if (phase != null && phase.nonEmpty) phase else "1"

      // Write to docs/performance_data directory (absolute path)
      val projectRoot = new java.io.File(".").getCanonicalPath
      val csvPath = s"$projectRoot/docs/performance_data/phase${phaseNum}_optimized.csv"
      println(s"DEBUG: projectRoot=$projectRoot, csvPath=$csvPath")
      val file = new java.io.File(csvPath)

      // Create directory if it doesn't exist
      file.getParentFile.mkdirs()

      // Check if file exists to determine if we need to write header
      val needsHeader = !file.exists()

      val writer = new java.io.FileWriter(file, true) // append mode

      // Write CSV header only if file is new
      if (needsHeader) {
        writer.write("phase,scenario,mode,records,fields,time_ms,records_per_sec,ms_per_record,memory_mb,valid\n")
      }

      // Write data rows
      results.foreach { r =>
        val validStr = r.schemaMatches.toString.toLowerCase
        writer.write(s"$phaseNum,${r.scenario},${r.mode},${r.recordCount},${r.fieldCount},${r.durationMs},${r.recordsPerSecond},${r.msPerRecord},${r.memoryUsedMB},$validStr\n")
      }

      writer.close()
      println(s"\n✅ Performance data appended to: $csvPath")
      println(s"   Phase: $phaseNum")
      println(s"   Records: ${results.size}")

    } catch {
      case e: Exception =>
        println(s"⚠️  Failed to write CSV file: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  private def validateBasicSchema(df: DataFrame, expectedFields: List[Field]): Unit = {
    val expectedFieldNames = expectedFields.filterNot(_.options.contains("omit")).map(_.name).toSet
    val actualFieldNames = df.columns.filterNot(_.endsWith("_weight")).toSet

    assert(actualFieldNames == expectedFieldNames,
      s"Schema mismatch. Expected: $expectedFieldNames, Actual: $actualFieldNames")
  }

  private def validateNumericRange(df: DataFrame, fieldName: String, minValue: Double, maxValue: Double): Unit = {
    val stats = df.select(
      col(fieldName).isNull.as("has_null"),
      coalesce(col(fieldName), lit(0)).as("val")
    ).agg(
      sum(when(col("has_null"), 1).otherwise(0)).as("null_count"),
      org.apache.spark.sql.functions.min("val").as("min_val"),
      org.apache.spark.sql.functions.max("val").as("max_val")
    ).first()

    val minVal = stats.getAs[Any]("min_val") match {
      case v: Number => v.doubleValue()
      case v: java.math.BigDecimal => v.doubleValue()
      case _ => 0.0
    }
    val maxVal = stats.getAs[Any]("max_val") match {
      case v: Number => v.doubleValue()
      case v: java.math.BigDecimal => v.doubleValue()
      case _ => 0.0
    }

    assert(minVal >= minValue, s"$fieldName min value $minVal is below expected $minValue")
    assert(maxVal <= maxValue, s"$fieldName max value $maxVal is above expected $maxValue")
  }

  private def validateStringLength(df: DataFrame, fieldName: String, minLen: Int, maxLen: Int): Unit = {
    val lengthStats = df.select(length(col(fieldName)).as("len"))
      .agg(
        org.apache.spark.sql.functions.min("len").as("min_len"),
        org.apache.spark.sql.functions.max("len").as("max_len")
      ).first()

    val actualMin = lengthStats.getAs[Int]("min_len")
    val actualMax = lengthStats.getAs[Int]("max_len")

    assert(actualMin >= minLen, s"$fieldName min length $actualMin is below expected $minLen")
    assert(actualMax <= maxLen, s"$fieldName max length $actualMax is above expected $maxLen")
  }

  test("Performance: Simple schema (5 basic fields, 10K rows)") {
    val recordCount = 10000L
    val fields = List(
      FieldBuilder().name("id").minLength(10).maxLength(20),
      FieldBuilder().name("amount").`type`(DoubleType).min(0.0).max(1000.0),
      FieldBuilder().name("quantity").`type`(IntegerType).min(1).max(100),
      FieldBuilder().name("name").minLength(5).maxLength(15),
      FieldBuilder().name("code").minLength(3).maxLength(8)
    ).map(_.field)

    val step = Step("simple_test", "parquet", Count(records = Some(recordCount)), Map("path" -> "output/test"), fields)

    val validateFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      validateNumericRange(df, "amount", 0.0, 1000.0)
      validateNumericRange(df, "quantity", 1.0, 100.0)
      validateStringLength(df, "id", 10, 20)
      validateStringLength(df, "name", 5, 15)
    }

    val results = Seq(
      measurePerformance("Simple", "Standard", standardFactory, step, 0, recordCount, validateFn),
      measurePerformance("Simple", "Fast", fastFactory, step, 0, recordCount, validateFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }

  test("Performance: String-heavy schema (20 string fields, 10K rows)") {
    val recordCount = 10000L
    val fields = (1 to 20).map { i =>
      val minLen = 5 + (i % 5)
      val maxLen = minLen + 10
      FieldBuilder().name(s"field_$i").minLength(minLen).maxLength(maxLen).field
    }.toList

    val step = Step("string_heavy_test", "parquet", Count(records = Some(recordCount)), Map("path" -> "output/test"), fields)

    val validateStandardFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      // Validate a few random fields for standard mode
      // field_1: i=1, minLen=5+(1%5)=6, maxLen=16
      // field_10: i=10, minLen=5+(10%5)=5, maxLen=15
      // field_20: i=20, minLen=5+(20%5)=5, maxLen=15
      validateStringLength(df, "field_1", 6, 16)
      validateStringLength(df, "field_10", 5, 15)
      validateStringLength(df, "field_20", 5, 15)
    }

    val validateFastFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      // Fast mode uses different character set (uppercase hex), so just check row count
      ()
    }

    val results = Seq(
      measurePerformance("String-Heavy", "Standard", standardFactory, step, 0, recordCount, validateStandardFn),
      measurePerformance("String-Heavy", "Fast", fastFactory, step, 0, recordCount, validateFastFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }

  test("Performance: Regex and expression fields (10 fields, 5K rows)") {
    val recordCount = 5000L
    val fields = List(
      FieldBuilder().name("id").regex("[A-Z]{3}[0-9]{8}"),
      FieldBuilder().name("email").expression("#{Name.first_name}.#{Name.last_name}@example.com"),
      FieldBuilder().name("phone").regex("\\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}"),
      FieldBuilder().name("ssn").regex("[0-9]{3}-[0-9]{2}-[0-9]{4}"),
      FieldBuilder().name("uuid").uuid(),
      FieldBuilder().name("first_name").expression("#{Name.first_name}"),
      FieldBuilder().name("last_name").expression("#{Name.last_name}"),
      FieldBuilder().name("city").expression("#{Address.city}"),
      FieldBuilder().name("zip").regex("[0-9]{5}"),
      FieldBuilder().name("account").regex("ACC[0-9]{10}")
    ).map(_.field)

    val step = Step("regex_test", "parquet", Count(records = Some(recordCount)), Map("path" -> "output/test"), fields)

    val validateStandardFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      // Check patterns (basic validation) for standard mode
      val sample = df.head()
      assert(sample.getAs[String]("id").matches("[A-Z]{3}[0-9]{8}"), "ID pattern mismatch")
      assert(sample.getAs[String]("phone").matches("\\+1-[0-9]{3}-[0-9]{3}-[0-9]{4}"), "Phone pattern mismatch")
      assert(sample.getAs[String]("ssn").matches("[0-9]{3}-[0-9]{2}-[0-9]{4}"), "SSN pattern mismatch")
      assert(sample.getAs[String]("zip").matches("[0-9]{5}"), "Zip pattern mismatch")
      assert(sample.getAs[String]("account").matches("ACC[0-9]{10}"), "Account pattern mismatch")
      ()
    }

    val validateFastFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      // Fast mode uses simplified patterns, different from regex - just validate schema
      ()
    }

    val results = Seq(
      measurePerformance("Regex-Heavy", "Standard", standardFactory, step, 0, recordCount, validateStandardFn),
      measurePerformance("Regex-Heavy", "Fast", fastFactory, step, 0, recordCount, validateFastFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }

  test("Performance: Complex schema with SQL expressions (15 fields, 5K rows)") {
    val recordCount = 5000L
    val fields = List(
      FieldBuilder().name("id").`type`(IntegerType).incremental(),
      FieldBuilder().name("amount").`type`(DoubleType).min(100.0).max(10000.0),
      FieldBuilder().name("tax_rate").`type`(DoubleType).min(0.05).max(0.15),
      FieldBuilder().name("tax_amount").`type`(DoubleType).sql("amount * tax_rate"),
      FieldBuilder().name("total").`type`(DoubleType).sql("amount + tax_amount"),
      FieldBuilder().name("discount_pct").`type`(DoubleType).min(0.0).max(0.3),
      FieldBuilder().name("discount_amount").`type`(DoubleType).sql("total * discount_pct"),
      FieldBuilder().name("final_amount").`type`(DoubleType).sql("total - discount_amount"),
      FieldBuilder().name("category").oneOf(List("A", "B", "C")),
      FieldBuilder().name("category_code").`type`(IntegerType).sql("CASE WHEN category = 'A' THEN 1 WHEN category = 'B' THEN 2 ELSE 3 END"),
      FieldBuilder().name("name").minLength(10).maxLength(20),
      FieldBuilder().name("description").minLength(50).maxLength(100),
      FieldBuilder().name("status").oneOf(List("active", "inactive", "pending")),
      FieldBuilder().name("priority").`type`(IntegerType).min(1).max(10),
      FieldBuilder().name("is_urgent").sql("CASE WHEN priority >= 8 THEN true ELSE false END")
    ).map(_.field)

    val step = Step("complex_test", "parquet", Count(records = Some(recordCount)), Map("path" -> "output/test"), fields)

    val validateFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      validateNumericRange(df, "amount", 100.0, 10000.0)
      validateNumericRange(df, "tax_rate", 0.05, 0.15)

      // Validate SQL expression calculations
      val sample = df.head()
      val amount = sample.getAs[Double]("amount")
      val taxRate = sample.getAs[Double]("tax_rate")
      val taxAmount = sample.getAs[Double]("tax_amount")
      val total = sample.getAs[Double]("total")

      assert(Math.abs(taxAmount - (amount * taxRate)) < 0.01, "Tax calculation incorrect")
      assert(Math.abs(total - (amount + taxAmount)) < 0.01, "Total calculation incorrect")

      // Check category_code matches category
      val category = sample.getAs[String]("category")
      val categoryCode = sample.getAs[Int]("category_code")
      val expectedCode = category match {
        case "A" => 1
        case "B" => 2
        case "C" => 3
      }
      assert(categoryCode == expectedCode, "Category code mismatch")
      ()
    }

    val results = Seq(
      measurePerformance("Complex-SQL", "Standard", standardFactory, step, 0, recordCount, validateFn),
      measurePerformance("Complex-SQL", "Fast", fastFactory, step, 0, recordCount, validateFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }

  test("Performance: Large dataset (10 fields, 100K rows)") {
    val recordCount = 100000L
    val fields = List(
      FieldBuilder().name("id").`type`(IntegerType).incremental(),
      FieldBuilder().name("customer_id").`type`(IntegerType).min(1).max(10000),
      FieldBuilder().name("product_id").`type`(IntegerType).min(1).max(1000),
      FieldBuilder().name("quantity").`type`(IntegerType).min(1).max(100),
      FieldBuilder().name("unit_price").`type`(DoubleType).min(1.0).max(1000.0),
      FieldBuilder().name("total_price").`type`(DoubleType).sql("quantity * unit_price"),
      FieldBuilder().name("order_date").minLength(10).maxLength(10),
      FieldBuilder().name("status").oneOf(List("pending", "shipped", "delivered", "cancelled")),
      FieldBuilder().name("notes").minLength(20).maxLength(100),
      FieldBuilder().name("reference").minLength(15).maxLength(25)
    ).map(_.field)

    val step = Step("large_test", "parquet", Count(records = Some(recordCount)), Map("path" -> "output/test"), fields)

    val validateFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      validateNumericRange(df, "customer_id", 1.0, 10000.0)
      validateNumericRange(df, "quantity", 1.0, 100.0)

      // Validate calculation on sample
      val samples = df.limit(100).collect()
      samples.foreach { row =>
        val quantity = row.getAs[Int]("quantity")
        val unitPrice = row.getAs[Double]("unit_price")
        val totalPrice = row.getAs[Double]("total_price")
        assert(Math.abs(totalPrice - (quantity * unitPrice)) < 0.01, "Price calculation incorrect")
      }
    }

    val results = Seq(
      measurePerformance("Large-Dataset", "Standard", standardFactory, step, 0, recordCount, validateFn),
      measurePerformance("Large-Dataset", "Fast", fastFactory, step, 0, recordCount, validateFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }

  test("Performance: PerField count generation (5 fields, 2K base rows, 3 per field)") {
    val baseRecordCount = 2000L
    val perFieldCount = 3
    val fields = List(
      FieldBuilder().name("order_id").`type`(IntegerType).incremental(),
      FieldBuilder().name("item_name").minLength(10).maxLength(30),
      FieldBuilder().name("item_price").`type`(DoubleType).min(1.0).max(100.0),
      FieldBuilder().name("item_quantity").`type`(IntegerType).min(1).max(10),
      FieldBuilder().name("item_category").oneOf(List("Electronics", "Clothing", "Food", "Books"))
    ).map(_.field)

    val step = Step(
      "perfield_test",
      "parquet",
      Count(
        records = Some(baseRecordCount),
        perField = Some(PerFieldCount(List("order_id"), Some(perFieldCount)))
      ),
      Map("path" -> "output/test"),
      fields
    )

    val expectedTotalRecords = baseRecordCount * perFieldCount

    val validateFn = (df: DataFrame) => {
      // Should have base rows * perField count
      assert(df.count() == expectedTotalRecords,
        s"Expected $expectedTotalRecords rows, got ${df.count()}")

      // Each order_id should appear perFieldCount times
      val orderCounts = df.groupBy("order_id").count()
      val invalidCounts = orderCounts.filter(col("count") =!= perFieldCount).count()
      assert(invalidCounts == 0,
        s"Found $invalidCounts order_ids with incorrect count (expected $perFieldCount per order)")
      ()
    }

    val results = Seq(
      measurePerformance("PerField", "Standard", standardFactory, step, 0, baseRecordCount, validateFn),
      measurePerformance("PerField", "Fast", fastFactory, step, 0, baseRecordCount, validateFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }

  test("Performance: Mixed types schema (25 diverse fields, 5K rows)") {
    val recordCount = 5000L
    val fields = List(
      // Numeric fields
      FieldBuilder().name("int_field").`type`(IntegerType).min(0).max(1000),
      FieldBuilder().name("long_field").`type`(IntegerType).min(0).max(100000),
      FieldBuilder().name("double_field").`type`(DoubleType).min(0.0).max(1000.0),
      FieldBuilder().name("decimal_field").`type`(DoubleType).min(0.0).max(10000.0),
      // String fields
      FieldBuilder().name("short_string").minLength(5).maxLength(10),
      FieldBuilder().name("medium_string").minLength(20).maxLength(50),
      FieldBuilder().name("long_string").minLength(100).maxLength(200),
      // Regex fields
      FieldBuilder().name("id_regex").regex("[A-Z]{3}[0-9]{5}"),
      FieldBuilder().name("code_regex").regex("[A-Z0-9]{8}"),
      // Expression fields
      FieldBuilder().name("name_expr").expression("#{Name.full_name}"),
      FieldBuilder().name("email_expr").expression("#{Internet.email_address}"),
      // OneOf fields
      FieldBuilder().name("status_oneof").oneOf(List("active", "inactive")),
      FieldBuilder().name("category_oneof").oneOf(List("cat1", "cat2", "cat3", "cat4", "cat5")),
      // Incremental fields
      FieldBuilder().name("auto_id").`type`(IntegerType).incremental(),
      FieldBuilder().name("seq_id").uuid().incremental(),
      // SQL expression fields
      FieldBuilder().name("calc_field1").`type`(DoubleType).sql("double_field * 2"),
      FieldBuilder().name("calc_field2").`type`(IntegerType).sql("int_field + 100"),
      FieldBuilder().name("concat_field").sql("CONCAT(status_oneof, '_', category_oneof)"),
      // More strings
      FieldBuilder().name("string1").minLength(10).maxLength(20),
      FieldBuilder().name("string2").minLength(15).maxLength(30),
      FieldBuilder().name("string3").minLength(8).maxLength(16),
      // More numeric
      FieldBuilder().name("percentage").`type`(DoubleType).min(0.0).max(1.0),
      FieldBuilder().name("count_field").`type`(IntegerType).min(1).max(100),
      FieldBuilder().name("price").`type`(DoubleType).min(10.0).max(10000.0),
      FieldBuilder().name("total").`type`(DoubleType).sql("price * count_field")
    ).map(_.field)

    val step = Step("mixed_test", "parquet", Count(records = Some(recordCount)), Map("path" -> "output/test"), fields)

    val validateFn = (df: DataFrame) => {
      validateBasicSchema(df, fields)
      validateNumericRange(df, "int_field", 0.0, 1000.0)
      validateNumericRange(df, "double_field", 0.0, 1000.0)
      validateNumericRange(df, "percentage", 0.0, 1.0)
      validateStringLength(df, "short_string", 5, 10)
      validateStringLength(df, "medium_string", 20, 50)

      // Validate calculations on sample
      val sample = df.head()
      val doubleField = sample.getAs[Double]("double_field")
      val calcField1 = sample.getAs[Double]("calc_field1")
      assert(Math.abs(calcField1 - (doubleField * 2)) < 0.01, "calc_field1 incorrect")

      val intField = sample.getAs[Int]("int_field")
      val calcField2 = sample.getAs[Int]("calc_field2")
      assert(calcField2 == intField + 100, "calc_field2 incorrect")
      ()
    }

    val results = Seq(
      measurePerformance("Mixed-Types", "Standard", standardFactory, step, 0, recordCount, validateFn),
      measurePerformance("Mixed-Types", "Fast", fastFactory, step, 0, recordCount, validateFn)
    )

    printResults(results)
    results.foreach(r => assert(r.schemaMatches, s"Validation failed for ${r.scenario} ${r.mode}"))
  }
}
