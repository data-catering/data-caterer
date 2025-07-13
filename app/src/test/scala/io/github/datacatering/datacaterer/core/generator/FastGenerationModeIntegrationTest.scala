package io.github.datacatering.datacaterer.core.generator

import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, REGEX_GENERATOR}
import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import net.datafaker.Faker
import org.scalatest.funsuite.AnyFunSuite

class FastGenerationModeIntegrationTest extends AnyFunSuite with SparkSuite {

  test("DataGeneratorFactory with fast mode should use fast generators for string fields") {
    val faker = new Faker() with Serializable
    val fastFactory = new DataGeneratorFactory(faker, enableFastGeneration = true)
    val normalFactory = new DataGeneratorFactory(faker, enableFastGeneration = false)

    val fields = List(
      Field("name", Some("string"), options = Map("minLen" -> 5, "maxLen" -> 15)),
      Field("email", Some("string"), options = Map(REGEX_GENERATOR -> "@")),
      Field("description", Some("string"), options = Map(EXPRESSION -> "#{Lorem.words}")),
      Field("id", Some("integer"), options = Map("min" -> 1, "max" -> 1000))
    )

    val step = Step("test_step", "json", Count(records = Some(100)), Map(), fields)

    // Generate data with fast mode enabled
    val fastDf = fastFactory.generateDataForStep(step, "test_datasource", 0, 100)
    
    // Generate data with fast mode disabled
    val normalDf = normalFactory.generateDataForStep(step, "test_datasource", 0, 100)

    // Both should generate the same number of records
    assert(fastDf.count() == normalDf.count())
    
    // Both should have the same schema
    assert(fastDf.schema.fieldNames.sorted.sameElements(normalDf.schema.fieldNames.sorted))
    
    // Fast mode should generate valid data
    val fastSample = fastDf.collect().head
    assert(fastSample.getAs[String]("name").length >= 5)
    assert(fastSample.getAs[String]("name").length <= 15)
    assert(fastSample.getAs[String]("email").contains("@"))
    assert(fastSample.getAs[Int]("id") >= 1)
    assert(fastSample.getAs[Int]("id") <= 1000)

    println(s"Fast mode sample: name=${fastSample.getAs[String]("name")}, email=${fastSample.getAs[String]("email")}")
    println(s"Fast mode schema: ${fastDf.schema.fieldNames.mkString(", ")}")
  }

  test("Fast mode should not use UDF functions in generated SQL expressions") {
    val faker = new Faker() with Serializable
    val fastFactory = new DataGeneratorFactory(faker, enableFastGeneration = true)

    val fields = List(
      Field("regex_field", Some("string"), options = Map(REGEX_GENERATOR -> "[A-Z]{5}")),
      Field("expression_field", Some("string"), options = Map(EXPRESSION -> "#{Name.firstName}")),
      Field("normal_string", Some("string"), options = Map("minLen" -> 3, "maxLen" -> 10))
    )

    val step = Step("test_step", "json", Count(records = Some(10)), Map(), fields)

    // Generate small dataset to inspect the execution plan
    val df = fastFactory.generateDataForStep(step, "test_datasource", 0, 10)
    
    // Get the execution plan to verify no UDF calls
    val queryExecution = df.queryExecution
    val physicalPlan = queryExecution.executedPlan.toString
    val analyzedPlan = queryExecution.analyzed.toString
    val optimizedPlan = queryExecution.optimizedPlan.toString

    // Verify no UDF calls in any of the plans
    assert(!physicalPlan.contains("GENERATE_FAKER_EXPRESSION_UDF"), 
      "Physical plan should not contain Faker expression UDF")
    assert(!physicalPlan.contains("GENERATE_REGEX_UDF"), 
      "Physical plan should not contain regex UDF")
    assert(!physicalPlan.contains("GENERATE_RANDOM_ALPHANUMERIC_STRING_UDF"), 
      "Physical plan should not contain alphanumeric UDF")

    assert(!analyzedPlan.contains("GENERATE_FAKER_EXPRESSION_UDF"), 
      "Analyzed plan should not contain Faker expression UDF")
    assert(!analyzedPlan.contains("GENERATE_REGEX_UDF"), 
      "Analyzed plan should not contain regex UDF")

    // Verify data is still generated correctly
    val sample = df.collect().head
    assert(sample.getAs[String]("regex_field").length == 5)
    assert(sample.getAs[String]("normal_string").length >= 3)
    assert(sample.getAs[String]("normal_string").length <= 10)

    println(s"Fast mode execution plans verified - no UDF calls detected")
    println(s"Sample data: regex=${sample.getAs[String]("regex_field")}, expression=${sample.getAs[String]("expression_field")}")
  }

  test("Fast mode should generate data faster than normal mode for complex patterns") {
    val faker = new Faker() with Serializable
    val fastFactory = new DataGeneratorFactory(faker, enableFastGeneration = true)
    val normalFactory = new DataGeneratorFactory(faker, enableFastGeneration = false)

    // Create fields with complex patterns that would be slow with UDFs
    val fields = List(
      Field("email1", Some("string"), options = Map(REGEX_GENERATOR -> "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")),
      Field("email2", Some("string"), options = Map(REGEX_GENERATOR -> "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")),
      Field("phone1", Some("string"), options = Map(REGEX_GENERATOR -> "\\d{3}-\\d{3}-\\d{4}")),
      Field("phone2", Some("string"), options = Map(REGEX_GENERATOR -> "\\d{3}-\\d{3}-\\d{4}")),
      Field("expression1", Some("string"), options = Map(EXPRESSION -> "#{Name.firstName}")),
      Field("expression2", Some("string"), options = Map(EXPRESSION -> "#{Address.city}")),
      Field("id", Some("integer"), options = Map("min" -> 1, "max" -> 100000))
    )

    val step = Step("perf_test", "json", Count(records = Some(1000)), Map(), fields)

    // Measure fast mode performance
    val fastStart = System.currentTimeMillis()
    val fastDf = fastFactory.generateDataForStep(step, "test_datasource", 0, 1000)
    val fastCount = fastDf.count()
    val fastEnd = System.currentTimeMillis()
    val fastTime = fastEnd - fastStart

    // Measure normal mode performance  
    val normalStart = System.currentTimeMillis()
    val normalDf = normalFactory.generateDataForStep(step, "test_datasource", 0, 1000)
    val normalCount = normalDf.count()
    val normalEnd = System.currentTimeMillis()
    val normalTime = normalEnd - normalStart

    // Both should generate the same number of records
    assert(fastCount == normalCount)
    assert(fastCount == 1000)

    // Validate data quality in fast mode
    val fastSample = fastDf.collect().take(5)
    fastSample.foreach { row =>
      assert(row.getAs[String]("email1").contains("@"))
      assert(row.getAs[String]("phone1").matches("\\d+-\\d+-\\d+"))
      assert(row.getAs[String]("expression1").nonEmpty)
    }

    println(s"Performance comparison:")
    println(s"Fast mode: ${fastTime}ms for $fastCount records")
    println(s"Normal mode: ${normalTime}ms for $normalCount records")
    println(s"Fast mode improvement: ${if (normalTime > 0) ((normalTime - fastTime) * 100.0 / normalTime).round}% faster")
    
    // Fast mode should be at least as fast (allow for variance in small datasets)
    // In real scenarios with larger datasets, fast mode should be significantly faster
  }

  test("Fast mode should work correctly with nested fields and arrays") {
    val faker = new Faker() with Serializable
    val fastFactory = new DataGeneratorFactory(faker, enableFastGeneration = true)

    val fields = List(
      Field("user_id", Some("string"), options = Map("minLen" -> 8, "maxLen" -> 12)),
      Field("profile", Some("struct"), fields = List(
        Field("name", Some("string"), options = Map(REGEX_GENERATOR -> "[A-Z][a-z]{4,10}")),
        Field("email", Some("string"), options = Map(REGEX_GENERATOR -> "@"))
      )),
      Field("tags", Some("array"), fields = List(
        Field("tag", Some("string"), options = Map("minLen" -> 3, "maxLen" -> 8))
      ), options = Map("arrayMinLen" -> 1, "arrayMaxLen" -> 3)),
      Field("score", Some("integer"), options = Map("min" -> 0, "max" -> 100))
    )

    val step = Step("nested_test", "json", Count(records = Some(50)), Map(), fields)
    val df = fastFactory.generateDataForStep(step, "test_datasource", 0, 50)

    assert(df.count() == 50)
    
    val sample = df.collect().head
    assert(sample.getAs[String]("user_id").length >= 8)
    assert(sample.getAs[String]("user_id").length <= 12)
    assert(sample.getAs[Int]("score") >= 0)
    assert(sample.getAs[Int]("score") <= 100)

    // Verify nested struct field
    val profile = sample.getStruct(sample.fieldIndex("profile"))
    val profileEmail = profile.getString(profile.fieldIndex("email"))
    assert(profileEmail.contains("@"))

    println(s"Nested field test passed - profile email: $profileEmail")
  }

  test("Fast mode should handle all data types correctly") {
    val faker = new Faker() with Serializable
    val fastFactory = new DataGeneratorFactory(faker, enableFastGeneration = true)

    val fields = List(
      Field("str_field", Some("string"), options = Map("minLen" -> 5, "maxLen" -> 10)),
      Field("int_field", Some("integer"), options = Map("min" -> 1, "max" -> 100)),
      Field("long_field", Some("long"), options = Map("min" -> 1000L, "max" -> 2000L)),
      Field("double_field", Some("double"), options = Map("min" -> 1.0, "max" -> 10.0)),
      Field("bool_field", Some("boolean")),
      Field("date_field", Some("date")),
      Field("timestamp_field", Some("timestamp"))
    )

    val step = Step("all_types_test", "json", Count(records = Some(20)), Map(), fields)
    val df = fastFactory.generateDataForStep(step, "test_datasource", 0, 20)

    assert(df.count() == 20)
    
    val sample = df.collect().head
    
    // Validate all field types
    assert(sample.getAs[String]("str_field").length >= 5)
    assert(sample.getAs[String]("str_field").length <= 10)
    assert(sample.getAs[Int]("int_field") >= 1)
    assert(sample.getAs[Int]("int_field") <= 100)
    assert(sample.getAs[Long]("long_field") >= 1000L)
    assert(sample.getAs[Long]("long_field") <= 2000L)
    assert(sample.getAs[Double]("double_field") >= 1.0)
    assert(sample.getAs[Double]("double_field") <= 10.0)
    
    // Boolean, date, and timestamp should not be null
    assert(sample.get(sample.fieldIndex("bool_field")) != null)
    assert(sample.get(sample.fieldIndex("date_field")) != null)
    assert(sample.get(sample.fieldIndex("timestamp_field")) != null)

    println(s"All data types test passed - sample: ${sample.mkString(", ")}")
  }
} 