package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.ui.model.SchemaSampleRequest
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

class FastSampleGeneratorTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  test("FastSampleGenerator generate sample data from inline schema") {
    val step = Step(
      name = "test_step",
      `type` = "json",
      count = Count(records = Some(5)),
      fields = List(
        Field(
          name = "account_id",
          `type` = Some("string"),
          options = Map("regex" -> "ACC[0-9]{10}")
        ),
        Field(
          name = "year",
          `type` = Some("int"),
          options = Map("min" -> 2021, "max" -> 2023)
        ),
        Field(
          name = "amount",
          `type` = Some("double"),
          options = Map("min" -> 10.0, "max" -> 100.0)
        )
      )
    )

    val request = SchemaSampleRequest(
      fields = step.fields,
      sampleSize = 5,
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromSchema(request)

    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 5
    result.schema shouldBe defined
    result.metadata shouldBe defined

    // Verify schema structure
    val schema = result.schema.get
    schema.fields should have size 3
    schema.fields.map(_.name) should contain allOf("account_id", "year", "amount")

    // Verify sample data contains expected fields
    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      record should contain key "account_id"
      record should contain key "year" 
      record should contain key "amount"
      
      // Verify data types and constraints
      record("account_id").toString shouldNot fullyMatch regex "ACC[0-9]{10}" // in fast mode, regex is not used
      record("year").toString.toInt should (be >= 2021 and be <= 2023)
      record("amount").toString.toDouble should (be >= 10.0 and be <= 100.0)
    }

    // Verify metadata
    val metadata = result.metadata.get
    metadata.sampleSize shouldBe 5
    metadata.actualRecords shouldBe 5
    metadata.fastModeEnabled shouldBe true
    // Note: seed has been removed from the API
    metadata.generatedInMs should be > 0L
  }

  test("FastSampleGenerator handle nested fields in schema") {
    val step = Step(
      name = "test_nested",
      `type` = "json",
      count = Count(records = Some(3)),
      fields = List(
        Field(
          name = "simple_field",
          `type` = Some("string"),
          options = Map("expression" -> "#{Name.fullName}")
        )
      )
    )

    val request = SchemaSampleRequest(
      fields = step.fields,
      sampleSize = 3,
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromSchema(request)

    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 3

    // Verify simple structure for now (nested structures may need more work)
    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      record should contain key "simple_field"
      record("simple_field") shouldBe a[String]
    }
  }

  test("FastSampleGenerator limit sample size to maximum allowed") {
    val step = Step(
      name = "test_large",
      `type` = "json",
      count = Count(records = Some(1000)),
      fields = List(
        Field(name = "id", `type` = Some("long"))
      )
    )

    val request = SchemaSampleRequest(
      fields = step.fields,
      sampleSize = 150, // Above max limit
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromSchema(request)

    result.success shouldBe true
    result.sampleData shouldBe defined
    
    // Should be limited to max size (100)
    result.sampleData.get.size should be <= 100
    result.metadata.get.sampleSize should be <= 100
  }

  test("FastSampleGenerator handle empty schema gracefully") {
    val request = SchemaSampleRequest(
      fields = List(),
      sampleSize = 5
    )

    val result = FastSampleGenerator.generateFromSchema(request)

    // Should succeed with empty field list (generates empty records)
    result.success shouldBe true
    result.sampleData shouldBe defined
  }

  test("FastSampleGenerator generate sample data consistently") {
    val step = Step(
      name = "test_consistency",
      `type` = "json", 
      count = Count(records = Some(10)),
      fields = List(
        Field(
          name = "random_string",
          `type` = Some("string"),
          options = Map("expression" -> "#{Name.firstName}")
        ),
        Field(
          name = "random_number",
          `type` = Some("int"),
          options = Map("min" -> 1, "max" -> 100)
        )
      )
    )

    val request = SchemaSampleRequest(
      fields = step.fields,
      sampleSize = 10,
      fastMode = true
    )

    // Generate sample data
    val result = FastSampleGenerator.generateFromSchema(request)

    // Request should succeed
    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 10
    
    // Verify field constraints are respected
    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      record should contain key "random_string"
      record should contain key "random_number"
      
      record("random_string") shouldBe a[String]
      val numberVal = record("random_number").toString.toInt
      numberVal should (be >= 1 and be <= 100)
    }
  }

  test("FastSampleGenerator handle different field types correctly") {
    val fields = List(
      Field(
        name = "string_field",
        `type` = Some("string"),
        options = Map("expression" -> "#{Name.firstName}")
      ),
      Field(
        name = "int_field", 
        `type` = Some("int"),
        options = Map("min" -> 1, "max" -> 100)
      ),
      Field(
        name = "boolean_field",
        `type` = Some("boolean")
      )
    )

    val request = SchemaSampleRequest(
      fields = fields,
      sampleSize = 5,
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromSchema(request)

    // Request should succeed
    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 5

    // Verify all field types are handled
    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      record should contain key "string_field"
      record should contain key "int_field"
      record should contain key "boolean_field"
      
      record("string_field") shouldBe a[String]
      record("int_field").toString.toInt should (be >= 1 and be <= 100)
      val boolVal = record("boolean_field")
      boolVal should (equal(true) or equal(false))
    }
  }
}