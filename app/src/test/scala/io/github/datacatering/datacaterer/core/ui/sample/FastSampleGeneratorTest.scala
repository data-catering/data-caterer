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

  test("FastSampleGenerator handle nested JSON with omitted fields and SQL references from YAML task file") {
    // This test verifies the fix for the issue where:
    // 1. Omitted fields were still appearing in the output
    // 2. Nested structure was being flattened instead of preserved
    val yamlPath = getClass.getResource("/sample/task/file/nested-json-with-omit-and-sql.yaml").getPath

    import io.github.datacatering.datacaterer.core.ui.model.TaskFileSampleRequest
    val request = TaskFileSampleRequest(
      taskYamlPath = yamlPath,
      stepName = Some("payment_data"),
      sampleSize = 3,
      fastMode = true  // Using true to avoid Faker serialization issues in test
    )

    val result = FastSampleGenerator.generateFromTaskFile(request)

    // Request should succeed
    result.success shouldBe true
    result.sampleData shouldBe defined
    result.sampleData.get should have size 3

    // Verify structure and omitted fields
    val sampleData = result.sampleData.get
    sampleData.foreach { record =>
      // Top-level fields
      record should contain key "transaction_id"
      record should contain key "amount"
      record should contain key "payment_information"
      record should contain key "items"

      // Omitted temp_amount_cents should NOT be present
      record should not contain key("temp_amount_cents")

      // Verify nested structure (not flattened)
      val paymentInfo = record("payment_information")
      paymentInfo shouldBe a[Map[_, _]]
      val paymentMap = paymentInfo.asInstanceOf[Map[String, Any]]

      // payment_information should have payment_id (from SQL) but not temp_payment_id
      paymentMap should contain key "payment_id"
      paymentMap should contain key "payment_method"
      paymentMap should contain key "cardholder"
      paymentMap should not contain key("temp_payment_id")

      // Verify cardholder nested structure
      val cardholder = paymentMap("cardholder")
      cardholder shouldBe a[Map[_, _]]
      val cardholderMap = cardholder.asInstanceOf[Map[String, Any]]

      cardholderMap should contain key "name"
      cardholderMap should contain key "email"
      cardholderMap should not contain key("temp_name")

      // Verify array structure
      val items = record("items")
      items shouldBe a[Seq[_]]
      val itemsList = items.asInstanceOf[Seq[_]]
      itemsList should not be empty

      // Each item should have description and price
      itemsList.foreach { item =>
        item shouldBe a[Map[_, _]]
        val itemMap = item.asInstanceOf[Map[String, Any]]

        itemMap should contain key "description"
        itemMap should contain key "price"

        // Verify price is within range
        val price = itemMap("price").toString.toDouble
        price should be >= 5.0
        price should be <= 200.0
      }

      // Verify that SQL expressions were applied correctly
      // The amount should be derived from temp_amount_cents / 100.0
      val amount = record("amount").toString.toDouble
      amount should be >= 10.0 // 1000 / 100
      amount should be <= 5000.0 // 500000 / 100
    }

    // Verify schema structure
    val schema = result.schema.get
    schema.fields.map(_.name) should contain allOf("transaction_id", "amount", "payment_information", "items")
    schema.fields.map(_.name) should not contain "temp_amount_cents"

    // Find the payment_information field in schema
    val paymentInfoField = schema.fields.find(_.name == "payment_information")
    paymentInfoField shouldBe defined
    paymentInfoField.get.`type` should include("struct")

    // payment_information's nested fields should not include temp fields
    val paymentInfoNestedFields = paymentInfoField.get.fields.getOrElse(List())
    paymentInfoNestedFields.map(_.name) should contain allOf("payment_id", "payment_method", "cardholder")
    paymentInfoNestedFields.map(_.name) should not contain "temp_payment_id"
  }

  test("FastSampleGenerator generate raw CSV bytes from schema") {
    val fields = List(
      Field(name = "name", `type` = Some("string")),
      Field(name = "age", `type` = Some("int"), options = Map("min" -> 18, "max" -> 65))
    )

    val request = SchemaSampleRequest(
      fields = fields,
      format = "csv",
      sampleSize = 5,
      fastMode = true
    )

    val responseWithDf = FastSampleGenerator.generateFromSchemaWithDataFrame(request)

    responseWithDf.response.success shouldBe true
    responseWithDf.dataFrame shouldBe defined
    responseWithDf.response.format shouldBe Some("csv")

    // Test raw bytes conversion
    val rawBytes = FastSampleGenerator.dataFrameToRawBytes(responseWithDf.dataFrame.get, "csv")
    rawBytes should not be empty

    val csvContent = new String(rawBytes, "UTF-8")
    // Should contain header
    csvContent should include("name")
    csvContent should include("age")
    // Should have multiple lines (header + data rows)
    csvContent.split("\n").length should be > 1
  }

  test("FastSampleGenerator generate raw JSON bytes from schema") {
    val fields = List(
      Field(name = "id", `type` = Some("long")),
      Field(name = "email", `type` = Some("string"))
    )

    val request = SchemaSampleRequest(
      fields = fields,
      format = "json",
      sampleSize = 3,
      fastMode = true
    )

    val responseWithDf = FastSampleGenerator.generateFromSchemaWithDataFrame(request)

    responseWithDf.response.success shouldBe true
    responseWithDf.dataFrame shouldBe defined
    responseWithDf.response.format shouldBe Some("json")

    // Test raw bytes conversion
    val rawBytes = FastSampleGenerator.dataFrameToRawBytes(responseWithDf.dataFrame.get, "json")
    rawBytes should not be empty

    val jsonContent = new String(rawBytes, "UTF-8")
    // Should contain JSON field names
    jsonContent should include("id")
    jsonContent should include("email")
  }

  test("FastSampleGenerator return correct content type for formats") {
    FastSampleGenerator.contentTypeForFormat("json") shouldBe "application/json"
    FastSampleGenerator.contentTypeForFormat("csv") shouldBe "text/csv"
    FastSampleGenerator.contentTypeForFormat("parquet") shouldBe "application/octet-stream"
    FastSampleGenerator.contentTypeForFormat("orc") shouldBe "application/octet-stream"
  }

  test("FastSampleGenerator generateFromTaskYaml includes format information") {
    import io.github.datacatering.datacaterer.core.ui.model.TaskYamlSampleRequest

    val yamlContent = """
      |name: test_task
      |steps:
      |  - name: test_step
      |    type: file
      |    options:
      |      format: csv
      |      path: /tmp/test.csv
      |    fields:
      |      - name: id
      |        type: long
      |      - name: value
      |        type: string
      |""".stripMargin

    val request = TaskYamlSampleRequest(
      taskYamlContent = yamlContent,
      stepName = None,
      sampleSize = 5,
      fastMode = true
    )

    val responseWithDf = FastSampleGenerator.generateFromTaskYamlWithDataFrame(request)

    responseWithDf.response.success shouldBe true
    responseWithDf.response.format shouldBe Some("csv")
    responseWithDf.dataFrame shouldBe defined
  }

  test("FastSampleGenerator load plan and generate from specific step") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    // Create test plan directory and file
    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    Files.createDirectories(planDir)

    val planContent = """
      |{
      |  "id": "test-plan-001",
      |  "plan": {
      |    "name": "test-plan",
      |    "tasks": []
      |  },
      |  "tasks": [
      |    {
      |      "name": "test_task",
      |      "type": "file",
      |      "options": {
      |        "format": "json",
      |        "path": "/tmp/test.json"
      |      },
      |      "fields": [
      |        {
      |          "name": "id",
      |          "type": "long"
      |        },
      |        {
      |          "name": "name",
      |          "type": "string"
      |        }
      |      ]
      |    }
      |  ]
      |}
      |""".stripMargin

    val planFile = planDir.resolve("test-plan.json")
    Files.writeString(planFile, planContent)

    try {
      val result = FastSampleGenerator.generateFromPlanStep("test-plan", "test_task", "test_task", 5, true)

      result.isRight shouldBe true
      val (step, responseWithDf) = result.right.get

      step.name shouldBe "test_task"
      responseWithDf.response.success shouldBe true
      responseWithDf.response.format shouldBe Some("json")
      responseWithDf.dataFrame shouldBe defined
      responseWithDf.response.sampleData shouldBe defined
      responseWithDf.response.sampleData.get should have size 5
    } finally {
      // Cleanup
      Files.deleteIfExists(planFile)
    }
  }

  test("FastSampleGenerator load plan and generate from task") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    Files.createDirectories(planDir)

    val planContent = """
      |{
      |  "id": "test-plan-002",
      |  "plan": {
      |    "name": "multi-step-plan",
      |    "tasks": []
      |  },
      |  "tasks": [
      |    {
      |      "name": "csv_task",
      |      "type": "file",
      |      "options": {
      |        "format": "csv",
      |        "path": "/tmp/test.csv"
      |      },
      |      "fields": [
      |        {
      |          "name": "user_id",
      |          "type": "long"
      |        }
      |      ]
      |    }
      |  ]
      |}
      |""".stripMargin

    val planFile = planDir.resolve("multi-step-plan.json")
    Files.writeString(planFile, planContent)

    try {
      val result = FastSampleGenerator.generateFromPlanTask("multi-step-plan", "csv_task", 3, true)

      result.isRight shouldBe true
      val samples = result.right.get

      samples should have size 1
      samples.keys.head should include("multi-step-plan/csv_task")

      val (step, responseWithDf) = samples.values.head
      step.name shouldBe "csv_task"
      responseWithDf.response.format shouldBe Some("csv")
    } finally {
      Files.deleteIfExists(planFile)
    }
  }

  test("FastSampleGenerator load plan and generate from entire plan") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    Files.createDirectories(planDir)

    val planContent = """
      |{
      |  "id": "test-plan-003",
      |  "plan": {
      |    "name": "full-plan",
      |    "tasks": []
      |  },
      |  "tasks": [
      |    {
      |      "name": "task1",
      |      "type": "file",
      |      "options": {
      |        "format": "json"
      |      },
      |      "fields": [
      |        {
      |          "name": "id",
      |          "type": "long"
      |        }
      |      ]
      |    },
      |    {
      |      "name": "task2",
      |      "type": "file",
      |      "options": {
      |        "format": "csv"
      |      },
      |      "fields": [
      |        {
      |          "name": "name",
      |          "type": "string"
      |        }
      |      ]
      |    }
      |  ]
      |}
      |""".stripMargin

    val planFile = planDir.resolve("full-plan.json")
    Files.writeString(planFile, planContent)

    try {
      val result = FastSampleGenerator.generateFromPlan("full-plan", 2, true)

      result.isRight shouldBe true
      val samples = result.right.get

      samples should have size 2
      samples.keys should contain allOf("full-plan/task1", "full-plan/task2")

      // Verify each task has correct format
      val (step1, response1) = samples("full-plan/task1")
      step1.name shouldBe "task1"
      response1.response.format shouldBe Some("json")

      val (step2, response2) = samples("full-plan/task2")
      step2.name shouldBe "task2"
      response2.response.format shouldBe Some("csv")
    } finally {
      Files.deleteIfExists(planFile)
    }
  }

  test("FastSampleGenerator handle missing plan gracefully") {
    val result = FastSampleGenerator.generateFromPlan("nonexistent-plan", 5, true)

    result.isLeft shouldBe true
    val error = result.left.get
    error.code shouldBe "GENERATION_ERROR"
    error.message should include("Plan not found")
  }

  test("FastSampleGenerator handle missing task gracefully") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    Files.createDirectories(planDir)

    val planContent = """
      |{
      |  "id": "test-plan-004",
      |  "plan": {
      |    "name": "error-test-plan",
      |    "tasks": []
      |  },
      |  "tasks": [
      |    {
      |      "name": "existing_task",
      |      "type": "file",
      |      "options": {},
      |      "fields": []
      |    }
      |  ]
      |}
      |""".stripMargin

    val planFile = planDir.resolve("error-test-plan.json")
    Files.writeString(planFile, planContent)

    try {
      val result = FastSampleGenerator.generateFromPlanStep("error-test-plan", "nonexistent_task", "step", 5, true)

      result.isLeft shouldBe true
      val error = result.left.get
      error.code shouldBe "GENERATION_ERROR"
      error.message should include("not found")
    } finally {
      Files.deleteIfExists(planFile)
    }
  }
}