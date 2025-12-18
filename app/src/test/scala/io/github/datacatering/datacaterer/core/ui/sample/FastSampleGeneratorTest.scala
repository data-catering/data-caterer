package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.model.SchemaSampleRequest
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers.{be, defined, have}
import org.scalatest.matchers.should.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.{Files, Paths}

trait FastSampleTestHelpers {

  // Common field definitions for reuse across tests
  def stringField(name: String, regex: Option[String] = None, expression: Option[String] = None): Field = {
    val options = Map.empty[String, Any] ++
      regex.map("regex" -> _) ++
      expression.map("expression" -> _)
    Field(name = name, `type` = Some("string"), options = options)
  }

  def intField(name: String, min: Int = 1, max: Int = 100): Field = {
    Field(name = name, `type` = Some("int"), options = Map("min" -> min, "max" -> max))
  }

  def doubleField(name: String, min: Double = 1.0, max: Double = 100.0): Field = {
    Field(name = name, `type` = Some("double"), options = Map("min" -> min, "max" -> max))
  }

  def booleanField(name: String): Field = {
    Field(name = name, `type` = Some("boolean"))
  }

  // Helper to create test plan and task files
  def createTestFiles(planContent: String, planName: String, taskContents: Map[String, String] = Map.empty): (java.nio.file.Path, Map[String, java.nio.file.Path]) = {
    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    val taskDir = Paths.get(s"$INSTALL_DIRECTORY/task")
    Files.createDirectories(planDir)
    Files.createDirectories(taskDir)

    val planFile = planDir.resolve(s"$planName.yaml")
    Files.writeString(planFile, planContent)

    val taskFiles = taskContents.map { case (taskName, content) =>
      val taskFile = taskDir.resolve(s"$taskName.yaml")
      Files.writeString(taskFile, content)
      taskName -> taskFile
    }

    (planFile, taskFiles)
  }

  // Helper to cleanup test files
  def cleanupFiles(planFile: java.nio.file.Path, taskFiles: Map[String, java.nio.file.Path]): Unit = {
    Files.deleteIfExists(planFile)
    taskFiles.values.foreach(Files.deleteIfExists)
  }

  // Common assertions for sample data validation
  def assertBasicSampleResponse(result: Any, expectedSize: Int, fastMode: Boolean = true): Unit = {
    import io.github.datacatering.datacaterer.core.ui.model.SampleResponse
    val response = result.asInstanceOf[SampleResponse]
    response.success should be(true)
    response.sampleData should be(defined)
    response.sampleData.get should have size expectedSize
    response.schema should be(defined)
    response.metadata should be(defined)
    response.metadata.get.fastModeEnabled should be(fastMode)
  }
}

class FastSampleGeneratorTest extends SparkSuite with Matchers with BeforeAndAfterEach with FastSampleTestHelpers {

  private var tempDir: java.nio.file.Path = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Files.createTempDirectory("datacaterer-test")
    System.setProperty("data-caterer-install-dir", tempDir.toString)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (tempDir != null) {
      import scala.reflect.io.Directory
      import java.io.File
      val directory = new Directory(new File(tempDir.toString))
      directory.deleteRecursively()
    }
    System.clearProperty("data-caterer-install-dir")
  }

  test("FastSampleGenerator generate sample data from inline schema") {
    val fields = List(
      stringField("account_id", regex = Some("ACC[0-9]{10}")),
      intField("year", min = 2021, max = 2023),
      doubleField("amount", min = 10.0, max = 100.0)
    )

    val request = SchemaSampleRequest(fields = fields, sampleSize = Some(5), fastMode = true)
    val result = FastSampleGenerator.generateFromSchemaWithDataFrame(request).response

    assertBasicSampleResponse(result, expectedSize = 5, fastMode = true)

    // Verify schema structure
    val schema = result.schema.get
    schema.fields should have size 3
    schema.fields.map(_.name) should contain allOf("account_id", "year", "amount")

    // Verify sample data constraints
    result.sampleData.get.foreach { record =>
      record should contain key "account_id"
      record should contain key "year"
      record should contain key "amount"

      record("account_id").toString should fullyMatch regex "ACC[0-9]{10}"
      record("year").toString.toInt should (be >= 2021 and be <= 2023)
      record("amount").toString.toDouble should (be >= 10.0 and be <= 100.0)
    }
  }

  test("FastSampleGenerator handle nested fields in schema") {
    val fields = List(stringField("simple_field", expression = Some("#{Name.fullName}")))
    val request = SchemaSampleRequest(fields = fields, sampleSize = Some(3), fastMode = true)
    val result = FastSampleGenerator.generateFromSchemaWithDataFrame(request).response

    assertBasicSampleResponse(result, expectedSize = 3, fastMode = true)

    result.sampleData.get.foreach { record =>
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
      sampleSize = Some(150), // Above max limit
      fastMode = true
    )

    val result = FastSampleGenerator.generateFromSchemaWithDataFrame(request).response

    result.success shouldBe true
    result.sampleData shouldBe defined

    // Should be limited to max size (100)
    result.sampleData.get.size should be <= 100
    result.metadata.get.sampleSize should be <= 100
  }

  test("FastSampleGenerator handle empty schema gracefully") {
    val request = SchemaSampleRequest(
      fields = List(),
      sampleSize = Some(5)
    )

    val result = FastSampleGenerator.generateFromSchemaWithDataFrame(request).response

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
      sampleSize = Some(10),
      fastMode = true
    )

    // Generate sample data
    val result = FastSampleGenerator.generateFromSchemaWithDataFrame(request).response

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
      stringField("string_field", expression = Some("#{Name.firstName}")),
      intField("int_field", min = 1, max = 100),
      booleanField("boolean_field")
    )

    val request = SchemaSampleRequest(fields = fields, sampleSize = Some(5), fastMode = true)
    val result = FastSampleGenerator.generateFromSchemaWithDataFrame(request).response

    assertBasicSampleResponse(result, expectedSize = 5, fastMode = true)

    result.sampleData.get.foreach { record =>
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
      sampleSize = Some(3)
    )

    val result = FastSampleGenerator.generateFromTaskFileWithDataFrame(request).response

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
      sampleSize = Some(5),
      fastMode = true
    )

    val responseWithDf = FastSampleGenerator.generateFromSchemaWithDataFrame(request)

    responseWithDf.response.success shouldBe true
    responseWithDf.dataFrame shouldBe defined
    responseWithDf.response.format shouldBe Some("csv")

    // Test raw bytes conversion
    val rawBytes = FastSampleGenerator.dataFrameToRawBytes(responseWithDf.dataFrame.get, "csv", Step())
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
      sampleSize = Some(3),
      fastMode = true
    )

    val responseWithDf = FastSampleGenerator.generateFromSchemaWithDataFrame(request)

    responseWithDf.response.success shouldBe true
    responseWithDf.dataFrame shouldBe defined
    responseWithDf.response.format shouldBe Some("json")

    // Test raw bytes conversion
    val rawBytes = FastSampleGenerator.dataFrameToRawBytes(responseWithDf.dataFrame.get, "json", Step())
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

    val yamlContent =
      """
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
      sampleSize = Some(5),
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

    val planContent =
      """
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
      val result = FastSampleGenerator.generateFromPlanStep("test-plan", "test_task", "test_task", Some(5),
        planDirectory = Some(planDir.toString), taskDirectory = Some(planDir.toString))

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

    val planContent =
      """
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
      val result = FastSampleGenerator.generateFromPlanTask("multi-step-plan", "csv_task", Some(3),
        planDirectory = Some(planDir.toString), taskDirectory = Some(planDir.toString))

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

    val planContent =
      """
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
      val result = FastSampleGenerator.generateFromPlan("full-plan", Some(2),
        planDirectory = Some(planDir.toString), taskDirectory = Some(planDir.toString))

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
    val result = FastSampleGenerator.generateFromPlan("nonexistent-plan", Some(5))

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

    val planContent =
      """
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
      val result = FastSampleGenerator.generateFromPlanStep("error-test-plan", "nonexistent_task", "step", Some(5),
        planDirectory = Some(planDir.toString), taskDirectory = Some(planDir.toString))

      result.isLeft shouldBe true
      val error = result.left.get
      error.code shouldBe "GENERATION_ERROR"
      error.message should include("not found")
    } finally {
      Files.deleteIfExists(planFile)
    }
  }

  test("FastSampleGenerator generate samples with foreign key relationships - YAML plan") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    // Create directories
    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    val taskDir = Paths.get(s"$INSTALL_DIRECTORY/task")
    Files.createDirectories(planDir)
    Files.createDirectories(taskDir)

    // Create a YAML plan with foreign key relationships
    val planContent =
      """
        |name: "foreign_key_test_plan"
        |description: "Test plan with foreign key relationships"
        |tasks:
        |  - name: "accounts_task"
        |    dataSourceName: "accounts_db"
        |    enabled: true
        |  - name: "transactions_task"
        |    dataSourceName: "transactions_db"
        |    enabled: true
        |
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: "accounts_db"
        |        step: "accounts"
        |        fields: ["account_id"]
        |      generate:
        |        - dataSource: "transactions_db"
        |          step: "transactions"
        |          fields: ["account_id"]
        |""".stripMargin

    // Create accounts task YAML
    val accountsTaskContent =
      """
        |name: "accounts_task"
        |steps:
        |  - name: "accounts"
        |    type: "file"
        |    options:
        |      format: "json"
        |      path: "/tmp/accounts.json"
        |    count:
        |      records: 5
        |    fields:
        |      - name: "account_id"
        |        type: "string"
        |        options:
        |          regex: "ACC[0-9]{5}"
        |          isUnique: true
        |      - name: "account_name"
        |        type: "string"
        |        options:
        |          expression: "#{Name.fullName}"
        |      - name: "balance"
        |        type: "double"
        |        options:
        |          min: 100.0
        |          max: 10000.0
        |""".stripMargin

    // Create transactions task YAML
    val transactionsTaskContent =
      """
        |name: "transactions_task"
        |steps:
        |  - name: "transactions"
        |    type: "file"
        |    options:
        |      format: "json"
        |      path: "/tmp/transactions.json"
        |    count:
        |      records: 10
        |    fields:
        |      - name: "transaction_id"
        |        type: "string"
        |        options:
        |          regex: "TXN[0-9]{6}"
        |      - name: "account_id"
        |        type: "string"
        |        # This will be populated by foreign key relationship
        |      - name: "amount"
        |        type: "double"
        |        options:
        |          min: 10.0
        |          max: 1000.0
        |      - name: "transaction_type"
        |        type: "string"
        |        options:
        |          oneOf: ["debit", "credit"]
        |""".stripMargin

    val planFile = planDir.resolve("foreign_key_test_plan.yaml")
    val accountsTaskFile = taskDir.resolve("accounts_task.yaml")
    val transactionsTaskFile = taskDir.resolve("transactions_task.yaml")

    Files.writeString(planFile, planContent)
    Files.writeString(accountsTaskFile, accountsTaskContent)
    Files.writeString(transactionsTaskFile, transactionsTaskContent)

    try {
      // Test 1: Generate from plan with relationships enabled
      val planResult = FastSampleGenerator.generateFromPlan("foreign_key_test_plan", Some(5), enableRelationships = true,
        planDirectory = Some(planDir.toString), taskDirectory = Some(taskDir.toString))

      planResult.isRight shouldBe true
      val planSamples = planResult.right.get

      planSamples should have size 2
      planSamples.keys should contain allOf("foreign_key_test_plan/accounts", "foreign_key_test_plan/transactions")

      // Get the generated data
      val (_, accountsResponse) = planSamples("foreign_key_test_plan/accounts")
      val (_, transactionsResponse) = planSamples("foreign_key_test_plan/transactions")

      // Verify accounts data
      accountsResponse.response.success shouldBe true
      accountsResponse.response.sampleData shouldBe defined
      val accountsData = accountsResponse.response.sampleData.get
      accountsData should have size 5

      // Verify transactions data
      transactionsResponse.response.success shouldBe true
      transactionsResponse.response.sampleData shouldBe defined
      val transactionsData = transactionsResponse.response.sampleData.get
      transactionsData should have size 5

      // Collect all account IDs from accounts data
      val accountIds = accountsData.map(record => record("account_id").toString).toSet
      accountIds should have size 5 // Should be unique

      // Verify that all transaction account_ids reference existing account_ids (foreign key constraint)
      val transactionAccountIds = transactionsData.map(record => record("account_id").toString).toSet
      transactionAccountIds should not be empty

      // All transaction account_ids should be from the accounts table
      transactionAccountIds.foreach { txnAccountId =>
        accountIds should contain(txnAccountId)
      }

      // Test 2: Generate from task with relationships enabled
      val taskResult = FastSampleGenerator.generateFromPlanTask("foreign_key_test_plan", "transactions_task", Some(3), fastMode = true, enableRelationships = true,
        planDirectory = Some(planDir.toString), taskDirectory = Some(taskDir.toString))

      taskResult.isRight shouldBe true
      val taskSamples = taskResult.right.get

      taskSamples should have size 1
      val (_, taskTransactionsResponse) = taskSamples.values.head
      taskTransactionsResponse.response.success shouldBe true
      taskTransactionsResponse.response.metadata.get.relationshipsEnabled shouldBe true

    } finally {
      Files.deleteIfExists(planFile)
      Files.deleteIfExists(accountsTaskFile)
      Files.deleteIfExists(transactionsTaskFile)
    }
  }

  test("FastSampleGenerator generate samples without relationships - should work independently") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    // Create directories
    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    val taskDir = Paths.get(s"$INSTALL_DIRECTORY/task")
    Files.createDirectories(planDir)
    Files.createDirectories(taskDir)

    // Reuse the same plan and tasks from the previous test
    val planContent =
      """
        |name: "no_relationships_test_plan"
        |description: "Test plan without relationships enabled"
        |tasks:
        |  - name: "accounts_task"
        |    dataSourceName: "accounts_db"
        |    enabled: true
        |  - name: "transactions_task"
        |    dataSourceName: "transactions_db"
        |    enabled: true
        |
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: "accounts_db"
        |        step: "accounts"
        |        fields: ["account_id"]
        |      generate:
        |        - dataSource: "transactions_db"
        |          step: "transactions"
        |          fields: ["account_id"]
        |""".stripMargin

    val accountsTaskContent =
      """
        |name: "accounts_task"
        |steps:
        |  - name: "accounts"
        |    type: "file"
        |    options:
        |      format: "json"
        |    fields:
        |      - name: "account_id"
        |        type: "string"
        |        options:
        |          regex: "ACC[0-9]{3}"
        |      - name: "balance"
        |        type: "double"
        |        options:
        |          min: 100.0
        |          max: 1000.0
        |""".stripMargin

    val transactionsTaskContent =
      """
        |name: "transactions_task"
        |steps:
        |  - name: "transactions"
        |    type: "file"
        |    options:
        |      format: "json"
        |    fields:
        |      - name: "account_id"
        |        type: "string"
        |        options:
        |          regex: "ACC[0-9]{3}"
        |      - name: "amount"
        |        type: "double"
        |        options:
        |          min: 10.0
        |          max: 1000.0
        |""".stripMargin

    val planFile = planDir.resolve("no_relationships_test_plan.yaml")
    val accountsTaskFile = taskDir.resolve("accounts_task.yaml")
    val transactionsTaskFile = taskDir.resolve("transactions_task.yaml")

    Files.writeString(planFile, planContent)
    Files.writeString(accountsTaskFile, accountsTaskContent)
    Files.writeString(transactionsTaskFile, transactionsTaskContent)

    try {
      // Generate samples with relationships disabled
      val planResult = FastSampleGenerator.generateFromPlan("no_relationships_test_plan", Some(3), fastMode = true, enableRelationships = false,
        planDirectory = Some(planDir.toString), taskDirectory = Some(taskDir.toString))

      planResult.isRight shouldBe true
      val planSamples = planResult.right.get

      planSamples should have size 2

      // Get the generated data  
      val (_, accountsResponse) = planSamples("no_relationships_test_plan/accounts")
      val (_, transactionsResponse) = planSamples("no_relationships_test_plan/transactions")

      // Verify both responses succeeded
      accountsResponse.response.success shouldBe true
      transactionsResponse.response.success shouldBe true

      // Verify relationships were not applied
      accountsResponse.response.metadata.get.relationshipsEnabled shouldBe false
      transactionsResponse.response.metadata.get.relationshipsEnabled shouldBe false

      // Verify that account_ids are generated independently (not following foreign key constraints)
      val accountsData = accountsResponse.response.sampleData.get
      val transactionsData = transactionsResponse.response.sampleData.get

      accountsData should have size 3
      transactionsData should have size 3

      val accountIds = accountsData.map(record => record("account_id").toString).toSet
      val transactionAccountIds = transactionsData.map(record => record("account_id").toString).toSet

      // Since relationships are disabled, transaction account_ids are independently generated
      // They might or might not match account_ids - no foreign key constraint applied
      accountIds should not be empty
      transactionAccountIds should not be empty

    } finally {
      Files.deleteIfExists(planFile)
      Files.deleteIfExists(accountsTaskFile)
      Files.deleteIfExists(transactionsTaskFile)
    }
  }

  test("FastSampleGenerator relationships not applied for step-level generation") {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY

    import java.nio.file.{Files, Paths}

    // Create directories
    val planDir = Paths.get(s"$INSTALL_DIRECTORY/plan")
    val taskDir = Paths.get(s"$INSTALL_DIRECTORY/task")
    Files.createDirectories(planDir)
    Files.createDirectories(taskDir)

    // Create a plan with foreign keys
    val planContent =
      """
        |name: "step_level_test_plan"
        |tasks:
        |  - name: "accounts_task"
        |    dataSourceName: "accounts_db"
        |    enabled: true
        |
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: "accounts_db"
        |        step: "accounts"
        |        fields: ["account_id"]
        |      generate: []
        |""".stripMargin

    val accountsTaskContent =
      """
        |name: "accounts_task"
        |steps:
        |  - name: "accounts"
        |    type: "file"
        |    options:
        |      format: "json"
        |    fields:
        |      - name: "account_id"
        |        type: "string"
        |        options:
        |          regex: "ACC[0-9]{3}"
        |""".stripMargin

    val planFile = planDir.resolve("step_level_test_plan.yaml")
    val accountsTaskFile = taskDir.resolve("accounts_task.yaml")

    Files.writeString(planFile, planContent)
    Files.writeString(accountsTaskFile, accountsTaskContent)

    try {
      // Test step-level generation - relationships should NOT be applied even if plan has them
      val stepResult = FastSampleGenerator.generateFromPlanStep("step_level_test_plan", "accounts_task", "accounts", Some(3), fastMode = true,
        planDirectory = Some(planDir.toString), taskDirectory = Some(taskDir.toString))

      stepResult.isRight shouldBe true
      val (_, response) = stepResult.right.get

      response.response.success shouldBe true
      response.response.metadata shouldBe defined

      // Step-level generation should never use relationships
      response.response.metadata.get.relationshipsEnabled shouldBe false

      // Note: generateFromStepName relies on global config paths (ConfigParser.foldersConfig.taskFolderPath)
      // and doesn't support custom directories, so we skip testing it here since the key assertion
      // (step-level generation doesn't use relationships) is already verified above with generateFromPlanStep

    } finally {
      Files.deleteIfExists(planFile)
      Files.deleteIfExists(accountsTaskFile)
    }
  }

  // Break down complex foreign key test into focused, smaller tests

  test("FastSampleGenerator setup complex foreign key plan with multiple tables") {
    val planContent =
      """
        |name: "complex_fk_test_plan"
        |description: "Complex foreign key relationships test"
        |tasks:
        |  - name: "customers_task"
        |    dataSourceName: "customers_db"
        |    enabled: true
        |  - name: "accounts_task"
        |    dataSourceName: "accounts_db"
        |    enabled: true
        |  - name: "transactions_task"
        |    dataSourceName: "transactions_db"
        |    enabled: true
        |
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: "customers_db"
        |        step: "customers"
        |        fields: ["customer_id"]
        |      generate:
        |        - dataSource: "accounts_db"
        |          step: "accounts"
        |          fields: ["customer_id"]
        |    - source:
        |        dataSource: "accounts_db"
        |        step: "accounts"
        |        fields: ["account_id"]
        |      generate:
        |        - dataSource: "transactions_db"
        |          step: "transactions"
        |          fields: ["account_id"]
        |""".stripMargin

    val taskContents = Map(
      "customers_task" ->
        """
          |name: "customers_task"
          |steps:
          |  - name: "customers"
          |    type: "file"
          |    options:
          |      format: "json"
          |    count:
          |      records: 3
          |    fields:
          |      - name: "customer_id"
          |        type: "string"
          |        options:
          |          regex: "CUST[0-9]{3}"
          |          isUnique: true
          |      - name: "name"
          |        type: "string"
          |        options:
          |          expression: "#{Name.fullName}"
          |""".stripMargin,
      "accounts_task" ->
        """
          |name: "accounts_task"
          |steps:
          |  - name: "accounts"
          |    type: "file"
          |    options:
          |      format: "json"
          |    count:
          |      records: 6
          |    fields:
          |      - name: "account_id"
          |        type: "string"
          |        options:
          |          regex: "ACC[0-9]{4}"
          |          isUnique: true
          |      - name: "customer_id"
          |        type: "string"
          |      - name: "account_type"
          |        type: "string"
          |        options:
          |          oneOf: ["checking", "savings", "credit"]
          |""".stripMargin,
      "transactions_task" ->
        """
          |name: "transactions_task"
          |steps:
          |  - name: "transactions"
          |    type: "file"
          |    options:
          |      format: "json"
          |    count:
          |      records: 12
          |      perField:
          |        count: 2
          |        fieldNames:
          |          - "account_id"
          |    fields:
          |      - name: "transaction_id"
          |        type: "string"
          |        options:
          |          regex: "TXN[0-9]{6}"
          |      - name: "account_id"
          |        type: "string"
          |      - name: "amount"
          |        type: "double"
          |        options:
          |          min: 1.0
          |          max: 500.0
          |""".stripMargin
    )

    val (planFile, taskFiles) = createTestFiles(planContent, "complex_fk_test_plan", taskContents)

    try {
      val planResult = FastSampleGenerator.generateFromPlan("complex_fk_test_plan", Some(3), fastMode = false, enableRelationships = true,
        planDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/plan").toString), taskDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/task").toString))

      planResult.isRight shouldBe true
      val planSamples = planResult.right.get

      planSamples should have size 3
      planSamples.keys should contain allOf(
        "complex_fk_test_plan/customers",
        "complex_fk_test_plan/accounts",
        "complex_fk_test_plan/transactions"
      )

      // Verify basic success for all tables
      planSamples.values.foreach { case (_, response) =>
        response.response.success shouldBe true
        response.response.metadata.get.relationshipsEnabled shouldBe true
      }
    } finally {
      cleanupFiles(planFile, taskFiles)
    }
  }

  test("FastSampleGenerator foreign key constraints validation") {
    val planContent =
      """
        |name: "fk_validation_plan"
        |tasks:
        |  - name: "customers_task"
        |    dataSourceName: "customers_db"
        |  - name: "accounts_task"
        |    dataSourceName: "accounts_db"
        |sinkOptions:
        |  foreignKeys:
        |    - source:
        |        dataSource: "customers_db"
        |        step: "customers"
        |        fields: ["customer_id"]
        |      generate:
        |        - dataSource: "accounts_db"
        |          step: "accounts"
        |          fields: ["customer_id"]
        |""".stripMargin

    val taskContents = Map(
      "customers_task" ->
        """
          |name: "customers_task"
          |steps:
          |  - name: "customers"
          |    type: "file"
          |    options: { format: "json" }
          |    count: { records: 2 }
          |    fields:
          |      - name: "customer_id"
          |        type: "string"
          |        options: { regex: "CUST[0-9]{3}", isUnique: true }
          |""".stripMargin,
      "accounts_task" ->
        """
          |name: "accounts_task"
          |steps:
          |  - name: "accounts"
          |    type: "file"
          |    options: { format: "json" }
          |    count: { records: 4 }
          |    fields:
          |      - name: "account_id"
          |        type: "string"
          |        options: { regex: "ACC[0-9]{3}", isUnique: true }
          |      - name: "customer_id"
          |        type: "string"
          |""".stripMargin
    )

    val (planFile, taskFiles) = createTestFiles(planContent, "fk_validation_plan", taskContents)

    try {
      val result = FastSampleGenerator.generateFromPlan("fk_validation_plan", Some(2), fastMode = false, enableRelationships = true,
        planDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/plan").toString), taskDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/task").toString))

      result.isRight shouldBe true
      val samples = result.right.get

      val customersData = samples("fk_validation_plan/customers")._2.response.sampleData.get
      val accountsData = samples("fk_validation_plan/accounts")._2.response.sampleData.get

      // Verify foreign key constraint
      val customerIds = customersData.map(record => record("customer_id").toString).toSet
      val accountCustomerIds = accountsData.map(record => record("customer_id").toString).toSet

      accountCustomerIds.foreach { accountCustomerId =>
        customerIds should contain(accountCustomerId)
      }
    } finally {
      cleanupFiles(planFile, taskFiles)
    }
  }

  test("FastSampleGenerator should cap large step record counts to MAX_SAMPLE_SIZE for HTTP endpoints") {
    // This test verifies that even if a step has count.records set to a very large value,
    // the sample endpoint will cap it to MAX_SAMPLE_SIZE (100) to prevent huge HTTP payloads
    val planContent =
      """
        |name: "large_count_plan"
        |tasks:
        |  - name: "large_data_task"
        |    dataSourceName: "large_count_plan"
        |""".stripMargin

    val taskContents = Map(
      "large_data_task" ->
        """
          |name: "large_data_task"
          |steps:
          |  - name: "big_step"
          |    type: "file"
          |    options: { format: "json" }
          |    count: { records: 1000000 }
          |    fields:
          |      - name: "id"
          |        type: "long"
          |      - name: "name"
          |        type: "string"
          |        options: { expression: "#{Name.fullName}" }
          |""".stripMargin
    )

    val (planFile, taskFiles) = createTestFiles(planContent, "large_count_plan", taskContents)

    try {
      // Test with relationships enabled (this path was previously uncapped)
      val result = FastSampleGenerator.generateFromPlan("large_count_plan", sampleSize = Some(10), fastMode = true, enableRelationships = true,
        planDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/plan").toString), taskDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/task").toString))

      result.isRight shouldBe true
      val samples = result.right.get

      val bigStepData = samples("large_count_plan/big_step")._2.response.sampleData.get

      // Should be capped to MAX_SAMPLE_SIZE (100) even though step.count.records is 1,000,000
      bigStepData.size should be <= 100
      bigStepData.size shouldBe 10 // Should use the requested sampleSize since it's below MAX

      // Verify the data is valid
      bigStepData.foreach { record =>
        record should contain key "id"
        record should contain key "name"
      }
    } finally {
      cleanupFiles(planFile, taskFiles)
    }
  }

  test("FastSampleGenerator should respect MAX_SAMPLE_SIZE even when sampleSize parameter exceeds it") {
    // Verify that requesting a sample size > MAX_SAMPLE_SIZE gets capped
    val planContent =
      """
        |name: "oversized_request_plan"
        |tasks:
        |  - name: "test_task"
        |    dataSourceName: "oversized_request_plan"
        |""".stripMargin

    val taskContents = Map(
      "test_task" ->
        """
          |name: "test_task"
          |steps:
          |  - name: "test_step"
          |    type: "file"
          |    options: { format: "json" }
          |    count: { records: 500 }
          |    fields:
          |      - name: "value"
          |        type: "int"
          |""".stripMargin
    )

    val (planFile, taskFiles) = createTestFiles(planContent, "oversized_request_plan", taskContents)

    try {
      // Request 150 samples, which exceeds MAX_SAMPLE_SIZE (100)
      val result = FastSampleGenerator.generateFromPlan("oversized_request_plan", sampleSize = Some(150), fastMode = true, enableRelationships = true,
        planDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/plan").toString), taskDirectory = Some(Paths.get(s"$INSTALL_DIRECTORY/task").toString))

      result.isRight shouldBe true
      val samples = result.right.get
      val testStepData = samples("oversized_request_plan/test_step")._2.response.sampleData.get

      // Should be capped to MAX_SAMPLE_SIZE (100)
      testStepData.size should be <= 100
    } finally {
      cleanupFiles(planFile, taskFiles)
    }
  }
}