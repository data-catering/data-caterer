package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite

import java.io.File
import java.nio.file.{Files, StandardOpenOption}

class UnifiedYamlIntegrationTest extends SparkSuite {

  private val testOutputDir = s"/tmp/data-caterer-integration-test-${java.util.UUID.randomUUID().toString.take(8)}"

  override def afterAll(): Unit = {
    cleanupTestFiles()
    super.afterAll()
  }

  private def cleanupTestFiles(): Unit = {
    val tempDir = new File(testOutputDir)
    if (tempDir.exists()) {
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  test("Can parse unified YAML with connections section") {
    val planContent =
      """name: "test_plan"
        |description: "Test unified YAML format"
        |
        |connections:
        |  - name: test_db
        |    type: postgres
        |    options:
        |      url: "jdbc:postgresql://localhost:5432/testdb"
        |      user: "testuser"
        |      password: "testpass"
        |
        |tasks:
        |  - name: test_task
        |    dataSourceName: test_db
        |    enabled: true
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-test-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(planContent)
    writer.close()

    val parsedPlan = PlanParser.parsePlanFile(tempFile.getAbsolutePath)

    assert(parsedPlan.name == "test_plan")
    assert(parsedPlan.connections.isDefined)
    assert(parsedPlan.connections.get.size == 1)

    val connection = parsedPlan.connections.get.head
    assert(connection.name.contains("test_db"))
    assert(connection.`type` == "postgres")
    assert(connection.options("url") == "jdbc:postgresql://localhost:5432/testdb")
    assert(connection.options("user") == "testuser")
    assert(connection.options("password") == "testpass")

    assert(parsedPlan.tasks.size == 1)
    assert(parsedPlan.tasks.head.dataSourceName == "test_db")
  }

  test("Can parse unified YAML with environment variables") {
    // Set a test environment variable
    val originalEnv = sys.env.get("TEST_DB_HOST")

    val planContent =
      """name: "env_test_plan"
        |
        |connections:
        |  - name: db
        |    type: postgres
        |    options:
        |      user: "${TEST_DB_USER:-postgres}"
        |      password: "${TEST_DB_PASSWORD:-secret}"
        |      url: "jdbc:postgresql://${TEST_DB_HOST:-localhost}:5432/db"
        |
        |tasks:
        |  - name: task1
        |    dataSourceName: db
        |    enabled: true
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-env-test-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(planContent)
    writer.close()

    val parsedPlan = PlanParser.parsePlan(tempFile.getAbsolutePath)

    // Environment variables should be interpolated
    val connection = parsedPlan.connections.get.head
    assert(connection.options("url") == "jdbc:postgresql://localhost:5432/db") // Default used
    assert(connection.options("user") == "postgres") // Default used
    assert(connection.options("password") == "secret") // Default used
  }

  test("Can parse unified YAML with inline task connection") {
    val planContent =
      """name: "inline_test_plan"
        |
        |tasks:
        |  - name: task1
        |    connection:
        |      type: csv
        |      options:
        |        path: "/tmp/test"
        |        header: "true"
        |    enabled: true
        |    steps:
        |      - name: users
        |        type: csv
        |        count:
        |          records: 100
        |        fields:
        |          - name: user_id
        |            type: string
        |            options:
        |              regex: "USER[0-9]{6}"
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-inline-conn-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(planContent)
    writer.close()

    val parsedPlan = PlanParser.parsePlanFile(tempFile.getAbsolutePath)

    assert(parsedPlan.tasks.size == 1)
    val taskSummary = parsedPlan.tasks.head

    assert(taskSummary.connection.isDefined)
    taskSummary.connection.get match {
      case Right(conn) =>
        assert(conn.`type` == "csv")
        assert(conn.options("path") == "/tmp/test")
        assert(conn.options("header") == "true")
      case Left(_) =>
        fail("Expected inline connection (Right), got connection reference (Left)")
    }

    assert(taskSummary.steps.isDefined)
    assert(taskSummary.steps.get.size == 1)
    assert(taskSummary.steps.get.head.name == "users")
  }

  test("Can parse unified YAML with connection reference") {
    val planContent =
      """name: "ref_test_plan"
        |
        |tasks:
        |  - name: task_with_ref
        |    connection: "my_database"
        |    enabled: true
        |    steps:
        |      - name: table1
        |        count:
        |          records: 50
        |        fields:
        |          - name: id
        |            type: integer
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-ref-test-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(planContent)
    writer.close()

    val parsedPlan = PlanParser.parsePlanFile(tempFile.getAbsolutePath)

    assert(parsedPlan.tasks.size == 1)
    val taskSummary = parsedPlan.tasks.head

    assert(taskSummary.connection.isDefined)
    taskSummary.connection.get match {
      case Left(connName) =>
        assert(connName == "my_database")
      case Right(_) =>
        fail("Expected connection reference (Left), got inline connection (Right)")
    }
  }

  test("Can parse multi-document unified YAML") {
    val multiDocContent =
      """name: "multi_doc_plan"
        |
        |connections:
        |  - name: db1
        |    type: postgres
        |    options:
        |      url: "jdbc:postgresql://localhost:5432/db1"
        |
        |tasks:
        |  - name: task1
        |    dataSourceName: db1
        |    enabled: true
        |
        |---
        |name: "task1"
        |
        |steps:
        |  - name: table1
        |    count:
        |      records: 100
        |    fields:
        |      - name: id
        |        type: string
        |        options:
        |          regex: "ID[0-9]{5}"
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-multi-doc-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(multiDocContent)
    writer.close()

    // Parse the plan (first document)
    val parsedPlan = PlanParser.parsePlanFile(tempFile.getAbsolutePath)

    assert(parsedPlan.name == "multi_doc_plan")
    assert(parsedPlan.connections.isDefined)
    assert(parsedPlan.connections.get.size == 1)
    assert(parsedPlan.tasks.size == 1)

    // Note: Jackson's YAML parser reads one document at a time
    // For multi-document support, we'd need additional parsing logic
    // This test validates the plan document can be parsed
  }

  test("Empty connections section is handled correctly") {
    val planContent =
      """name: "no_conn_plan"
        |
        |tasks:
        |  - name: task1
        |    dataSourceName: legacy_db
        |    enabled: true
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-no-conn-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(planContent)
    writer.close()

    val parsedPlan = PlanParser.parsePlanFile(tempFile.getAbsolutePath)

    assert(parsedPlan.name == "no_conn_plan")
    assert(parsedPlan.connections.isEmpty)
    assert(parsedPlan.tasks.size == 1)
    // This plan would rely on application.conf for connections
  }

  test("Task validations field can be parsed") {
    val taskContent =
      """name: "task_with_validations"
        |
        |steps:
        |  - name: users
        |    count:
        |      records: 10
        |    fields:
        |      - name: email
        |        type: string
        |    validations:
        |      - field: email
        |        validation:
        |          - type: matches
        |            regex: "^[A-Z0-9+_.-]+@(.+)$"
        |      - metric: count
        |        validation:
        |          - type: equal
        |            value: 10
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-validations-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(taskContent)
    writer.close()

    val parsedTask = PlanParser.parseTaskFile(tempFile.getAbsolutePath)

    assert(parsedTask.steps.size == 1)
    val step = parsedTask.steps.head

    // Validations should be parsed (as Any type for now)
    assert(step.validations.isDefined)
    assert(step.validations.get.nonEmpty)
  }

  test("Configuration section can be parsed") {
    val planContent =
      """name: "config_test_plan"
        |
        |tasks:
        |  - name: task1
        |    dataSourceName: db
        |    enabled: true
        |
        |configuration:
        |  flags:
        |    enableFastGeneration: true
        |    enableValidation: true
        |  folders:
        |    generatedReportsFolderPath: "/tmp/reports"
        |""".stripMargin

    val tempFile = java.io.File.createTempFile("unified-config-", ".yaml")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.write(planContent)
    writer.close()

    val parsedPlan = PlanParser.parsePlanFile(tempFile.getAbsolutePath)

    assert(parsedPlan.name == "config_test_plan")
    assert(parsedPlan.configuration.isDefined)

    // Configuration is parsed as Map[String, Any]
    val config = parsedPlan.configuration.get
    assert(config.contains("flags"))
    assert(config.contains("folders"))
  }

  // End-to-end tests that generate actual data files

  test("End-to-end: Generate JSON file with inline connection and task definition") {
    val jsonOutputPath = s"$testOutputDir/json-inline-test"
    val planContent =
      s"""name: "json_inline_e2e_test"
        |description: "End-to-end test for JSON generation with inline connection"
        |
        |tasks:
        |  - name: json_users
        |    connection:
        |      type: json
        |      options:
        |        path: "$jsonOutputPath"
        |    enabled: true
        |    steps:
        |      - name: users
        |        type: json
        |        count:
        |          records: 10
        |        fields:
        |          - name: user_id
        |            type: string
        |            options:
        |              regex: "USER[0-9]{6}"
        |              unique: true
        |          - name: email
        |            type: string
        |            options:
        |              faker: "#{Internet.emailAddress}"
        |          - name: age
        |            type: integer
        |            options:
        |              min: 18
        |              max: 65
        |        validations:
        |          - field: user_id
        |            validation:
        |              - type: unique
        |          - field: age
        |            validation:
        |              - type: between
        |                min: 18
        |                max: 65
        |          - metric: count
        |            validation:
        |              - type: equal
        |                value: 10
        |
        |configuration:
        |  flags:
        |    enableValidation: true
        |    enableFastGeneration: true
        |  folders:
        |    generatedReportsFolderPath: "$testOutputDir/reports"
        |""".stripMargin

    // Write plan to file
    val planDir = new File(s"$testOutputDir/plan")
    planDir.mkdirs()
    val planFile = new File(s"${planDir.getAbsolutePath}/json-inline-e2e.yaml")
    Files.writeString(planFile.toPath, planContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute the plan
    val taskDir = new File(s"$testOutputDir/task")
    taskDir.mkdirs()
    val result = PlanProcessor.executeFromYamlFiles(planFile.getAbsolutePath, taskDir.getAbsolutePath)

    // Verify data was generated
    val generatedFiles = new File(jsonOutputPath).listFiles()
    assert(generatedFiles != null && generatedFiles.nonEmpty, "JSON files should be generated")

    // Read and validate the generated data
    val df = sparkSession.read.json(jsonOutputPath)
    assert(df.count() == 10, "Should generate exactly 10 records")
    assert(df.columns.contains("user_id"))
    assert(df.columns.contains("email"))
    assert(df.columns.contains("age"))

    // Verify age is within bounds (handle both Int and Long types from JSON)
    val ages = df.select("age").collect().map(row => {
      row.get(0) match {
        case i: Int => i
        case l: Long => l.toInt
        case other => throw new IllegalStateException(s"Unexpected type for age: ${other.getClass.getSimpleName}")
      }
    })
    assert(ages.forall(a => a >= 18 && a <= 65), "All ages should be between 18 and 65")

    // Verify user_id uniqueness
    val userIds = df.select("user_id").collect().map(_.getString(0))
    assert(userIds.distinct.length == 10, "All user_ids should be unique")
  }

  test("End-to-end: Generate CSV file with shared connection") {
    val csvOutputPath = s"$testOutputDir/csv-shared-test"
    val planContent =
      s"""name: "csv_shared_e2e_test"
        |description: "End-to-end test for CSV generation with shared connection"
        |
        |connections:
        |  - name: csv_output
        |    type: csv
        |    options:
        |      path: "$csvOutputPath"
        |      header: "true"
        |
        |tasks:
        |  - name: csv_products
        |    dataSourceName: csv_output
        |    enabled: true
        |    steps:
        |      - name: products
        |        type: csv
        |        options:
        |          path: "$csvOutputPath"
        |        count:
        |          records: 20
        |        fields:
        |          - name: product_id
        |            type: string
        |            options:
        |              regex: "PROD[0-9]{4}"
        |          - name: product_name
        |            type: string
        |            options:
        |              faker: "#{Commerce.productName}"
        |          - name: price
        |            type: decimal
        |            options:
        |              min: 10.0
        |              max: 1000.0
        |          - name: status
        |            type: string
        |            options:
        |              oneOf: ["active", "discontinued", "pending"]
        |        validations:
        |          - field: price
        |            validation:
        |              - type: greaterThan
        |                value: 0
        |                strictly: true
        |          - field: status
        |            validation:
        |              - type: in
        |                values: ["active", "discontinued", "pending"]
        |
        |configuration:
        |  flags:
        |    enableValidation: true
        |  folders:
        |    generatedReportsFolderPath: "$testOutputDir/reports"
        |""".stripMargin

    // Write plan to file
    val planDir = new File(s"$testOutputDir/plan")
    planDir.mkdirs()
    val planFile = new File(s"${planDir.getAbsolutePath}/csv-shared-e2e.yaml")
    Files.writeString(planFile.toPath, planContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute the plan
    val taskDir = new File(s"$testOutputDir/task")
    taskDir.mkdirs()
    val result = PlanProcessor.executeFromYamlFiles(planFile.getAbsolutePath, taskDir.getAbsolutePath)

    // Verify data was generated
    val generatedFiles = new File(csvOutputPath).listFiles()
    assert(generatedFiles != null && generatedFiles.nonEmpty, "CSV files should be generated")

    // Read and validate the generated data
    val df = sparkSession.read.option("header", "true").csv(csvOutputPath)
    assert(df.count() == 20, "Should generate exactly 20 records")
    assert(df.columns.contains("product_id"))
    assert(df.columns.contains("product_name"))
    assert(df.columns.contains("price"))
    assert(df.columns.contains("status"))

    // Verify status values
    val statuses = df.select("status").collect().map(_.getString(0))
    assert(statuses.forall(s => Set("active", "discontinued", "pending").contains(s)),
      "All status values should be from the allowed set")
  }

  test("End-to-end: Generate multiple formats in one plan") {
    val jsonOutputPath = s"$testOutputDir/multi-json"
    val parquetOutputPath = s"$testOutputDir/multi-parquet"

    val planContent =
      s"""name: "multi_format_e2e_test"
        |description: "End-to-end test for multiple formats in single plan"
        |
        |tasks:
        |  - name: json_customers
        |    connection:
        |      type: json
        |      options:
        |        path: "$jsonOutputPath"
        |    enabled: true
        |    steps:
        |      - name: customers
        |        type: json
        |        count:
        |          records: 15
        |        fields:
        |          - name: customer_id
        |            type: string
        |            options:
        |              regex: "CUST[0-9]{5}"
        |          - name: name
        |            type: string
        |            options:
        |              faker: "#{Name.fullName}"
        |
        |  - name: parquet_orders
        |    connection:
        |      type: parquet
        |      options:
        |        path: "$parquetOutputPath"
        |    enabled: true
        |    steps:
        |      - name: orders
        |        type: parquet
        |        count:
        |          records: 25
        |        fields:
        |          - name: order_id
        |            type: string
        |            options:
        |              regex: "ORD[0-9]{7}"
        |          - name: amount
        |            type: decimal
        |            options:
        |              min: 5.0
        |              max: 500.0
        |          - name: order_date
        |            type: date
        |            options:
        |              faker: "#{Date.birthday}"
        |
        |configuration:
        |  folders:
        |    generatedReportsFolderPath: "$testOutputDir/reports"
        |""".stripMargin

    // Write plan to file
    val planDir = new File(s"$testOutputDir/plan")
    planDir.mkdirs()
    val planFile = new File(s"${planDir.getAbsolutePath}/multi-format-e2e.yaml")
    Files.writeString(planFile.toPath, planContent, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute the plan
    val taskDir = new File(s"$testOutputDir/task")
    taskDir.mkdirs()
    val result = PlanProcessor.executeFromYamlFiles(planFile.getAbsolutePath, taskDir.getAbsolutePath)

    // Verify JSON data was generated
    val jsonFiles = new File(jsonOutputPath).listFiles()
    assert(jsonFiles != null && jsonFiles.nonEmpty, "JSON files should be generated")
    val jsonDf = sparkSession.read.json(jsonOutputPath)
    assert(jsonDf.count() == 15, "Should generate exactly 15 JSON records")
    assert(jsonDf.columns.contains("customer_id"))
    assert(jsonDf.columns.contains("name"))

    // Verify Parquet data was generated
    val parquetFiles = new File(parquetOutputPath).listFiles()
    assert(parquetFiles != null && parquetFiles.nonEmpty, "Parquet files should be generated")
    val parquetDf = sparkSession.read.parquet(parquetOutputPath)
    assert(parquetDf.count() == 25, "Should generate exactly 25 Parquet records")
    assert(parquetDf.columns.contains("order_id"))
    assert(parquetDf.columns.contains("amount"))
    assert(parquetDf.columns.contains("order_date"))
  }
}
