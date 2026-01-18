package io.github.datacatering.datacaterer.core.parser.unified

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.functions.{col, current_date, datediff, dayofweek, hour, to_date, to_timestamp}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}

/**
 * Integration tests for unified YAML configuration format.
 * Tests end-to-end execution from parsing unified YAML to data generation.
 */
class UnifiedYamlIntegrationTest extends SparkSuite with Matchers with BeforeAndAfterEach {

  private val testDataPath = "/tmp/data-caterer-unified-yaml-test"
  private val testConfigPath = s"$testDataPath/config"

  override def beforeEach(): Unit = {
    super.beforeEach()
    new File(testDataPath).mkdirs()
    new File(testConfigPath).mkdirs()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    deleteRecursively(new File(testDataPath))
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }

  test("UnifiedConfigDetector correctly identifies unified format") {
    val unifiedYaml =
      """version: "1.0"
        |name: "test_unified"
        |dataSources:
        |  - name: "my_json"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data"
        |    steps:
        |      - name: "accounts"
        |        fields:
        |          - name: "id"
        |""".stripMargin

    val legacyPlanYaml =
      """name: "legacy_plan"
        |tasks:
        |  - name: "task1"
        |    dataSourceName: "postgres"
        |    enabled: true
        |""".stripMargin

    val legacyTaskYaml =
      """name: "legacy_task"
        |steps:
        |  - name: "step1"
        |    type: "json"
        |    fields: []
        |""".stripMargin

    UnifiedConfigDetector.detectFormatFromContent(unifiedYaml) shouldBe UnifiedConfigDetector.UnifiedFormat
    UnifiedConfigDetector.detectFormatFromContent(legacyPlanYaml) shouldBe UnifiedConfigDetector.LegacyPlanFormat
    UnifiedConfigDetector.detectFormatFromContent(legacyTaskYaml) shouldBe UnifiedConfigDetector.LegacyTaskFormat
  }

  test("UnifiedYamlParser parses basic unified config from file") {
    val outputPath = s"$testDataPath/output/basic-test"
    val unifiedYaml =
      s"""version: "1.0"
         |name: "basic_test"
         |description: "Basic unified YAML test"
         |dataSources:
         |  - name: "test_json"
         |    connection:
         |      type: "json"
         |      options:
         |        path: "$outputPath"
         |    steps:
         |      - name: "test_data"
         |        count:
         |          records: 10
         |        fields:
         |          - name: "id"
         |            options:
         |              regex: "ID[0-9]{4}"
         |          - name: "name"
         |            options:
         |              expression: "#{Name.firstName}"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/basic-unified.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val config = UnifiedYamlParser.parse(configFile.toString)(null)

    config.name shouldBe "basic_test"
    config.description shouldBe "Basic unified YAML test"
    config.dataSources should have size 1
    config.dataSources.head.name shouldBe "test_json"
    config.dataSources.head.connection.connectionType shouldBe "json"
    config.dataSources.head.steps should have size 1
    config.dataSources.head.steps.head.name shouldBe "test_data"
    config.dataSources.head.steps.head.count.get.records shouldBe Some(10L)
    config.dataSources.head.steps.head.fields should have size 2
  }

  test("UnifiedConfigConverter produces valid Plan and Tasks") {
    val outputPath = s"$testDataPath/output/converter-test"
    val unifiedYaml =
      s"""version: "1.0"
         |name: "converter_test"
         |dataSources:
         |  - name: "test_postgres"
         |    connection:
         |      type: "postgres"
         |      options:
         |        url: "jdbc:postgresql://localhost:5432/test"
         |        user: "user"
         |        password: "pass"
         |    steps:
         |      - name: "users"
         |        options:
         |          dbtable: "public.users"
         |        count:
         |          records: 100
         |        fields:
         |          - name: "user_id"
         |            options:
         |              regex: "USR[0-9]{6}"
         |          - name: "email"
         |            options:
         |              expression: "#{Internet.emailAddress}"
         |sinkOptions:
         |  seed: "12345"
         |  locale: "en-US"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/converter-test.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val config = UnifiedYamlParser.parse(configFile.toString)(null)
    val (plan, tasks, validations, dataCatererConfig) = UnifiedConfigConverter.convert(config)

    // Verify plan
    plan.name shouldBe "converter_test"
    plan.tasks should have size 1
    plan.tasks.head.name shouldBe "test_postgres_task"
    plan.tasks.head.dataSourceName shouldBe "test_postgres"
    plan.sinkOptions.get.seed shouldBe Some("12345")
    plan.sinkOptions.get.locale shouldBe Some("en-US")

    // Verify tasks
    tasks should have size 1
    tasks.head.name shouldBe "test_postgres_task"
    tasks.head.steps should have size 1
    tasks.head.steps.head.name shouldBe "users"
    tasks.head.steps.head.`type` shouldBe "postgres"
    tasks.head.steps.head.count.records shouldBe Some(100L)
    tasks.head.steps.head.fields should have size 2

    // Verify connection config
    dataCatererConfig.connectionConfigByName should contain key "test_postgres"
    val connConfig = dataCatererConfig.connectionConfigByName("test_postgres")
    connConfig.get(FORMAT) shouldBe Some(JDBC)
    connConfig.get(URL) shouldBe Some("jdbc:postgresql://localhost:5432/test")
    connConfig.get(USERNAME) shouldBe Some("user")
    connConfig.get(PASSWORD) shouldBe Some("pass")
    connConfig.get(DRIVER) shouldBe Some(POSTGRES_DRIVER)

    // Verify no validations (none defined)
    validations shouldBe empty
  }

  test("UnifiedConfigConverter handles foreign keys correctly") {
    val unifiedYaml =
      s"""version: "1.0"
         |name: "fk_test"
         |dataSources:
         |  - name: "db"
         |    connection:
         |      type: "postgres"
         |      options:
         |        url: "jdbc:postgresql://localhost:5432/test"
         |    steps:
         |      - name: "parents"
         |        fields:
         |          - name: "parent_id"
         |      - name: "children"
         |        fields:
         |          - name: "child_id"
         |          - name: "parent_id"
         |foreignKeys:
         |  - source:
         |      dataSource: "db"
         |      step: "parents"
         |      fields: ["parent_id"]
         |    generate:
         |      - dataSource: "db"
         |        step: "children"
         |        fields: ["parent_id"]
         |        cardinality:
         |          min: 2
         |          max: 5
         |        nullability:
         |          nullPercentage: 0.1
         |          strategy: "random"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/fk-test.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val config = UnifiedYamlParser.parse(configFile.toString)(null)
    val (plan, _, _, _) = UnifiedConfigConverter.convert(config)

    val foreignKeys = plan.sinkOptions.get.foreignKeys
    foreignKeys should have size 1
    foreignKeys.head.source.dataSource shouldBe "db"
    foreignKeys.head.source.step shouldBe "parents"
    foreignKeys.head.generate.head.cardinality.get.min shouldBe Some(2)
    foreignKeys.head.generate.head.cardinality.get.max shouldBe Some(5)
    foreignKeys.head.generate.head.nullability.get.nullPercentage shouldBe 0.1
    foreignKeys.head.generate.head.nullability.get.strategy shouldBe "random"
  }

  test("UnifiedConfigConverter extracts inline validations") {
    val unifiedYaml =
      s"""version: "1.0"
         |name: "validation_test"
         |dataSources:
         |  - name: "my_json"
         |    connection:
         |      type: "json"
         |      options:
         |        path: "/tmp/data"
         |    steps:
         |      - name: "accounts"
         |        fields:
         |          - name: "id"
         |          - name: "amount"
         |            type: "double"
         |        validations:
         |          - expr: "amount > 0"
         |          - field: "id"
         |            validation:
         |              - type: "unique"
         |              - type: "matches"
         |                regex: "ACC[0-9]+"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/validation-test.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    val config = UnifiedYamlParser.parse(configFile.toString)(null)
    val (_, _, validations, _) = UnifiedConfigConverter.convert(config)

    validations shouldBe defined
    validations.get should have size 1
    val validationConfig = validations.get.head
    validationConfig.name shouldBe "unified_validations"
    validationConfig.dataSources should have size 1
    validationConfig.dataSources should contain key "my_json"
    // dataSources is Map[String, List[DataSourceValidation]]
    val dsValidations = validationConfig.dataSources("my_json")
    dsValidations should have size 1
    dsValidations.head.validations should have size 2
  }

  test("Unified YAML helper options generate expected data") {
    val outputPath = s"$testDataPath/output/helper-options"
    val unifiedYaml =
      s"""version: "1.0"
         |name: "helper_yaml_generation"
         |dataSources:
         |  - name: "helper_json"
         |    connection:
         |      type: "json"
         |      options:
         |        path: "$outputPath"
         |    steps:
         |      - name: "helper_step"
         |        count:
         |          records: 50
         |        fields:
         |          - name: "email"
         |            options:
         |              email: ""
         |          - name: "user_uuid"
         |            options:
         |              uuidPattern: true
         |          - name: "recent_date"
         |            type: "date"
         |            options:
         |              withinDays: 7
         |              excludeWeekends: true
         |          - name: "work_time"
         |            type: "timestamp"
         |            options:
         |              businessHours:
         |                startHour: 8
         |                endHour: 18
         |          - name: "order_id"
         |            options:
         |              sequence:
         |                start: 100
         |                step: 2
         |                prefix: "ORD-"
         |                padding: 4
         |          - name: "version"
         |            options:
         |              semanticVersion:
         |                major: 2
         |                minor: 1
         |                patchIncrement: true
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/helper-options.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    PlanProcessor.executeFromUnifiedYaml(configFile.toString)

    val df = sparkSession.read.json(outputPath)
    df.count() shouldBe 50

    val emails = df.select("email").collect().map(_.getString(0))
    emails.foreach(email => assert(email.contains("@"), s"Invalid email: $email"))

    val uuids = df.select("user_uuid").collect().map(_.getString(0))
    uuids.foreach(uuid => assert(uuid.matches("^[0-9a-fA-F-]{36}$"), s"Invalid UUID: $uuid"))

    val recentDates = df.select(to_date(col("recent_date")).as("recent_date"))
    val daysAgo = recentDates.select(datediff(current_date(), col("recent_date"))).collect().map(_.getInt(0))
    daysAgo.foreach(days => assert(days >= 0 && days <= 7, s"Date out of range: $days"))
    val dayOfWeekValues = recentDates.select(dayofweek(col("recent_date"))).collect().map(_.getInt(0))
    dayOfWeekValues.foreach(day => assert(day >= 2 && day <= 6, s"Weekend date found: $day"))

    val workHours = df.select(hour(to_timestamp(col("work_time")))).collect().map(_.getInt(0))
    workHours.foreach(hourVal => assert(hourVal >= 8 && hourVal < 18, s"Hour out of range: $hourVal"))

    val orderIds = df.select("order_id").collect().map(_.getString(0))
    val orderNumbers = orderIds.map(_.stripPrefix("ORD-").toInt).sorted
    val expectedNumbers = (0 until 50).map(i => 100 + (i * 2)).toList
    orderNumbers.toList shouldBe expectedNumbers

    val versions = df.select("version").collect().map(_.getString(0))
    val patchNumbers = versions.map(_.split("\\.")(2).toInt).sorted
    patchNumbers.toList shouldBe (0 until 50).toList
  }

  test("Unified YAML with JSON data source generates data correctly") {
    val outputPath = s"$testDataPath/output/json-generation"
    val unifiedYaml =
      s"""version: "1.0"
         |name: "json_generation_test"
         |config:
         |  flags:
         |    enableGenerateData: true
         |    enableSaveReports: false
         |  generation:
         |    numRecordsPerBatch: 100
         |dataSources:
         |  - name: "test_json"
         |    connection:
         |      type: "json"
         |      options:
         |        path: "$outputPath"
         |    steps:
         |      - name: "accounts"
         |        count:
         |          records: 50
         |        fields:
         |          - name: "account_id"
         |            options:
         |              regex: "ACC[0-9]{8}"
         |          - name: "balance"
         |            type: "double"
         |            options:
         |              min: 0.0
         |              max: 10000.0
         |          - name: "status"
         |            options:
         |              oneOf:
         |                - "ACTIVE"
         |                - "INACTIVE"
         |                - "PENDING"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/json-generation.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute using PlanProcessor
    val results = PlanProcessor.executeFromUnifiedYaml(configFile.toString)

    // Verify execution completed
    results should not be null
    results.generationResults should not be empty

    // Verify data was generated
    val outputDir = new File(outputPath)
    outputDir.exists() shouldBe true

    // Read and verify the generated data
    val generatedData = sparkSession.read.json(outputPath)
    generatedData.count() shouldBe 50

    // Verify field types and patterns
    val sample = generatedData.collect()
    sample.foreach { row =>
      val accountId = row.getAs[String]("account_id")
      accountId should startWith("ACC")
      accountId.length shouldBe 11 // ACC + 8 digits

      val balance = row.getAs[Double]("balance")
      balance should be >= 0.0
      balance should be <= 10000.0

      val status = row.getAs[String]("status")
      List("ACTIVE", "INACTIVE", "PENDING") should contain(status)
    }
  }

  test("Unified YAML with nested fields generates correct structure") {
    val outputPath = s"$testDataPath/output/nested-fields"
    val unifiedYaml =
      s"""version: "1.0"
         |name: "nested_fields_test"
         |config:
         |  flags:
         |    enableGenerateData: true
         |    enableSaveReports: false
         |dataSources:
         |  - name: "test_json"
         |    connection:
         |      type: "json"
         |      options:
         |        path: "$outputPath"
         |    steps:
         |      - name: "users"
         |        count:
         |          records: 20
         |        fields:
         |          - name: "user_id"
         |            options:
         |              regex: "USR[0-9]{6}"
         |          - name: "profile"
         |            type: "struct"
         |            fields:
         |              - name: "firstName"
         |                options:
         |                  expression: "#{Name.firstName}"
         |              - name: "lastName"
         |                options:
         |                  expression: "#{Name.lastName}"
         |              - name: "address"
         |                type: "struct"
         |                fields:
         |                  - name: "city"
         |                    options:
         |                      expression: "#{Address.city}"
         |                  - name: "country"
         |                    options:
         |                      oneOf:
         |                        - "USA"
         |                        - "UK"
         |                        - "Australia"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/nested-fields.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute
    val results = PlanProcessor.executeFromUnifiedYaml(configFile.toString)
    results should not be null

    // Verify data
    val generatedData = sparkSession.read.json(outputPath)
    generatedData.count() shouldBe 20

    // Verify nested structure
    val schema = generatedData.schema
    schema.fieldNames should contain("user_id")
    schema.fieldNames should contain("profile")

    val profileField = schema("profile").dataType.asInstanceOf[org.apache.spark.sql.types.StructType]
    profileField.fieldNames should contain("firstName")
    profileField.fieldNames should contain("lastName")
    profileField.fieldNames should contain("address")

    val addressField = profileField("address").dataType.asInstanceOf[org.apache.spark.sql.types.StructType]
    addressField.fieldNames should contain("city")
    addressField.fieldNames should contain("country")
  }

  test("Unified YAML with multiple data sources generates data for each") {
    val jsonPath = s"$testDataPath/output/multi-ds/json"
    val csvPath = s"$testDataPath/output/multi-ds/csv"

    val unifiedYaml =
      s"""version: "1.0"
         |name: "multi_datasource_test"
         |config:
         |  flags:
         |    enableGenerateData: true
         |    enableSaveReports: false
         |dataSources:
         |  - name: "json_ds"
         |    connection:
         |      type: "json"
         |      options:
         |        path: "$jsonPath"
         |    steps:
         |      - name: "products"
         |        count:
         |          records: 30
         |        fields:
         |          - name: "product_id"
         |            options:
         |              regex: "PROD[0-9]{5}"
         |          - name: "name"
         |            options:
         |              expression: "#{Commerce.productName}"
         |
         |  - name: "csv_ds"
         |    connection:
         |      type: "csv"
         |      options:
         |        path: "$csvPath"
         |    steps:
         |      - name: "categories"
         |        count:
         |          records: 10
         |        fields:
         |          - name: "category_id"
         |            options:
         |              regex: "CAT[0-9]{3}"
         |          - name: "category_name"
         |            options:
         |              oneOf:
         |                - "Electronics"
         |                - "Clothing"
         |                - "Books"
         |                - "Home"
         |""".stripMargin

    val configFile = Path.of(s"$testConfigPath/multi-datasource.yaml")
    Files.writeString(configFile, unifiedYaml, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    // Execute
    val results = PlanProcessor.executeFromUnifiedYaml(configFile.toString)
    results should not be null
    results.generationResults should have size 2

    // Verify JSON data
    val jsonData = sparkSession.read.json(jsonPath)
    jsonData.count() shouldBe 30
    jsonData.schema.fieldNames should contain("product_id")
    jsonData.schema.fieldNames should contain("name")

    // Verify CSV data - Spark writes to multiple part files
    // The key thing is that data was generated successfully
    val csvDir = new File(csvPath)
    csvDir.exists() shouldBe true
    // CSV files exist in the directory
    Option(csvDir.listFiles()).map(_.filter(_.getName.endsWith(".csv")).length).getOrElse(0) should be >= 1
  }
}
