package io.github.datacatering.datacaterer.core.parser.unified

import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.unified._
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.scalatest.funsuite.AnyFunSuite

class UnifiedConfigTest extends AnyFunSuite {

  private val yamlMapper = ObjectMapperUtil.yamlObjectMapper

  test("can parse minimal unified config") {
    val yaml =
      """
        |version: "1.0"
        |name: "test_plan"
        |dataSources:
        |  - name: "my_json"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data"
        |    steps:
        |      - name: "accounts"
        |        count:
        |          records: 100
        |        fields:
        |          - name: "id"
        |            options:
        |              regex: "ACC[0-9]{8}"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    assert(config.version == "1.0")
    assert(config.name == "test_plan")
    assert(config.dataSources.size == 1)
    assert(config.dataSources.head.name == "my_json")
    assert(config.dataSources.head.connection.connectionType == "json")
    assert(config.dataSources.head.connection.options.get("path").contains("/tmp/data"))
    assert(config.dataSources.head.steps.size == 1)
    assert(config.dataSources.head.steps.head.name == "accounts")
    assert(config.dataSources.head.steps.head.count.get.records.contains(100L))
    assert(config.dataSources.head.steps.head.fields.size == 1)
    assert(config.dataSources.head.steps.head.fields.head.name == "id")
  }

  test("can parse unified config with postgres connection") {
    val yaml =
      """
        |version: "1.0"
        |name: "postgres_plan"
        |dataSources:
        |  - name: "ecommerce_postgres"
        |    connection:
        |      type: "postgres"
        |      options:
        |        url: "jdbc:postgresql://localhost:5432/ecommerce"
        |        user: "postgres"
        |        password: "secret"
        |    steps:
        |      - name: "customers"
        |        options:
        |          dbtable: "public.customers"
        |        count:
        |          records: 1000
        |        fields:
        |          - name: "customer_id"
        |            options:
        |              regex: "CUST[0-9]{8}"
        |              isPrimaryKey: true
        |          - name: "email"
        |            options:
        |              expression: "#{Internet.emailAddress}"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    assert(config.dataSources.head.connection.connectionType == "postgres")
    assert(config.dataSources.head.connection.options.get("url").contains("jdbc:postgresql://localhost:5432/ecommerce"))
    assert(config.dataSources.head.connection.options.get("user").contains("postgres"))
    assert(config.dataSources.head.connection.options.get("password").contains("secret"))
    assert(config.dataSources.head.steps.head.options.get("dbtable").contains("public.customers"))
  }

  test("can parse unified config with foreign keys") {
    val yaml =
      """
        |version: "1.0"
        |name: "fk_plan"
        |dataSources:
        |  - name: "my_postgres"
        |    connection:
        |      type: "postgres"
        |      options:
        |        url: "jdbc:postgresql://localhost:5432/db"
        |    steps:
        |      - name: "customers"
        |        fields:
        |          - name: "customer_id"
        |      - name: "orders"
        |        fields:
        |          - name: "order_id"
        |          - name: "customer_id"
        |foreignKeys:
        |  - source:
        |      dataSource: "my_postgres"
        |      step: "customers"
        |      fields: ["customer_id"]
        |    generate:
        |      - dataSource: "my_postgres"
        |        step: "orders"
        |        fields: ["customer_id"]
        |    cardinality:
        |      min: 1
        |      max: 10
        |      distribution: "uniform"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    assert(config.foreignKeys.size == 1)
    val fk = config.foreignKeys.head
    assert(fk.source.dataSource == "my_postgres")
    assert(fk.source.step == "customers")
    assert(fk.source.fields == List("customer_id"))
    assert(fk.generate.size == 1)
    assert(fk.generate.head.step == "orders")
  }

  test("can parse unified config with inline validations") {
    val yaml =
      """
        |version: "1.0"
        |name: "validation_plan"
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

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    val validations = config.dataSources.head.steps.head.validations
    assert(validations.size == 2)
    assert(validations.head.expr.contains("amount > 0"))
    assert(validations(1).field.contains("id"))
    assert(validations(1).validation.get.size == 2)
    assert(validations(1).validation.get.head.validationType == "unique")
    assert(validations(1).validation.get(1).validationType == "matches")
  }

  test("can parse unified config with runtime configuration") {
    val yaml =
      """
        |version: "1.0"
        |name: "config_plan"
        |config:
        |  flags:
        |    enableGenerateData: true
        |    enableValidation: true
        |    enableFastGeneration: true
        |  generation:
        |    numRecordsPerBatch: 50000
        |  folders:
        |    generatedReportsFolderPath: "/custom/reports"
        |  runtime:
        |    master: "local[4]"
        |    sparkConfig:
        |      "spark.sql.shuffle.partitions": "20"
        |dataSources:
        |  - name: "my_json"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data"
        |    steps:
        |      - name: "data"
        |        fields:
        |          - name: "id"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    assert(config.config.isDefined)
    val runtimeConfig = config.config.get
    assert(runtimeConfig.flags.get.enableGenerateData.contains(true))
    assert(runtimeConfig.flags.get.enableValidation.contains(true))
    assert(runtimeConfig.flags.get.enableFastGeneration.contains(true))
    assert(runtimeConfig.generation.get.numRecordsPerBatch.contains(50000L))
    assert(runtimeConfig.folders.get.generatedReportsFolderPath.contains("/custom/reports"))
    assert(runtimeConfig.runtime.get.master.contains("local[4]"))
    assert(runtimeConfig.runtime.get.sparkConfig.get.get("spark.sql.shuffle.partitions").contains("20"))
  }

  test("can parse unified config with nested fields") {
    val yaml =
      """
        |version: "1.0"
        |name: "nested_plan"
        |dataSources:
        |  - name: "my_json"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data"
        |    steps:
        |      - name: "users"
        |        fields:
        |          - name: "user_id"
        |          - name: "profile"
        |            type: "struct"
        |            fields:
        |              - name: "firstName"
        |              - name: "lastName"
        |              - name: "address"
        |                type: "struct"
        |                fields:
        |                  - name: "street"
        |                  - name: "city"
        |                  - name: "zipcode"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    val fields = config.dataSources.head.steps.head.fields
    assert(fields.size == 2)
    assert(fields(1).name == "profile")
    assert(fields(1).fieldType.contains("struct"))
    assert(fields(1).fields.size == 3)
    assert(fields(1).fields(2).name == "address")
    assert(fields(1).fields(2).fields.size == 3)
  }

  test("applies helper conversions for unified tasks") {
    val yaml =
      """
        |version: "1.0"
        |name: "helper_plan"
        |dataSources:
        |  - name: "messaging"
        |    connection:
        |      type: "kafka"
        |      options:
        |        bootstrapServers: "localhost:9092"
        |    steps:
        |      - name: "messages"
        |        count:
        |          records: 10
        |          options:
        |            min: 1
        |            max: 5
        |          perField:
        |            fieldNames: ["message_id"]
        |            count: 2
        |            options:
        |              max: 3
        |        fields:
        |          - name: "messageHeaders"
        |            fields:
        |              - name: "traceId"
        |                static: "trace-123"
        |          - name: "messageBody"
        |            fields:
        |              - name: "message_id"
        |                options:
        |                  uuid: ""
        |                  incremental: 5
        |              - name: "seq"
        |                options:
        |                  incremental: 1
        |  - name: "http"
        |    connection:
        |      type: "http"
        |      options:
        |        url: "https://example.com"
        |    steps:
        |      - name: "requests"
        |        fields:
        |          - name: "httpUrl"
        |            fields:
        |              - name: "url"
        |                static: "https://example.com/users/{userId}"
        |              - name: "method"
        |                static: "POST"
        |              - name: "pathParam"
        |                fields:
        |                  - name: "userId"
        |                    type: "string"
        |              - name: "queryParam"
        |                fields:
        |                  - name: "verbose"
        |                    type: "boolean"
        |                    options:
        |                      style: "FORM"
        |                      explode: false
        |          - name: "httpHeaders"
        |            fields:
        |              - name: "Content-Type"
        |          - name: "httpBody"
        |            fields:
        |              - name: "choice"
        |                options:
        |                  oneOf: "a->0.7,b->0.3"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])
    val (_, tasks, _, _) = UnifiedConfigConverter.convert(config)

    val messagingTask = tasks.find(_.name == "messaging_task").get
    val messagingStep = messagingTask.steps.head
    val messagingFieldNames = messagingStep.fields.map(_.name).toSet
    assert(messagingFieldNames.contains(REAL_TIME_HEADERS_FIELD))
    assert(messagingFieldNames.contains(REAL_TIME_BODY_FIELD))
    assert(messagingFieldNames.contains(REAL_TIME_BODY_CONTENT_FIELD))
    assert(!messagingFieldNames.contains(YAML_REAL_TIME_HEADERS_FIELD))
    assert(!messagingFieldNames.contains(YAML_REAL_TIME_BODY_FIELD))
    assert(messagingStep.count.options.values.forall(_.isInstanceOf[String]))
    messagingStep.count.perField.foreach(perField =>
      assert(perField.options.values.forall(_.isInstanceOf[String]))
    )

    val messagingBodyField = messagingStep.fields.find(_.name == REAL_TIME_BODY_CONTENT_FIELD).get
    val messagingBodyNames = messagingBodyField.fields.map(_.name).toSet
    assert(messagingBodyNames.contains("message_id"))
    assert(messagingBodyNames.contains("seq"))

    val httpTask = tasks.find(_.name == "http_task").get
    val httpStep = httpTask.steps.head
    val httpFieldNames = httpStep.fields.map(_.name).toSet
    assert(httpFieldNames.contains(REAL_TIME_URL_FIELD))
    assert(httpFieldNames.contains(REAL_TIME_METHOD_FIELD))
    assert(httpFieldNames.contains(s"$HTTP_PATH_PARAM_FIELD_PREFIX" + "userId"))
    assert(httpFieldNames.contains(s"$HTTP_QUERY_PARAM_FIELD_PREFIX" + "verbose"))
    assert(httpFieldNames.contains(s"$HTTP_HEADER_FIELD_PREFIX" + "Content_Type"))
    assert(httpFieldNames.contains(REAL_TIME_BODY_FIELD))
    assert(httpFieldNames.contains(REAL_TIME_BODY_CONTENT_FIELD))
    assert(!httpFieldNames.contains(YAML_HTTP_URL_FIELD))
    assert(!httpFieldNames.contains(YAML_HTTP_HEADERS_FIELD))
    assert(!httpFieldNames.contains(YAML_HTTP_BODY_FIELD))

    val httpBodyField = httpStep.fields.find(_.name == REAL_TIME_BODY_CONTENT_FIELD).get
    val httpBodyNames = httpBodyField.fields.map(_.name).toSet
    assert(httpBodyNames.contains("choice"))
    assert(httpBodyNames.contains("choice_weight"))
  }

  test("applies YAML helper options for new generator helpers") {
    val yaml =
      """
        |version: "1.0"
        |name: "yaml_helper_options"
        |dataSources:
        |  - name: "json"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data"
        |    steps:
        |      - name: "helpers"
        |        fields:
        |          - name: "email"
        |            options:
        |              email: "company.com"
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
        |          - name: "priority"
        |            options:
        |              conditionalValue:
        |                cases:
        |                  - when: "amount > 1000"
        |                    then: "HIGH"
        |                  - when: "amount > 500"
        |                    then: "MEDIUM"
        |                else: "LOW"
        |          - name: "status_code"
        |            options:
        |              mapping:
        |                sourceField: "status"
        |                mappings:
        |                  OPEN: 1
        |                  CLOSED: 2
        |                defaultValue: 0
        |          - name: "base_score"
        |            type: "double"
        |            options:
        |              min: 0
        |              max: 100
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])
    val (_, tasks, _, _) = UnifiedConfigConverter.convert(config)
    val fields = tasks.head.steps.head.fields

    val emailField = fields.find(_.name == "email").get
    assert(emailField.options(EXPRESSION) == "#{Name.first_name}.#{Name.last_name}@company.com")

    val recentDateField = fields.find(_.name == "recent_date").get
    assert(recentDateField.options.contains(MINIMUM))
    assert(recentDateField.options.contains(MAXIMUM))
    assert(recentDateField.options(DATE_EXCLUDE_WEEKENDS) == "true")

    val workTimeField = fields.find(_.name == "work_time").get
    assert(workTimeField.options(SQL_GENERATOR).toString.contains("TIMESTAMP_SECONDS"))

    val orderIdField = fields.find(_.name == "order_id").get
    assert(orderIdField.options(SQL_GENERATOR).toString.contains("ORD-"))

    val priorityField = fields.find(_.name == "priority").get
    assert(priorityField.options(SQL_GENERATOR).toString.contains("CASE WHEN amount > 1000 THEN 'HIGH'"))

    val statusCodeField = fields.find(_.name == "status_code").get
    assert(statusCodeField.options(SQL_GENERATOR).toString.contains("status = 'OPEN'"))
  }

  test("UnifiedConfigDetector correctly identifies unified format") {
    val unifiedYaml =
      """
        |version: "1.0"
        |name: "test"
        |dataSources:
        |  - name: "ds"
        |    connection:
        |      type: "json"
        |    steps: []
        |""".stripMargin

    val legacyPlanYaml =
      """
        |name: "legacy_plan"
        |tasks:
        |  - name: "task1"
        |    dataSourceName: "postgres"
        |    enabled: true
        |""".stripMargin

    val legacyTaskYaml =
      """
        |name: "legacy_task"
        |steps:
        |  - name: "step1"
        |    type: "json"
        |    fields: []
        |""".stripMargin

    assert(UnifiedConfigDetector.detectFormatFromContent(unifiedYaml) == UnifiedConfigDetector.UnifiedFormat)
    assert(UnifiedConfigDetector.detectFormatFromContent(legacyPlanYaml) == UnifiedConfigDetector.LegacyPlanFormat)
    assert(UnifiedConfigDetector.detectFormatFromContent(legacyTaskYaml) == UnifiedConfigDetector.LegacyTaskFormat)
  }

  test("UnifiedConfigConverter produces valid Plan and Tasks") {
    val yaml =
      """
        |version: "1.0"
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
        |          records: 500
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

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])
    val (plan, tasks, validations, dataCatererConfig) = UnifiedConfigConverter.convert(config)

    // Verify plan
    assert(plan.name == "converter_test")
    assert(plan.tasks.size == 1)
    assert(plan.tasks.head.name == "test_postgres_task")
    assert(plan.tasks.head.dataSourceName == "test_postgres")
    assert(plan.sinkOptions.get.seed.contains("12345"))
    assert(plan.sinkOptions.get.locale.contains("en-US"))

    // Verify tasks
    assert(tasks.size == 1)
    assert(tasks.head.name == "test_postgres_task")
    assert(tasks.head.steps.size == 1)
    assert(tasks.head.steps.head.name == "users")
    assert(tasks.head.steps.head.`type` == "postgres")
    assert(tasks.head.steps.head.count.records.contains(500L))
    assert(tasks.head.steps.head.fields.size == 2)

    // Verify connection config
    assert(dataCatererConfig.connectionConfigByName.contains("test_postgres"))
    val connConfig = dataCatererConfig.connectionConfigByName("test_postgres")
    assert(connConfig.get(FORMAT).contains(JDBC))
    assert(connConfig.get(URL).contains("jdbc:postgresql://localhost:5432/test"))
    assert(connConfig.get(USERNAME).contains("user"))
    assert(connConfig.get(PASSWORD).contains("pass"))
    assert(connConfig.get(DRIVER).contains(POSTGRES_DRIVER))

    // Verify no validations (none defined)
    assert(validations.isEmpty)
  }

  test("UnifiedConfigConverter handles foreign keys correctly") {
    val yaml =
      """
        |version: "1.0"
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

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])
    val (plan, _, _, _) = UnifiedConfigConverter.convert(config)

    val foreignKeys = plan.sinkOptions.get.foreignKeys
    assert(foreignKeys.size == 1)
    assert(foreignKeys.head.source.dataSource == "db")
    assert(foreignKeys.head.source.step == "parents")
    assert(foreignKeys.head.generate.head.cardinality.get.min.contains(2))
    assert(foreignKeys.head.generate.head.cardinality.get.max.contains(5))
    assert(foreignKeys.head.generate.head.nullability.get.nullPercentage == 0.1)
    assert(foreignKeys.head.generate.head.nullability.get.strategy == "random")
  }

  test("can parse unified config with file-based connections") {
    val yaml =
      """
        |version: "1.0"
        |name: "file_test"
        |dataSources:
        |  - name: "json_ds"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data/json"
        |    steps:
        |      - name: "events"
        |        count:
        |          records: 100
        |        fields:
        |          - name: "event_id"
        |          - name: "timestamp"
        |            type: "timestamp"
        |  - name: "csv_ds"
        |    connection:
        |      type: "csv"
        |      options:
        |        path: "/tmp/data/csv"
        |    steps:
        |      - name: "users"
        |        count:
        |          records: 50
        |        fields:
        |          - name: "user_id"
        |          - name: "name"
        |  - name: "parquet_ds"
        |    connection:
        |      type: "parquet"
        |      options:
        |        path: "/tmp/data/parquet"
        |    steps:
        |      - name: "transactions"
        |        count:
        |          records: 200
        |        fields:
        |          - name: "txn_id"
        |          - name: "amount"
        |            type: "double"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])

    assert(config.dataSources.size == 3)
    assert(config.dataSources.map(_.name) == List("json_ds", "csv_ds", "parquet_ds"))
    assert(config.dataSources.map(_.connection.connectionType) == List("json", "csv", "parquet"))
    assert(config.dataSources.flatMap(_.connection.options.get("path")) == List("/tmp/data/json", "/tmp/data/csv", "/tmp/data/parquet"))
  }

  test("UnifiedConfigConverter handles multiple data sources correctly") {
    val yaml =
      """
        |version: "1.0"
        |name: "multi_ds_test"
        |dataSources:
        |  - name: "postgres_ds"
        |    connection:
        |      type: "postgres"
        |      options:
        |        url: "jdbc:postgresql://localhost:5432/db"
        |        user: "user"
        |        password: "pass"
        |    steps:
        |      - name: "users"
        |        options:
        |          dbtable: "public.users"
        |        fields:
        |          - name: "id"
        |  - name: "json_ds"
        |    connection:
        |      type: "json"
        |      options:
        |        path: "/tmp/data"
        |    steps:
        |      - name: "events"
        |        fields:
        |          - name: "event_id"
        |""".stripMargin

    val config = yamlMapper.readValue(yaml, classOf[UnifiedConfig])
    val (plan, tasks, _, dataCatererConfig) = UnifiedConfigConverter.convert(config)

    // Verify plan
    assert(plan.tasks.size == 2)
    assert(plan.tasks.map(_.dataSourceName).toSet == Set("postgres_ds", "json_ds"))

    // Verify tasks
    assert(tasks.size == 2)

    // Verify connection configs
    assert(dataCatererConfig.connectionConfigByName.contains("postgres_ds"))
    assert(dataCatererConfig.connectionConfigByName.contains("json_ds"))
  }
}
