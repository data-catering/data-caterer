package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.connection.FileBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{ColumnNamesValidation, ExpressionValidation, GroupByValidation, UpstreamDataSourceValidation}
import io.github.datacatering.datacaterer.api.{DataCatererConfigurationBuilder, FieldBuilder, PlanBuilder}
import io.github.datacatering.datacaterer.core.ui.model.{ConfigurationRequest, DataSourceRequest, FieldRequest, FieldRequests, ForeignKeyRequest, ForeignKeyRequestItem, PlanRunRequest, RecordCountRequest, ValidationItemRequest, ValidationItemRequests, ValidationRequest}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UiMapperTest extends AnyFunSuite {

  test("Can convert UI plan run request to plan run") {
    val planRunRequest = PlanRunRequest("plan-name", "some-id", List(DataSourceRequest("csv", "my-csv", Some(CSV), Some(Map(PATH -> "/tmp/csv")))), List(), Some(ConfigurationRequest()))
    val res = UiMapper.mapToPlanRun(planRunRequest, "/tmp/my-install")._plan
    assertResult("plan-name")(res.name)
    assertResult(Some("some-id"))(res.runId)
    assertResult(1)(res.tasks.size)
    assertResult(true)(res.sinkOptions.isEmpty)
    assertResult(0)(res.validations.size)
  }

  test("Can convert UI foreign key config") {
    val foreignKeyRequests = List(ForeignKeyRequest(Some(ForeignKeyRequestItem("task-1", "account_id,year")), List(ForeignKeyRequestItem("task-2", "account_number,year"))))
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_number"), FieldBuilder().name("year"))
    )
    val planBuilder = PlanBuilder()
    val res = UiMapper.foreignKeyMapping(foreignKeyRequests, connections, planBuilder)
    assertResult(1)(res.plan.sinkOptions.get.foreignKeys.size)
    assert(res.plan.sinkOptions.get.foreignKeys.head._1.startsWith("json"))
    assert(res.plan.sinkOptions.get.foreignKeys.head._1.endsWith("account_id,year"))
    assert(res.plan.sinkOptions.get.foreignKeys.head._2.size == 1)
    assert(res.plan.sinkOptions.get.foreignKeys.head._2.head.startsWith("json"))
    assert(res.plan.sinkOptions.get.foreignKeys.head._2.head.endsWith("account_number,year"))
  }

  test("Can convert UI flag config") {
    val configRequest = ConfigurationRequest(flag = Map(
      CONFIG_FLAGS_COUNT -> "false",
      CONFIG_FLAGS_GENERATE_DATA -> "false",
      CONFIG_FLAGS_RECORD_TRACKING -> "false",
      CONFIG_FLAGS_DELETE_GENERATED_RECORDS -> "false",
      CONFIG_FLAGS_GENERATE_PLAN_AND_TASKS -> "false",
      CONFIG_FLAGS_FAIL_ON_ERROR -> "false",
      CONFIG_FLAGS_UNIQUE_CHECK -> "false",
      CONFIG_FLAGS_SINK_METADATA -> "false",
      CONFIG_FLAGS_SAVE_REPORTS -> "false",
      CONFIG_FLAGS_VALIDATION -> "false",
      CONFIG_FLAGS_GENERATE_VALIDATIONS -> "false",
      CONFIG_FLAGS_ALERTS -> "false",
      "blah" -> "false"
    ))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapFlagsConfiguration(configRequest, baseConf).build

    assert(!res.flagsConfig.enableCount)
    assert(!res.flagsConfig.enableGenerateData)
    assert(!res.flagsConfig.enableRecordTracking)
    assert(!res.flagsConfig.enableDeleteGeneratedRecords)
    assert(!res.flagsConfig.enableGeneratePlanAndTasks)
    assert(!res.flagsConfig.enableFailOnError)
    assert(!res.flagsConfig.enableUniqueCheck)
    assert(!res.flagsConfig.enableSinkMetadata)
    assert(!res.flagsConfig.enableSaveReports)
    assert(!res.flagsConfig.enableValidation)
    assert(!res.flagsConfig.enableGenerateValidations)
    assert(!res.flagsConfig.enableAlerts)
  }

  test("Can convert UI alert config") {
    val configRequest = ConfigurationRequest(alert = Map(CONFIG_ALERT_TRIGGER_ON -> "failure", CONFIG_ALERT_SLACK_TOKEN -> "abc123",
      CONFIG_ALERT_SLACK_CHANNELS -> "job-fail", "blah" -> "hello"))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapAlertConfiguration(configRequest, baseConf).build

    assert(res.alertConfig.triggerOn == "failure")
    assert(res.alertConfig.slackAlertConfig.token == "abc123")
    assert(res.alertConfig.slackAlertConfig.channels == List("job-fail"))
  }

  test("Can convert UI validation config") {
    val configRequest = ConfigurationRequest(validation = Map(CONFIG_VALIDATION_NUM_SAMPLE_ERROR_RECORDS -> "2",
      CONFIG_VALIDATION_ENABLE_DELETE_RECORD_TRACKING_FILES -> "false", "blah" -> "hello"))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapValidationConfiguration(configRequest, baseConf).build

    assert(res.validationConfig.numSampleErrorRecords == 2)
    assert(!res.validationConfig.enableDeleteRecordTrackingFiles)
  }

  test("Can convert UI generation config") {
    val configRequest = ConfigurationRequest(generation = Map(CONFIG_GENERATION_NUM_RECORDS_PER_BATCH -> "100",
      CONFIG_GENERATION_NUM_RECORDS_PER_STEP -> "10", "blah" -> "hello"))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapGenerationConfiguration(configRequest, baseConf).build

    assert(res.generationConfig.numRecordsPerBatch == 100)
    assert(res.generationConfig.numRecordsPerStep.contains(10))
  }

  test("Can convert UI metadata config") {
    val configRequest = ConfigurationRequest(metadata = Map(
      CONFIG_METADATA_NUM_RECORDS_FROM_DATA_SOURCE -> "100",
      CONFIG_METADATA_NUM_RECORDS_FOR_ANALYSIS -> "10",
      CONFIG_METADATA_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD -> "1",
      CONFIG_METADATA_ONE_OF_MIN_COUNT -> "5",
      CONFIG_METADATA_NUM_GENERATED_SAMPLES -> "7",
      "blah" -> "hello"
    ))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapMetadataConfiguration(configRequest, baseConf).build

    assert(res.metadataConfig.numRecordsFromDataSource == 100)
    assert(res.metadataConfig.numRecordsForAnalysis == 10)
    assert(res.metadataConfig.oneOfDistinctCountVsCountThreshold == 1)
    assert(res.metadataConfig.oneOfMinCount == 5)
    assert(res.metadataConfig.numGeneratedSamples == 7)
  }

  test("Can convert UI folder config") {
    val configRequest = ConfigurationRequest(folder = Map(
      CONFIG_FOLDER_PLAN_FILE_PATH -> "/tmp/plan-file",
      CONFIG_FOLDER_TASK_FOLDER_PATH -> "/tmp/task-folder",
      CONFIG_FOLDER_GENERATED_PLAN_AND_TASK_FOLDER_PATH -> "/tmp/gen",
      CONFIG_FOLDER_GENERATED_REPORTS_FOLDER_PATH -> "/tmp/report",
      CONFIG_FOLDER_RECORD_TRACKING_FOLDER_PATH -> "/tmp/record",
      CONFIG_FOLDER_VALIDATION_FOLDER_PATH -> "/tmp/valid",
      CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH -> "/tmp/record-valid",
      "blah" -> "hello"
    ))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapFolderConfiguration(configRequest, "/my-install", baseConf).build

    assert(res.foldersConfig.planFilePath == "/tmp/plan-file")
    assert(res.foldersConfig.taskFolderPath == "/tmp/task-folder")
    assert(res.foldersConfig.generatedPlanAndTaskFolderPath == "/tmp/gen")
    assert(res.foldersConfig.generatedReportsFolderPath == "/tmp/report")
    assert(res.foldersConfig.recordTrackingFolderPath == "/tmp/record")
    assert(res.foldersConfig.validationFolderPath == "/tmp/valid")
    assert(res.foldersConfig.recordTrackingForValidationFolderPath == "/tmp/record-valid")
  }

  test("Can convert UI folder config with install directory") {
    val configRequest = ConfigurationRequest(folder = Map(
      CONFIG_FOLDER_PLAN_FILE_PATH -> "",
      CONFIG_FOLDER_TASK_FOLDER_PATH -> "",
      CONFIG_FOLDER_GENERATED_PLAN_AND_TASK_FOLDER_PATH -> "",
      CONFIG_FOLDER_GENERATED_REPORTS_FOLDER_PATH -> "",
      CONFIG_FOLDER_RECORD_TRACKING_FOLDER_PATH -> "",
      CONFIG_FOLDER_VALIDATION_FOLDER_PATH -> "",
      CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH -> "",
      "blah" -> "hello"
    ))
    val baseConf = DataCatererConfigurationBuilder()
    val res = UiMapper.mapFolderConfiguration(configRequest, "/my-install", baseConf).build

    assert(res.foldersConfig.planFilePath == DEFAULT_PLAN_FILE_PATH)
    assert(res.foldersConfig.taskFolderPath == "/my-install/task")
    assert(res.foldersConfig.generatedPlanAndTaskFolderPath == "/my-install/generated-plan-task")
    assert(res.foldersConfig.generatedReportsFolderPath == "/my-install/report")
    assert(res.foldersConfig.recordTrackingFolderPath == "/my-install/record-tracking")
    assert(res.foldersConfig.validationFolderPath == "/my-install/validation")
    assert(res.foldersConfig.recordTrackingForValidationFolderPath == "/my-install/record-tracking-validation")
  }

  test("Can convert UI count configuration with base record count") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(Some(10))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.records.contains(10))
  }

  test("Can convert UI count configuration with base record min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(None, Some(1), Some(10))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.generator.isDefined)
    assert(res.generator.get.options.get(MINIMUM).contains("1"))
    assert(res.generator.get.options.get(MAXIMUM).contains("10"))
  }

  test("Can convert UI count configuration with per column count") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecords = Some(10))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assert(res.perColumn.get.columnNames.size == 1)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.count.contains(10))
  }

  test("Can convert UI count configuration with per column min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsMin = Some(10), perColumnRecordsMax = Some(20))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assert(res.perColumn.get.columnNames.size == 1)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(MINIMUM).contains("10"))
    assert(res.perColumn.get.generator.get.options.get(MAXIMUM).contains("20"))
  }

  test("Can convert UI count configuration with per column distribution") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsDistribution = Some(DISTRIBUTION_EXPONENTIAL), perColumnRecordsDistributionRateParam = Some("0.5"))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assert(res.perColumn.get.columnNames.size == 1)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION).contains(DISTRIBUTION_EXPONENTIAL))
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION_RATE_PARAMETER).contains("0.5"))
  }

  test("Can convert UI count configuration with per column distribution with min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsDistribution = Some(DISTRIBUTION_EXPONENTIAL), perColumnRecordsDistributionRateParam = Some("0.5"), perColumnRecordsMin = Some(1), perColumnRecordsMax = Some(3))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assert(res.perColumn.get.columnNames.size == 1)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION).contains(DISTRIBUTION_EXPONENTIAL))
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION_RATE_PARAMETER).contains("0.5"))
    assert(res.perColumn.get.generator.get.options.get(MINIMUM).contains("1"))
    assert(res.perColumn.get.generator.get.options.get(MAXIMUM).contains("3"))
  }

  test("Can convert UI count configuration with per column normal distribution with min and max") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", count = Some(RecordCountRequest(perColumnNames = Some(List("account_id")), perColumnRecordsDistribution = Some(DISTRIBUTION_NORMAL), perColumnRecordsMin = Some(1), perColumnRecordsMax = Some(3))))
    val res = UiMapper.countMapping(dataSourceRequest).count
    assert(res.perColumn.isDefined)
    assert(res.perColumn.get.columnNames.size == 1)
    assert(res.perColumn.get.columnNames.contains("account_id"))
    assert(res.perColumn.get.generator.isDefined)
    assert(res.perColumn.get.generator.get.options.get(DISTRIBUTION).contains(DISTRIBUTION_NORMAL))
    assert(res.perColumn.get.generator.get.options.get(MINIMUM).contains("1"))
    assert(res.perColumn.get.generator.get.options.get(MAXIMUM).contains("3"))
  }

  test("Can convert UI field mapping") {
    val dataSourceRequest = DataSourceRequest("plan-name", "task-1", fields = Some(FieldRequests(Some(List(
      FieldRequest("name", "string"),
      FieldRequest("account_id", "string", Some(Map(REGEX_GENERATOR -> "acc[0-9]{1}"))),
      FieldRequest("status", "string", Some(Map(ONE_OF_GENERATOR -> "open,closed"))),
      FieldRequest("details", "struct", nested = Some(FieldRequests(Some(List(FieldRequest("age", "integer")))))),
    )))))
    val res = UiMapper.fieldMapping(dataSourceRequest)
    assert(res.size == 4)
    val nameField = res.find(_.field.name == "name")
    assert(nameField.exists(_.field.`type`.contains("string")))
    val accountField = res.find(_.field.name == "account_id")
    assert(accountField.exists(_.field.`type`.contains("string")))
    assert(accountField.exists(_.field.generator.exists(_.`type` == REGEX_GENERATOR)))
    assert(accountField.exists(_.field.generator.exists(_.options.get(REGEX_GENERATOR).contains("acc[0-9]{1}"))))
    val statusField = res.find(_.field.name == "status")
    assert(statusField.exists(_.field.generator.exists(_.`type` == ONE_OF_GENERATOR)))
    assert(statusField.exists(_.field.generator.exists(_.options.get(ONE_OF_GENERATOR).contains(List("open", "closed")))))
    val detailsField = res.find(_.field.name == "details")
    assertResult(Some("struct<>"))(detailsField.get.field.`type`)
    assert(detailsField.exists(_.field.schema.isDefined))
    assert(detailsField.exists(_.field.schema.get.fields.isDefined))
    assert(detailsField.exists(_.field.schema.get.fields.get.size == 1))
    assert(detailsField.exists(_.field.schema.get.fields.get.head.name == "age"))
    assert(detailsField.exists(_.field.schema.get.fields.get.head.`type`.contains("integer")))
  }

  test("Can convert UI connection mapping for Cassandra") {
    val dataSourceRequest = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("cassandra-name")(res.dataSourceName)
    assertResult(8)(res.options.size)
    assertResult(Some(CASSANDRA))(res.options.get(FORMAT))
    assertResult(Some("localhost:9092"))(res.options.get(URL))
    assertResult(Some("localhost"))(res.options.get("spark.cassandra.connection.host"))
    assertResult(Some("9092"))(res.options.get("spark.cassandra.connection.port"))
    assertResult(Some("cassandra"))(res.options.get(USERNAME))
    assertResult(Some("cassandra"))(res.options.get(PASSWORD))
    assertResult(Some("cassandra"))(res.options.get("spark.cassandra.auth.username"))
    assertResult(Some("cassandra"))(res.options.get("spark.cassandra.auth.password"))
  }

  test("Can convert UI connection mapping for Cassandra with keyspace and table") {
    val dataSourceRequest = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra", CASSANDRA_KEYSPACE -> "account", CASSANDRA_TABLE -> "accounts")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("cassandra-name")(res.dataSourceName)
    assertResult(10)(res.options.size)
    assertResult(Some(CASSANDRA))(res.options.get(FORMAT))
    assertResult(Some("localhost:9092"))(res.options.get(URL))
    assertResult(Some("account"))(res.options.get(CASSANDRA_KEYSPACE))
    assertResult(Some("accounts"))(res.options.get(CASSANDRA_TABLE))
  }

  test("Throw error if only keyspace or table is defined for Cassandra") {
    val dataSourceRequest = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra", CASSANDRA_KEYSPACE -> "account")))
    val dataSourceRequest1 = DataSourceRequest("cassandra-name", "task-1", Some(CASSANDRA_NAME), Some(Map(URL -> "localhost:9092", USERNAME -> "cassandra", PASSWORD -> "cassandra", CASSANDRA_TABLE -> "accounts")))
    assertThrows[IllegalArgumentException](UiMapper.connectionMapping(dataSourceRequest))
    assertThrows[IllegalArgumentException](UiMapper.connectionMapping(dataSourceRequest1))
  }

  test("Can convert UI connection mapping for Postgres") {
    val dataSourceRequest = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER)))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("postgres-name")(res.dataSourceName)
    assertResult(5)(res.options.size)
    assertResult(Some(JDBC))(res.options.get(FORMAT))
    assertResult(Some(POSTGRES_DRIVER))(res.options.get(DRIVER))
    assertResult(Some("localhost:5432"))(res.options.get(URL))
    assertResult(Some("postgres"))(res.options.get(USERNAME))
    assertResult(Some("postgres"))(res.options.get(PASSWORD))
  }

  test("Can convert UI connection mapping for MySQL") {
    val dataSourceRequest = DataSourceRequest("mysql-name", "task-1", Some(MYSQL), Some(Map(URL -> "localhost:5432", USERNAME -> "root", PASSWORD -> "root", DRIVER -> MYSQL_DRIVER)))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("mysql-name")(res.dataSourceName)
    assertResult(5)(res.options.size)
    assertResult(Some(JDBC))(res.options.get(FORMAT))
    assertResult(Some(MYSQL_DRIVER))(res.options.get(DRIVER))
    assertResult(Some("localhost:5432"))(res.options.get(URL))
    assertResult(Some("root"))(res.options.get(USERNAME))
    assertResult(Some("root"))(res.options.get(PASSWORD))
  }

  test("Can convert UI connection mapping for Postgres with schema and table") {
    val dataSourceRequest = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER, SCHEMA -> "account", TABLE -> "accounts")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult("postgres-name")(res.dataSourceName)
    assertResult(7)(res.options.size)
    assertResult(Some(JDBC))(res.options.get(FORMAT))
    assertResult(Some(POSTGRES_DRIVER))(res.options.get(DRIVER))
    assertResult(Some("localhost:5432"))(res.options.get(URL))
    assertResult(Some("postgres"))(res.options.get(USERNAME))
    assertResult(Some("postgres"))(res.options.get(PASSWORD))
    assertResult(Some("account"))(res.options.get(SCHEMA))
    assertResult(Some("accounts"))(res.options.get(TABLE))
  }

  test("Throw error if only schema or table is defined for Postgres") {
    val dataSourceRequest = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER, SCHEMA -> "account")))
    val dataSourceRequest1 = DataSourceRequest("postgres-name", "task-1", Some(POSTGRES), Some(Map(URL -> "localhost:5432", USERNAME -> "postgres", PASSWORD -> "postgres", DRIVER -> POSTGRES_DRIVER, TABLE -> "accounts")))
    assertThrows[IllegalArgumentException](UiMapper.connectionMapping(dataSourceRequest))
    assertThrows[IllegalArgumentException](UiMapper.connectionMapping(dataSourceRequest1))
  }

  test("Can convert UI connection mapping for CSV") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", Some(CSV), Some(Map(PATH -> "/tmp/my-csv")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(CSV))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-csv"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for CSV with partitions and partitionBy") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", Some(CSV), Some(Map(PATH -> "/tmp/my-csv", PARTITIONS -> "2", PARTITION_BY -> "account_id,year")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(4)(res.options.size)
    assertResult(Some(CSV))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-csv"))(res.options.get(PATH))
    assertResult(Some("2"))(res.options.get(PARTITIONS))
    assertResult(Some("account_id,year"))(res.options.get(PARTITION_BY))
  }

  test("Can convert UI connection mapping for JSON") {
    val dataSourceRequest = DataSourceRequest("json-name", "task-1", Some(JSON), Some(Map(PATH -> "/tmp/my-json")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(JSON))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-json"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for Parquet") {
    val dataSourceRequest = DataSourceRequest("parquet-name", "task-1", Some(PARQUET), Some(Map(PATH -> "/tmp/my-parquet")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(PARQUET))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-parquet"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for ORC") {
    val dataSourceRequest = DataSourceRequest("orc-name", "task-1", Some(ORC), Some(Map(PATH -> "/tmp/my-orc")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(2)(res.options.size)
    assertResult(Some(ORC))(res.options.get(FORMAT))
    assertResult(Some("/tmp/my-orc"))(res.options.get(PATH))
  }

  test("Can convert UI connection mapping for Solace") {
    val dataSourceRequest = DataSourceRequest("solace-name", "task-1", Some(SOLACE), Some(Map(URL -> "localhost:55554", USERNAME -> "solace", PASSWORD -> "solace", JMS_DESTINATION_NAME -> "/JNDI/my-queue", JMS_VPN_NAME -> "default", JMS_CONNECTION_FACTORY -> "jms-connection", JMS_INITIAL_CONTEXT_FACTORY -> "jms-init")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(8)(res.options.size)
    assertResult(Some(JMS))(res.options.get(FORMAT))
    assertResult(Some("localhost:55554"))(res.options.get(URL))
    assertResult(Some("solace"))(res.options.get(USERNAME))
    assertResult(Some("solace"))(res.options.get(PASSWORD))
    assertResult(Some("/JNDI/my-queue"))(res.options.get(JMS_DESTINATION_NAME))
    assertResult(Some("default"))(res.options.get(JMS_VPN_NAME))
    assertResult(Some("jms-connection"))(res.options.get(JMS_CONNECTION_FACTORY))
    assertResult(Some("jms-init"))(res.options.get(JMS_INITIAL_CONTEXT_FACTORY))
  }

  test("Can convert UI connection mapping for Kafka") {
    val dataSourceRequest = DataSourceRequest("kafka-name", "task-1", Some(KAFKA), Some(Map(URL -> "localhost:1234", KAFKA_TOPIC -> "my-topic")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(4)(res.options.size)
    assertResult(Some(KAFKA))(res.options.get(FORMAT))
    assertResult(Some("localhost:1234"))(res.options.get(URL))
    assertResult(Some("localhost:1234"))(res.options.get("kafka.bootstrap.servers"))
    assertResult(Some("my-topic"))(res.options.get(KAFKA_TOPIC))
  }

  test("Can convert UI connection mapping for HTTP") {
    val dataSourceRequest = DataSourceRequest("http-name", "task-1", Some(HTTP), Some(Map(USERNAME -> "root", PASSWORD -> "root")))
    val res = UiMapper.connectionMapping(dataSourceRequest).connectionConfigWithTaskBuilder
    assertResult(3)(res.options.size)
    assertResult(Some(HTTP))(res.options.get(FORMAT))
    assertResult(Some("root"))(res.options.get(USERNAME))
    assertResult(Some("root"))(res.options.get(PASSWORD))
  }

  test("Throw exception if provided unknown data source") {
    val dataSourceRequest = DataSourceRequest("unknown-name", "task-1", Some("unknown"))
    assertThrows[IllegalArgumentException](UiMapper.connectionMapping(dataSourceRequest))
  }

  test("Throw exception if no data source type provided") {
    val dataSourceRequest = DataSourceRequest("unknown-name", "task-1", None)
    assertThrows[IllegalArgumentException](UiMapper.connectionMapping(dataSourceRequest))
  }

  test("Can convert UI validation mapping for basic column validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "account_id", VALIDATION_EQUAL -> "abc123", "description" -> "valid desc", "errorThreshold" -> "2"))))
    ))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val exprValid = res.head.validation.asInstanceOf[ExpressionValidation]
    assertResult("account_id == abc123")(exprValid.expr)
    assertResult(Some("valid desc"))(exprValid.description)
    assertResult(Some(2.0))(exprValid.errorThreshold)
    assertResult(1)(exprValid.selectExpr.size)
    assertResult("*")(exprValid.selectExpr.head)
  }

  test("Can convert UI validation mapping for column name validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_COUNT_EQUAL -> "5"))))
    ))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_COUNT_EQUAL)(valid.columnNameType)
    assertResult(5)(valid.count)
  }

  test("Can convert UI validation mapping for column name validation count between") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_COUNT_BETWEEN -> "blah", VALIDATION_MIN -> "1", VALIDATION_MAX -> "2"))))
    ))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_COUNT_BETWEEN)(valid.columnNameType)
    assertResult(1)(valid.minCount)
    assertResult(2)(valid.maxCount)
  }

  test("Can convert UI validation mapping for column name validation match order") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_MATCH_ORDER -> "account_id,year"))))
    ))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_MATCH_ORDER)(valid.columnNameType)
    assertResult(Array("account_id", "year"))(valid.names)
  }

  test("Can convert UI validation mapping for column name validation match set") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map(VALIDATION_COLUMN_NAMES_MATCH_SET -> "account_id,year"))))
    ))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_MATCH_SET)(valid.columnNameType)
    assertResult(Array("account_id", "year"))(valid.names)
  }

  test("Can convert UI validation mapping, when unknown option, default to column name count equals 1") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_COLUMN_NAMES, Some(Map("unknown" -> "hello"))))
    ))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[ColumnNamesValidation]
    assertResult(VALIDATION_COLUMN_NAME_COUNT_EQUAL)(valid.columnNameType)
    assertResult(1)(valid.count)
  }

  test("Can convert UI validation mapping with min group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_MIN, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_MIN)(valid.aggType)
    assertResult("min(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with max group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_MAX, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_MAX)(valid.aggType)
    assertResult("max(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with count group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(
      List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_COUNT, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_COUNT)(valid.aggType)
    assertResult("count(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with sum group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_SUM, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult(VALIDATION_SUM)(valid.aggType)
    assertResult("sum(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with average group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_AVERAGE, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult("avg")(valid.aggType)
    assertResult("avg(amount) == 10")(valid.aggExpr)
  }

  test("Can convert UI validation mapping with standard deviation group by validation") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> VALIDATION_STANDARD_DEVIATION, "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val res = UiMapper.validationMapping(dataSourceRequest)
    assertResult(1)(res.size)
    val valid = res.head.validation.asInstanceOf[GroupByValidation]
    assertResult(Seq("account_id"))(valid.groupByCols)
    assertResult("amount")(valid.aggCol)
    assertResult("stddev")(valid.aggType)
    assertResult("stddev(amount) == 10")(valid.aggExpr)
  }

  test("Throw error when given unknown aggregation type") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> "unknown", "aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    assertThrows[IllegalArgumentException](UiMapper.validationMapping(dataSourceRequest))
  }

  test("Throw error when no aggType or aggCol is given") {
    val dataSourceRequest = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggCol" -> "amount", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val dataSourceRequest1 = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map("aggType" -> "max", VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    val dataSourceRequest2 = DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(
      Some(List(ValidationItemRequest(VALIDATION_GROUP_BY, Some(Map(VALIDATION_GROUP_BY_COLUMNS -> "account_id")),
        Some(ValidationItemRequests(List(ValidationItemRequest(
          VALIDATION_COLUMN, Some(Map(VALIDATION_EQUAL -> "10"))
        ))))
      ))))))
    assertThrows[RuntimeException](UiMapper.validationMapping(dataSourceRequest))
    assertThrows[RuntimeException](UiMapper.validationMapping(dataSourceRequest1))
    assertThrows[RuntimeException](UiMapper.validationMapping(dataSourceRequest2))
  }

  test("Can convert UI upstream validation mapping") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_TYPE -> "outer",
        VALIDATION_UPSTREAM_JOIN_COLUMNS -> "account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = UiMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("account_id"))(upstreamValidation.joinColumns)
    assertResult("outer")(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("year == 2020")(exprValid.expr)
  }

  test("Can convert UI upstream validation mapping with join expression") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_TYPE -> "outer",
        VALIDATION_UPSTREAM_JOIN_EXPR -> "account_id == task-2_account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = UiMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("expr:account_id == task-2_account_id"))(upstreamValidation.joinColumns)
    assertResult("outer")(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("year == 2020")(exprValid.expr)
  }

  test("Can convert UI upstream validation mapping with join columns only") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_COLUMNS -> "account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = UiMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("account_id"))(upstreamValidation.joinColumns)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("year == 2020")(exprValid.expr)
  }

  test("Can convert UI upstream validation mapping with join expression only") {
    val connections = List(
      FileBuilder().name("task-1").schema(FieldBuilder().name("account_id"), FieldBuilder().name("year")),
      FileBuilder().name("task-2").schema(FieldBuilder().name("account_id"), FieldBuilder().name("date"))
    )
    val dataSourceRequest = List(
      DataSourceRequest("csv-name", "task-1", validations = Some(ValidationRequest(Some(List(ValidationItemRequest(VALIDATION_UPSTREAM, Some(Map(
        VALIDATION_UPSTREAM_TASK_NAME -> "task-2",
        VALIDATION_UPSTREAM_JOIN_EXPR -> "account_id == task-2_account_id",
      )),
        Some(ValidationItemRequests(List(ValidationItemRequest(VALIDATION_COLUMN, Some(Map(VALIDATION_FIELD -> "year", VALIDATION_EQUAL -> "2020"))))))
      ))))))
    )
    val res = UiMapper.connectionsWithUpstreamValidationMapping(connections, dataSourceRequest)
    assertResult(2)(res.size)
    val taskWithValidation = res.find(_.task.get.task.name == "task-1").get
    val taskValidations = taskWithValidation.step.get.optValidation.get.dataSourceValidation.validations
    assertResult(1)(taskValidations.size)
    val upstreamValidation = taskValidations.head.validation.asInstanceOf[UpstreamDataSourceValidation]
    assertResult(List("expr:account_id == task-2_account_id"))(upstreamValidation.joinColumns)
    assertResult(DEFAULT_VALIDATION_JOIN_TYPE)(upstreamValidation.joinType)
    assertResult("task-2")(upstreamValidation.upstreamDataSource.task.get.task.name)
    val exprValid = upstreamValidation.validation.validation.asInstanceOf[ExpressionValidation]
    assertResult(List("*"))(exprValid.selectExpr)
    assertResult("year == 2020")(exprValid.expr)
  }
}
