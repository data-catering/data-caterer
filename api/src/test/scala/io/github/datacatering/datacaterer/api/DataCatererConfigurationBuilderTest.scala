package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_CASSANDRA_PASSWORD, DEFAULT_CASSANDRA_USERNAME, DEFAULT_KAFKA_URL, DEFAULT_MYSQL_PASSWORD, DEFAULT_MYSQL_URL, DEFAULT_MYSQL_USERNAME, DEFAULT_POSTGRES_PASSWORD, DEFAULT_POSTGRES_URL, DEFAULT_POSTGRES_USERNAME, DEFAULT_SOLACE_CONNECTION_FACTORY, DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY, DEFAULT_SOLACE_PASSWORD, DEFAULT_SOLACE_URL, DEFAULT_SOLACE_USERNAME, DEFAULT_SOLACE_VPN_NAME}
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, FoldersConfig, GenerationConfig, MetadataConfig}
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataCatererConfigurationBuilderTest extends AnyFunSuite {

  test("Can create basic configuration with defaults") {
    val result = DataCatererConfigurationBuilder().build

    assertResult(FlagsConfig())(result.flagsConfig)
    assertResult(FoldersConfig())(result.foldersConfig)
    assertResult(MetadataConfig())(result.metadataConfig)
    assertResult(GenerationConfig())(result.generationConfig)
    assert(result.connectionConfigByName.isEmpty)
    assertResult(16)(result.runtimeConfig.size)
    assertResult("local[*]")(result.master)
  }

  test("Can create postgres connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .postgres("my_postgres")
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_postgres"))
    val config = result("my_postgres")
    assertResult(DEFAULT_POSTGRES_URL)(config("url"))
    assertResult(DEFAULT_POSTGRES_USERNAME)(config("user"))
    assertResult(DEFAULT_POSTGRES_PASSWORD)(config("password"))
    assertResult("jdbc")(config("format"))
    assertResult("org.postgresql.Driver")(config("driver"))
  }

  test("Can create postgres connection with custom configuration") {
    val result = DataCatererConfigurationBuilder()
      .postgres("my_postgres", "jdbc:postgresql://localhost:5432/customer", options = Map("stringtype" -> "undefined"))
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_postgres"))
    val config = result("my_postgres")
    assertResult(6)(config.size)
    assertResult("jdbc:postgresql://localhost:5432/customer")(config("url"))
    assertResult("undefined")(config("stringtype"))
  }

  test("Can create mysql connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .mysql("my_mysql")
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_mysql"))
    val config = result("my_mysql")
    assertResult(DEFAULT_MYSQL_URL)(config("url"))
    assertResult(DEFAULT_MYSQL_USERNAME)(config("user"))
    assertResult(DEFAULT_MYSQL_PASSWORD)(config("password"))
    assertResult("jdbc")(config("format"))
    assertResult("com.mysql.cj.jdbc.Driver")(config("driver"))
  }

  test("Can create cassandra connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .cassandra("my_cassandra")
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_cassandra"))
    val config = result("my_cassandra")
    assertResult("cassandraserver")(config("spark.cassandra.connection.host"))
    assertResult("9042")(config("spark.cassandra.connection.port"))
    assertResult(DEFAULT_CASSANDRA_USERNAME)(config("spark.cassandra.auth.username"))
    assertResult(DEFAULT_CASSANDRA_PASSWORD)(config("spark.cassandra.auth.password"))
    assertResult("org.apache.spark.sql.cassandra")(config("format"))
  }

  test("Can create solace connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .solace("my_solace")
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_solace"))
    val config = result("my_solace")
    assertResult(DEFAULT_SOLACE_URL)(config("url"))
    assertResult(DEFAULT_SOLACE_USERNAME)(config("user"))
    assertResult(DEFAULT_SOLACE_PASSWORD)(config("password"))
    assertResult("jms")(config("format"))
    assertResult(DEFAULT_SOLACE_VPN_NAME)(config("vpnName"))
    assertResult(DEFAULT_SOLACE_CONNECTION_FACTORY)(config("connectionFactory"))
    assertResult(DEFAULT_SOLACE_INITIAL_CONTEXT_FACTORY)(config("initialContextFactory"))
  }

  test("Can create kafka connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .kafka("my_kafka")
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_kafka"))
    val config = result("my_kafka")
    assertResult(DEFAULT_KAFKA_URL)(config("kafka.bootstrap.servers"))
    assertResult("kafka")(config("format"))
  }

  test("Can create http connection configuration") {
    val result = DataCatererConfigurationBuilder()
      .http("my_http", "user", "pw")
      .build
      .connectionConfigByName

    assertResult(1)(result.size)
    assert(result.contains("my_http"))
    val config = result("my_http")
    assertResult("user")(config("user"))
    assertResult("pw")(config("password"))
  }

  test("Can enable/disable flags") {
    val result = DataCatererConfigurationBuilder()
      .enableCount(false)
      .enableGenerateData(false)
      .enableDeleteGeneratedRecords(true)
      .enableGeneratePlanAndTasks(true)
      .enableUniqueCheck(true)
      .enableFailOnError(false)
      .enableRecordTracking(true)
      .enableSaveReports(true)
      .enableSinkMetadata(true)
      .enableValidation(true)
      .build
      .flagsConfig

    assert(!result.enableCount)
    assert(!result.enableGenerateData)
    assert(result.enableDeleteGeneratedRecords)
    assert(result.enableGeneratePlanAndTasks)
    assert(result.enableUniqueCheck)
    assert(!result.enableFailOnError)
    assert(result.enableRecordTracking)
    assert(result.enableSaveReports)
    assert(result.enableSinkMetadata)
    assert(result.enableValidation)
  }

  test("Can alter folder paths") {
    val result = DataCatererConfigurationBuilder()
      .planFilePath("/my_plan")
      .taskFolderPath("/my_task")
      .recordTrackingFolderPath("/my_record_tracking")
      .validationFolderPath("/my_validation")
      .generatedReportsFolderPath("/my_generation_results")
      .generatedPlanAndTaskFolderPath("/my_generated_plan_tasks")
      .build
      .foldersConfig

    assertResult("/my_plan")(result.planFilePath)
    assertResult("/my_task")(result.taskFolderPath)
    assertResult("/my_record_tracking")(result.recordTrackingFolderPath)
    assertResult("/my_validation")(result.validationFolderPath)
    assertResult("/my_generation_results")(result.generatedReportsFolderPath)
    assertResult("/my_generated_plan_tasks")(result.generatedPlanAndTaskFolderPath)
  }

  test("Can alter metadata configurations") {
    val result = DataCatererConfigurationBuilder()
      .numRecordsFromDataSourceForDataProfiling(1)
      .numRecordsForAnalysisForDataProfiling(2)
      .numGeneratedSamples(3)
      .oneOfMinCount(100)
      .oneOfDistinctCountVsCountThreshold(0.3)
      .build
      .metadataConfig

    assertResult(1)(result.numRecordsFromDataSource)
    assertResult(2)(result.numRecordsForAnalysis)
    assertResult(3)(result.numGeneratedSamples)
    assertResult(100)(result.oneOfMinCount)
    assertResult(0.3)(result.oneOfDistinctCountVsCountThreshold)
  }

  test("Can alter generation configurations") {
    val result = DataCatererConfigurationBuilder()
      .numRecordsPerBatch(100)
      .numRecordsPerStep(10)
      .build
      .generationConfig

    assertResult(100)(result.numRecordsPerBatch)
    assert(result.numRecordsPerStep.contains(10))
  }
}
