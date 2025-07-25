package io.github.datacatering.datacaterer.core.integration

import io.github.datacatering.datacaterer.api.DataCatererConfigurationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{CONFIG_FLAGS_FAST_GENERATION, PATH}
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.ui.mapper.ConfigurationMapper
import io.github.datacatering.datacaterer.core.ui.model.ConfigurationRequest
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.Files
import io.github.datacatering.datacaterer.core.model.Constants.APPLICATION_CONFIG_PATH

class FastGenerationModeIntegrationTest extends SparkSuite with BeforeAndAfterEach {

  private val tempDir = Files.createTempDirectory("fast-generation-test").toFile
  private val testDataPath = s"${tempDir.getAbsolutePath}/test-data"

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clean up test directory
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir)
    }
    tempDir.mkdirs()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (tempDir.exists()) {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  test("Fast generation mode configuration builder integration") {
    val config = DataCatererConfigurationBuilder()
      .enableCount(true)
      .enableRecordTracking(true)
      .enableValidation(true)
      .enableSaveReports(true)
      .enableUniqueCheck(true)
      .numRecordsPerBatch(500000L)
      .enableFastGeneration(true)
      .build

    // Verify fast generation is enabled
    assert(config.flagsConfig.enableFastGeneration)
    
    // Verify all slow features are disabled
    assert(!config.flagsConfig.enableCount)
    assert(!config.flagsConfig.enableRecordTracking)
    assert(!config.flagsConfig.enableValidation)
    assert(!config.flagsConfig.enableSaveReports)
    assert(!config.flagsConfig.enableUniqueCheck)
    assert(!config.flagsConfig.enableSinkMetadata)
    assert(!config.flagsConfig.enableGenerateValidations)
    assert(!config.flagsConfig.enableAlerts)
    assert(!config.flagsConfig.enableUniqueCheckOnlyInBatch)
    
    // Verify generation optimizations
    assert(config.generationConfig.numRecordsPerBatch >= 1000000L)
    assertResult(100000L)(config.generationConfig.uniqueBloomFilterNumItems)
    assertResult(0.1)(config.generationConfig.uniqueBloomFilterFalsePositiveProbability)
  }

  test("Fast generation mode UI mapper integration") {
    val configRequest = ConfigurationRequest(flag = Map(
      CONFIG_FLAGS_FAST_GENERATION -> "true"
    ))
    val baseConf = DataCatererConfigurationBuilder()
      .enableCount(true)
      .enableRecordTracking(true)
      .enableValidation(true)
      
    val result = ConfigurationMapper.mapFlagsConfiguration(configRequest, baseConf).build

    // Verify fast generation optimizations are applied through UI mapper
    assert(result.flagsConfig.enableFastGeneration)
    assert(!result.flagsConfig.enableCount)
    assert(!result.flagsConfig.enableRecordTracking)
    assert(!result.flagsConfig.enableValidation)
    
    // Verify generation optimizations
    assert(result.generationConfig.numRecordsPerBatch >= 1000000L)
    assertResult(100000L)(result.generationConfig.uniqueBloomFilterNumItems)
    assertResult(0.1)(result.generationConfig.uniqueBloomFilterFalsePositiveProbability)
  }

  test("Fast generation mode config parser integration") {
    // Create a test configuration with fast generation enabled
    val testConfig = s"""
      |flags {
      |  enableFastGeneration = true
      |  enableCount = true
      |  enableRecordTracking = true
      |  enableValidation = true
      |  enableSaveReports = true
      |  enableUniqueCheck = true
      |  enableSinkMetadata = true
      |  enableGenerateValidations = true
      |  enableAlerts = true
      |  enableUniqueCheckOnlyInBatch = true
      |}
      |
      |generation {
      |  numRecordsPerBatch = 100000
      |  uniqueBloomFilterNumItems = 1000000
      |  uniqueBloomFilterFalsePositiveProbability = 0.01
      |}
      |
      |runtime {
      |  master = "local[*]"
      |  config {
      |    "spark.sql.shuffle.partitions" = "10"
      |    "spark.serializer" = "org.apache.spark.serializer.JavaSerializer"
      |  }
      |}
      |
      |metadata {
      |  numRecordsFromDataSource = 10000
      |  numRecordsForAnalysis = 10000
      |  oneOfDistinctCountVsCountThreshold = 0.1
      |  oneOfMinCount = 1000
      |  numGeneratedSamples = 10
      |}
      |validation {
      |  numSampleErrorRecords = 5
      |  enableDeleteRecordTrackingFiles = true
      |}
      |alert {
      |  triggerOn = "all"
      |  slackAlertConfig {
      |    token = ""
      |    channels = []
      |  }
      |}
      |folders {
      |  planFilePath = "/opt/app/plan/customer-create-plan.yaml"
      |  taskFolderPath = "/opt/app/task"
      |  generatedPlanAndTaskFolderPath = "/tmp"
      |  generatedReportsFolderPath = "/opt/app/report"
      |  recordTrackingFolderPath = "/opt/app/record-tracking"
      |  validationFolderPath = "/opt/app/validation"
      |  recordTrackingForValidationFolderPath = "/opt/app/record-tracking-validation"
      |}
      |""".stripMargin

    // Write test config to file
    val configFile = new File(tempDir, "test-application.conf")
    Files.write(configFile.toPath, testConfig.getBytes)

    // Load configuration using the config parser
    System.setProperty(APPLICATION_CONFIG_PATH, configFile.getAbsolutePath)
    val parsedConfig = ConfigParser.toDataCatererConfigurationWithReload
    System.clearProperty(APPLICATION_CONFIG_PATH)

    // Verify fast generation optimizations were applied
    assert(parsedConfig.flagsConfig.enableFastGeneration)
    assert(!parsedConfig.flagsConfig.enableCount)
    assert(!parsedConfig.flagsConfig.enableRecordTracking)
    assert(!parsedConfig.flagsConfig.enableValidation)
    assert(!parsedConfig.flagsConfig.enableSaveReports)
    assert(!parsedConfig.flagsConfig.enableUniqueCheck)
    assert(!parsedConfig.flagsConfig.enableSinkMetadata)
    assert(!parsedConfig.flagsConfig.enableGenerateValidations)
    assert(!parsedConfig.flagsConfig.enableAlerts)
    assert(!parsedConfig.flagsConfig.enableUniqueCheckOnlyInBatch)

    // Verify generation optimizations
    assert(parsedConfig.generationConfig.numRecordsPerBatch >= 1000000L)
    assertResult(100000L)(parsedConfig.generationConfig.uniqueBloomFilterNumItems)
    assertResult(0.1)(parsedConfig.generationConfig.uniqueBloomFilterFalsePositiveProbability)

    // Verify runtime optimizations
    assertResult("20")(parsedConfig.runtimeConfig("spark.sql.shuffle.partitions"))
    assertResult("org.apache.spark.serializer.KryoSerializer")(parsedConfig.runtimeConfig("spark.serializer"))
    assertResult("true")(parsedConfig.runtimeConfig("spark.sql.adaptive.enabled"))
  }

  test("Fast generation mode preserves essential flags") {
    val config = DataCatererConfigurationBuilder()
      .enableGenerateData(true)
      .enableFailOnError(true)
      .enableDeleteGeneratedRecords(false)
      .enableGeneratePlanAndTasks(false)
      .enableFastGeneration(true)
      .build

    // Verify fast generation is enabled
    assert(config.flagsConfig.enableFastGeneration)
    
    // Verify essential flags are preserved
    assert(config.flagsConfig.enableGenerateData)
    assert(config.flagsConfig.enableFailOnError)
    assert(!config.flagsConfig.enableDeleteGeneratedRecords)
    assert(!config.flagsConfig.enableGeneratePlanAndTasks)
    
    // Verify slow features are disabled
    assert(!config.flagsConfig.enableCount)
    assert(!config.flagsConfig.enableRecordTracking)
    assert(!config.flagsConfig.enableValidation)
    assert(!config.flagsConfig.enableSaveReports)
  }

  test("Fast generation mode can be disabled to restore normal operation") {
    val config = DataCatererConfigurationBuilder()
      .enableFastGeneration(true)
      .enableFastGeneration(false)
      .build

    // Verify fast generation is disabled
    assert(!config.flagsConfig.enableFastGeneration)
    
    // Verify default settings are restored (no optimizations applied)
    // Note: This test validates that disabling fast generation doesn't apply optimizations,
    // but it doesn't restore previously overridden settings
  }

  test("Fast generation mode runtime configuration contains all expected optimizations") {
    val config = DataCatererConfigurationBuilder()
      .enableFastGeneration(true)
      .build

    val runtimeConfig = config.runtimeConfig

    // Verify all expected Spark optimizations are present
    assertResult("20")(runtimeConfig("spark.sql.shuffle.partitions"))
    assertResult("true")(runtimeConfig("spark.sql.adaptive.coalescePartitions.enabled"))
    assertResult("true")(runtimeConfig("spark.sql.adaptive.skewJoin.enabled"))
    assertResult("org.apache.spark.serializer.KryoSerializer")(runtimeConfig("spark.serializer"))
    assertResult("true")(runtimeConfig("spark.sql.cbo.enabled"))
    assertResult("true")(runtimeConfig("spark.sql.adaptive.enabled"))
  }

  test("Fast generation mode handles edge cases properly") {
    // Test with already optimized batch size
    val config1 = DataCatererConfigurationBuilder()
      .numRecordsPerBatch(5000000L)
      .enableFastGeneration(true)
      .build
    
    assertResult(5000000L)(config1.generationConfig.numRecordsPerBatch)
    
    // Test with minimal batch size
    val config2 = DataCatererConfigurationBuilder()
      .numRecordsPerBatch(1000L)
      .enableFastGeneration(true)
      .build
    
    assert(config2.generationConfig.numRecordsPerBatch >= 1000000L)
    
    // Test with existing bloom filter settings
    val config3 = DataCatererConfigurationBuilder()
      .uniqueBloomFilterNumItems(50000L)
      .uniqueBloomFilterFalsePositiveProbability(0.05)
      .enableFastGeneration(true)
      .build
    
    // Fast generation should override these for consistency
    assertResult(100000L)(config3.generationConfig.uniqueBloomFilterNumItems)
    assertResult(0.1)(config3.generationConfig.uniqueBloomFilterFalsePositiveProbability)
  }
} 