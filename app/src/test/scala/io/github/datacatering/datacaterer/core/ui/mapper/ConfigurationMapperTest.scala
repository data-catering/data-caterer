package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.DataCatererConfigurationBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.ui.model.ConfigurationRequest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConfigurationMapperTest extends AnyFunSuite {

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
    val res = ConfigurationMapper.mapFlagsConfiguration(configRequest, baseConf).build

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
    val res = ConfigurationMapper.mapAlertConfiguration(configRequest, baseConf).build

    assert(res.alertConfig.triggerOn == "failure")
    assert(res.alertConfig.slackAlertConfig.token == "abc123")
    assert(res.alertConfig.slackAlertConfig.channels == List("job-fail"))
  }

  test("Can convert UI validation config") {
    val configRequest = ConfigurationRequest(validation = Map(CONFIG_VALIDATION_NUM_SAMPLE_ERROR_RECORDS -> "2",
      CONFIG_VALIDATION_ENABLE_DELETE_RECORD_TRACKING_FILES -> "false", "blah" -> "hello"))
    val baseConf = DataCatererConfigurationBuilder()
    val res = ConfigurationMapper.mapValidationConfiguration(configRequest, baseConf).build

    assert(res.validationConfig.numSampleErrorRecords == 2)
    assert(!res.validationConfig.enableDeleteRecordTrackingFiles)
  }

  test("Can convert UI generation config") {
    val configRequest = ConfigurationRequest(generation = Map(CONFIG_GENERATION_NUM_RECORDS_PER_BATCH -> "100",
      CONFIG_GENERATION_NUM_RECORDS_PER_STEP -> "10", "blah" -> "hello"))
    val baseConf = DataCatererConfigurationBuilder()
    val res = ConfigurationMapper.mapGenerationConfiguration(configRequest, baseConf).build

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
    val res = ConfigurationMapper.mapMetadataConfiguration(configRequest, baseConf).build

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
    val res = ConfigurationMapper.mapFolderConfiguration(configRequest, "/my-install", baseConf).build

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
    val res = ConfigurationMapper.mapFolderConfiguration(configRequest, "/my-install", baseConf).build

    assert(res.foldersConfig.planFilePath == DEFAULT_PLAN_FILE_PATH)
    assert(res.foldersConfig.taskFolderPath == "/my-install/task")
    assert(res.foldersConfig.generatedPlanAndTaskFolderPath == "/my-install/generated-plan-task")
    assert(res.foldersConfig.generatedReportsFolderPath == "/my-install/report")
    assert(res.foldersConfig.recordTrackingFolderPath == "/my-install/record-tracking")
    assert(res.foldersConfig.validationFolderPath == "/my-install/validation")
    assert(res.foldersConfig.recordTrackingForValidationFolderPath == "/my-install/record-tracking-validation")
  }
}
