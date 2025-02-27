package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.DataCatererConfigurationBuilder
import io.github.datacatering.datacaterer.api.connection.ConnectionTaskBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.core.ui.model.ConfigurationRequest
import org.apache.log4j.Logger

object ConfigurationMapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def configurationMapping(
                            configurationRequest: ConfigurationRequest,
                            installDirectory: String,
                            connections: List[ConnectionTaskBuilder[_]] = List()
                          ): DataCatererConfigurationBuilder = {
    val isConnectionContainsMetadataSource = connections.exists(conn => conn.connectionConfigWithTaskBuilder.options.contains(METADATA_SOURCE_TYPE))
    val configUpdatedFromConnections = if (isConnectionContainsMetadataSource) {
      configurationRequest.copy(flag = configurationRequest.flag ++ Map(CONFIG_FLAGS_GENERATE_PLAN_AND_TASKS -> isConnectionContainsMetadataSource.toString))
    } else configurationRequest

    val baseConfig = DataCatererConfigurationBuilder()
    val withFlagConf = mapFlagsConfiguration(configUpdatedFromConnections, baseConfig)
    val withFolderConf = mapFolderConfiguration(configUpdatedFromConnections, installDirectory, withFlagConf)
    val withMetadataConf = mapMetadataConfiguration(configUpdatedFromConnections, withFolderConf)
    val withGenerationConf = mapGenerationConfiguration(configUpdatedFromConnections, withMetadataConf)
    val withValidationConf = mapValidationConfiguration(configUpdatedFromConnections, withGenerationConf)
    val withAlertConf = mapAlertConfiguration(configUpdatedFromConnections, withValidationConf)

    withAlertConf
  }

  def mapFlagsConfiguration(configurationRequest: ConfigurationRequest, baseConfig: DataCatererConfigurationBuilder): DataCatererConfigurationBuilder = {
    configurationRequest.flag.foldLeft(baseConfig)((conf, c) => {
      val boolVal = c._2.toBoolean
      c._1 match {
        case CONFIG_FLAGS_COUNT => conf.enableCount(boolVal)
        case CONFIG_FLAGS_GENERATE_DATA => conf.enableGenerateData(boolVal)
        case CONFIG_FLAGS_RECORD_TRACKING => conf.enableRecordTracking(boolVal)
        case CONFIG_FLAGS_DELETE_GENERATED_RECORDS => conf.enableDeleteGeneratedRecords(boolVal)
        case CONFIG_FLAGS_GENERATE_PLAN_AND_TASKS => conf.enableGeneratePlanAndTasks(boolVal)
        case CONFIG_FLAGS_FAIL_ON_ERROR => conf.enableFailOnError(boolVal)
        case CONFIG_FLAGS_UNIQUE_CHECK => conf.enableUniqueCheck(boolVal)
        case CONFIG_FLAGS_SINK_METADATA => conf.enableSinkMetadata(boolVal)
        case CONFIG_FLAGS_SAVE_REPORTS => conf.enableSaveReports(boolVal)
        case CONFIG_FLAGS_VALIDATION => conf.enableValidation(boolVal)
        case CONFIG_FLAGS_GENERATE_VALIDATIONS => conf.enableGenerateValidations(boolVal)
        case CONFIG_FLAGS_ALERTS => conf.enableAlerts(boolVal)
        case CONFIG_FLAGS_UNIQUE_CHECK_ONLY_IN_BATCH => conf.enableUniqueCheckOnlyInBatch(boolVal)
        case _ =>
          LOGGER.warn(s"Unexpected flags configuration key, key=${c._1}")
          conf
      }
    })
  }

  def mapAlertConfiguration(configurationRequest: ConfigurationRequest, baseConfig: DataCatererConfigurationBuilder): DataCatererConfigurationBuilder = {
    configurationRequest.alert.foldLeft(baseConfig)((conf, c) => {
      c._1 match {
        case CONFIG_ALERT_TRIGGER_ON => conf.alertTriggerOn(c._2)
        case CONFIG_ALERT_SLACK_TOKEN => conf.slackAlertToken(c._2)
        case CONFIG_ALERT_SLACK_CHANNELS => conf.slackAlertChannels(c._2.split(",").map(_.trim): _*)
        case _ =>
          LOGGER.warn(s"Unexpected alert configuration key, key=${c._1}")
          conf
      }
    })
  }

  def mapValidationConfiguration(configurationRequest: ConfigurationRequest, baseConfig: DataCatererConfigurationBuilder): DataCatererConfigurationBuilder = {
    configurationRequest.validation.foldLeft(baseConfig)((conf, c) => {
      c._1 match {
        case CONFIG_VALIDATION_NUM_SAMPLE_ERROR_RECORDS => conf.numErrorSampleRecords(c._2.toInt)
        case CONFIG_VALIDATION_ENABLE_DELETE_RECORD_TRACKING_FILES => conf.enableDeleteRecordTrackingFiles(c._2.toBoolean)
        case _ =>
          LOGGER.warn(s"Unexpected validation configuration key, key=${c._1}")
          conf
      }
    })
  }

  def mapGenerationConfiguration(configurationRequest: ConfigurationRequest, baseConfig: DataCatererConfigurationBuilder): DataCatererConfigurationBuilder = {
    configurationRequest.generation.foldLeft(baseConfig)((conf, c) => {
      c._1 match {
        case CONFIG_GENERATION_NUM_RECORDS_PER_BATCH => conf.numRecordsPerBatch(c._2.toLong)
        case CONFIG_GENERATION_NUM_RECORDS_PER_STEP =>
          val parsedNum = c._2.toLong
          if (parsedNum != -1) conf.numRecordsPerStep(c._2.toLong) else conf
        case CONFIG_GENERATION_UNIQUE_BLOOM_FILTER_NUM_ITEMS => conf.uniqueBloomFilterNumItems(c._2.toLong)
        case CONFIG_GENERATION_UNIQUE_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY => conf.uniqueBloomFilterFalsePositiveProbability(c._2.toDouble)
        case _ =>
          LOGGER.warn(s"Unexpected generation configuration key, key=${c._1}")
          conf
      }
    })
  }

  def mapMetadataConfiguration(configurationRequest: ConfigurationRequest, baseConfig: DataCatererConfigurationBuilder): DataCatererConfigurationBuilder = {
    configurationRequest.metadata.foldLeft(baseConfig)((conf, c) => {
      c._1 match {
        case CONFIG_METADATA_NUM_RECORDS_FROM_DATA_SOURCE => conf.numRecordsFromDataSourceForDataProfiling(c._2.toInt)
        case CONFIG_METADATA_NUM_RECORDS_FOR_ANALYSIS => conf.numRecordsForAnalysisForDataProfiling(c._2.toInt)
        case CONFIG_METADATA_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD => conf.oneOfDistinctCountVsCountThreshold(c._2.toDouble)
        case CONFIG_METADATA_ONE_OF_MIN_COUNT => conf.oneOfMinCount(c._2.toLong)
        case CONFIG_METADATA_NUM_GENERATED_SAMPLES => conf.numGeneratedSamples(c._2.toInt)
        case _ =>
          LOGGER.warn(s"Unexpected metadata configuration key, key=${c._1}")
          conf
      }
    })
  }

  def mapFolderConfiguration(configurationRequest: ConfigurationRequest, installDirectory: String, baseConfig: DataCatererConfigurationBuilder): DataCatererConfigurationBuilder = {
    val nonEmptyFolderConfig = configurationRequest.folder.filter(_._2.nonEmpty).foldLeft(baseConfig)((conf, c) => {
      c._1 match {
        case CONFIG_FOLDER_PLAN_FILE_PATH => conf.planFilePath(c._2)
        case CONFIG_FOLDER_TASK_FOLDER_PATH => conf.taskFolderPath(c._2)
        case CONFIG_FOLDER_GENERATED_PLAN_AND_TASK_FOLDER_PATH => conf.generatedPlanAndTaskFolderPath(c._2)
        case CONFIG_FOLDER_GENERATED_REPORTS_FOLDER_PATH => conf.generatedReportsFolderPath(c._2)
        case CONFIG_FOLDER_RECORD_TRACKING_FOLDER_PATH => conf.recordTrackingFolderPath(c._2)
        case CONFIG_FOLDER_VALIDATION_FOLDER_PATH => conf.validationFolderPath(c._2)
        case CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH => conf.recordTrackingForValidationFolderPath(c._2)
        case _ =>
          LOGGER.warn(s"Unexpected folder configuration key, key=${c._1}")
          conf
      }
    })
    // should set the base directory to the install directory for most folders if not overridden
    configurationRequest.folder.filter(_._2.isEmpty).foldLeft(nonEmptyFolderConfig)((conf, c) => {
      c._1 match {
        case CONFIG_FOLDER_PLAN_FILE_PATH => conf
        case CONFIG_FOLDER_TASK_FOLDER_PATH => conf.taskFolderPath(s"$installDirectory/task")
        case CONFIG_FOLDER_GENERATED_PLAN_AND_TASK_FOLDER_PATH => conf.generatedPlanAndTaskFolderPath(s"$installDirectory/generated-plan-task")
        case CONFIG_FOLDER_GENERATED_REPORTS_FOLDER_PATH => conf.generatedReportsFolderPath(s"$installDirectory/report")
        case CONFIG_FOLDER_RECORD_TRACKING_FOLDER_PATH => conf.recordTrackingFolderPath(s"$installDirectory/record-tracking")
        case CONFIG_FOLDER_VALIDATION_FOLDER_PATH => conf.validationFolderPath(s"$installDirectory/validation")
        case CONFIG_FOLDER_RECORD_TRACKING_FOR_VALIDATION_FOLDER_PATH => conf.recordTrackingForValidationFolderPath(s"$installDirectory/record-tracking-validation")
        case _ =>
          LOGGER.warn(s"Unexpected folder configuration key, key=${c._1}")
          conf
      }
    })
  }
}
