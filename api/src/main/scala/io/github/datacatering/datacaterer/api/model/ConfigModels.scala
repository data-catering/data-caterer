package io.github.datacatering.datacaterer.api.model

import io.github.datacatering.datacaterer.api.model.Constants._

case class FlagsConfig(
                        enableCount: Boolean = DEFAULT_ENABLE_COUNT,
                        enableGenerateData: Boolean = DEFAULT_ENABLE_GENERATE_DATA,
                        enableRecordTracking: Boolean = DEFAULT_ENABLE_RECORD_TRACKING,
                        enableDeleteGeneratedRecords: Boolean = DEFAULT_ENABLE_DELETE_GENERATED_RECORDS,
                        enableGeneratePlanAndTasks: Boolean = DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS,
                        enableFailOnError: Boolean = DEFAULT_ENABLE_FAIL_ON_ERROR,
                        enableUniqueCheck: Boolean = DEFAULT_ENABLE_UNIQUE_CHECK,
                        enableSinkMetadata: Boolean = DEFAULT_ENABLE_SINK_METADATA,
                        enableSaveReports: Boolean = DEFAULT_ENABLE_SAVE_REPORTS,
                        enableValidation: Boolean = DEFAULT_ENABLE_VALIDATION,
                        enableGenerateValidations: Boolean = DEFAULT_ENABLE_SUGGEST_VALIDATIONS,
                        enableAlerts: Boolean = DEFAULT_ENABLE_ALERTS,
                        enableUniqueCheckOnlyInBatch: Boolean = DEFAULT_ENABLE_UNIQUE_CHECK_ONLY_WITHIN_BATCH,
                        enableFastGeneration: Boolean = DEFAULT_ENABLE_FAST_GENERATION
                      )

case class FoldersConfig(
                          planFilePath: String = DEFAULT_PLAN_FILE_PATH,
                          taskFolderPath: String = DEFAULT_TASK_FOLDER_PATH,
                          generatedPlanAndTaskFolderPath: String = DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH,
                          generatedReportsFolderPath: String = DEFAULT_GENERATED_REPORTS_FOLDER_PATH,
                          recordTrackingFolderPath: String = DEFAULT_RECORD_TRACKING_FOLDER_PATH,
                          validationFolderPath: String = DEFAULT_VALIDATION_FOLDER_PATH,
                          recordTrackingForValidationFolderPath: String = DEFAULT_RECORD_TRACKING_VALIDATION_FOLDER_PATH,
                        )

case class MetadataConfig(
                           numRecordsFromDataSource: Int = DEFAULT_NUM_RECORD_FROM_DATA_SOURCE,
                           numRecordsForAnalysis: Int = DEFAULT_NUM_RECORD_FOR_ANALYSIS,
                           oneOfDistinctCountVsCountThreshold: Double = DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD,
                           oneOfMinCount: Long = DEFAULT_ONE_OF_MIN_COUNT,
                           numGeneratedSamples: Int = DEFAULT_NUM_GENERATED_SAMPLES,
                         )

case class GenerationConfig(
                             numRecordsPerBatch: Long = DEFAULT_NUM_RECORDS_PER_BATCH,
                             numRecordsPerStep: Option[Long] = None,
                             uniqueBloomFilterNumItems: Long = DEFAULT_UNIQUE_BLOOM_FILTER_NUM_ITEMS,
                             uniqueBloomFilterFalsePositiveProbability: Double = DEFAULT_UNIQUE_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY
                           )

case class ValidationConfig(
                             numSampleErrorRecords: Int = DEFAULT_VALIDATION_NUM_ERROR_RECORDS,
                             enableDeleteRecordTrackingFiles: Boolean = DEFAULT_VALIDATION_DELETE_RECORD_TRACKING_FILES,
                           )

case class AlertConfig(
                        triggerOn: String = ALERT_TRIGGER_ON_ALL,
                        slackAlertConfig: SlackAlertConfig = SlackAlertConfig()
                      )

case class SlackAlertConfig(
                             token: String = "",
                             channels: List[String] = List()
                           )

case class DataCatererConfiguration(
                                     flagsConfig: FlagsConfig = FlagsConfig(),
                                     foldersConfig: FoldersConfig = FoldersConfig(),
                                     metadataConfig: MetadataConfig = MetadataConfig(),
                                     generationConfig: GenerationConfig = GenerationConfig(),
                                     validationConfig: ValidationConfig = ValidationConfig(),
                                     alertConfig: AlertConfig = AlertConfig(),
                                     connectionConfigByName: Map[String, Map[String, String]] = Map(),
                                     runtimeConfig: Map[String, String] = DEFAULT_RUNTIME_CONFIG,
                                     master: String = DEFAULT_MASTER
                                   )
