package io.github.datacatering.datacaterer.api.model.unified

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

/**
 * Unified YAML configuration model for Data Caterer.
 * Allows users to define all configuration in a single YAML file:
 * connections, data sources, steps, fields, foreign keys, validations, and runtime config.
 *
 * This is the external/public API model, separate from internal models to allow
 * internal changes without breaking the external YAML contract.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedConfig(
                          version: String = "1.0",
                          name: String = "unified_plan",
                          description: String = "",
                          config: Option[UnifiedRuntimeConfig] = None,
                          dataSources: List[UnifiedDataSource] = List(),
                          foreignKeys: List[UnifiedForeignKey] = List(),
                          sinkOptions: Option[UnifiedSinkOptions] = None,
                          runId: Option[String] = None
                        ) {
  def this() = this("1.0", "unified_plan", "", None, List(), List(), None, None)
}

// ==================== Runtime Configuration ====================

/**
 * Runtime configuration section - replaces application.conf settings
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedRuntimeConfig(
                                 flags: Option[UnifiedFlagsConfig] = None,
                                 folders: Option[UnifiedFoldersConfig] = None,
                                 generation: Option[UnifiedGenerationConfig] = None,
                                 metadata: Option[UnifiedMetadataConfig] = None,
                                 validation: Option[UnifiedValidationRuntimeConfig] = None,
                                 alert: Option[UnifiedAlertConfig] = None,
                                 runtime: Option[UnifiedSparkConfig] = None
                               ) {
  def this() = this(None, None, None, None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedFlagsConfig(
                               enableCount: Option[Boolean] = None,
                               enableGenerateData: Option[Boolean] = None,
                               enableRecordTracking: Option[Boolean] = None,
                               enableDeleteGeneratedRecords: Option[Boolean] = None,
                               enableGeneratePlanAndTasks: Option[Boolean] = None,
                               enableFailOnError: Option[Boolean] = None,
                               enableUniqueCheck: Option[Boolean] = None,
                               enableSinkMetadata: Option[Boolean] = None,
                               enableSaveReports: Option[Boolean] = None,
                               enableValidation: Option[Boolean] = None,
                               enableGenerateValidations: Option[Boolean] = None,
                               enableAlerts: Option[Boolean] = None,
                               enableUniqueCheckOnlyInBatch: Option[Boolean] = None,
                               enableFastGeneration: Option[Boolean] = None
                             ) {
  def this() = this(None, None, None, None, None, None, None, None, None, None, None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedFoldersConfig(
                                 planFilePath: Option[String] = None,
                                 taskFolderPath: Option[String] = None,
                                 generatedPlanAndTaskFolderPath: Option[String] = None,
                                 generatedReportsFolderPath: Option[String] = None,
                                 recordTrackingFolderPath: Option[String] = None,
                                 validationFolderPath: Option[String] = None,
                                 recordTrackingForValidationFolderPath: Option[String] = None
                               ) {
  def this() = this(None, None, None, None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedGenerationConfig(
                                    @JsonDeserialize(contentAs = classOf[java.lang.Long])
                                    numRecordsPerBatch: Option[Long] = None,
                                    @JsonDeserialize(contentAs = classOf[java.lang.Long])
                                    numRecordsPerStep: Option[Long] = None,
                                    @JsonDeserialize(contentAs = classOf[java.lang.Long])
                                    uniqueBloomFilterNumItems: Option[Long] = None,
                                    uniqueBloomFilterFalsePositiveProbability: Option[Double] = None
                                  ) {
  def this() = this(None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedMetadataConfig(
                                  numRecordsFromDataSource: Option[Int] = None,
                                  numRecordsForAnalysis: Option[Int] = None,
                                  oneOfDistinctCountVsCountThreshold: Option[Double] = None,
                                  @JsonDeserialize(contentAs = classOf[java.lang.Long])
                                  oneOfMinCount: Option[Long] = None,
                                  numGeneratedSamples: Option[Int] = None
                                ) {
  def this() = this(None, None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedValidationRuntimeConfig(
                                           numSampleErrorRecords: Option[Int] = None,
                                           enableDeleteRecordTrackingFiles: Option[Boolean] = None
                                         ) {
  def this() = this(None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedAlertConfig(
                               triggerOn: Option[String] = None,
                               slackToken: Option[String] = None,
                               slackChannels: Option[List[String]] = None
                             ) {
  def this() = this(None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedSparkConfig(
                               master: Option[String] = None,
                               sparkConfig: Option[Map[String, String]] = None
                             ) {
  def this() = this(None, None)
}

// ==================== Data Source Models ====================

/**
 * Data source with inline connection configuration
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedDataSource(
                              name: String = "",
                              connection: UnifiedConnection = UnifiedConnection(),
                              steps: List[UnifiedStep] = List(),
                              enabled: Boolean = true
                            ) {
  def this() = this("", UnifiedConnection(), List(), true)
}

/**
 * Connection configuration - replaces application.conf connection blocks
 * Supports all connection types: JDBC, Kafka, Cassandra, files, HTTP, JMS
 *
 * All connection-specific properties are defined in the options map using standardized keys:
 * - Common: url, path, user, password, host, port
 * - JDBC: url, user, password, driver
 * - Cassandra: host, port, user, password, keyspace
 * - Kafka: bootstrapServers, schemaRegistryUrl
 * - JMS: url, user, password, connectionFactory, initialContextFactory, vpnName
 * - HTTP: url, user, password
 * - Files: path
 * - Iceberg: path, catalogType, catalogUri
 * - BigQuery: temporaryGcsBucket
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedConnection(
                              @JsonProperty("type") connectionType: String = "json",
                              options: Map[String, String] = Map()
                            ) {
  def this() = this("json", Map())
}

/**
 * Step definition with fields and inline validations
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedStep(
                        name: String = "",
                        @JsonProperty("type") stepType: Option[String] = None,
                        count: Option[UnifiedCount] = None,
                        options: Map[String, String] = Map(),
                        fields: List[UnifiedField] = List(),
                        validations: List[UnifiedValidation] = List(),
                        enabled: Boolean = true,
                        transformation: Option[UnifiedTransformation] = None
                      ) {
  def this() = this("", None, None, Map(), List(), List(), true, None)
}

/**
 * Count configuration for record generation
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedCount(
                         @JsonDeserialize(contentAs = classOf[java.lang.Long])
                         records: Option[Long] = None,
                         perField: Option[UnifiedPerFieldCount] = None,
                         options: Map[String, Any] = Map(),
                         duration: Option[String] = None,
                         rate: Option[Int] = None,
                         rateUnit: Option[String] = None,
                         pattern: Option[UnifiedLoadPattern] = None
                       ) {
  def this() = this(None, None, Map(), None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedPerFieldCount(
                                 fieldNames: List[String] = List(),
                                 @JsonDeserialize(contentAs = classOf[java.lang.Long])
                                 count: Option[Long] = None,
                                 options: Map[String, Any] = Map()
                               ) {
  def this() = this(List(), None, Map())
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedLoadPattern(
                               @JsonProperty("type") patternType: String = "ramp",
                               startRate: Option[Int] = None,
                               endRate: Option[Int] = None,
                               baseRate: Option[Int] = None,
                               spikeRate: Option[Int] = None,
                               spikeStart: Option[Double] = None,
                               spikeDuration: Option[Double] = None,
                               steps: Option[List[UnifiedLoadPatternStep]] = None,
                               amplitude: Option[Int] = None,
                               frequency: Option[Double] = None,
                               rateIncrement: Option[Int] = None,
                               incrementInterval: Option[String] = None,
                               maxRate: Option[Int] = None
                             ) {
  def this() = this("ramp", None, None, None, None, None, None, None, None, None, None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedLoadPatternStep(
                                   rate: Int = 0,
                                   duration: String = ""
                                 ) {
  def this() = this(0, "")
}

/**
 * Field definition with generation options
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedField(
                         name: String = "",
                         @JsonProperty("type") fieldType: Option[String] = Some("string"),
                         options: Map[String, Any] = Map(),
                         nullable: Boolean = true,
                         static: Option[String] = None,
                         fields: List[UnifiedField] = List()
                       ) {
  def this() = this("", Some("string"), Map(), true, None, List())
}

/**
 * Transformation configuration
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedTransformation(
                                  className: String = "",
                                  methodName: Option[String] = None,
                                  mode: Option[String] = None,
                                  outputPath: Option[String] = None,
                                  deleteOriginal: Option[Boolean] = None,
                                  enabled: Boolean = true,
                                  options: Map[String, String] = Map()
                                ) {
  def this() = this("", None, None, None, None, true, Map())
}

// ==================== Validation Models ====================

/**
 * Inline validation at the step level
 * Supports expression validations, field validations, group by, and upstream validations
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedValidation(
                              // Expression validation
                              expr: Option[String] = None,
                              selectExpr: Option[List[String]] = None,
                              // Field validation
                              field: Option[String] = None,
                              validation: Option[List[UnifiedFieldValidation]] = None,
                              // Group by validation
                              groupByFields: Option[List[String]] = None,
                              aggField: Option[String] = None,
                              aggType: Option[String] = None,
                              aggExpr: Option[String] = None,
                              // Field names validation
                              fieldNameType: Option[String] = None,
                              count: Option[Int] = None,
                              min: Option[Int] = None,
                              max: Option[Int] = None,
                              names: Option[List[String]] = None,
                              // Upstream data source validation
                              upstreamDataSource: Option[String] = None,
                              upstreamReadOptions: Option[Map[String, String]] = None,
                              joinFields: Option[List[String]] = None,
                              joinType: Option[String] = None,
                              // Common
                              description: Option[String] = None,
                              @JsonDeserialize(contentAs = classOf[java.lang.Double])
                              errorThreshold: Option[Double] = None,
                              preFilterExpr: Option[String] = None,
                              // Wait condition for validation
                              waitCondition: Option[UnifiedWaitCondition] = None
                            ) {
  def this() = this(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None)
}

/**
 * Field-level validation definitions
 * Supports all 30+ validation types from ValidationModels.scala
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedFieldValidation(
                                   @JsonProperty("type") validationType: String = "",
                                   value: Option[Any] = None,
                                   values: Option[List[Any]] = None,
                                   min: Option[Any] = None,
                                   max: Option[Any] = None,
                                   regex: Option[String] = None,
                                   regexes: Option[List[String]] = None,
                                   size: Option[Int] = None,
                                   strictly: Option[Boolean] = None,
                                   negate: Boolean = false,
                                   matchAll: Option[Boolean] = None,
                                   format: Option[String] = None,
                                   schema: Option[String] = None,
                                   quantileRanges: Option[Map[Double, List[List[Double]]]] = None,
                                   description: Option[String] = None,
                                   @JsonDeserialize(contentAs = classOf[java.lang.Double])
                                   errorThreshold: Option[Double] = None
                                 ) {
  def this() = this("", None, None, None, None, None, None, None, None, false, None, None, None, None, None, None)
}

/**
 * Wait condition for validation timing
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedWaitCondition(
                                 @JsonProperty("type") conditionType: String = "pause",
                                 pauseInSeconds: Option[Int] = None,
                                 path: Option[String] = None,
                                 dataSourceName: Option[String] = None,
                                 options: Option[Map[String, String]] = None,
                                 expr: Option[String] = None,
                                 url: Option[String] = None,
                                 method: Option[String] = None,
                                 statusCodes: Option[List[Int]] = None,
                                 maxRetries: Option[Int] = None,
                                 waitBeforeRetrySeconds: Option[Int] = None
                               ) {
  def this() = this("pause", None, None, None, None, None, None, None, None, None, None)
}

// ==================== Foreign Key Models ====================

/**
 * Foreign key relationship definition
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedForeignKey(
                              source: UnifiedForeignKeyRelation = UnifiedForeignKeyRelation(),
                              generate: List[UnifiedForeignKeyRelation] = List(),
                              delete: List[UnifiedForeignKeyRelation] = List()
                            ) {
  def this() = this(UnifiedForeignKeyRelation(), List(), List())
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedForeignKeyRelation(
                                      dataSource: String = "",
                                      step: String = "",
                                      fields: List[String] = List(),
                                      cardinality: Option[UnifiedCardinalityConfig] = None,
                                      nullability: Option[UnifiedNullabilityConfig] = None,
                                      generationMode: Option[String] = None
                                    ) {
  def this() = this("", "", List(), None, None, None)
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedCardinalityConfig(
                                     min: Option[Int] = None,
                                     max: Option[Int] = None,
                                     ratio: Option[Double] = None,
                                     distribution: String = "uniform"
                                   ) {
  def this() = this(None, None, None, "uniform")
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedNullabilityConfig(
                                     nullPercentage: Double = 0.0,
                                     strategy: String = "random"
                                   ) {
  def this() = this(0.0, "random")
}

// ==================== Sink Options ====================

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedSinkOptions(
                               seed: Option[String] = None,
                               locale: Option[String] = None
                             ) {
  def this() = this(None, None)
}

// ==================== Test Configuration ====================

@JsonIgnoreProperties(ignoreUnknown = true)
case class UnifiedTestConfig(
                              executionMode: Option[String] = None,
                              warmup: Option[String] = None,
                              cooldown: Option[String] = None
                            ) {
  def this() = this(None, None, None)
}
