package io.github.datacatering.datacaterer.core.parser.unified

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.api.model.unified._
import io.github.datacatering.datacaterer.core.parser.TaskConversionRegistry
import org.apache.log4j.Logger

/**
 * Converts UnifiedConfig to internal Data Caterer models.
 * This provides the bridge between the external YAML API and internal execution.
 */
object UnifiedConfigConverter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert unified config to internal Plan, Tasks, ValidationConfiguration, and DataCatererConfiguration
   */
  def convert(unified: UnifiedConfig): (
    Plan,
    List[Task],
    Option[List[ValidationConfiguration]],
    DataCatererConfiguration
  ) = {
    LOGGER.debug(s"Converting unified config: name=${unified.name}, dataSources=${unified.dataSources.size}")

    val connectionConfigByName = buildConnectionConfigMap(unified.dataSources)
    val dataCatererConfig = convertRuntimeConfig(unified.config, connectionConfigByName)
    val (tasks, taskSummaries) = convertDataSourcesToTasks(unified.dataSources)
    val foreignKeys = convertForeignKeys(unified.foreignKeys)
    val validations = extractValidations(unified.dataSources, connectionConfigByName)

    val plan = Plan(
      name = unified.name,
      description = unified.description,
      tasks = taskSummaries,
      sinkOptions = Some(SinkOptions(
        seed = unified.sinkOptions.flatMap(_.seed),
        locale = unified.sinkOptions.flatMap(_.locale),
        foreignKeys = foreignKeys
      )),
      validations = List(),
      runId = unified.runId
    )

    LOGGER.info(s"Converted unified config: plan=${plan.name}, tasks=${tasks.size}, validations=${validations.map(_.size).getOrElse(0)}")
    (plan, tasks, validations, dataCatererConfig)
  }

  // ==================== Connection Configuration ====================

  /**
   * Build connection configuration map from data sources
   */
  private def buildConnectionConfigMap(dataSources: List[UnifiedDataSource]): Map[String, Map[String, String]] = {
    dataSources.map { ds =>
      val conn = ds.connection
      val format = resolveFormat(conn.connectionType)
      val baseConfig = Map(FORMAT -> format)

      val typeSpecificConfig = conn.connectionType.toLowerCase match {
        case POSTGRES | MYSQL =>
          Map(
            URL -> conn.options.getOrElse("url", ""),
            USERNAME -> conn.options.getOrElse("user", ""),
            PASSWORD -> resolveEnvVar(conn.options.getOrElse("password", "")),
            DRIVER -> conn.options.getOrElse("driver", if (conn.connectionType.toLowerCase == POSTGRES) POSTGRES_DRIVER else MYSQL_DRIVER)
          ).filter(_._2.nonEmpty)

        case CASSANDRA_NAME | CASSANDRA =>
          val hostPort = Map(
            "spark.cassandra.connection.host" -> conn.options.getOrElse("host", ""),
            "spark.cassandra.connection.port" -> conn.options.getOrElse("port", "9042")
          ).filter(_._2.nonEmpty)
          val auth = Map(
            "spark.cassandra.auth.username" -> conn.options.getOrElse("user", ""),
            "spark.cassandra.auth.password" -> resolveEnvVar(conn.options.getOrElse("password", ""))
          ).filter(_._2.nonEmpty)
          val ks = conn.options.get("keyspace").map(k => Map(CASSANDRA_KEYSPACE -> k)).getOrElse(Map())
          hostPort ++ auth ++ ks

        case KAFKA =>
          Map(
            "kafka.bootstrap.servers" -> conn.options.getOrElse("bootstrapServers", "")
          ).filter(_._2.nonEmpty) ++
            conn.options.get("schemaRegistryUrl").map(u => Map("schema.registry.url" -> u)).getOrElse(Map())

        case JMS =>
          Map(
            JMS_CONNECTION_FACTORY -> conn.options.getOrElse("connectionFactory", ""),
            JMS_INITIAL_CONTEXT_FACTORY -> conn.options.getOrElse("initialContextFactory", ""),
            URL -> conn.options.getOrElse("url", ""),
            USERNAME -> conn.options.getOrElse("user", ""),
            PASSWORD -> resolveEnvVar(conn.options.getOrElse("password", "")),
            JMS_VPN_NAME -> conn.options.getOrElse("vpnName", "")
          ).filter(_._2.nonEmpty)

        case HTTP =>
          Map(
            URL -> conn.options.getOrElse("url", ""),
            USERNAME -> conn.options.getOrElse("user", ""),
            PASSWORD -> resolveEnvVar(conn.options.getOrElse("password", ""))
          ).filter(_._2.nonEmpty)

        case JSON | CSV | PARQUET | ORC | DELTA | ICEBERG | HUDI =>
          conn.options.get("path").map(p => Map(PATH -> p)).getOrElse(Map())

        case _ => Map()
      }

      // Extract processed keys to avoid duplication
      val processedKeys = conn.connectionType.toLowerCase match {
        case POSTGRES | MYSQL => Set("url", "user", "password", "driver")
        case CASSANDRA_NAME | CASSANDRA => Set("host", "port", "user", "password", "keyspace")
        case KAFKA => Set("bootstrapServers", "schemaRegistryUrl")
        case JMS => Set("connectionFactory", "initialContextFactory", "url", "user", "password", "vpnName")
        case HTTP => Set("url", "user", "password")
        case JSON | CSV | PARQUET | ORC | DELTA | ICEBERG | HUDI => Set("path")
        case _ => Set.empty[String]
      }

      // Merge baseConfig + typeSpecificConfig + remaining options (excluding processed keys)
      val remainingOptions = conn.options.filterKeys(k => !processedKeys.contains(k))
      ds.name -> (baseConfig ++ typeSpecificConfig ++ remainingOptions)
    }.toMap
  }

  /**
   * Resolve format string from connection type
   */
  private def resolveFormat(connectionType: String): String = connectionType.toLowerCase match {
    case POSTGRES | MYSQL => JDBC
    case CASSANDRA_NAME => CASSANDRA
    case other => other
  }

  /**
   * Resolve environment variable substitution (${VAR_NAME} syntax)
   */
  private def resolveEnvVar(value: String): String = {
    val envVarPattern = """\$\{([^}]+)\}""".r
    envVarPattern.replaceAllIn(value, m => {
      sys.env.getOrElse(m.group(1), m.matched)
    })
  }

  // ==================== Data Source to Task Conversion ====================

  /**
   * Convert data sources to internal Task and TaskSummary
   */
  private def convertDataSourcesToTasks(
                                         dataSources: List[UnifiedDataSource]
                                       ): (List[Task], List[TaskSummary]) = {
    val tasksAndSummaries = dataSources.filter(_.enabled).map { ds =>
      val steps = ds.steps.filter(_.enabled).map { step =>
        convertStep(step, ds.connection.connectionType)
      }

      val taskName = s"${ds.name}_task"
      val task = TaskConversionRegistry.applyTaskConversions(Task(name = taskName, steps = steps))
      val summary = TaskSummary(
        name = taskName,
        dataSourceName = ds.name,
        enabled = ds.enabled
      )

      (task, summary)
    }

    (tasksAndSummaries.map(_._1), tasksAndSummaries.map(_._2))
  }

  /**
   * Convert unified step to internal Step
   */
  private def convertStep(unified: UnifiedStep, connectionType: String): Step = {
    Step(
      name = unified.name,
      `type` = unified.stepType.getOrElse(connectionType),
      count = unified.count.map(convertCount).getOrElse(Count()),
      options = unified.options,
      fields = unified.fields.map(convertField),
      enabled = unified.enabled,
      transformation = unified.transformation.map(convertTransformation)
    )
  }

  /**
   * Convert unified count to internal Count
   */
  private def convertCount(unified: UnifiedCount): Count = {
    Count(
      records = unified.records,
      perField = unified.perField.map(pf =>
        PerFieldCount(pf.fieldNames, pf.count, pf.options)
      ),
      options = unified.options,
      duration = unified.duration,
      rate = unified.rate,
      rateUnit = unified.rateUnit,
      pattern = unified.pattern.map(convertLoadPattern)
    )
  }

  /**
   * Convert unified load pattern to internal LoadPattern
   */
  private def convertLoadPattern(unified: UnifiedLoadPattern): LoadPattern = {
    LoadPattern(
      `type` = unified.patternType,
      startRate = unified.startRate,
      endRate = unified.endRate,
      baseRate = unified.baseRate,
      spikeRate = unified.spikeRate,
      spikeStart = unified.spikeStart,
      spikeDuration = unified.spikeDuration,
      steps = unified.steps.map(_.map(s => LoadPatternStep(s.rate, s.duration))),
      amplitude = unified.amplitude,
      frequency = unified.frequency,
      rateIncrement = unified.rateIncrement,
      incrementInterval = unified.incrementInterval,
      maxRate = unified.maxRate
    )
  }

  /**
   * Convert unified field to internal Field
   */
  private def convertField(unified: UnifiedField): Field = {
    Field(
      name = unified.name,
      `type` = unified.fieldType,
      options = unified.options,
      nullable = unified.nullable,
      static = unified.static,
      fields = unified.fields.map(convertField)
    )
  }

  /**
   * Convert unified transformation to internal TransformationConfig
   */
  private def convertTransformation(unified: UnifiedTransformation): TransformationConfig = {
    TransformationConfig(
      className = unified.className,
      methodName = unified.methodName.getOrElse("transform"),
      mode = unified.mode.getOrElse("whole-file"),
      outputPath = unified.outputPath,
      deleteOriginal = unified.deleteOriginal.getOrElse(false),
      enabled = unified.enabled,
      options = unified.options
    )
  }

  // ==================== Foreign Key Conversion ====================

  /**
   * Convert foreign keys
   */
  private def convertForeignKeys(unified: List[UnifiedForeignKey]): List[ForeignKey] = {
    unified.map { fk =>
      ForeignKey(
        source = convertForeignKeyRelation(fk.source),
        generate = fk.generate.map(convertForeignKeyRelation),
        delete = fk.delete.map(convertForeignKeyRelation)
      )
    }
  }

  private def convertForeignKeyRelation(unified: UnifiedForeignKeyRelation): ForeignKeyRelation = {
    ForeignKeyRelation(
      dataSource = unified.dataSource,
      step = unified.step,
      fields = unified.fields,
      cardinality = unified.cardinality.map(c =>
        CardinalityConfig(c.min, c.max, c.ratio, c.distribution)
      ),
      nullability = unified.nullability.map(n =>
        NullabilityConfig(n.nullPercentage, n.strategy)
      ),
      generationMode = unified.generationMode
    )
  }

  // ==================== Validation Extraction ====================

  /**
   * Extract validations from data sources
   */
  private def extractValidations(
                                  dataSources: List[UnifiedDataSource],
                                  connectionConfigByName: Map[String, Map[String, String]]
                                ): Option[List[ValidationConfiguration]] = {
    val validationsByDataSource = dataSources.flatMap { ds =>
      ds.steps.filter(_.validations.nonEmpty).map { step =>
        (ds.name, ds.connection.connectionType, step)
      }
    }

    if (validationsByDataSource.isEmpty) return None

    // Group by data source name
    val grouped = validationsByDataSource.groupBy(_._1).map { case (dsName, items) =>
      val dataSourceValidations = items.map { case (_, connType, step) =>
        val waitCondition = step.validations.flatMap(_.waitCondition).headOption
          .map(convertWaitCondition)
          .getOrElse(PauseWaitCondition())

        val validationBuilders = step.validations.flatMap(v => convertStepValidation(v, connectionConfigByName))

        DataSourceValidation(
          options = step.options,
          waitCondition = waitCondition,
          validations = validationBuilders
        )
      }
      dsName -> dataSourceValidations
    }

    Some(List(ValidationConfiguration(
      name = "unified_validations",
      description = "Validations from unified configuration",
      dataSources = grouped
    )))
  }

  /**
   * Convert step validation to internal Validation types
   */
  private def convertStepValidation(
                                     unified: UnifiedValidation,
                                     connectionConfigByName: Map[String, Map[String, String]]
                                   ): List[ValidationBuilder] = {
    val result = scala.collection.mutable.ListBuffer[ValidationBuilder]()

    // Expression validation
    unified.expr.foreach { expr =>
      val validation = ExpressionValidation(
        expr = expr,
        selectExpr = unified.selectExpr.getOrElse(List("*"))
      )
      validation.description = unified.description
      validation.errorThreshold = unified.errorThreshold
      validation.preFilterExpr = unified.preFilterExpr
      result += ValidationBuilder(validation)
    }

    // Field validation
    (unified.field, unified.validation) match {
      case (Some(field), Some(validations)) =>
        val fieldValidation = FieldValidations(
          field = field,
          validation = validations.map(convertFieldValidation)
        )
        fieldValidation.description = unified.description
        fieldValidation.errorThreshold = unified.errorThreshold
        fieldValidation.preFilterExpr = unified.preFilterExpr
        result += ValidationBuilder(fieldValidation)
      case _ =>
    }

    // Group by validation
    (unified.groupByFields, unified.aggExpr) match {
      case (Some(fields), Some(aggExpr)) =>
        val groupByValidation = GroupByValidation(
          groupByFields = fields,
          aggField = unified.aggField.getOrElse(""),
          aggType = unified.aggType.getOrElse(AGGREGATION_SUM),
          aggExpr = aggExpr
        )
        groupByValidation.description = unified.description
        groupByValidation.errorThreshold = unified.errorThreshold
        groupByValidation.preFilterExpr = unified.preFilterExpr
        result += ValidationBuilder(groupByValidation)
      case _ =>
    }

    // Field names validation
    unified.fieldNameType.foreach { fnType =>
      val fieldNamesValidation = FieldNamesValidation(
        fieldNameType = fnType,
        count = unified.count.getOrElse(0),
        min = unified.min.getOrElse(0),
        max = unified.max.getOrElse(0),
        names = unified.names.map(_.toArray).getOrElse(Array())
      )
      fieldNamesValidation.description = unified.description
      fieldNamesValidation.errorThreshold = unified.errorThreshold
      result += ValidationBuilder(fieldNamesValidation)
    }

    // Upstream data source validation
    unified.upstreamDataSource.foreach { upstreamDs =>
      val upstreamValidation = YamlUpstreamDataSourceValidation(
        upstreamDataSource = upstreamDs,
        upstreamReadOptions = unified.upstreamReadOptions.getOrElse(Map()),
        joinFields = unified.joinFields.getOrElse(List()),
        joinType = unified.joinType.getOrElse(DEFAULT_VALIDATION_JOIN_TYPE),
        validation = unified.validation.map(_.map(v => convertFieldValidation(v))).getOrElse(List())
      )
      result += ValidationBuilder(upstreamValidation)
    }

    result.toList
  }

  private def convertFieldValidation(unified: UnifiedFieldValidation): FieldValidation = {
    val validation = unified.validationType.toLowerCase match {
      case VALIDATION_NULL => NullFieldValidation(unified.negate)
      case VALIDATION_UNIQUE => UniqueFieldValidation(unified.negate)
      case VALIDATION_EQUAL => EqualFieldValidation(unified.value.getOrElse(""), unified.negate)
      case VALIDATION_CONTAINS => ContainsFieldValidation(unified.value.map(_.toString).getOrElse(""), unified.negate)
      case VALIDATION_BETWEEN => BetweenFieldValidation(
        unified.min.map(_.toString.toDouble).getOrElse(0.0),
        unified.max.map(_.toString.toDouble).getOrElse(0.0),
        unified.negate
      )
      case VALIDATION_LESS_THAN => LessThanFieldValidation(unified.value.getOrElse(0), unified.strictly.getOrElse(true))
      case VALIDATION_GREATER_THAN => GreaterThanFieldValidation(unified.value.getOrElse(0), unified.strictly.getOrElse(true))
      case VALIDATION_IN => InFieldValidation(unified.values.getOrElse(List()), unified.negate)
      case VALIDATION_MATCHES => MatchesFieldValidation(unified.regex.getOrElse(""), unified.negate)
      case VALIDATION_MATCHES_LIST => MatchesListFieldValidation(
        unified.regexes.getOrElse(List()),
        unified.matchAll.getOrElse(true),
        unified.negate
      )
      case VALIDATION_STARTS_WITH => StartsWithFieldValidation(unified.value.map(_.toString).getOrElse(""), unified.negate)
      case VALIDATION_ENDS_WITH => EndsWithFieldValidation(unified.value.map(_.toString).getOrElse(""), unified.negate)
      case VALIDATION_SIZE => SizeFieldValidation(unified.size.getOrElse(0), unified.negate)
      case VALIDATION_LESS_THAN_SIZE => LessThanSizeFieldValidation(unified.size.getOrElse(0), unified.strictly.getOrElse(true))
      case VALIDATION_GREATER_THAN_SIZE => GreaterThanSizeFieldValidation(unified.size.getOrElse(0), unified.strictly.getOrElse(true))
      case VALIDATION_LUHN_CHECK => LuhnCheckFieldValidation(unified.negate)
      case VALIDATION_HAS_TYPE => HasTypeFieldValidation(unified.value.map(_.toString).getOrElse(""), unified.negate)
      case VALIDATION_HAS_TYPES => HasTypesFieldValidation(unified.values.map(_.map(_.toString)).getOrElse(List()), unified.negate)
      case VALIDATION_DISTINCT_IN_SET => DistinctInSetFieldValidation(unified.values.getOrElse(List()), unified.negate)
      case VALIDATION_DISTINCT_CONTAINS_SET => DistinctContainsSetFieldValidation(unified.values.getOrElse(List()), unified.negate)
      case VALIDATION_DISTINCT_EQUAL => DistinctEqualFieldValidation(unified.values.getOrElse(List()), unified.negate)
      case VALIDATION_MAX_BETWEEN => MaxBetweenFieldValidation(unified.min.getOrElse(0), unified.max.getOrElse(0), unified.negate)
      case VALIDATION_MEAN_BETWEEN => MeanBetweenFieldValidation(unified.min.getOrElse(0), unified.max.getOrElse(0), unified.negate)
      case VALIDATION_MEDIAN_BETWEEN => MedianBetweenFieldValidation(unified.min.getOrElse(0), unified.max.getOrElse(0), unified.negate)
      case VALIDATION_MIN_BETWEEN => MinBetweenFieldValidation(unified.min.getOrElse(0), unified.max.getOrElse(0), unified.negate)
      case VALIDATION_STD_DEV_BETWEEN => StdDevBetweenFieldValidation(unified.min.getOrElse(0), unified.max.getOrElse(0), unified.negate)
      case VALIDATION_SUM_BETWEEN => SumBetweenFieldValidation(unified.min.getOrElse(0), unified.max.getOrElse(0), unified.negate)
      case VALIDATION_LENGTH_BETWEEN => LengthBetweenFieldValidation(
        unified.min.map(_.toString.toInt).getOrElse(0),
        unified.max.map(_.toString.toInt).getOrElse(0),
        unified.negate
      )
      case VALIDATION_LENGTH_EQUAL => LengthEqualFieldValidation(unified.size.getOrElse(0), unified.negate)
      case VALIDATION_IS_DECREASING => IsDecreasingFieldValidation(unified.strictly.getOrElse(true))
      case VALIDATION_IS_INCREASING => IsIncreasingFieldValidation(unified.strictly.getOrElse(true))
      case VALIDATION_IS_JSON_PARSABLE => IsJsonParsableFieldValidation(unified.negate)
      case VALIDATION_MATCH_JSON_SCHEMA => MatchJsonSchemaFieldValidation(unified.schema.getOrElse(""), unified.negate)
      case VALIDATION_MATCH_DATE_TIME_FORMAT => MatchDateTimeFormatFieldValidation(unified.format.getOrElse(""), unified.negate)
      case VALIDATION_MOST_COMMON_VALUE_IN_SET => MostCommonValueInSetFieldValidation(unified.values.getOrElse(List()), unified.negate)
      case VALIDATION_UNIQUE_VALUES_PROPORTION_BETWEEN => UniqueValuesProportionBetweenFieldValidation(
        unified.min.map(_.toString.toDouble).getOrElse(0.0),
        unified.max.map(_.toString.toDouble).getOrElse(1.0),
        unified.negate
      )
      case VALIDATION_QUANTILE_VALUES_BETWEEN =>
        val ranges = unified.quantileRanges.map { qr =>
          qr.map { case (quantile, range) =>
            quantile -> (range.head.head, range.head.last)
          }
        }.getOrElse(Map())
        QuantileValuesBetweenFieldValidation(ranges, unified.negate)
      case other =>
        LOGGER.warn(s"Unknown validation type: $other, using expression validation")
        NullFieldValidation(false)
    }
    validation.description = unified.description
    validation.errorThreshold = unified.errorThreshold
    validation
  }

  private def convertWaitCondition(unified: UnifiedWaitCondition): WaitCondition = {
    unified.conditionType.toLowerCase match {
      case "pause" => PauseWaitCondition(unified.pauseInSeconds.getOrElse(0))
      case "fileexists" => FileExistsWaitCondition(unified.path.getOrElse(""))
      case "dataexists" => DataExistsWaitCondition(
        unified.dataSourceName.getOrElse(""),
        unified.options.getOrElse(Map()),
        unified.expr.getOrElse("")
      )
      case "webhook" => WebhookWaitCondition(
        unified.dataSourceName.getOrElse(""),
        unified.url.getOrElse(""),
        unified.method.getOrElse("GET"),
        unified.statusCodes.getOrElse(List(200))
      )
      case _ => PauseWaitCondition()
    }
  }

  // ==================== Runtime Configuration ====================

  /**
   * Convert runtime configuration
   */
  private def convertRuntimeConfig(
                                    config: Option[UnifiedRuntimeConfig],
                                    connectionConfigByName: Map[String, Map[String, String]]
                                  ): DataCatererConfiguration = {
    config match {
      case Some(rc) =>
        DataCatererConfiguration(
          flagsConfig = convertFlagsConfig(rc.flags),
          foldersConfig = convertFoldersConfig(rc.folders),
          metadataConfig = convertMetadataConfig(rc.metadata),
          generationConfig = convertGenerationConfig(rc.generation),
          validationConfig = convertValidationConfig(rc.validation),
          alertConfig = convertAlertConfig(rc.alert),
          connectionConfigByName = connectionConfigByName,
          runtimeConfig = rc.runtime.flatMap(_.sparkConfig).getOrElse(DEFAULT_RUNTIME_CONFIG),
          master = rc.runtime.flatMap(_.master).getOrElse(DEFAULT_MASTER)
        )
      case None =>
        DataCatererConfiguration(connectionConfigByName = connectionConfigByName)
    }
  }

  private def convertFlagsConfig(flags: Option[UnifiedFlagsConfig]): FlagsConfig = {
    flags.map { f =>
      FlagsConfig(
        enableCount = f.enableCount.getOrElse(DEFAULT_ENABLE_COUNT),
        enableGenerateData = f.enableGenerateData.getOrElse(DEFAULT_ENABLE_GENERATE_DATA),
        enableRecordTracking = f.enableRecordTracking.getOrElse(DEFAULT_ENABLE_RECORD_TRACKING),
        enableDeleteGeneratedRecords = f.enableDeleteGeneratedRecords.getOrElse(DEFAULT_ENABLE_DELETE_GENERATED_RECORDS),
        enableGeneratePlanAndTasks = f.enableGeneratePlanAndTasks.getOrElse(DEFAULT_ENABLE_GENERATE_PLAN_AND_TASKS),
        enableFailOnError = f.enableFailOnError.getOrElse(DEFAULT_ENABLE_FAIL_ON_ERROR),
        enableUniqueCheck = f.enableUniqueCheck.getOrElse(DEFAULT_ENABLE_UNIQUE_CHECK),
        enableSinkMetadata = f.enableSinkMetadata.getOrElse(DEFAULT_ENABLE_SINK_METADATA),
        enableSaveReports = f.enableSaveReports.getOrElse(DEFAULT_ENABLE_SAVE_REPORTS),
        enableValidation = f.enableValidation.getOrElse(DEFAULT_ENABLE_VALIDATION),
        enableGenerateValidations = f.enableGenerateValidations.getOrElse(DEFAULT_ENABLE_SUGGEST_VALIDATIONS),
        enableAlerts = f.enableAlerts.getOrElse(DEFAULT_ENABLE_ALERTS),
        enableUniqueCheckOnlyInBatch = f.enableUniqueCheckOnlyInBatch.getOrElse(DEFAULT_ENABLE_UNIQUE_CHECK_ONLY_WITHIN_BATCH),
        enableFastGeneration = f.enableFastGeneration.getOrElse(DEFAULT_ENABLE_FAST_GENERATION)
      )
    }.getOrElse(FlagsConfig())
  }

  private def convertFoldersConfig(folders: Option[UnifiedFoldersConfig]): FoldersConfig = {
    folders.map { f =>
      FoldersConfig(
        planFilePath = f.planFilePath.getOrElse(DEFAULT_PLAN_FILE_PATH),
        taskFolderPath = f.taskFolderPath.getOrElse(DEFAULT_TASK_FOLDER_PATH),
        generatedPlanAndTaskFolderPath = f.generatedPlanAndTaskFolderPath.getOrElse(DEFAULT_GENERATED_PLAN_AND_TASK_FOLDER_PATH),
        generatedReportsFolderPath = f.generatedReportsFolderPath.getOrElse(DEFAULT_GENERATED_REPORTS_FOLDER_PATH),
        recordTrackingFolderPath = f.recordTrackingFolderPath.getOrElse(DEFAULT_RECORD_TRACKING_FOLDER_PATH),
        validationFolderPath = f.validationFolderPath.getOrElse(DEFAULT_VALIDATION_FOLDER_PATH),
        recordTrackingForValidationFolderPath = f.recordTrackingForValidationFolderPath.getOrElse(DEFAULT_RECORD_TRACKING_VALIDATION_FOLDER_PATH)
      )
    }.getOrElse(FoldersConfig())
  }

  private def convertMetadataConfig(metadata: Option[UnifiedMetadataConfig]): MetadataConfig = {
    metadata.map { m =>
      MetadataConfig(
        numRecordsFromDataSource = m.numRecordsFromDataSource.getOrElse(DEFAULT_NUM_RECORD_FROM_DATA_SOURCE),
        numRecordsForAnalysis = m.numRecordsForAnalysis.getOrElse(DEFAULT_NUM_RECORD_FOR_ANALYSIS),
        oneOfDistinctCountVsCountThreshold = m.oneOfDistinctCountVsCountThreshold.getOrElse(DEFAULT_ONE_OF_DISTINCT_COUNT_VS_COUNT_THRESHOLD),
        oneOfMinCount = m.oneOfMinCount.getOrElse(DEFAULT_ONE_OF_MIN_COUNT),
        numGeneratedSamples = m.numGeneratedSamples.getOrElse(DEFAULT_NUM_GENERATED_SAMPLES)
      )
    }.getOrElse(MetadataConfig())
  }

  private def convertGenerationConfig(generation: Option[UnifiedGenerationConfig]): GenerationConfig = {
    generation.map { g =>
      GenerationConfig(
        numRecordsPerBatch = g.numRecordsPerBatch.getOrElse(DEFAULT_NUM_RECORDS_PER_BATCH),
        numRecordsPerStep = g.numRecordsPerStep,
        uniqueBloomFilterNumItems = g.uniqueBloomFilterNumItems.getOrElse(DEFAULT_UNIQUE_BLOOM_FILTER_NUM_ITEMS),
        uniqueBloomFilterFalsePositiveProbability = g.uniqueBloomFilterFalsePositiveProbability.getOrElse(DEFAULT_UNIQUE_BLOOM_FILTER_FALSE_POSITIVE_PROBABILITY)
      )
    }.getOrElse(GenerationConfig())
  }

  private def convertValidationConfig(validation: Option[UnifiedValidationRuntimeConfig]): ValidationConfig = {
    validation.map { v =>
      ValidationConfig(
        numSampleErrorRecords = v.numSampleErrorRecords.getOrElse(DEFAULT_VALIDATION_NUM_ERROR_RECORDS),
        enableDeleteRecordTrackingFiles = v.enableDeleteRecordTrackingFiles.getOrElse(DEFAULT_VALIDATION_DELETE_RECORD_TRACKING_FILES)
      )
    }.getOrElse(ValidationConfig())
  }

  private def convertAlertConfig(alert: Option[UnifiedAlertConfig]): AlertConfig = {
    alert.map { a =>
      AlertConfig(
        triggerOn = a.triggerOn.getOrElse(ALERT_TRIGGER_ON_ALL),
        slackAlertConfig = SlackAlertConfig(
          token = a.slackToken.getOrElse(""),
          channels = a.slackChannels.getOrElse(List())
        )
      )
    }.getOrElse(AlertConfig())
  }
}
