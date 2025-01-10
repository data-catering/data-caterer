package io.github.datacatering.datacaterer.core.validator

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_ENABLE_VALIDATION, DELTA, DELTA_LAKE_SPARK_CONF, ENABLE_DATA_VALIDATION, FORMAT, HTTP, ICEBERG, ICEBERG_SPARK_CONF, JMS, TABLE, VALIDATION_IDENTIFIER}
import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, DataSourceValidationResult, ExpressionValidation, FoldersConfig, GroupByValidation, UpstreamDataSourceValidation, ValidationConfig, ValidationConfigResult, ValidationConfiguration, ValidationResult}
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import io.github.datacatering.datacaterer.core.util.ValidationUtil.cleanValidationIdentifier
import io.github.datacatering.datacaterer.core.validator.ValidationHelper.getValidationType
import io.github.datacatering.datacaterer.core.validator.ValidationWaitImplicits.ValidationWaitConditionOps
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.time.LocalDateTime
import scala.reflect.io.Directory
import scala.util.{Failure, Success, Try}

/*
Given a list of validations, check and report on the success and failure of each
Flag to enable
Validations can occur on any data source defined in application config
Validations will only occur on datasets not on the response from the data source (i.e. no HTTP status code validations)
Defined at plan level what validations are run post data generation
Validations lie within separate files
Validations have a wait condition. Wait for: webhook, pause, file exists, data exists
Different types of validations:
- simple field validations (amount < 100)
- aggregates (sum of amount per account is > 500)
- ordering (transactions are ordered by date)
- relationship (one account entry in history table per account in accounts table)
- data profile (how close the generated data profile is compared to the expected data profile)
 */
class ValidationProcessor(
                           connectionConfigsByName: Map[String, Map[String, String]],
                           optValidationConfigs: Option[List[ValidationConfiguration]],
                           validationConfig: ValidationConfig,
                           foldersConfig: FoldersConfig
                         )(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def executeValidations: List[ValidationConfigResult] = {
    LOGGER.debug("Executing data validations")
    val validationResults = getValidations.map(vc => {
      val startTime = LocalDateTime.now()
      val dataSourceValidationResults = vc.dataSources.flatMap(dataSource => {
        val dataSourceName = dataSource._1
        val dataSourceValidations = dataSource._2
        val numValidations = dataSourceValidations.flatMap(_.validations).size

        LOGGER.debug(s"Executing data validations for data source, name=${vc.name}, " +
          s"data-source-name=$dataSourceName, num-validations=$numValidations")
        dataSourceValidations.map(dataSourceValidation => executeDataValidations(vc, dataSourceName, dataSourceValidation))
      }).toList
      val endTime = LocalDateTime.now()
      ValidationConfigResult(vc.name, vc.description, dataSourceValidationResults, startTime, endTime)
    }).toList

    logValidationErrors(validationResults)
    validationResults
  }

  private def executeDataValidations(
                                      vc: ValidationConfiguration,
                                      dataSourceName: String,
                                      dataSourceValidation: DataSourceValidation
                                    ): DataSourceValidationResult = {
    val isValidationsEnabled = dataSourceValidation.options.get(ENABLE_DATA_VALIDATION).map(_.toBoolean).getOrElse(DEFAULT_ENABLE_VALIDATION)
    if (isValidationsEnabled) {
      if (dataSourceValidation.validations.isEmpty) {
        LOGGER.debug(s"No validations defined, skipping data validations, name=${vc.name}, data-source-name=$dataSourceName")
        DataSourceValidationResult(dataSourceName, dataSourceValidation.options, List())
      } else {
        LOGGER.debug(s"Waiting for validation condition to be successful before running validations, name=${vc.name}," +
          s"data-source-name=$dataSourceName, details=${dataSourceValidation.options}, num-validations=${dataSourceValidation.validations.size}")
        dataSourceValidation.waitCondition.waitBeforeValidation(connectionConfigsByName)

        val df = getDataFrame(dataSourceName, dataSourceValidation.options)
        if (df.isEmpty) {
          LOGGER.info("No data found to run validations")
          DataSourceValidationResult(dataSourceName, dataSourceValidation.options, List())
        } else {
          if (!df.storageLevel.useMemory) df.cache()
          val results = dataSourceValidation.validations.flatMap(validBuilder => tryValidate(df, validBuilder))
          df.unpersist()
          LOGGER.debug(s"Finished data validations, name=${vc.name}," +
            s"data-source-name=$dataSourceName, details=${dataSourceValidation.options}, num-validations=${dataSourceValidation.validations.size}")
          cleanRecordTrackingFiles()
          DataSourceValidationResult(dataSourceName, dataSourceValidation.options, results)
        }
      }
    } else {
      LOGGER.debug(s"Data validations are disabled, data-source-name=$dataSourceName, details=${dataSourceValidation.options}")
      DataSourceValidationResult(dataSourceName, dataSourceValidation.options, List())
    }
  }

  def tryValidate(df: DataFrame, validBuilder: ValidationBuilder): List[ValidationResult] = {
    val validationDescription = validBuilder.validation.toOptions.map(l => s"${l.head}=${l.last}").mkString(", ")
    val validation = getValidationType(validBuilder.validation, foldersConfig.recordTrackingForValidationFolderPath)
    val preFilterData = validation.filterData(df)
    if (!preFilterData.storageLevel.useMemory) preFilterData.cache()
    val count = preFilterData.count()
    Try(validation.validate(preFilterData, count, validationConfig.numSampleErrorRecords)) match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to run data validation, $validationDescription", exception)
        List(ValidationResult(validBuilder.validation, false, count, count, Some(Array(Map("exception" -> exception.getLocalizedMessage)))))
      case Success(value) =>
        LOGGER.debug(s"Successfully ran data validation, $validationDescription")
        value
    }
  }

  private def cleanRecordTrackingFiles(): Unit = {
    if (validationConfig.enableDeleteRecordTrackingFiles) {
      LOGGER.debug(s"Deleting all record tracking files from directory, " +
        s"record-tracking-for-validation-directory=${foldersConfig.recordTrackingForValidationFolderPath}")
      new Directory(new File(foldersConfig.recordTrackingForValidationFolderPath)).deleteRecursively()
    }
  }

  private def getValidations: Array[ValidationConfiguration] = {
    optValidationConfigs.map(_.toArray)
      .getOrElse(PlanParser.parseValidations(foldersConfig.validationFolderPath, connectionConfigsByName).toArray)
  }

  private def getDataFrame(dataSourceName: String, options: Map[String, String]): DataFrame = {
    val connectionConfig = connectionConfigsByName(dataSourceName) ++ options
    val format = connectionConfig(FORMAT)
    val configWithFormatConfigs = getAdditionalSparkConfig(format, connectionConfig)

    configWithFormatConfigs.filter(_._1.startsWith("spark.sql"))
      .foreach(conf => sparkSession.sqlContext.setConf(conf._1, conf._2))

    val df = if (format == JMS) {
      LOGGER.warn("No support for JMS data validations, will skip validations")
      sparkSession.emptyDataFrame
    } else if (format == HTTP) {
      if (!options.contains(VALIDATION_IDENTIFIER)) {
        LOGGER.error(s"Required option '$VALIDATION_IDENTIFIER' not found in validation options, unable to validate HTTP responses")
        sparkSession.emptyDataFrame
      } else {
        val cleanMetadataIdentifier = cleanValidationIdentifier(options(VALIDATION_IDENTIFIER))
        val validationFolder = s"${foldersConfig.recordTrackingForValidationFolderPath}/$cleanMetadataIdentifier"
        LOGGER.debug(s"Checking for HTTP responses saved as JSON for validation, folder=$validationFolder")
        sparkSession.read
          .json(validationFolder)
      }
    } else if (format == ICEBERG) {
      val tableName = configWithFormatConfigs(TABLE)
      val tableNameWithCatalog = if (tableName.split("\\.").length == 2) {
        s"iceberg.$tableName"
      } else tableName
      sparkSession.read
        .options(configWithFormatConfigs)
        .table(tableNameWithCatalog)
    } else {
      sparkSession.read
        .format(format)
        .options(configWithFormatConfigs)
        .load()
    }

    if (!df.storageLevel.useMemory) df.cache()
    df
  }

  private def logValidationErrors(validationResults: List[ValidationConfigResult]): Unit = {
    validationResults.foreach(vcr => vcr.dataSourceValidationResults.map(dsr => {
      val failedValidations = dsr.validationResults.filter(r => !r.isSuccess)

      if (dsr.validationResults.isEmpty) {
        LOGGER.debug(s"Validation results are empty, no validations run, name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}")
      } else if (failedValidations.isEmpty) {
        LOGGER.info(s"Data validations successful for validation, name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
          s"data-source-options=${dsr.options}, num-validations=${dsr.validationResults.size}, is-success=true")
      } else {
        LOGGER.error(s"Data validations failed, name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
          s"data-source-options=${dsr.options}, num-validations=${dsr.validationResults.size}, num-failed=${failedValidations.size}, is-success=false")
        failedValidations.foreach(validationRes => {
          val (validationType, validationCheck) = validationRes.validation match {
            case ExpressionValidation(expr, _) => ("expression", expr)
            case GroupByValidation(_, _, _, expr, _) => ("groupByAggregate", expr)
            //TODO get validationCheck from validationBuilder -> make this a recursive method to get validationCheck
            case UpstreamDataSourceValidation(_, upstreamDataSource, _, _, _) => ("upstreamDataSource", upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
            case _ => ("Unknown", "")
          }
          val sampleErrors = validationRes.sampleErrorValues.getOrElse(Array())
            .map(ObjectMapperUtil.jsonObjectMapper.writeValueAsString)
            .mkString(",")
          LOGGER.error(s"Failed validation: validation-name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
            s"data-source-options=${dsr.options}, is-success=${validationRes.isSuccess}, validation-type=$validationType, " +
            s"check=$validationCheck, sample-errors=$sampleErrors")
        })
      }
    }))
  }

  private def getAdditionalSparkConfig(format: String, connectionConfig: Map[String, String]): Map[String, String] = {
    format match {
      case DELTA => connectionConfig ++ DELTA_LAKE_SPARK_CONF
      case ICEBERG => connectionConfig ++ ICEBERG_SPARK_CONF
      case _ => connectionConfig
    }
  }
}
