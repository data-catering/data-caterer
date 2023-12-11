package com.github.pflooky.datagen.core.validator

import com.github.pflooky.datacaterer.api.ValidationBuilder
import com.github.pflooky.datacaterer.api.model.Constants.{DEFAULT_ENABLE_VALIDATION, ENABLE_DATA_VALIDATION, FORMAT, HTTP, JMS}
import com.github.pflooky.datacaterer.api.model.{DataSourceValidation, ExpressionValidation, FoldersConfig, GroupByValidation, UpstreamDataSourceValidation, ValidationConfig, ValidationConfiguration}
import com.github.pflooky.datagen.core.model.{DataSourceValidationResult, ValidationConfigResult, ValidationResult}
import com.github.pflooky.datagen.core.parser.ValidationParser
import com.github.pflooky.datagen.core.validator.ValidationHelper.getValidationType
import com.github.pflooky.datagen.core.validator.ValidationWaitImplicits.WaitConditionOps
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
- simple column validations (amount < 100)
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
    LOGGER.info("Executing data validations")
    val validationResults = getValidations.map(vc => {
      val startTime = LocalDateTime.now()
      val dataSourceValidationResults = vc.dataSources.flatMap(dataSource => {
        val dataSourceName = dataSource._1
        val dataSourceValidations = dataSource._2
        val numValidations = dataSourceValidations.flatMap(_.validations).size

        LOGGER.info(s"Executing data validations for data source, name=${vc.name}, " +
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
      LOGGER.debug(s"Waiting for validation condition to be successful before running validations, name=${vc.name}," +
        s"data-source-name=$dataSourceName, details=${dataSourceValidation.options}, num-validations=${dataSourceValidation.validations.size}")
      dataSourceValidation.waitCondition.waitForCondition(connectionConfigsByName)

      val df = getDataFrame(dataSourceName, dataSourceValidation.options)
      if (df.isEmpty) {
        LOGGER.info("No data found to run validations")
        DataSourceValidationResult(dataSourceName, dataSourceValidation.options, List())
      } else {
        val count = df.count()
        val results = dataSourceValidation.validations.map(validBuilder => tryValidate(df, count, validBuilder))
        df.unpersist()
        LOGGER.debug(s"Finished data validations, name=${vc.name}," +
          s"data-source-name=$dataSourceName, details=${dataSourceValidation.options}, num-validations=${dataSourceValidation.validations.size}")
        DataSourceValidationResult(dataSourceName, dataSourceValidation.options, results)
      }
    } else {
      LOGGER.debug(s"Data validations are disabled, data-source-name=$dataSourceName, details=${dataSourceValidation.options}")
      DataSourceValidationResult(dataSourceName, dataSourceValidation.options, List())
    }
  }

  private def tryValidate(df: DataFrame, count: Long, validBuilder: ValidationBuilder): ValidationResult = {
    val validationDescription = validBuilder.validation.toOptions.map(l => s"${l.head}=${l.last}").mkString(", ")
    Try(getValidationType(validBuilder.validation, foldersConfig.recordTrackingForValidationFolderPath).validate(df, count)) match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to run data validation, $validationDescription", exception)
        ValidationResult(validBuilder.validation, false, count, count, Some(sparkSession.createDataFrame(Seq(CustomErrorSample(exception.getLocalizedMessage)))))
      case Success(value) =>
        LOGGER.debug(s"Successfully ran data validation, $validationDescription")
        value
    }
  }

  private def getValidations: Array[ValidationConfiguration] = {
    optValidationConfigs.map(_.toArray).getOrElse(ValidationParser.parseValidation(foldersConfig.validationFolderPath))
  }

  private def getDataFrame(dataSourceName: String, options: Map[String, String]): DataFrame = {
    val connectionConfig = connectionConfigsByName(dataSourceName)
    val format = connectionConfig(FORMAT)
    if (format == HTTP || format == JMS) {
      LOGGER.warn("No support for HTTP or JMS data validations, will skip validations")
      sparkSession.emptyDataFrame
    } else {
      val df = sparkSession.read
        .format(format)
        .options(connectionConfig ++ options)
        .load()
      if (!df.storageLevel.useMemory) df.cache()
      df
    }
  }

  private def logValidationErrors(validationResults: List[ValidationConfigResult]): Unit = {
    validationResults.foreach(vcr => vcr.dataSourceValidationResults.map(dsr => {
      val failedValidations = dsr.validationResults.filter(r => !r.isSuccess)

      if (failedValidations.isEmpty) {
        LOGGER.info(s"Data validations successful for validation, name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
          s"data-source-options=${dsr.options}, is-success=true")
      } else {
        failedValidations.foreach(validationRes => {
          val (validationType, validationCheck) = validationRes.validation match {
            case ExpressionValidation(expr, selectExpr) => ("expression", expr)
            case GroupByValidation(_, _, _, expr) => ("groupByAggregate", expr)
            //TODO get validationCheck from validationBuilder -> make this a recursive method to get validationCheck
            case UpstreamDataSourceValidation(validationBuilder, upstreamDataSource, _, _, _) => ("upstreamDataSource", "")
            case _ => ("Unknown", "")
          }
          val sampleErrors = validationRes.sampleErrorValues.get.take(validationConfig.numSampleErrorRecords).map(_.json).mkString(",")
          LOGGER.error(s"Failed validation: validation-name=${vcr.name}, description=${vcr.description}, data-source-name=${dsr.dataSourceName}, " +
            s"data-source-options=${dsr.options}, is-success=${validationRes.isSuccess}, validation-type=$validationType, check=$validationCheck, sample-errors=$sampleErrors")
        })
      }
    }))
  }
}
