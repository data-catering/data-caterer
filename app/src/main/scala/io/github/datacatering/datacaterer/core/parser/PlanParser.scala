package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP_PATH_PARAM_FIELD_PREFIX, HTTP_QUERY_PARAM_FIELD_PREFIX, REAL_TIME_METHOD_FIELD, REAL_TIME_URL_FIELD, YAML_HTTP_BODY_FIELD, YAML_HTTP_HEADERS_FIELD, YAML_HTTP_URL_FIELD, YAML_REAL_TIME_BODY_FIELD, YAML_REAL_TIME_HEADERS_FIELD}
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceValidation, DataType, Field, Plan, Task, UpstreamDataSourceValidation, ValidationConfiguration, YamlUpstreamDataSourceValidation, YamlValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ConnectionConfigWithTaskBuilder, FieldBuilder, HttpMethodEnum, HttpQueryParameterStyleEnum, ValidationBuilder}
import io.github.datacatering.datacaterer.core.exception.DataValidationMissingUpstreamDataSourceException
import io.github.datacatering.datacaterer.core.util.FileUtil.{getFileContentFromFileSystem, isCloudStoragePath}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.FieldOps
import io.github.datacatering.datacaterer.core.util.{FileUtil, ObjectMapperUtil}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object PlanParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

  def getPlanTasksFromYaml(dataCatererConfiguration: DataCatererConfiguration)
                          (implicit sparkSession: SparkSession): (Plan, List[Task], Option[List[ValidationConfiguration]]) = {
    val parsedPlan = PlanParser.parsePlan(dataCatererConfiguration.foldersConfig.planFilePath)
    val enabledPlannedTasks = parsedPlan.tasks.filter(_.enabled)
    val enabledTaskMap = enabledPlannedTasks.map(t => (t.name, t)).toMap
    val planWithEnabledTasks = parsedPlan.copy(tasks = enabledPlannedTasks)

    val tasks = PlanParser.parseTasks(dataCatererConfiguration.foldersConfig.taskFolderPath)
    val enabledTasks = tasks.filter(t => enabledTaskMap.contains(t.name)).toList
    val validations = if (dataCatererConfiguration.flagsConfig.enableValidation) {
      Some(PlanParser.parseValidations(dataCatererConfiguration.foldersConfig.validationFolderPath, dataCatererConfiguration.connectionConfigByName))
    } else None
    (planWithEnabledTasks, enabledTasks, validations)
  }

  def parsePlan(planFilePath: String)(implicit sparkSession: SparkSession): Plan = {
    val parsedPlan = if (isCloudStoragePath(planFilePath)) {
      val fileContent = getFileContentFromFileSystem(FileSystem.get(sparkSession.sparkContext.hadoopConfiguration), planFilePath)
      OBJECT_MAPPER.readValue(fileContent, classOf[Plan])
    } else {
      val planFile = FileUtil.getFile(planFilePath)
      OBJECT_MAPPER.readValue(planFile, classOf[Plan])
    }
    LOGGER.info(s"Found plan file and parsed successfully, plan-file-path=$planFilePath, plan-name=${parsedPlan.name}, plan-description=${parsedPlan.description}")
    parsedPlan
  }

  def parseTasks(taskFolderPath: String)(implicit sparkSession: SparkSession): Array[Task] = {
    val parsedTasks = YamlFileParser.parseFiles[Task](taskFolderPath)
    parsedTasks.map(convertTaskNumbersToString)
      .map(convertToSpecificFields)
  }

  def parseValidations(
                        validationFolderPath: String,
                        connectionConfigsByName: Map[String, Map[String, String]]
                      )(implicit sparkSession: SparkSession): List[ValidationConfiguration] = {
    val yamlConfig = YamlFileParser.parseFiles[YamlValidationConfiguration](validationFolderPath).toList
    yamlConfig.map(y => {
      val dataSourceValidations = y.dataSources.map(d => {
        val mappedValidations = d._2.map(dataSourceValidations => {
          val parsedValidations = dataSourceValidations.validations.map {
            case yamlUpstream: YamlUpstreamDataSourceValidation =>
              getYamlUpstreamValidationAsValidationWithConnection(connectionConfigsByName, yamlUpstream)
            case v => ValidationBuilder(v)
          }
          DataSourceValidation(dataSourceValidations.options, dataSourceValidations.waitCondition, parsedValidations)
        })
        d._1 -> mappedValidations
      })
      ValidationConfiguration(y.name, y.description, dataSourceValidations)
    })
  }

  def getYamlUpstreamValidationAsValidationWithConnection(
                                                           connectionConfigsByName: Map[String, Map[String, String]],
                                                           yamlUpstream: YamlUpstreamDataSourceValidation
                                                         ): ValidationBuilder = {
    val upstreamConnectionConfig = connectionConfigsByName.get(yamlUpstream.upstreamDataSource)
    upstreamConnectionConfig match {
      case Some(value) =>
        val connectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder(yamlUpstream.upstreamDataSource, value).noop()
        val baseValidation = UpstreamDataSourceValidation(
          yamlUpstream.validation.map(v => ValidationBuilder(v)),
          connectionConfigWithTaskBuilder,
          yamlUpstream.upstreamReadOptions,
          yamlUpstream.joinFields,
          yamlUpstream.joinType
        )
        ValidationBuilder(baseValidation)
      case None =>
        throw DataValidationMissingUpstreamDataSourceException(yamlUpstream.upstreamDataSource)
    }
  }

  private def convertTaskNumbersToString(task: Task): Task = {
    val stringSteps = task.steps.map(step => {
      val countPerColGenerator = step.count.perField.map(perFieldCount => {
        val stringOpts = toStringValues(perFieldCount.options)
        perFieldCount.copy(options = stringOpts)
      })
      val countStringOpts = toStringValues(step.count.options)
      val mappedSchema = step.fields.map(_.fieldToStringOptions)
      step.copy(
        count = step.count.copy(perField = countPerColGenerator, options = countStringOpts),
        fields = mappedSchema
      )
    })
    task.copy(steps = stringSteps)
  }

  private def convertToSpecificFields(task: Task): Task = {
    val specificFields = task.steps.map(step => {
      val mappedFields = step.fields.flatMap(field => {
        field.name match {
          case YAML_REAL_TIME_HEADERS_FIELD =>
            val headerFields = field.fields.map(innerField => FieldBuilder(innerField))
            val messageHeaders = FieldBuilder().messageHeaders(headerFields: _*)
            List(messageHeaders.field)
          case YAML_REAL_TIME_BODY_FIELD =>
            val innerFields = field.fields.map(innerField => FieldBuilder(innerField))
            val messageBodyBuilder = FieldBuilder().messageBody(innerFields: _*)
            messageBodyBuilder.map(_.field)
          case YAML_HTTP_URL_FIELD =>
            val innerFields = field.fields.map(innerField => FieldBuilder(innerField))
            val urlField = innerFields.find(f => f.field.name == REAL_TIME_URL_FIELD).flatMap(f => f.field.static)
            val methodField = innerFields.find(f => f.field.name == REAL_TIME_METHOD_FIELD).flatMap(f => f.field.static)
            val pathParams = innerFields.find(f => f.field.name == HTTP_PATH_PARAM_FIELD_PREFIX)
              .map(f => f.field.fields.map(f1 => FieldBuilder(f1).httpPathParam(f1.name)))
              .getOrElse(List())
            val queryParams = innerFields.find(f => f.field.name == HTTP_QUERY_PARAM_FIELD_PREFIX)
              .map(f => f.field.fields.map(f1 =>
                FieldBuilder(f1).httpQueryParam(
                  f1.name,
                  DataType.fromString(f1.`type`.getOrElse("string")),
                  f1.options.get("style").map(style => HttpQueryParameterStyleEnum.withName(style.toString)).getOrElse(HttpQueryParameterStyleEnum.FORM),
                  f1.options.get("explode").forall(_.toString.toBoolean)
                )
              ))
              .getOrElse(List())
            FieldBuilder().httpUrl(
              urlField.get,
              HttpMethodEnum.withName(methodField.get),
              pathParams,
              queryParams
            ).map(_.field)
          case YAML_HTTP_HEADERS_FIELD =>
            val headerFields = field.fields.map(innerField => FieldBuilder(innerField).httpHeader(innerField.name))
            headerFields.map(_.field)
          case YAML_HTTP_BODY_FIELD =>
            val innerFields = field.fields.map(innerField => FieldBuilder(innerField))
            val httpBody = FieldBuilder().httpBody(innerFields: _*)
            httpBody.map(_.field)
          case _ => List(field)
        }
      })
      step.copy(fields = mappedFields)
    })
    task.copy(steps = specificFields)
  }

  private def toStringValues(options: Map[String, Any]): Map[String, String] = {
    options.map(x => (x._1, x._2.toString))
  }
}
