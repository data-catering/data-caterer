package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.Constants.ONE_OF_GENERATOR
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceValidation, Field, Plan, Task, UpstreamDataSourceValidation, ValidationConfiguration, YamlUpstreamDataSourceValidation, YamlValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ConnectionConfigWithTaskBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.exception.DataValidationMissingUpstreamDataSourceException
import io.github.datacatering.datacaterer.core.util.FileUtil.{getFileContentFromFileSystem, isCloudStoragePath}
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
      val mappedSchema = fieldsToString(step.fields)
      step.copy(
        count = step.count.copy(perField = countPerColGenerator, options = countStringOpts),
        fields = mappedSchema
      )
    })
    task.copy(steps = stringSteps)
  }

  private def fieldsToString(fields: List[Field]): List[Field] = {
    fields.map(field => {
      if (field.options.contains(ONE_OF_GENERATOR)) {
        val fieldGenOpt = toStringValues(field.options)
        field.copy(options = fieldGenOpt)
      } else {
        field.copy(fields = fieldsToString(field.fields))
      }
    })
  }

  private def toStringValues(options: Map[String, Any]): Map[String, Any] = {
    options.map(x => (x._1, x._2.toString))
  }
}
