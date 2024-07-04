package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.Constants.ONE_OF_GENERATOR
import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, Plan, Schema, Task, UpstreamDataSourceValidation, ValidationConfiguration, YamlUpstreamDataSourceValidation, YamlValidationConfiguration}
import io.github.datacatering.datacaterer.api.{ConnectionConfigWithTaskBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.util.FileUtil.{getFileContentFromFileSystem, isCloudStoragePath}
import io.github.datacatering.datacaterer.core.util.{FileUtil, ObjectMapperUtil}
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object PlanParser {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val OBJECT_MAPPER = ObjectMapperUtil.yamlObjectMapper

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
              val upstreamConnectionConfig = connectionConfigsByName.get(yamlUpstream.upstreamDataSource)
              upstreamConnectionConfig match {
                case Some(value) =>
                  val connectionConfigWithTaskBuilder = ConnectionConfigWithTaskBuilder(yamlUpstream.upstreamDataSource, value).noop()
                  val baseValidation = UpstreamDataSourceValidation(
                    ValidationBuilder(yamlUpstream.validation),
                    connectionConfigWithTaskBuilder,
                    yamlUpstream.upstreamReadOptions,
                    yamlUpstream.joinColumns,
                    yamlUpstream.joinType
                  )
                  ValidationBuilder(baseValidation)
                case None =>
                  throw new RuntimeException("Failed to find upstream data source configuration")
              }
            case v => ValidationBuilder(v)
          }
          DataSourceValidation(dataSourceValidations.options, dataSourceValidations.waitCondition, parsedValidations)
        })
        d._1 -> mappedValidations
      })
      ValidationConfiguration(y.name, y.description, dataSourceValidations)
    })
  }

  private def convertTaskNumbersToString(task: Task): Task = {
    val stringSteps = task.steps.map(step => {
      val countPerColGenerator = step.count.perColumn.map(perColumnCount => {
        val generator = perColumnCount.generator.map(gen => gen.copy(options = toStringValues(gen.options)))
        perColumnCount.copy(generator = generator)
      })
      val countGenerator = step.count.generator.map(gen => gen.copy(options = toStringValues(gen.options)))
      val mappedSchema = schemaToString(step.schema)
      step.copy(
        count = step.count.copy(perColumn = countPerColGenerator, generator = countGenerator),
        schema = mappedSchema
      )
    })
    task.copy(steps = stringSteps)
  }

  private def schemaToString(schema: Schema): Schema = {
    val mappedFields = schema.fields.map(fields => {
      fields.map(field => {
        if (field.generator.isDefined && field.generator.get.`type` != ONE_OF_GENERATOR) {
          val fieldGenOpt = toStringValues(field.generator.get.options)
          field.copy(generator = Some(field.generator.get.copy(options = fieldGenOpt)))
        } else {
          field.copy(schema = field.schema.map(schemaToString))
        }
      })
    })
    schema.copy(fields = mappedFields)
  }

  private def toStringValues(options: Map[String, Any]): Map[String, Any] = {
    options.map(x => (x._1, x._2.toString))
  }
}
