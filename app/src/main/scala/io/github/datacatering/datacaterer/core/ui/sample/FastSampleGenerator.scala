package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG}
import io.github.datacatering.datacaterer.api.model.{Count, Field, Step, Task}
import io.github.datacatering.datacaterer.core.generator.DataGeneratorFactory
import io.github.datacatering.datacaterer.core.ui.model._
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkProvider}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.nio.file.{Files, Paths}
import java.util.{Locale, Random, UUID}
import scala.util.{Failure, Success, Try}

object FastSampleGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val MAX_SAMPLE_SIZE = 100
  
  def generateFromTaskFile(request: TaskFileSampleRequest)(implicit sparkSession: SparkSession): SampleResponse = {
    LOGGER.info(s"Generating sample from task file: ${request.taskYamlPath}, step: ${request.stepName}")
    
    Try {
      val step = parseStepFromYaml(request.taskYamlPath, request.stepName)
      generateSample(step.fields, request.sampleSize, request.fastMode)
    } match {
      case Success(response) => response
      case Failure(ex: java.io.FileNotFoundException) =>
        LOGGER.error(s"Task file not found: ${request.taskYamlPath}", ex)
        SampleResponse(
          success = false, 
          executionId = generateId(),
          error = Some(SampleError("FILE_NOT_FOUND", s"Task file not found: ${request.taskYamlPath}"))
        )
      case Failure(ex: IllegalArgumentException) =>
        LOGGER.error(s"Invalid request: ${ex.getMessage}", ex)
        SampleResponse(
          success = false,
          executionId = generateId(), 
          error = Some(SampleError("INVALID_REQUEST", ex.getMessage))
        )
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from task file", ex)
        handleError(ex)
    }
  }
  
  def generateFromSchema(request: SchemaSampleRequest)(implicit sparkSession: SparkSession): SampleResponse = {
    LOGGER.info(s"Generating sample from inline fields: ${request.fields.size} fields")
    
    Try {
      generateSample(request.fields, request.sampleSize, request.fastMode)
    } match {
      case Success(response) => response
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from schema", ex)
        handleError(ex)
    }
  }
  
  def generateFromTaskYaml(request: TaskYamlSampleRequest)(implicit sparkSession: SparkSession): SampleResponse = {
    LOGGER.info(s"Generating sample from task YAML content, step: ${request.stepName}")
    
    Try {
      val step = parseStepFromYamlContent(request.taskYamlContent, request.stepName)
      generateSample(step.fields, request.sampleSize, request.fastMode)
    } match {
      case Success(response) => response
      case Failure(ex: IllegalArgumentException) =>
        LOGGER.error(s"Invalid YAML content: ${ex.getMessage}", ex)
        SampleResponse(
          success = false,
          executionId = generateId(), 
          error = Some(SampleError("INVALID_YAML", ex.getMessage))
        )
      case Failure(ex: com.fasterxml.jackson.core.JsonParseException) =>
        LOGGER.error(s"Failed to parse YAML content: ${ex.getMessage}", ex)
        SampleResponse(
          success = false,
          executionId = generateId(),
          error = Some(SampleError("YAML_PARSE_ERROR", s"Failed to parse YAML content: ${ex.getMessage}"))
        )
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from task YAML", ex)
        handleError(ex)
    }
  }
  
  
  private def generateSample(fields: List[Field], sampleSize: Int, fastMode: Boolean)(implicit sparkSession: SparkSession): SampleResponse = {
    val startTime = System.currentTimeMillis()
    val executionId = generateId()
    
    LOGGER.debug(s"Generating sample: fields=${fields.size}, size=$sampleSize, fastMode=$fastMode")
    
    // Validate and sanitize sample size
    val validatedSampleSize = Math.min(Math.max(sampleSize, 1), MAX_SAMPLE_SIZE)
    if (validatedSampleSize != sampleSize) {
      LOGGER.warn(s"Sample size adjusted from $sampleSize to $validatedSampleSize (max: $MAX_SAMPLE_SIZE)")
    }
    
    // Create a minimal step for generation with the provided fields
    val step = Step(
      name = "sample",
      `type` = "file",
      options = Map("path" -> "sample", "format" -> "json"),
      count = Count(records = Some(validatedSampleSize.toLong)),
      fields = fields
    )
    
    val optimizedStep = step.copy(
      options = step.options ++ Map(
        "enableFastGeneration" -> fastMode.toString
      )
    )
    
    // Generate using core logic (bypass all orchestration)
    val locale = Locale.ENGLISH
    val faker = new Faker(locale)
    
    val factory = new DataGeneratorFactory(faker, enableFastGeneration = fastMode)
    
    val df = factory.generateDataForStep(optimizedStep, "sample", 0L, validatedSampleSize.toLong)
    
    // Convert to response format
    val sampleData = df.collect().map(row => {
      df.columns.map(col => col -> row.getAs[Any](col)).toMap
    }).toList
    
    val schemaInfo = SchemaInfo.fromSparkSchema(df.schema)
    val generationTime = System.currentTimeMillis() - startTime
    
    LOGGER.info(s"Sample generation completed: records=${sampleData.length}, time=${generationTime}ms")
    
    SampleResponse(
      success = true,
      executionId = executionId,
      schema = Some(schemaInfo),
      sampleData = Some(sampleData),
      metadata = Some(SampleMetadata(
        sampleSize = validatedSampleSize,
        actualRecords = sampleData.length,
        generatedInMs = generationTime,
        fastModeEnabled = fastMode
      ))
    )
  }
  
  private def filterComplexOptions(options: Map[String, String]): Map[String, String] = {
    val complexFeatures = Set(
      "foreignKeys", "validations", "relationships", 
      "enableValidation", "enableRecordTracking"
    )
    options.filterKeys(!complexFeatures.contains(_))
  }
  
  private def parseStepFromYaml(taskPath: String, stepName: Option[String]): Step = {
    LOGGER.debug(s"Parsing step from YAML file: $taskPath, step: $stepName")
    
    if (!Files.exists(Paths.get(taskPath))) {
      throw new java.io.FileNotFoundException(s"Task file not found: $taskPath")
    }
    
    val yamlContent = Files.readString(Paths.get(taskPath))
    val task = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[Task])
    
    if (task.steps.isEmpty) {
      throw new IllegalArgumentException("No steps found in task file")
    }
    
    stepName match {
      case Some(name) => 
        task.steps.find(_.name == name) match {
          case Some(step) => step
          case None => 
            val availableSteps = task.steps.map(_.name).mkString(", ")
            throw new IllegalArgumentException(
              s"Step '$name' not found in task. Available steps: $availableSteps"
            )
        }
      case None => task.steps.head
    }
  }
  
  private def parseStepFromYamlContent(yamlContent: String, stepName: Option[String]): Step = {
    LOGGER.debug(s"Parsing step from YAML content, step: $stepName")
    
    if (yamlContent.trim.isEmpty) {
      throw new IllegalArgumentException("YAML content is empty")
    }
    
    val task = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[Task])
    
    if (task.steps.isEmpty) {
      throw new IllegalArgumentException("No steps found in task YAML")
    }
    
    stepName match {
      case Some(name) => 
        task.steps.find(_.name == name) match {
          case Some(step) => step
          case None => 
            val availableSteps = task.steps.map(_.name).mkString(", ")
            throw new IllegalArgumentException(
              s"Step '$name' not found in task. Available steps: $availableSteps"
            )
        }
      case None => task.steps.head
    }
  }
  
  
  private def handleError(ex: Throwable): SampleResponse = {
    val errorCode = ex match {
      case _: IllegalArgumentException => "INVALID_SCHEMA"
      case _: java.io.FileNotFoundException => "FILE_NOT_FOUND"
      case _: com.fasterxml.jackson.core.JsonParseException => "INVALID_YAML"
      case _ => "INTERNAL_ERROR"
    }
    
    SampleResponse(
      success = false,
      executionId = generateId(),
      error = Some(SampleError(
        code = errorCode,
        message = ex.getMessage,
        details = Some(ex.getClass.getSimpleName)
      ))
    )
  }
  
  private def generateId(): String = UUID.randomUUID().toString.split("-").head
}