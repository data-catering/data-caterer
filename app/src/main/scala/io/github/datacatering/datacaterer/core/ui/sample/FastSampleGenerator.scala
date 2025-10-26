package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Count, Field, Plan, Step, Task}
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.generator.DataGeneratorFactory
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.ui.model._
import io.github.datacatering.datacaterer.core.ui.resource.YamlResourceCache
import io.github.datacatering.datacaterer.core.ui.service.{DataFrameManager, PlanLoaderService, TaskLoaderService}
import io.github.datacatering.datacaterer.core.util.{ForeignKeyUtil, ObjectMapperUtil}
import net.datafaker.Faker
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.{Files, Paths}
import java.util.{Locale, UUID}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

object FastSampleGenerator {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val MAX_SAMPLE_SIZE = 100

  /**
   * Converts a DataFrame to raw bytes in the specified format
   */
  def dataFrameToRawBytes(df: DataFrame, format: String): Array[Byte] = {
    import java.nio.file.{Files, Paths}

    val tempDir = Files.createTempDirectory("sample_output_")
    val tempPath = tempDir.resolve(s"output.$format").toString

    try {
      // Write to temp file using Spark's native writers
      format.toLowerCase match {
        case "json" =>
          // For JSON, write as a single file and read back
          df.coalesce(1).write.mode("overwrite").json(tempPath)

        case "csv" =>
          // For CSV, write with header
          df.coalesce(1).write.mode("overwrite")
            .option("header", "true")
            .csv(tempPath)

        case "parquet" =>
          df.coalesce(1).write.mode("overwrite").parquet(tempPath)

        case "orc" =>
          df.coalesce(1).write.mode("overwrite").orc(tempPath)

        case _ =>
          throw new IllegalArgumentException(s"Unsupported format for raw output: $format")
      }

      // Find the part file (Spark creates part-* files)
      val partFiles = Files.list(Paths.get(tempPath))
        .filter(p => {
          val fileName = p.getFileName.toString
          fileName.startsWith("part-") && !fileName.endsWith(".crc")
        })
        .toArray()
        .map(_.asInstanceOf[java.nio.file.Path])

      if (partFiles.isEmpty) {
        throw new RuntimeException(s"No output file generated in $tempPath")
      }

      // Read the content as bytes
      val bytes = Files.readAllBytes(partFiles.head)
      bytes

    } finally {
      // Clean up temp directory
      try {
        Files.walk(tempDir)
          .sorted(java.util.Comparator.reverseOrder())
          .forEach(Files.delete)
      } catch {
        case ex: Exception =>
          LOGGER.warn(s"Failed to clean up temp directory: $tempDir", ex)
      }
    }
  }

  /**
   * Gets the Content-Type header for a given format
   */
  def contentTypeForFormat(format: String): String = {
    format.toLowerCase match {
      case "json" => "application/json"
      case "csv" => "text/csv"
      case "parquet" => "application/octet-stream"
      case "orc" => "application/octet-stream"
      case _ => "application/octet-stream"
    }
  }

  case class SampleResponseWithDataFrame(response: SampleResponse, dataFrame: Option[DataFrame] = None)

  def generateFromTaskFile(request: TaskFileSampleRequest)(implicit sparkSession: SparkSession): SampleResponse = {
    generateFromTaskFileWithDataFrame(request).response
  }

  def generateFromTaskFileWithDataFrame(request: TaskFileSampleRequest)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    LOGGER.info(s"Generating sample from task file: ${request.taskYamlPath}, step: ${request.stepName}")

    Try {
      val step = parseStepFromYaml(request.taskYamlPath, request.stepName)
      val format = step.options.getOrElse("format", "json")

      // For file-based step requests, we don't have full plan context for relationships
      // So we generate the step individually
      generateSampleForSingleStep(step.fields, request.sampleSize, request.fastMode, format)
    } match {
      case Success(responseWithDf) => responseWithDf
      case Failure(ex: java.io.FileNotFoundException) =>
        LOGGER.error(s"Task file not found: ${request.taskYamlPath}", ex)
        SampleResponseWithDataFrame(
          SampleResponse(
            success = false,
            executionId = generateId(),
            error = Some(SampleError("FILE_NOT_FOUND", s"Task file not found: ${request.taskYamlPath}"))
          )
        )
      case Failure(ex: IllegalArgumentException) =>
        LOGGER.error(s"Invalid request: ${ex.getMessage}", ex)
        SampleResponseWithDataFrame(
          SampleResponse(
            success = false,
            executionId = generateId(),
            error = Some(SampleError("INVALID_REQUEST", ex.getMessage))
          )
        )
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from task file", ex)
        SampleResponseWithDataFrame(handleError(ex))
    }
  }

  def generateFromSchema(request: SchemaSampleRequest)(implicit sparkSession: SparkSession): SampleResponse = {
    generateFromSchemaWithDataFrame(request).response
  }

  def generateFromSchemaWithDataFrame(request: SchemaSampleRequest)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    LOGGER.info(s"Generating sample from inline fields: ${request.fields.size} fields")

    Try {
      // For schema-based requests, we don't have step/plan context for relationships
      // So we generate the fields individually
      generateSampleForSingleStep(request.fields, request.sampleSize, request.fastMode, request.format)
    } match {
      case Success(responseWithDf) => responseWithDf
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from schema", ex)
        SampleResponseWithDataFrame(handleError(ex))
    }
  }

  def generateFromTaskYaml(request: TaskYamlSampleRequest)(implicit sparkSession: SparkSession): SampleResponse = {
    generateFromTaskYamlWithDataFrame(request).response
  }

  def generateFromTaskYamlWithDataFrame(request: TaskYamlSampleRequest)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    LOGGER.info(s"Generating sample from task YAML content, step: ${request.stepName}")

    Try {
      val step = parseStepFromYamlContent(request.taskYamlContent, request.stepName)
      val format = step.options.getOrElse("format", "json")

      // For YAML content-based step requests, we don't have full plan context for relationships
      // So we generate the step individually
      generateSampleForSingleStep(step.fields, request.sampleSize, request.fastMode, format)
    } match {
      case Success(responseWithDf) => responseWithDf
      case Failure(ex: IllegalArgumentException) =>
        LOGGER.error(s"Invalid YAML content: ${ex.getMessage}", ex)
        SampleResponseWithDataFrame(
          SampleResponse(
            success = false,
            executionId = generateId(),
            error = Some(SampleError("INVALID_YAML", ex.getMessage))
          )
        )
      case Failure(ex: com.fasterxml.jackson.core.JsonParseException) =>
        LOGGER.error(s"Failed to parse YAML content: ${ex.getMessage}", ex)
        SampleResponseWithDataFrame(
          SampleResponse(
            success = false,
            executionId = generateId(),
            error = Some(SampleError("YAML_PARSE_ERROR", s"Failed to parse YAML content: ${ex.getMessage}"))
          )
        )
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from task YAML", ex)
        SampleResponseWithDataFrame(handleError(ex))
    }
  }


  private def parseStepFromYaml(taskPath: String, stepName: Option[String])(implicit sparkSession: SparkSession): Step = {
    LOGGER.debug(s"Parsing step from YAML file: $taskPath, step: $stepName")

    if (!Files.exists(Paths.get(taskPath))) {
      throw new java.io.FileNotFoundException(s"Task file not found: $taskPath")
    }

    // Use consolidated YAML parsing from PlanParser
    val convertedTask = PlanParser.parseTaskFile(taskPath)

    if (convertedTask.steps.isEmpty) {
      throw new IllegalArgumentException("No steps found in task file")
    }

    stepName match {
      case Some(name) =>
        convertedTask.steps.find(_.name == name) match {
          case Some(step) => step
          case None =>
            val availableSteps = convertedTask.steps.map(_.name).mkString(", ")
            throw new IllegalArgumentException(
              s"Step '$name' not found in task. Available steps: $availableSteps"
            )
        }
      case None => convertedTask.steps.head
    }
  }

  private def parseStepFromYamlContent(yamlContent: String, stepName: Option[String]): Step = {
    LOGGER.debug(s"Parsing step from YAML content, step: $stepName")

    if (yamlContent.trim.isEmpty) {
      throw new IllegalArgumentException("YAML content is empty")
    }

    // Use consolidated YAML parsing from PlanParser
    val convertedTask = PlanParser.parseTaskFromContent(yamlContent)

    if (convertedTask.steps.isEmpty) {
      throw new IllegalArgumentException("No steps found in task YAML")
    }

    stepName match {
      case Some(name) =>
        convertedTask.steps.find(_.name == name) match {
          case Some(step) => step
          case None =>
            val availableSteps = convertedTask.steps.map(_.name).mkString(", ")
            throw new IllegalArgumentException(
              s"Step '$name' not found in task. Available steps: $availableSteps"
            )
        }
      case None => convertedTask.steps.head
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

  /**
   * Generate samples with relationship-aware processing
   * This is the unified method that handles both individual and relationship-aware generation efficiently
   */
  private def generateSamplesWithRelationships(
                                                plan: Option[Plan],
                                                requestedSteps: List[(Step, String)], // (step, dataSourceName)
                                                sampleSize: Int,
                                                fastMode: Boolean,
                                                enableRelationships: Boolean,
                                                taskDirectory: Option[String] = None,
                                                useV2: Boolean = true
                                              )(implicit sparkSession: SparkSession): Map[String, (Step, SampleResponseWithDataFrame)] = {

    if (!enableRelationships || plan.isEmpty) {
      // Simple case: generate each step independently
      LOGGER.debug(s"Generating ${requestedSteps.size} steps independently (relationships disabled or no plan)")
      requestedSteps.map { case (step, dataSourceName) =>
        val format = step.options.getOrElse("format", "json")
        val responseWithDf = generateSampleForSingleStep(step.fields, sampleSize, fastMode, format)
        val key = s"$dataSourceName/${step.name}"
        (key, (step, responseWithDf))
      }.toMap
    } else {
      // Complex case: generate all plan steps together to handle relationships
      LOGGER.info(s"Generating samples with relationships for plan: ${plan.get.name}")

      try {
        // Generate data for all plan steps to establish relationship context
        val allPlanSteps = plan.get.tasks.filter(_.enabled).flatMap { planTask =>
          try {
            val task = findTaskByName(planTask.name, taskDirectory)
            val steps = task.steps.filter(_.enabled).map((_, planTask.dataSourceName))
            LOGGER.debug(s"Found ${steps.size} steps in task ${planTask.name}: ${steps.map(_._1.name).mkString(", ")}")
            steps
          } catch {
            case ex: Exception =>
              LOGGER.warn(s"Failed to load task ${planTask.name}: ${ex.getMessage}")
              List.empty
          }
        }

        LOGGER.debug(s"All plan steps: ${allPlanSteps.size} steps: ${allPlanSteps.map { case (step, ds) => s"${ds}.${step.name}" }.mkString(", ")}")
        LOGGER.debug(s"Requested steps: ${requestedSteps.size} steps: ${requestedSteps.map { case (step, ds) => s"${ds}.${step.name}" }.mkString(", ")}")

        // Generate DataFrames for all plan steps
        val factory = new DataGeneratorFactory(new Faker(Locale.ENGLISH) with Serializable, enableFastGeneration = fastMode)
        val allGeneratedData = allPlanSteps.map { case (step, dataSourceName) =>
          val recordCount = step.count.records.getOrElse(sampleSize.toLong)
          LOGGER.debug(s"Generating step ${step.name} with recordCount=$recordCount (step.count.records=${step.count.records}, sampleSize=$sampleSize)")
          val df = factory.generateDataForStep(step, dataSourceName, 0L, sampleSize)
          val stepKey = s"$dataSourceName.${step.name}"
          LOGGER.debug(s"Generated step ${step.name}: ${df.count()} records")
          df.show(false)
          // Use DataFrameManager to track cached DataFrames for proper cleanup
          val cachedDf = DataFrameManager.cacheDataFrame(stepKey, df)
          (stepKey, cachedDf)
        }.toMap

        LOGGER.info(s"Generated ${allGeneratedData.size} DataFrames, applying foreign key relationships")

        // Apply foreign key relationships
        val dataFramesWithForeignKeys = ForeignKeyUtil.getDataFramesWithForeignKeys(plan.get, allGeneratedData.toList, useV2)
        val updatedDataMap = dataFramesWithForeignKeys.toMap

        try {
          // Convert requested steps to response format using relationship-aware data
          val results = requestedSteps.map { case (step, dataSourceName) =>
            val stepKey = s"$dataSourceName.${step.name}"
            LOGGER.debug(s"Looking for step key: $stepKey in updatedDataMap with keys: ${updatedDataMap.keys.mkString(", ")}")
            val df = updatedDataMap.getOrElse(stepKey, {
              LOGGER.warn(s"Step $stepKey not found in relationship-processed data, falling back to individual generation")
              val factory = new DataGeneratorFactory(new Faker(Locale.ENGLISH) with Serializable, enableFastGeneration = fastMode)
              // Use step's configured record count, fallback to sampleSize if not configured
              val recordCount = step.count.records.getOrElse(sampleSize.toLong)
              factory.generateDataForStep(step, dataSourceName, 0L, recordCount)
            })
            LOGGER.debug(s"Using dataframe for step $stepKey with ${df.count()} records")

            val format = step.options.getOrElse("format", "json")
            val responseWithDf = convertDataFrameToSampleResponse(df, sampleSize, fastMode, format, enableRelationships)
            val responseKey = s"$dataSourceName/${step.name}"
            (responseKey, (step, responseWithDf))
          }.toMap

          results
        } finally {
          // Clean up cached DataFrames to prevent memory leaks
          LOGGER.debug(s"Cleaning up ${allGeneratedData.size} cached DataFrames")
          val unpersisted = DataFrameManager.unpersistMultiple(allGeneratedData.keys.toList)
          LOGGER.debug(s"Unpersisted $unpersisted DataFrames")
        }

      } catch {
        case ex: Exception =>
          LOGGER.error(s"Error in relationship-aware generation, falling back to individual generation: ${ex.getMessage}", ex)
          // Fallback to individual generation
          requestedSteps.map { case (step, dataSourceName) =>
            val format = step.options.getOrElse("format", "json")
            val responseWithDf = generateSampleForSingleStep(step.fields, sampleSize, fastMode, format)
            val key = s"$dataSourceName/${step.name}"
            (key, (step, responseWithDf))
          }.toMap
      }
    }
  }

  /**
   * Generate sample for a single step (no relationships)
   */
  private def generateSampleForSingleStep(fields: List[Field], sampleSize: Int, fastMode: Boolean, format: String)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    val startTime = System.currentTimeMillis()
    val executionId = generateId()

    // Validate and sanitize sample size
    val validatedSampleSize = Math.min(Math.max(sampleSize, 1), MAX_SAMPLE_SIZE)

    // Create a minimal step for generation
    val sampleStep = Step(
      name = "sample",
      `type` = "file",
      options = Map("path" -> "sample", "format" -> format, "enableFastGeneration" -> fastMode.toString),
      count = Count(records = Some(validatedSampleSize.toLong)),
      fields = fields
    )

    val factory = new DataGeneratorFactory(new Faker(Locale.ENGLISH) with Serializable, enableFastGeneration = fastMode)
    val df = factory.generateDataForStep(sampleStep, "sample", 0L, validatedSampleSize.toLong)

    convertDataFrameToSampleResponse(df, validatedSampleSize, fastMode, format, enableRelationships = false)
  }

  /**
   * Convert a DataFrame to SampleResponseWithDataFrame
   */
  private def convertDataFrameToSampleResponse(df: DataFrame, sampleSize: Int, fastMode: Boolean, format: String, enableRelationships: Boolean): SampleResponseWithDataFrame = {
    val startTime = System.currentTimeMillis()
    val executionId = generateId()

    // Collect omitted field paths for filtering
    val omittedPaths = io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.collectOmittedPaths(df.schema)

    // Convert to response format
    val sampleData = df.collect().map(row => {
      val jsonString = row.json
      val jsonNode = ObjectMapperUtil.jsonObjectMapper.readTree(jsonString)
      io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.removeOmittedFromJson(jsonNode, omittedPaths)
      ObjectMapperUtil.jsonObjectMapper.convertValue(jsonNode, classOf[java.util.Map[String, Any]])
        .asInstanceOf[java.util.Map[String, Any]]
        .asScala
        .toMap
    }).toList

    val cleanSchema = io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.removeOmittedFieldsFromSchema(df.schema)
    val cleanDf = io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.removeOmitFields(df)
    val schemaInfo = SchemaInfo.fromSparkSchema(cleanSchema)
    val generationTime = System.currentTimeMillis() - startTime

    val response = SampleResponse(
      success = true,
      executionId = executionId,
      schema = Some(schemaInfo),
      sampleData = Some(sampleData),
      metadata = Some(SampleMetadata(
        sampleSize = sampleSize,
        actualRecords = sampleData.length,
        generatedInMs = generationTime,
        fastModeEnabled = fastMode,
        relationshipsEnabled = enableRelationships
      )),
      format = Some(format)
    )

    SampleResponseWithDataFrame(response, Some(cleanDf))
  }


  /**
   * Generate sample data from a specific step (which may contain nested sub-steps) from a saved plan
   * Note: In PlanRunRequest, "tasks" is actually List[Step] where each Step represents a task definition
   */
  def generateFromPlanStep(
                                    planName: String, 
                                    taskName: String, 
                                    stepName: String, 
                                    sampleSize: Int = 10, 
                                    fastMode: Boolean = true,
                                    planDirectory: Option[String] = None,
                                    taskDirectory: Option[String] = None
                                  )(implicit sparkSession: SparkSession): Either[SampleError, (Step, SampleResponseWithDataFrame)] = {
    LOGGER.info(s"Generating sample from plan step: plan=$planName, task=$taskName, step=$stepName")

    Try {
      // Load plan and find the specific step within the task
      val planRequest = loadPlan(planName, planDirectory, taskDirectory)
      val step = findSpecificStepInTask(planRequest, taskName, stepName, taskDirectory)

      val format = step.options.getOrElse("format", "json")
      // For step-level generation, relationships are not enabled as per requirement
      val responseWithDf = generateSampleForSingleStep(step.fields, sampleSize, fastMode, format)
      (step, responseWithDf)
    } match {
      case Success((step, responseWithDf)) => Right((step, responseWithDf))
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from plan step", ex)
        Left(SampleError("GENERATION_ERROR", ex.getMessage, Some(ex.getClass.getSimpleName)))
    }
  }

  /**
   * Generate sample data from a specific task from a saved plan
   * Note: In PlanRunRequest, "tasks" is actually List[Step] where each Step represents a task definition
   */
  def generateFromPlanTask(
                            planName: String,
                            taskName: String,
                            sampleSize: Int = 10,
                            fastMode: Boolean = true,
                            enableRelationships: Boolean = false,
                            planDirectory: Option[String] = None,
                            taskDirectory: Option[String] = None,
                            useV2: Boolean = true
                          )(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from plan task: plan=$planName, task=$taskName, enableRelationships=$enableRelationships")

    Try {
      val planRequest = loadPlan(planName, planDirectory, taskDirectory)

      // Handle both JSON and YAML plans
      val stepsToGenerate = if (planRequest.tasks.nonEmpty) {
        // JSON plan - step is directly in planRequest.tasks
        findStepsInPlan(planRequest, taskName, taskDirectory)
      } else {
        // YAML plan - load task from task files and get first step
        val task = findTaskByName(taskName, taskDirectory)
        if (task.steps.isEmpty) {
          throw new IllegalArgumentException(s"No steps found in task '$taskName'")
        }
        task.steps
      }

      // Prepare steps with their data source names for unified generation
      val requestedSteps = stepsToGenerate.map { step =>
        val planTask = planRequest.plan.tasks.find(_.name == taskName)
        val dataSourceName = planTask.map(_.dataSourceName).getOrElse(taskName)
        (step, dataSourceName)
      }

      // Use the unified generation method
      val results = generateSamplesWithRelationships(
        plan = Some(planRequest.plan),
        requestedSteps = requestedSteps,
        sampleSize = sampleSize,
        fastMode = fastMode,
        enableRelationships = enableRelationships,
        taskDirectory = taskDirectory,
        useV2 = useV2
      )

      // Convert key format from "dataSource/stepName" to "planName/stepName" for backward compatibility
      results.map { case (_, (step, response)) =>
        val newKey = s"$planName/${step.name}"
        (newKey, (step, response))
      }
    } match {
      case Success(results) => Right(results)
      case Failure(ex) =>
        LOGGER.error(s"Error generating samples from plan task", ex)
        Left(SampleError("GENERATION_ERROR", ex.getMessage, Some(ex.getClass.getSimpleName)))
    }
  }

  /**
   * Generate sample data from all tasks in a plan
   * Note: In PlanRunRequest, "tasks" is actually List[Step] where each Step represents a task definition
   */
  def generateFromPlan(planName: String, sampleSize: Int = 10, fastMode: Boolean = true, enableRelationships: Boolean = false, planDirectory: Option[String] = None, taskDirectory: Option[String] = None, useV2: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from plan: plan=$planName, enableRelationships=$enableRelationships")

    Try {
      val planRequest = loadPlan(planName, planDirectory, taskDirectory)

      // Prepare steps with their data source names for unified generation
      val requestedSteps = if (planRequest.tasks.nonEmpty) {
        // JSON plan - steps are in planRequest.tasks
        planRequest.tasks.map(step => (step, planName))
      } else {
        // YAML plan - load tasks from task files
        val taskNames = planRequest.plan.tasks.map(_.name)
        val allTasks = if (taskNames.nonEmpty) {
          taskNames.flatMap(taskName => {
            try {
              val task = findTaskByName(taskName, taskDirectory)
              val planTask = planRequest.plan.tasks.find(_.name == taskName)
              task.steps.map(step => (step, planTask.map(_.dataSourceName).getOrElse(taskName)))
            } catch {
              case ex: Exception =>
                LOGGER.warn(s"Failed to load task $taskName: ${ex.getMessage}")
                List.empty
            }
          })
        } else {
          List()
        }
        allTasks
      }

      // Use the unified generation method
      val results = generateSamplesWithRelationships(
        plan = Some(planRequest.plan),
        requestedSteps = requestedSteps,
        sampleSize = sampleSize,
        fastMode = fastMode,
        enableRelationships = enableRelationships,
        taskDirectory = taskDirectory,
        useV2 = useV2
      )

      // Convert key format from "dataSource/stepName" to "planName/stepName" for backward compatibility  
      results.map { case (key, (step, response)) =>
        val newKey = s"$planName/${step.name}"
        (newKey, (step, response))
      }
    } match {
      case Success(results) => Right(results)
      case Failure(ex) =>
        LOGGER.error(s"Error generating samples from plan", ex)
        Left(SampleError("GENERATION_ERROR", ex.getMessage, Some(ex.getClass.getSimpleName)))
    }
  }

  private def loadPlan(planName: String, planDirectory: Option[String] = None, taskDirectory: Option[String] = None)(implicit sparkSession: SparkSession): io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest = {
    PlanLoaderService.loadPlan(planName, planDirectory)
  }

  private def findStepsInPlan(planRequest: io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest, taskName: String, taskDirectory: Option[String] = None)(implicit sparkSession: SparkSession): List[io.github.datacatering.datacaterer.api.model.Step] = {
    // For JSON plans, steps are in planRequest.tasks
    if (planRequest.tasks.nonEmpty) {
      planRequest.tasks.find(_.name == taskName) match {
        case Some(step) => List(step)
        case None =>
          val availableSteps = planRequest.tasks.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Step/Task '$taskName' not found in plan. Available: $availableSteps")
      }
    } else {
      // For YAML plans, tasks are empty and need to be loaded from task files
      // First check if stepName is a task name in the plan
      val taskNames = planRequest.plan.tasks.map(_.name)
      if (!taskNames.contains(taskName)) {
        throw new IllegalArgumentException(s"Step/Task '$taskName' not found in plan. Available: ${taskNames.mkString(", ")}")
      }

      // Load the task from YAML files using the same logic as PlanRepository
      findTaskByName(taskName, taskDirectory).steps.headOption match {
        case Some(step) => List(step)
        case None =>
          throw new IllegalArgumentException(s"No steps found in task '$taskName'")
      }
    }
  }

  /**
   * Find a task by name using TaskLoaderService
   */
  private def findTaskByName(taskName: String, taskDirectory: Option[String] = None)(implicit sparkSession: SparkSession): Task = {
    TaskLoaderService.findTaskByName(taskName, taskDirectory)
  }

  /**
   * Generate sample from a task by name
   */
  def generateFromTaskName(taskName: String, sampleSize: Int = 10, fastMode: Boolean = true, enableRelationships: Boolean = false, taskDirectory: Option[String] = None)(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from task: task=$taskName, enableRelationships=$enableRelationships")

    Try {
      val task = findTaskByName(taskName, taskDirectory)

      // Try to find a plan that contains this task for relationship processing
      val planOption = if (enableRelationships) {
        val allYamlPlans = PlanLoaderService.getAllYamlPlansAsPlanRunRequests
        allYamlPlans.find(_.plan.tasks.exists(_.name == taskName)).map(_.plan)
      } else {
        None
      }

      // Prepare steps with their data source names for unified generation
      val requestedSteps = task.steps.map { step =>
        val dataSourceName = planOption
          .flatMap(_.tasks.find(_.name == taskName))
          .map(_.dataSourceName)
          .getOrElse(taskName)
        (step, dataSourceName)
      }

      // Use the unified generation method
      val results = generateSamplesWithRelationships(
        plan = planOption,
        requestedSteps = requestedSteps,
        sampleSize = sampleSize,
        fastMode = fastMode,
        enableRelationships = enableRelationships,
        taskDirectory = taskDirectory
      )

      // Convert key format from "dataSource/stepName" to just "stepName" for backward compatibility
      results.map { case (key, (step, response)) =>
        (step.name, (step, response))
      }
    } match {
      case Success(results) => Right(results)
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from task: $taskName", ex)
        Left(SampleError("TASK_SAMPLE_ERROR", ex.getMessage))
    }
  }

  /**
   * Generate sample from a step by name (searches across all tasks)
   */
  def generateFromStepName(stepName: String, sampleSize: Int = 10, fastMode: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, (Step, SampleResponseWithDataFrame)] = {
    LOGGER.info(s"Generating sample from step: step=$stepName")

    Try {
      val taskFolderPath = ConfigParser.foldersConfig.taskFolderPath
      val allTasks = PlanParser.parseTasks(taskFolderPath)

      // Find the step across all tasks
      val stepOption = allTasks.flatMap(_.steps).find(_.name == stepName)

      stepOption match {
        case Some(step) =>
          val format = step.options.getOrElse("format", "json")
          // For step-level generation, relationships are not enabled as per requirement
          val responseWithDf = generateSampleForSingleStep(step.fields, sampleSize, fastMode, format)
          (step, responseWithDf)
        case None =>
          val allStepNames = allTasks.flatMap(_.steps).map(_.name).mkString(", ")
          throw new IllegalArgumentException(
            s"Step '$stepName' not found in any task. Available steps: $allStepNames"
          )
      }
    } match {
      case Success(result) => Right(result)
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from step: $stepName", ex)
        Left(SampleError("STEP_SAMPLE_ERROR", ex.getMessage))
    }
  }

  /**
   * Find a specific step within a task from a plan
   */
  private def findSpecificStepInTask(planRequest: io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest, taskName: String, stepName: String, taskDirectory: Option[String] = None)(implicit sparkSession: SparkSession): io.github.datacatering.datacaterer.api.model.Step = {
    // For JSON plans, steps are in planRequest.tasks
    if (planRequest.tasks.nonEmpty) {
      // In JSON plans, each "task" in planRequest.tasks is actually a Step representing the entire task
      planRequest.tasks.find(_.name == taskName) match {
        case Some(taskStep) =>
          // The task itself is the step we want (no sub-steps in JSON plans)
          taskStep
        case None =>
          val availableTasks = planRequest.tasks.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Task '$taskName' not found in plan. Available: $availableTasks")
      }
    } else {
      // For YAML plans, load the task from task files and find the specific step
      val task = findTaskByName(taskName, taskDirectory)
      task.steps.find(_.name == stepName) match {
        case Some(step) => step
        case None =>
          val availableSteps = task.steps.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Step '$stepName' not found in task '$taskName'. Available steps: $availableSteps")
      }
    }
  }
}