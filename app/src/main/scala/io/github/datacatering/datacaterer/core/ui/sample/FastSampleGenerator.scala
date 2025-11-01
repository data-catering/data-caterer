package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Count, Field, Plan, Step, Task}
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.generator.DataGeneratorFactory
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.transformer.{PerRecordTransformer, WholeFileTransformer}
import io.github.datacatering.datacaterer.core.ui.model._
import io.github.datacatering.datacaterer.core.ui.service.{DataFrameManager, PlanLoaderService, TaskLoaderService}
import io.github.datacatering.datacaterer.core.util.{DataFrameOmitUtil, ForeignKeyUtil, ObjectMapperUtil}
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
   * Cached DataGeneratorFactory instances to avoid redundant instantiation and UDF registration.
   * FastSampleGenerator always uses Locale.ENGLISH, so we cache one instance per fastMode setting.
   * These are stored per SparkSession to handle multiple sessions correctly.
   */
  private val factoryCache = new scala.collection.mutable.HashMap[(SparkSession, Boolean), DataGeneratorFactory]()

  /**
   * Get or create a cached DataGeneratorFactory instance for the given fastMode setting.
   */
  private def getFactory(fastMode: Boolean)(implicit sparkSession: SparkSession): DataGeneratorFactory = {
    factoryCache.synchronized {
      factoryCache.getOrElseUpdate(
        (sparkSession, fastMode),
        new DataGeneratorFactory(new Faker(Locale.ENGLISH) with Serializable, enableFastGeneration = fastMode)
      )
    }
  }

  /**
   * Converts a DataFrame to raw bytes in the specified format
   *
   * Delegates to SampleDataConverter for implementation.
   */
  def dataFrameToRawBytes(df: DataFrame, format: String, step: Step): Array[Byte] = {
    SampleDataConverter.dataFrameToRawBytes(df, format, step)
  }

  /**
   * Gets the Content-Type header for a given format
   *
   * Delegates to SampleDataConverter for implementation.
   */
  def contentTypeForFormat(format: String): String = {
    SampleDataConverter.contentTypeForFormat(format)
  }

  case class SampleResponseWithDataFrame(response: SampleResponse, dataFrame: Option[DataFrame] = None, step: Option[Step] = None)

  def generateFromTaskFileWithDataFrame(request: TaskFileSampleRequest)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    LOGGER.info(s"Generating sample from task file: ${request.taskYamlPath}, step: ${request.stepName}")

    Try {
      val step = StepParser.parseStepFromYaml(request.taskYamlPath, request.stepName)
      val format = step.options.getOrElse("format", "json")

      val effectiveSampleSize = SampleSizeCalculator.getEffectiveSampleSize(request.sampleSize, Some(step))
      LOGGER.debug(s"Effective sample size: $effectiveSampleSize (request.sampleSize=${request.sampleSize}, step.count.records=${step.count.records})")

      // For file-based step requests, we don't have full plan context for relationships
      // So we generate the step individually
      val factory = getFactory(request.fastMode)
      generateSampleForSingleStep(step.fields, effectiveSampleSize, request.fastMode, format, Some(step), factory)
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

  def generateFromSchemaWithDataFrame(request: SchemaSampleRequest)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    LOGGER.info(s"Generating sample from inline fields: ${request.fields.size} fields")

    Try {
      val effectiveSampleSize = SampleSizeCalculator.getEffectiveSampleSize(request.sampleSize)
      LOGGER.debug(s"Effective sample size: $effectiveSampleSize (request.sampleSize=${request.sampleSize})")

      // For schema-based requests, we don't have step/plan context for relationships
      // So we generate the fields individually
      val factory = getFactory(request.fastMode)
      generateSampleForSingleStep(request.fields, effectiveSampleSize, request.fastMode, request.format, None, factory)
    } match {
      case Success(responseWithDf) => responseWithDf
      case Failure(ex) =>
        LOGGER.error(s"Error generating sample from schema", ex)
        SampleResponseWithDataFrame(handleError(ex))
    }
  }

  def generateFromTaskYamlWithDataFrame(request: TaskYamlSampleRequest)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    LOGGER.info(s"Generating sample from task YAML content, step: ${request.stepName}")

    Try {
      val step = StepParser.parseStepFromYamlContent(request.taskYamlContent, request.stepName)
      val format = step.options.getOrElse("format", "json")

      val effectiveSampleSize = SampleSizeCalculator.getEffectiveSampleSize(request.sampleSize, Some(step))
      LOGGER.debug(s"Effective sample size: $effectiveSampleSize (request.sampleSize=${request.sampleSize}, step.count.records=${step.count.records})")

      // For YAML content-based step requests, we don't have full plan context for relationships
      // So we generate the step individually
      val factory = getFactory(request.fastMode)
      generateSampleForSingleStep(step.fields, effectiveSampleSize, request.fastMode, format, Some(step), factory)
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

  private def generateId(): String = SampleDataConverter.generateId()

  /**
   * Generate samples with relationship-aware processing
   *
   * Delegates to RelationshipAwareSampleGenerator for implementation.
   */
  private def generateSamplesWithRelationships(
    plan: Option[Plan],
    requestedSteps: List[(Step, String)], // (step, dataSourceName)
    sampleSize: Option[Int],
    fastMode: Boolean,
    enableRelationships: Boolean,
    taskDirectory: Option[String] = None,
    useV2: Boolean = true
  )(implicit sparkSession: SparkSession): Map[String, (Step, SampleResponseWithDataFrame)] = {
    val factory = getFactory(fastMode)
    RelationshipAwareSampleGenerator.generateSamplesWithRelationships(
      plan, requestedSteps, sampleSize, fastMode, enableRelationships,
      factory, taskDirectory, useV2
    )
  }

  /**
   * Generate sample for a single step (no relationships)
   */
  private def generateSampleForSingleStep(
    fields: List[Field],
    sampleSize: Int,
    fastMode: Boolean,
    format: String,
    step: Option[Step] = None,
    factory: DataGeneratorFactory
  )(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    // Validate and sanitize sample size
    val validatedSampleSize = SampleSizeCalculator.validateSampleSize(sampleSize, MAX_SAMPLE_SIZE)

    // Create a minimal step for generation if not provided
    val sampleStep = step.getOrElse(Step(
      name = "sample",
      `type` = "file",
      options = Map("path" -> "sample", "format" -> format, "enableFastGeneration" -> fastMode.toString),
      count = Count(records = Some(validatedSampleSize.toLong)),
      fields = fields
    ))

    val df = factory.generateDataForStep(sampleStep, "sample", 0L, validatedSampleSize.toLong)

    SampleDataConverter.convertDataFrameToSampleResponse(df, validatedSampleSize, fastMode, format, enableRelationships = false, sampleStep)
  }


  /**
   * Generate sample data from a specific step (which may contain nested sub-steps) from a saved plan
   * Note: In PlanRunRequest, "tasks" is actually List[Step] where each Step represents a task definition
   */
  def generateFromPlanStep(
                            planName: String,
                            taskName: String,
                            stepName: String,
                            sampleSize: Option[Int] = None,
                            fastMode: Boolean = true,
                            planDirectory: Option[String] = None,
                            taskDirectory: Option[String] = None
                          )(implicit sparkSession: SparkSession): Either[SampleError, (Step, SampleResponseWithDataFrame)] = {
    LOGGER.info(s"Generating sample from plan step: plan=$planName, task=$taskName, step=$stepName")

    Try {
      // Load plan and find the specific step within the task
      val planRequest = loadPlan(planName, planDirectory)
      val step = StepParser.findSpecificStepInTask(planRequest, taskName, stepName, taskDirectory)

      val format = step.options.getOrElse("format", "json")

      val effectiveSampleSize = SampleSizeCalculator.getEffectiveSampleSize(sampleSize, Some(step))
      LOGGER.debug(s"Effective sample size: $effectiveSampleSize (sampleSize=$sampleSize, step.count.records=${step.count.records})")

      // For step-level generation, relationships are not enabled as per requirement
      val factory = getFactory(fastMode)
      val responseWithDf = generateSampleForSingleStep(step.fields, effectiveSampleSize, fastMode, format, Some(step), factory)
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
                            sampleSize: Option[Int] = None,
                            fastMode: Boolean = true,
                            enableRelationships: Boolean = false,
                            planDirectory: Option[String] = None,
                            taskDirectory: Option[String] = None,
                            useV2: Boolean = true
                          )(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from plan task: plan=$planName, task=$taskName, enableRelationships=$enableRelationships")

    Try {
      val planRequest = loadPlan(planName, planDirectory)

      // Handle both JSON and YAML plans
      val stepsToGenerate = if (planRequest.tasks.nonEmpty) {
        // JSON plan - step is directly in planRequest.tasks
        StepParser.findStepsInPlan(planRequest, taskName, taskDirectory)
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
  def generateFromPlan(planName: String, sampleSize: Option[Int] = None, fastMode: Boolean = true, enableRelationships: Boolean = false, planDirectory: Option[String] = None, taskDirectory: Option[String] = None, useV2: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from plan: plan=$planName, enableRelationships=$enableRelationships")

    Try {
      val planRequest = loadPlan(planName, planDirectory)

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
      results.map { case (_, (step, response)) =>
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

  private def loadPlan(planName: String, planDirectory: Option[String] = None)(implicit sparkSession: SparkSession): PlanRunRequest = {
    PlanLoaderService.loadPlan(planName, planDirectory)
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
  def generateFromTaskName(
                            taskName: String,
                            sampleSize: Option[Int] = None,
                            fastMode: Boolean = true,
                            enableRelationships: Boolean = false,
                            taskDirectory: Option[String] = None
                          )(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from task: task=$taskName, enableRelationships=$enableRelationships")

    Try {
      val task = findTaskByName(taskName, taskDirectory)

      // Try to find a plan that contains this task for relationship processing
      val planOption = if (enableRelationships) {
        val allYamlPlans = PlanLoaderService.getAllYamlPlansAsPlanRunRequests()
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
      results.map { case (_, (step, response)) =>
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
  def generateFromStepName(
                            stepName: String,
                            sampleSize: Option[Int] = None,
                            fastMode: Boolean = true
                          )(implicit sparkSession: SparkSession): Either[SampleError, (Step, SampleResponseWithDataFrame)] = {
    LOGGER.info(s"Generating sample from step: step=$stepName")

    Try {
      val taskFolderPath = ConfigParser.foldersConfig.taskFolderPath
      val allTasks = PlanParser.parseTasks(taskFolderPath)

      // Find the step across all tasks
      val stepOption = allTasks.flatMap(_.steps).find(_.name == stepName)

      stepOption match {
        case Some(step) =>
          val format = step.options.getOrElse("format", "json")

          // Use step.count.records if sampleSize is not provided, otherwise use provided sampleSize, default to 10
          val effectiveSampleSize = sampleSize.orElse(step.count.records.map(_.toInt)).getOrElse(10)
          LOGGER.debug(s"Effective sample size: $effectiveSampleSize (sampleSize=$sampleSize, step.count.records=${step.count.records})")

          // For step-level generation, relationships are not enabled as per requirement
          val factory = getFactory(fastMode)
          val responseWithDf = generateSampleForSingleStep(step.fields, effectiveSampleSize, fastMode, format, Some(step), factory)
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

}