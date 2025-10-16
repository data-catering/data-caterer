package io.github.datacatering.datacaterer.core.ui.sample

import io.github.datacatering.datacaterer.api.model.{Count, Field, Plan, Step, Task}
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.generator.DataGeneratorFactory
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.ui.model._
import io.github.datacatering.datacaterer.core.util.{FileUtil, ObjectMapperUtil}
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
      generateSampleWithDataFrame(step.fields, request.sampleSize, request.fastMode, format)
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
      generateSampleWithDataFrame(request.fields, request.sampleSize, request.fastMode, request.format)
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
      generateSampleWithDataFrame(step.fields, request.sampleSize, request.fastMode, format)
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


  private def generateSampleWithDataFrame(fields: List[Field], sampleSize: Int, fastMode: Boolean, format: String)(implicit sparkSession: SparkSession): SampleResponseWithDataFrame = {
    val startTime = System.currentTimeMillis()
    val executionId = generateId()

    LOGGER.debug(s"Generating sample: fields=${fields.size}, size=$sampleSize, fastMode=$fastMode, format=$format")

    // Validate and sanitize sample size
    val validatedSampleSize = Math.min(Math.max(sampleSize, 1), MAX_SAMPLE_SIZE)
    if (validatedSampleSize != sampleSize) {
      LOGGER.warn(s"Sample size adjusted from $sampleSize to $validatedSampleSize (max: $MAX_SAMPLE_SIZE)")
    }

    // Create a minimal step for generation with the provided fields
    val step = Step(
      name = "sample",
      `type` = "file",
      options = Map("path" -> "sample", "format" -> format),
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

    // Collect omitted field paths for filtering
    val omittedPaths = io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.collectOmittedPaths(df.schema)

    // Convert to response format using Spark's native JSON serialization to preserve nested structures
    val sampleData = df.collect().map(row => {
      // Use Spark's row.json method to properly serialize nested structures
      val jsonString = row.json
      val jsonNode = ObjectMapperUtil.jsonObjectMapper.readTree(jsonString)

      // Remove omitted fields from the JSON
      io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.removeOmittedFromJson(jsonNode, omittedPaths)

      // Convert back to Map
      ObjectMapperUtil.jsonObjectMapper.convertValue(jsonNode, classOf[java.util.Map[String, Any]])
        .asInstanceOf[java.util.Map[String, Any]]
        .asScala
        .toMap
    }).toList

    // Build a new schema without omitted fields
    val cleanSchema = io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.removeOmittedFieldsFromSchema(df.schema)
    // Create clean DataFrame without omitted fields for raw output
    val cleanDf = io.github.datacatering.datacaterer.core.util.DataFrameOmitUtil.removeOmitFields(df)
    val schemaInfo = SchemaInfo.fromSparkSchema(cleanSchema)
    val generationTime = System.currentTimeMillis() - startTime

    LOGGER.info(s"Sample generation completed: records=${sampleData.length}, time=${generationTime}ms")

    val response = SampleResponse(
      success = true,
      executionId = executionId,
      schema = Some(schemaInfo),
      sampleData = Some(sampleData),
      metadata = Some(SampleMetadata(
        sampleSize = validatedSampleSize,
        actualRecords = sampleData.length,
        generatedInMs = generationTime,
        fastModeEnabled = fastMode
      )),
      format = Some(format)
    )

    SampleResponseWithDataFrame(response, Some(cleanDf))
  }
  
  private def parseStepFromYaml(taskPath: String, stepName: Option[String])(implicit sparkSession: SparkSession): Step = {
    LOGGER.debug(s"Parsing step from YAML file: $taskPath, step: $stepName")
    
    if (!Files.exists(Paths.get(taskPath))) {
      throw new java.io.FileNotFoundException(s"Task file not found: $taskPath")
    }
    
    // Use existing YAML parsing infrastructure and apply field conversions
    val taskFile = new java.io.File(taskPath)
    val rawTask = ObjectMapperUtil.yamlObjectMapper.readValue(taskFile, classOf[Task])
    val convertedTask = PlanParser.convertToSpecificFields(rawTask)
    
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
    
    // Use existing YAML parsing infrastructure and apply field conversions
    val rawTask = ObjectMapperUtil.yamlObjectMapper.readValue(yamlContent, classOf[Task])
    val convertedTask = PlanParser.convertToSpecificFields(rawTask)
    
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
   * Generate sample data from a specific step (which may contain nested sub-steps) from a saved plan
   * Note: In PlanRunRequest, "tasks" is actually List[Step] where each Step represents a task definition
   */
  def generateFromPlanStep(planName: String, taskName: String, stepName: String, sampleSize: Int = 10, fastMode: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, (Step, SampleResponseWithDataFrame)] = {
    LOGGER.info(s"Generating sample from plan step: plan=$planName, task=$taskName, step=$stepName")

    Try {
      // Load plan and find the specific step within the task
      val planRequest = loadPlan(planName)
      val step = findSpecificStepInTask(planRequest, taskName, stepName)

      val format = step.options.getOrElse("format", "json")
      val responseWithDf = generateSampleWithDataFrame(step.fields, sampleSize, fastMode, format)
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
  def generateFromPlanTask(planName: String, taskName: String, sampleSize: Int = 10, fastMode: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from plan task: plan=$planName, task=$taskName")

    Try {
      val planRequest = loadPlan(planName)
      
      // Handle both JSON and YAML plans
      val stepsToGenerate = if (planRequest.tasks.nonEmpty) {
        // JSON plan - step is directly in planRequest.tasks
        findStepsInPlan(planRequest, taskName)
      } else {
        // YAML plan - load task from task files and get first step
        val task = findTaskByName(taskName)
        if (task.steps.isEmpty) {
          throw new IllegalArgumentException(s"No steps found in task '$taskName'")
        }
        task.steps
      }

      // Generate samples for each step
      stepsToGenerate.map { step =>
        val format = step.options.getOrElse("format", "json")
        val responseWithDf = generateSampleWithDataFrame(step.fields, sampleSize, fastMode, format)
        val key = s"$planName/${step.name}"
        (key, (step, responseWithDf))
      }.toMap
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
  def generateFromPlan(planName: String, sampleSize: Int = 10, fastMode: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from plan: plan=$planName")

    Try {
      val planRequest = loadPlan(planName)

      // Handle both JSON and YAML plans
      val stepsToGenerate = if (planRequest.tasks.nonEmpty) {
        // JSON plan - steps are in planRequest.tasks
        planRequest.tasks
      } else {
        // YAML plan - load tasks from task files
        val taskNames = planRequest.plan.tasks.map(_.name)
        val allTasks = if (taskNames.nonEmpty) {
          taskNames.map(taskName => findTaskByName(taskName))
        } else {
          List()
        }
        // Get first step from each task
        allTasks.flatMap(_.steps)
      }

      // Generate samples for each step
      stepsToGenerate.map { step =>
        val format = step.options.getOrElse("format", "json")
        val responseWithDf = generateSampleWithDataFrame(step.fields, sampleSize, fastMode, format)
        val key = s"$planName/${step.name}"
        (key, (step, responseWithDf))
      }.toMap
    } match {
      case Success(results) => Right(results)
      case Failure(ex) =>
        LOGGER.error(s"Error generating samples from plan", ex)
        Left(SampleError("GENERATION_ERROR", ex.getMessage, Some(ex.getClass.getSimpleName)))
    }
  }

  private def loadPlan(planName: String): io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest = {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
    val planSaveFolder = s"$INSTALL_DIRECTORY/plan"
    
    // Try JSON first
    val jsonFile = java.nio.file.Paths.get(s"$planSaveFolder/$planName.json")
    if (Files.exists(jsonFile)) {
      val fileContent = Files.readString(jsonFile)
      return ObjectMapperUtil.jsonObjectMapper.readValue(fileContent, classOf[io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest])
    }

    // Try YAML plans
    val yamlPlanRequest = loadYamlPlanByName(planName)
    if (yamlPlanRequest.isDefined) {
      return yamlPlanRequest.get
    }

    throw new java.io.FileNotFoundException(s"Plan not found: $planName (searched in JSON and YAML sources)")
  }

  /**
   * Load YAML plan by plan name, reusing logic from PlanRepository
   */
  private def loadYamlPlanByName(planName: String): Option[io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest] = {
    val allYamlPlans = getYamlPlansAsPlanRunRequests
    allYamlPlans.find(_.plan.name == planName)
  }

  /**
   * Get all YAML plan files and convert them to PlanRunRequest format
   * Reuses logic from PlanRepository.getYamlPlansAsPlanRunRequests
   */
  private def getYamlPlansAsPlanRunRequests: List[io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest] = {
    val yamlPlanFiles = getYamlPlanFiles
    yamlPlanFiles.flatMap(planFile => {
      val tryParse = scala.util.Try {
        val parsedPlan = ObjectMapperUtil.yamlObjectMapper.readValue(planFile, classOf[Plan])
        // Convert YAML Plan to PlanRunRequest format
        convertYamlPlanToPlanRunRequest(parsedPlan, planFile.getName.replaceAll("\\.yaml$", ""))
      }
      tryParse match {
        case scala.util.Failure(exception) =>
          LOGGER.error(s"Failed to parse YAML plan file, file=${planFile.getAbsolutePath}, exception=${exception.getMessage}")
          None
        case scala.util.Success(value) => Some(value)
      }
    })
  }

  /**
   * Get all YAML plan files from configured folders
   * Reuses logic from PlanRepository.getYamlPlanFiles
   */
  private def getYamlPlanFiles: List[java.io.File] = {
    import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
    
    // First, check the plan save folder (where UI stores plans)
    val planSaveFolder = s"$INSTALL_DIRECTORY/plan"
    val planFolder = new java.io.File(planSaveFolder)
    val planFolderYamlFiles = if (planFolder.exists() && planFolder.isDirectory) {
      LOGGER.debug(s"Scanning for YAML plan files in plan save folder: ${planFolder.getAbsolutePath}")
      val files = planFolder.listFiles()
      if (files != null) {
        files.filter(f => f.isFile && f.getName.endsWith(".yaml")).toList
      } else {
        List()
      }
    } else {
      List()
    }

    // Also check the directory from the configured planFilePath
    val planFilePath = ConfigParser.foldersConfig.planFilePath
    val planDirPath = new java.io.File(planFilePath).getParent

    val configuredYamlFiles = if (planDirPath != null) {
      // Try multiple approaches to find the directory (direct, classpath, classloader)
      val tryPlanDir = getPlanDirectory(planDirPath)

      tryPlanDir match {
        case Some(planDir) if planDir.exists() && planDir.isDirectory =>
          LOGGER.debug(s"Scanning for YAML plan files in configured path: ${planDir.getAbsolutePath}")
          val files = planDir.listFiles()
          if (files != null) {
            files.filter(f => f.isFile && f.getName.endsWith(".yaml")).toList
          } else {
            LOGGER.warn(s"Could not list files in directory: ${planDir.getAbsolutePath}")
            List()
          }
        case Some(planDir) =>
          LOGGER.warn(s"Plan directory does not exist or is not a directory: ${planDir.getAbsolutePath}")
          List()
        case None =>
          LOGGER.warn(s"Could not find plan directory: $planDirPath")
          List()
      }
    } else {
      LOGGER.warn(s"Could not determine parent directory from planFilePath: $planFilePath")
      List()
    }

    // Combine and deduplicate based on file name
    val allFiles = planFolderYamlFiles ++ configuredYamlFiles
    allFiles.groupBy(_.getName).values.map(_.head).toList
  }

  /**
   * Try to get the plan directory using multiple approaches (similar to FileUtil.getDirectory)
   * Reuses logic from PlanRepository.getPlanDirectory
   */
  private def getPlanDirectory(dirPath: String): Option[java.io.File] = {
    val directFile = new java.io.File(dirPath)
    val classFile = scala.util.Try(new java.io.File(getClass.getResource(s"/$dirPath").getPath))
    val classLoaderFile = scala.util.Try(new java.io.File(getClass.getClassLoader.getResource(dirPath).getPath))

    if (directFile.isDirectory) {
      Some(directFile)
    } else if (classFile.isSuccess && classFile.get.isDirectory) {
      Some(classFile.get)
    } else if (classLoaderFile.isSuccess && classLoaderFile.get.isDirectory) {
      Some(classLoaderFile.get)
    } else {
      None
    }
  }

  /**
   * Convert a YAML Plan to PlanRunRequest format for API consistency
   * Reuses logic from PlanRepository.convertYamlPlanToPlanRunRequest
   */
  private def convertYamlPlanToPlanRunRequest(plan: Plan, planName: String): io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest = {
    // Create an empty PlanRunRequest with the YAML plan details
    // Tasks and other details will be loaded when the plan is executed
    // The Plan type is from api.model.Plan which is what PlanRunRequest expects
    io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest(
      id = java.util.UUID.randomUUID().toString,
      plan = plan,
      tasks = List(), // Tasks will be loaded from YAML task files when executed
      validation = List(),
      configuration = None
    )
  }

  private def findStepsInPlan(planRequest: io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest, taskName: String)(implicit sparkSession: SparkSession): List[io.github.datacatering.datacaterer.api.model.Step] = {
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
      findTaskByName(taskName).steps.headOption match {
        case Some(step) => List(step)
        case None =>
          throw new IllegalArgumentException(s"No steps found in task '$taskName'")
      }
    }
  }

  /**
   * Find a task by name by parsing all task files in the configured task folder
   * This uses the same logic as PlanParser.parseTasks to ensure consistency
   * Reuses logic from PlanRepository.findTaskByName
   */
  private def findTaskByName(taskName: String)(implicit sparkSession: SparkSession): Task = {
    val taskFolderPath = ConfigParser.foldersConfig.taskFolderPath
    val allTasks = PlanParser.parseTasks(taskFolderPath)

    allTasks.find(_.name == taskName) match {
      case Some(task) => task
      case None =>
        throw new IllegalArgumentException(
          s"Task '$taskName' not found in task folder: $taskFolderPath. " +
          s"Available tasks: ${allTasks.map(_.name).mkString(", ")}"
        )
    }
  }

  /**
   * Generate sample from a task by name
   */
  def generateFromTaskName(taskName: String, sampleSize: Int = 10, fastMode: Boolean = true)(implicit sparkSession: SparkSession): Either[SampleError, Map[String, (Step, SampleResponseWithDataFrame)]] = {
    LOGGER.info(s"Generating samples from task: task=$taskName")

    Try {
      val task = findTaskByName(taskName)
      
      // Generate samples for all steps in the task
      val results = task.steps.map { step =>
        val format = step.options.getOrElse("format", "json")
        val responseWithDf = generateSampleWithDataFrame(step.fields, sampleSize, fastMode, format)
        (step.name, (step, responseWithDf))
      }.toMap

      results
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
          val responseWithDf = generateSampleWithDataFrame(step.fields, sampleSize, fastMode, format)
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
  private def findSpecificStepInTask(planRequest: io.github.datacatering.datacaterer.core.ui.model.PlanRunRequest, taskName: String, stepName: String)(implicit sparkSession: SparkSession): io.github.datacatering.datacaterer.api.model.Step = {
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
      val task = findTaskByName(taskName)
      task.steps.find(_.name == stepName) match {
        case Some(step) => step
        case None =>
          val availableSteps = task.steps.map(_.name).mkString(", ")
          throw new IllegalArgumentException(s"Step '$stepName' not found in task '$taskName'. Available steps: $availableSteps")
      }
    }
  }
}