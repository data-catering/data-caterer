package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.Constants.{CONFIG_FLAGS_DELETE_GENERATED_RECORDS, CONFIG_FLAGS_GENERATE_DATA, CONFIG_FLAGS_GENERATE_VALIDATIONS, DATA_CATERER_INTERFACE_UI, DRIVER, FORMAT, JDBC, METADATA_SOURCE_NAME, MYSQL, MYSQL_DRIVER, POSTGRES, POSTGRES_DRIVER}
import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, Field, Plan, Task, ValidationConfiguration, YamlUpstreamDataSourceValidation}
import io.github.datacatering.datacaterer.api.{DataCatererConfigurationBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.exception.{GetPlanRunStatusException, SaveFileException}
import io.github.datacatering.datacaterer.core.model.Constants.{DATA_CATERER_UI, FAILED, FINISHED, PARSED_PLAN, STARTED}
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.plan.{PlanProcessor, YamlPlanRun}
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.mapper.ConfigurationMapper.configurationMapping
import io.github.datacatering.datacaterer.core.ui.model.{ConfigurationRequest, Connection, EnhancedPlanRunRequest, PlanRunExecution, PlanRunRequest, PlanRunRequests, SampleResponse, SchemaSampleRequest}
import io.github.datacatering.datacaterer.core.ui.plan.PlanResponseHandler.{KO, OK, Response}
import io.github.datacatering.datacaterer.core.ui.resource.SparkSessionManager
import io.github.datacatering.datacaterer.core.ui.sample.FastSampleGenerator
import io.github.datacatering.datacaterer.core.ui.service.{ConnectionService, PlanLoaderService, TaskLoaderService}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Seconds}

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


object PlanRepository {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /*
  Data flow of plan run:
  - Kick off plan run
  - Running process
  - Success -> show generation/validation results
  - Failure -> show step it failed at and reason
   */
  sealed trait History

  final case class PlanRunExecutionDetails(planExecutionByPlan: List[GroupedPlanRunsByName]) extends History

  final case class GroupedPlanRunsByName(name: String, executions: List[ExecutionsById])

  final case class ExecutionsById(id: String, runs: List[PlanRunExecution])


  sealed trait PlanCommand

  // Response messages
  sealed trait PlanResponse
  final case class PlanSaved(name: String) extends PlanResponse
  final case class PlanRemoved(name: String, success: Boolean) extends PlanResponse

  final case class RunPlan(planRunRequest: PlanRunRequest, replyTo: ActorRef[Response]) extends PlanCommand

  final case class RunPlanDeleteData(planRunRequest: PlanRunRequest, replyTo: ActorRef[Response]) extends PlanCommand

  final case class RunEnhancedPlan(request: EnhancedPlanRunRequest, replyTo: ActorRef[Response]) extends PlanCommand

  final case class SavePlan(planRunRequest: PlanRunRequest, replyTo: Option[ActorRef[PlanResponse]] = None) extends PlanCommand

  final case class GetPlans(replyTo: ActorRef[PlanRunRequests]) extends PlanCommand

  final case class GetPlan(name: String, replyTo: ActorRef[PlanRunRequest]) extends PlanCommand

  final case class GetPlanRunStatus(id: String, replyTo: ActorRef[PlanRunExecution]) extends PlanCommand

  final case class GetPlanRuns(replyTo: ActorRef[PlanRunExecutionDetails]) extends PlanCommand

  final case class GetPlanRunReportPath(id: String, replyTo: ActorRef[String]) extends PlanCommand

  final case class RemovePlan(name: String, replyTo: Option[ActorRef[PlanResponse]] = None) extends PlanCommand

  final case class StartupSpark() extends PlanCommand

  final case class GenerateFromSchema(request: SchemaSampleRequest, replyTo: ActorRef[SampleResponse]) extends PlanCommand

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def apply(baseDirectory: String = INSTALL_DIRECTORY): Behavior[PlanCommand] = {
    val executionSaveFolder = s"$baseDirectory/execution"
    val planSaveFolder = s"$baseDirectory/plan"
    Behaviors.supervise[PlanCommand] {
      Behaviors.receiveMessage {
        case RunPlan(planRunRequest, replyTo) =>
          runPlan(planRunRequest, replyTo, baseDirectory, executionSaveFolder, planSaveFolder)
          Behaviors.same
        case RunPlanDeleteData(planRunRequest, replyTo) =>
          runPlanWithDeleteFlags(planRunRequest, replyTo, baseDirectory, executionSaveFolder, planSaveFolder)
          Behaviors.same
        case RunEnhancedPlan(request, replyTo) =>
          runEnhancedPlan(request, replyTo, executionSaveFolder, planSaveFolder)
          Behaviors.same
        case SavePlan(planRunRequest, replyTo) =>
          savePlan(planRunRequest, planSaveFolder)
          replyTo.foreach(_ ! PlanSaved(planRunRequest.plan.name))
          Behaviors.same
        case GetPlans(replyTo) =>
          replyTo ! getPlans(planSaveFolder)
          Behaviors.same
        case GetPlan(name, replyTo) =>
          replyTo ! getPlan(name, planSaveFolder)
          Behaviors.same
        case GetPlanRunStatus(id, replyTo) =>
          replyTo ! getPlanRunStatus(id, executionSaveFolder)
          Behaviors.same
        case GetPlanRunReportPath(id, replyTo) =>
          replyTo ! getPlanRunReportPath(id, executionSaveFolder)
          Behaviors.same
        case RemovePlan(name, replyTo) =>
          val success = removePlan(name, planSaveFolder)
          replyTo.foreach(_ ! PlanRemoved(name, success))
          Behaviors.same
        case GetPlanRuns(replyTo) =>
          replyTo ! getAllPlanExecutions(executionSaveFolder)
          Behaviors.same
        case StartupSpark() =>
          startupSpark()
          Behaviors.same
        case GenerateFromSchema(request, replyTo) =>
          replyTo ! generateFromSchema(request)
          Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  private def runPlanWithDeleteFlags(
                                      planRunRequest: PlanRunRequest,
                                      replyTo: ActorRef[Response],
                                      baseDirectory: String,
                                      executionSaveFolder: String,
                                      planSaveFolder: String
                                    ): Unit = {
    // alter plan run request to enable delete, disable data generation and validation
    val optUpdatedConfig = planRunRequest.configuration.map(config => {
      val updatedFlagConfig = config.flag ++
        Map(
          CONFIG_FLAGS_DELETE_GENERATED_RECORDS -> "true",
          CONFIG_FLAGS_GENERATE_DATA -> "false",
          CONFIG_FLAGS_GENERATE_VALIDATIONS -> "false",
        )
      config.copy(flag = updatedFlagConfig)
    })
    val updatedPlanRunRequest = planRunRequest.copy(configuration = optUpdatedConfig)
    runPlan(updatedPlanRunRequest, replyTo, baseDirectory, executionSaveFolder, planSaveFolder, true, false)
  }

  private def runPlan(
                       planRunRequest: PlanRunRequest,
                       replyTo: ActorRef[Response],
                       baseDirectory: String,
                       executionSaveFolder: String,
                       planSaveFolder: String,
                       isDeleteRun: Boolean = false,
                       savePlan: Boolean = true
                     ): Unit = {
    LOGGER.debug("Received request to run plan, attempting to parse plan request")
    val planRunExecution = PlanRunExecution(planRunRequest.plan.name, planRunRequest.id, STARTED)
    savePlanRunExecution(planRunRequest, planRunExecution, executionSaveFolder, planSaveFolder, isDeleteRun, savePlan)

    val tryPlanAsYaml = Try(getPlanAsYaml(planRunRequest, baseDirectory))
    tryPlanAsYaml match {
      case Success(planAsYaml) =>
        updatePlanRunExecution(planRunExecution, PARSED_PLAN, executionSaveFolder)
        val runPlanFuture = Future {
          PlanProcessor.determineAndExecutePlan(Some(planAsYaml), DATA_CATERER_INTERFACE_UI)
        }

        runPlanFuture.onComplete {
          case Failure(planException) =>
            updatePlanRunExecution(planRunExecution, FAILED, executionSaveFolder, Some(planException.getMessage))
            replyTo ! KO(planException.getMessage, planException)
          case Success(results) =>
            updatePlanRunExecution(planRunExecution, FINISHED, executionSaveFolder, None, Some(results))
            replyTo ! OK
        }
      case Failure(parseException) =>
        updatePlanRunExecution(planRunExecution, FAILED, executionSaveFolder, Some(parseException.getMessage))
        replyTo ! KO(parseException.getMessage, parseException)
    }

  }

  private def getPlanAsYaml(parsedRequest: PlanRunRequest, baseDirectory: String): YamlPlanRun = {
    // Only process tasks that are actually defined in the UI request
    // Tasks referenced in the plan but not provided here will be loaded from YAML files
    val providedTaskNames = parsedRequest.tasks.map(_.name).toSet
    val taskToDataSourceMap = parsedRequest.plan.tasks
      .filter(t => providedTaskNames.contains(t.name))
      .map(t => t.name -> t.dataSourceName)
      .toMap

    val dataSourceConnectionInfo = getConnectionDetails(taskToDataSourceMap)
      .map(c => {
        val additionalConfig = c.`type` match {
          case POSTGRES => Map(FORMAT -> JDBC, DRIVER -> POSTGRES_DRIVER)
          case MYSQL => Map(FORMAT -> JDBC, DRIVER -> MYSQL_DRIVER)
          case format => Map(FORMAT -> format)
        }
        c.name -> (c.options ++ additionalConfig)
      })
      .toMap

    //find tasks and validation using data source connection
    val updatedValidation = validationWithConnectionInfo(parsedRequest, dataSourceConnectionInfo)
    val updatedTasks = tasksWithConnectionInfo(parsedRequest, taskToDataSourceMap, dataSourceConnectionInfo)
    val updatedConfiguration = parsedRequest.configuration
      .map(c => configurationMapping(c, baseDirectory))
      .getOrElse(DataCatererConfigurationBuilder())
    val dataCatererConfiguration = updatedConfiguration.build.copy(connectionConfigByName = dataSourceConnectionInfo)
    new YamlPlanRun(parsedRequest.plan, updatedTasks, Some(updatedValidation), dataCatererConfiguration)
  }

  private def validationWithConnectionInfo(
                                            parsedRequest: PlanRunRequest,
                                            dataSourceConnectionInfo: Map[String, Map[String, String]]
                                          ): List[ValidationConfiguration] = {
    parsedRequest.validation.map(yamlV => {
      val updatedDataSources = yamlV.dataSources.map(ds => {
        val dataSourceName = ds._1
        val connectionInfo = dataSourceConnectionInfo(dataSourceName)
        val updatedValidationOptions = ds._2.map(yamlDs => {
          val metadataOpts = getMetadataSourceInfo(dataSourceConnectionInfo, yamlDs.options)
          val allOpts = yamlDs.options ++ connectionInfo ++ metadataOpts
          val listValidationBuilders = yamlDs.validations.map {
            case yamlUpstreamDataSourceValidation: YamlUpstreamDataSourceValidation =>
              val validationWithDataSourceName = yamlUpstreamDataSourceValidation.copy(upstreamDataSource = ds._1)
              PlanParser.getYamlUpstreamValidationAsValidationWithConnection(dataSourceConnectionInfo, validationWithDataSourceName)
            case v => ValidationBuilder(v)
          }
          DataSourceValidation(allOpts, yamlDs.waitCondition, listValidationBuilders)
        })
        dataSourceName -> updatedValidationOptions
      })

      ValidationConfiguration(yamlV.name, yamlV.description, updatedDataSources)
    })
  }

  private def tasksWithConnectionInfo(
                                       parsedRequest: PlanRunRequest,
                                       taskToDataSourceMap: Map[String, String],
                                       dataSourceConnectionInfo: Map[String, Map[String, String]]
                                     ): List[Task] = {
    val updatedTasks = parsedRequest.tasks.map(s => {
      val taskName = s.name
      if (!taskToDataSourceMap.contains(taskName)) {
        throw new IllegalArgumentException(s"Task name not found in data source map, task-name=$taskName")
      }
      val dataSourceName = taskToDataSourceMap(taskName)
      val connectionInfo = dataSourceConnectionInfo(dataSourceName)
      val metadataOpts = getMetadataSourceInfo(dataSourceConnectionInfo, s.options)
      val updatedStep = s.copy(options = s.options ++ connectionInfo ++ metadataOpts)
      Task(taskName, List(updatedStep))
    })
    updatedTasks
  }

  private def getMetadataSourceInfo(dataSourceConnectionInfo: Map[String, Map[String, String]], options: Map[String, String]): Map[String, String] = {
    if (options.contains(METADATA_SOURCE_NAME)) {
      ConnectionService.getMetadataSourceInfo(options(METADATA_SOURCE_NAME), dataSourceConnectionInfo)
    } else {
      Map()
    }
  }

  private def getConnectionDetails(taskToDataSourceMap: Map[String, String]): List[Connection] = {
    ConnectionService.getConnections(taskToDataSourceMap.values.toList, masking = false)
  }

  private def savePlanRunExecution(
                                    planRunRequest: PlanRunRequest,
                                    planRunExecution: PlanRunExecution,
                                    executionSaveFolder: String,
                                    planSaveFolder: String,
                                    isDeleteRun: Boolean = false,
                                    isSavePlan: Boolean = true
                                  ): Unit = {
    LOGGER.debug(s"Saving plan run execution details, plan-name=${planRunRequest.plan.name}, plan-run-id=${planRunRequest.id}")
    if (!isDeleteRun && isSavePlan) savePlan(planRunRequest, planSaveFolder)
    val filePath = s"$executionSaveFolder/${planRunExecution.id}.csv"
    try {
      val basePath = Path.of(executionSaveFolder).toFile
      if (!basePath.exists()) basePath.mkdirs()
      val executionFile = Path.of(filePath)
      Files.writeString(
        executionFile,
        planRunExecution.toString,
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
      )
    } catch {
      case ex: Exception => throw SaveFileException(filePath, ex)
    }
  }

  private def savePlan(planRunRequest: PlanRunRequest, planSaveFolder: String): Unit = {
    LOGGER.debug(s"Saving plan details, plan-name=${planRunRequest.plan.name}")
    val filePath = s"$planSaveFolder/${planRunRequest.plan.name}.json"
    try {
      val basePath = Path.of(planSaveFolder).toFile
      if (!basePath.exists()) basePath.mkdirs()
      val planFile = Path.of(filePath)
      val fileContent = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(planRunRequest)
      Files.writeString(
        planFile,
        fileContent,
        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
      )
    } catch {
      case ex: Exception => throw SaveFileException(filePath, ex)
    }
  }

  private def getPlans(planSaveFolder: String): PlanRunRequests = {
    LOGGER.debug(s"Getting all plans from folder: $planSaveFolder")
    implicit val sparkSession: SparkSession = createSparkSession()
    val allPlans = PlanLoaderService.getAllPlans(Some(planSaveFolder), includeConfiguredPath = false)
    PlanRunRequests(allPlans)
  }

  private def getPlan(name: String, planSaveFolder: String): PlanRunRequest = {
    LOGGER.debug(s"Getting plan details, plan-name=$name")
    val planFile = Path.of(s"$planSaveFolder/$name.json")
    val fileContent = Files.readString(planFile)
    ObjectMapperUtil.jsonObjectMapper.readValue(fileContent, classOf[PlanRunRequest])
  }

  private def updatePlanRunExecution(
                                      planRunExecution: PlanRunExecution,
                                      status: String,
                                      executionSaveFolder: String,
                                      failedReason: Option[String] = None,
                                      results: Option[PlanRunResults] = None
                                    ): Unit = {
    LOGGER.debug(s"Update plan execution, plan-name=${planRunExecution.name}, plan-run-id=${planRunExecution.id}, status=$status")
    val executionFile = Path.of(s"$executionSaveFolder/${planRunExecution.id}.csv")
    val cleanFailReason = failedReason.map(s => s.replaceAll("\n", " "))
    val generationSummary = results.map(res => res.generationResults.map(_.summarise)).getOrElse(List())
    val validationSummary = results.map(res => res.validationResults.map(_.summarise)).getOrElse(List())
    val reportLink = results.map(_.optReportPath.getOrElse(""))
    val updatedTs = DateTime.now()
    val timeTaken = Seconds.secondsBetween(planRunExecution.createdTs, updatedTs).getSeconds.toString

    val updatedPlanRun = planRunExecution.copy(status = status, updatedTs = updatedTs,
      failedReason = cleanFailReason, generationSummary = generationSummary, validationSummary = validationSummary,
      reportLink = reportLink, timeTaken = Some(timeTaken))
    Files.writeString(executionFile, updatedPlanRun.toString, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
  }

  private def getPlanRunStatus(id: String, executionSaveFolder: String): PlanRunExecution = {
    LOGGER.debug(s"Getting current plan status, plan-run-id=$id")
    try {
      val executionFile = Path.of(s"$executionSaveFolder/$id.csv")
      val latestUpdate = Files.readAllLines(executionFile).asScala.last
      PlanRunExecution.fromString(latestUpdate)
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to get plan run status, plan-run-id=$id, exception=${ex.getMessage}")
        throw GetPlanRunStatusException(id, ex)
    }
  }

  private def getPlanRunReportPath(id: String, executionSaveFolder: String): String = {
    val planRunExecution = getPlanRunStatus(id, executionSaveFolder)
    LOGGER.debug(s"Report link pathway, id=$id, path=${planRunExecution.reportLink.getOrElse("")}")
    planRunExecution.reportLink.getOrElse(s"/tmp/report/$id")
  }

  private def getAllPlanExecutions(executionSaveFolder: String): PlanRunExecutionDetails = {
    LOGGER.debug("Getting all plan executions")
    val executionPath = Path.of(executionSaveFolder)
    if (!executionPath.toFile.exists()) executionPath.toFile.mkdirs()
    val allPlanRunExecutions = Files.list(executionPath)
      .iterator()
      .asScala
      .flatMap(execFile => {
        val lines = Files.readAllLines(execFile).asScala
        lines.map(line => {
            val tryParse = Try(PlanRunExecution.fromString(line))
            if (tryParse.isFailure) LOGGER.error(s"Failed to parse execution details for file, file-name=$execFile")
            tryParse
          })
          .filter(_.isSuccess)
          .map(_.get)
      }).toList
    val groupedExecutions = allPlanRunExecutions.groupBy(_.name)
      .map(grp => {
        val groupById = grp._2.groupBy(_.id)
          .map(x => ExecutionsById(x._1, x._2))
          .toList
        GroupedPlanRunsByName(grp._1, groupById)
      })
      .toList
      .sortBy(_.name)

    PlanRunExecutionDetails(groupedExecutions)
  }

  private def removePlan(name: String, planSaveFolder: String): Boolean = {
    LOGGER.debug(s"Removing plan, plan-name=$name")
    val planFile = Path.of(s"$planSaveFolder/$name.json").toFile
    if (planFile.exists()) {
      planFile.delete()
    } else {
      LOGGER.warn(s"Plan file does not exist, unable to delete, plan-name=$name")
      false
    }
  }

  private def createSparkSession(): SparkSession = {
    SparkSessionManager.getOrCreate()
  }

  private def generateFromSchema(request: SchemaSampleRequest): SampleResponse = {
    LOGGER.debug(s"Generating sample from inline fields: ${request.fields.size} fields")
    try {
      implicit val sparkSession: SparkSession = createSparkSession()
      FastSampleGenerator.generateFromSchemaWithDataFrame(request).response
    } catch {
      case ex: Throwable =>
        LOGGER.error(s"Error generating sample from schema", ex)
        SampleResponse(
          success = false,
          executionId = java.util.UUID.randomUUID().toString.split("-").head,
          error = Some(io.github.datacatering.datacaterer.core.ui.model.SampleError("INTERNAL_ERROR", ex.getMessage))
        )
    }
  }

  private def startupSpark(): Response = {
    LOGGER.debug("Starting up Spark")
    setUiRunning
    try {
      implicit val sparkSession: SparkSession = createSparkSession()
      //run some dummy query
      sparkSession.sql("SELECT 1").collect()

      //warm up data generation pipeline with a simple sample request
      LOGGER.debug("Warming up data generation pipeline")
      val warmupRequest = SchemaSampleRequest(
        fields = List(
          Field(
            name = "warmup_id",
            `type` = Some("long"),
            options = Map("min" -> 1L, "max" -> 10L)
          )
        ),
        sampleSize = Some(1),
        fastMode = true
      )
      val warmupResult = FastSampleGenerator.generateFromSchemaWithDataFrame(warmupRequest).response
      if (warmupResult.success) {
        LOGGER.debug("Data generation pipeline warmed up successfully")
      } else {
        LOGGER.warn(s"Warmup failed: ${warmupResult.error}")
      }

      OK
    } catch {
      case ex: Throwable => KO("Failed to start up Spark", ex)
    }
  }

  private def setUiRunning: Unit = {
    System.setProperty(DATA_CATERER_UI, "data_caterer_ui_is_cool")
  }

  /**
   * Enhanced plan execution with support for task/step filtering and multiple source types
   */
  private def runEnhancedPlan(request: EnhancedPlanRunRequest, replyTo: ActorRef[Response], executionSaveFolder: String, planSaveFolder: String): Unit = {
    LOGGER.info(s"Running enhanced plan, plan-name=${request.planName}, source=${request.sourceType}, " +
      s"mode=${request.mode}, task-filter=${request.taskFilter}, step-filter=${request.stepFilter}")

    val baseDirectory = planSaveFolder.stripSuffix("/plan")
    val tryRunPlan = Try {
      if (request.sourceType == "yaml" || (request.sourceType == "auto" && !planExistsInJson(request.planName))) {
        // For YAML plans, use the enhanced parsing pipeline that supports filtering
        LOGGER.debug(s"Running YAML plan, plan-name=${request.planName}")
        runEnhancedYamlPlan(request, replyTo, executionSaveFolder, planSaveFolder)
      } else {
        // For JSON plans, use the existing filtering approach
        LOGGER.debug(s"Running JSON plan, plan-name=${request.planName}")
        val planRunRequest = loadPlanBySourceType(request)
        val filteredPlanRunRequest = applyTaskAndStepFiltering(planRunRequest, request)
        val finalPlanRunRequest = applyExecutionMode(filteredPlanRunRequest, request)

        if (request.isDeleteMode) {
          runPlanWithDeleteFlags(finalPlanRunRequest, replyTo, baseDirectory, executionSaveFolder, planSaveFolder)
        } else {
          runPlan(finalPlanRunRequest, replyTo, baseDirectory, executionSaveFolder, planSaveFolder, false, false)
        }
      }
    }

    tryRunPlan match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to run enhanced plan, plan-name=${request.planName}", exception)
        replyTo ! KO(exception.getMessage, exception)
      case Success(_) => // Future handling done in runPlan/runPlanWithDeleteFlags
    }
  }

  /**
   * Load plan based on source type (auto, json, yaml, file)
   */
  private def loadPlanBySourceType(request: EnhancedPlanRunRequest): PlanRunRequest = {
    implicit val sparkSession: SparkSession = createSparkSession()

    request.sourceType match {
      case "json" =>
        // Load from JSON plan folder
        LOGGER.debug(s"Loading JSON plan, plan-name=${request.planName}")
        PlanLoaderService.loadJsonPlan(request.planName).getOrElse(
          throw new java.io.FileNotFoundException(s"JSON plan not found: ${request.planName}")
        )

      case "yaml" =>
        // Load from YAML plan files
        LOGGER.debug(s"Loading YAML plan, plan-name=${request.planName}")
        PlanLoaderService.loadYamlPlanByName(request.planName).getOrElse(
          throw new java.io.FileNotFoundException(s"YAML plan not found: ${request.planName}")
        )

      case "file" if request.fileName.isDefined =>
        // Load from specific file name
        LOGGER.debug(s"Loading YAML plan by file name, file-name=${request.fileName.get}")
        PlanLoaderService.loadYamlPlanByFileName(request.fileName.get).getOrElse(
          throw new java.io.FileNotFoundException(s"YAML plan file not found: ${request.fileName.get}")
        )

      case "auto" =>
        // Use PlanLoaderService which tries JSON first, then YAML
        PlanLoaderService.loadPlan(request.planName, None)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported source type: ${request.sourceType}")
    }
  }

  /**
   * Find a task by name using cached task folder parsing
   * This uses TaskLoaderService for efficient task lookups
   */
  private def findTaskByName(taskName: String)(implicit sparkSession: SparkSession): Task = {
    TaskLoaderService.findTaskByName(taskName, None)
  }

  /**
   * Apply task and step filtering to plan
   * When loading from YAML, tasks need to be parsed from task files to access their steps
   */
  private def applyTaskAndStepFiltering(planRunRequest: PlanRunRequest, request: EnhancedPlanRunRequest): PlanRunRequest = {
    if (!request.hasTaskFilter && !request.hasStepFilter) {
      return planRunRequest
    }

    validateFilterRequirements(request)

    val filteredPlanTasks = filterPlanTasksByName(planRunRequest, request.taskFilter)
    val filteredSteps = filterStepsByTaskAndStep(planRunRequest, request.taskFilter, request.stepFilter)

    planRunRequest.copy(
      tasks = filteredSteps,
      plan = planRunRequest.plan.copy(tasks = filteredPlanTasks)
    )
  }

  /**
   * Validate that filter requirements are met
   */
  private def validateFilterRequirements(request: EnhancedPlanRunRequest): Unit = {
    if (request.hasStepFilter && !request.hasTaskFilter) {
      throw new IllegalArgumentException("Step filter requires task filter to be specified")
    }
  }

  /**
   * Filter plan tasks by task name
   */
  private def filterPlanTasksByName(planRunRequest: PlanRunRequest, taskFilter: Option[String]): List[io.github.datacatering.datacaterer.api.model.TaskSummary] = {
    taskFilter match {
      case Some(taskName) =>
        val matchingTasks = planRunRequest.plan.tasks.filter(_.name == taskName)
        if (matchingTasks.isEmpty) {
          throw new IllegalArgumentException(
            s"Task not found in plan: $taskName, plan-tasks=${planRunRequest.plan.tasks.map(_.name).mkString(", ")}"
          )
        }
        matchingTasks
      case None =>
        planRunRequest.plan.tasks
    }
  }

  /**
   * Filter steps by task name and optionally by step name
   * For YAML plans, validates step exists by loading task files
   */
  private def filterStepsByTaskAndStep(
    planRunRequest: PlanRunRequest,
    taskFilter: Option[String],
    stepFilter: Option[String]
  ): List[io.github.datacatering.datacaterer.api.model.Step] = {
    (taskFilter, stepFilter) match {
      case (Some(taskName), Some(stepName)) =>
        validateStepExistsInTask(taskName, stepName)
        planRunRequest.tasks.filter(_.name == taskName)

      case (Some(taskName), None) =>
        planRunRequest.tasks.filter(_.name == taskName)

      case _ =>
        planRunRequest.tasks
    }
  }

  /**
   * Validate that a step exists within a task by loading the task file
   */
  private def validateStepExistsInTask(taskName: String, stepName: String): Unit = {
    implicit val sparkSession: SparkSession = createSparkSession()
    val parsedTask = findTaskByName(taskName)
    val matchingSteps = parsedTask.steps.filter(_.name == stepName)

    if (matchingSteps.isEmpty) {
      throw new IllegalArgumentException(
        s"Step '$stepName' not found in task '$taskName'. Available steps: ${parsedTask.steps.map(_.name).mkString(", ")}"
      )
    }
  }

  /**
   * Apply execution mode (generate, delete, validate) to plan configuration
   */
  private def applyExecutionMode(planRunRequest: PlanRunRequest, request: EnhancedPlanRunRequest): PlanRunRequest = {
    request.mode.toLowerCase match {
      case "delete" =>
        // Delete mode is handled by runPlanWithDeleteFlags, so just return the plan
        planRunRequest

      case "validate" =>
        // Enable validations, disable data generation
        val updatedConfig = planRunRequest.configuration.map(config => {
          val updatedFlagConfig = config.flag ++
            Map(
              CONFIG_FLAGS_GENERATE_DATA -> "false",
              CONFIG_FLAGS_GENERATE_VALIDATIONS -> "true",
            )
          config.copy(flag = updatedFlagConfig)
        }).orElse(Some(ConfigurationRequest(
          flag = Map(
            CONFIG_FLAGS_GENERATE_DATA -> "false",
            CONFIG_FLAGS_GENERATE_VALIDATIONS -> "true",
          )
        )))
        planRunRequest.copy(configuration = updatedConfig)

      case "generate" =>
        // Default mode, just return the plan
        planRunRequest

      case _ =>
        throw new IllegalArgumentException(s"Unsupported execution mode: ${request.mode}")
    }
  }

  /**
   * Check if a plan exists in the JSON plan storage
   */
  private def planExistsInJson(planName: String): Boolean = {
    PlanLoaderService.planExistsInJson(planName)
  }

  /**
   * Run YAML plan with enhanced filtering support by using PlanProcessor's filtering-aware parsing
   */
  private def runEnhancedYamlPlan(request: EnhancedPlanRunRequest, replyTo: ActorRef[Response], executionSaveFolder: String, planSaveFolder: String): Unit = {
    val tryRunPlan = Try {
      import io.github.datacatering.datacaterer.core.plan.PlanProcessor
      implicit val sparkSession: SparkSession = createSparkSession()

      val runId = java.util.UUID.randomUUID().toString
      val planRunExecution = PlanRunExecution(request.planName, runId, STARTED)
      savePlanRunExecution(PlanRunRequest(runId, Plan(name = request.planName)), planRunExecution, executionSaveFolder, planSaveFolder, request.isDeleteMode, false)

      val runPlanFuture = Future {
        val dataCatererConfiguration = if (request.isDeleteMode) {
          ConfigParser.toDataCatererConfiguration.copy(
            flagsConfig = ConfigParser.toDataCatererConfiguration.flagsConfig.copy(
              enableDeleteGeneratedRecords = true,
              enableGenerateData = false,
              enableValidation = false
            )
          )
        } else if (request.mode == "validate") {
          ConfigParser.toDataCatererConfiguration.copy(
            flagsConfig = ConfigParser.toDataCatererConfiguration.flagsConfig.copy(
              enableDeleteGeneratedRecords = false,
              enableGenerateData = false,
              enableValidation = true
            )
          )
        } else {
          ConfigParser.toDataCatererConfiguration
        }

        // Find the correct YAML plan file by name, similar to PlanProcessor logic
        val yamlPlanFilePath = PlanParser.findYamlPlanFile(dataCatererConfiguration.foldersConfig.planFilePath, request.planName)
        if (yamlPlanFilePath.isEmpty) {
          throw new RuntimeException(s"Failed to find YAML plan, folder-path=${dataCatererConfiguration.foldersConfig.planFilePath}, plan-name=${request.planName}")
        }
        LOGGER.debug(s"Found YAML plan file path, plan-file-path=$yamlPlanFilePath")
        val configForParsing = yamlPlanFilePath.map { planPath =>
          dataCatererConfiguration.copy(
            foldersConfig = dataCatererConfiguration.foldersConfig.copy(planFilePath = planPath)
          )
        }.getOrElse(dataCatererConfiguration)

        // Use the enhanced parsing that supports filtering
        val (planRun, _) = PlanProcessor.parsePlanWithFiltering(
          configForParsing,
          None, // No pre-existing plan run
          io.github.datacatering.datacaterer.api.model.Constants.DATA_CATERER_INTERFACE_YAML,
          request.taskFilter,
          request.stepFilter
        )

        PlanProcessor.executePlanWithConfig(dataCatererConfiguration, Some(planRun), io.github.datacatering.datacaterer.api.model.Constants.DATA_CATERER_INTERFACE_YAML)
      }

      runPlanFuture.onComplete {
        case Failure(planException) =>
          updatePlanRunExecution(planRunExecution, FAILED, executionSaveFolder, Some(planException.getMessage))
          replyTo ! KO(planException.getMessage, planException)
        case Success(results) =>
          updatePlanRunExecution(planRunExecution, FINISHED, executionSaveFolder, None, Some(results))
          replyTo ! OK
      }
    }

    tryRunPlan match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to run enhanced YAML plan, plan-name=${request.planName}", exception)
        replyTo ! KO(exception.getMessage, exception)
      case Success(_) => // Future handling done above
    }
  }
}
