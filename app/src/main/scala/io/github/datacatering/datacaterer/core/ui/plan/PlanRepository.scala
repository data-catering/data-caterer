package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.Constants.{CONFIG_FLAGS_DELETE_GENERATED_RECORDS, CONFIG_FLAGS_GENERATE_DATA, CONFIG_FLAGS_GENERATE_VALIDATIONS, DATA_CATERER_INTERFACE_UI, DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG, FORMAT, METADATA_SOURCE_NAME}
import io.github.datacatering.datacaterer.api.model.{DataSourceValidation, Task, ValidationConfiguration, YamlUpstreamDataSourceValidation}
import io.github.datacatering.datacaterer.api.{DataCatererConfigurationBuilder, ValidationBuilder}
import io.github.datacatering.datacaterer.core.exception.SaveFileException
import io.github.datacatering.datacaterer.core.model.Constants.{FAILED, FINISHED, PARSED_PLAN, STARTED}
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.plan.{PlanProcessor, YamlPlanRun}
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.mapper.ConfigurationMapper.configurationMapping
import io.github.datacatering.datacaterer.core.ui.model.{Connection, PlanRunExecution, PlanRunRequest, PlanRunRequests}
import io.github.datacatering.datacaterer.core.ui.plan.PlanResponseHandler.{KO, OK, Response}
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
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

  final case class RunPlan(planRunRequest: PlanRunRequest, replyTo: ActorRef[Response]) extends PlanCommand

  final case class RunPlanDeleteData(planRunRequest: PlanRunRequest, replyTo: ActorRef[Response]) extends PlanCommand

  final case class SavePlan(planRunRequest: PlanRunRequest) extends PlanCommand

  final case class GetPlans(replyTo: ActorRef[PlanRunRequests]) extends PlanCommand

  final case class GetPlan(name: String, replyTo: ActorRef[PlanRunRequest]) extends PlanCommand

  final case class GetPlanRunStatus(id: String, replyTo: ActorRef[PlanRunExecution]) extends PlanCommand

  final case class GetPlanRuns(replyTo: ActorRef[PlanRunExecutionDetails]) extends PlanCommand

  final case class GetPlanRunReportPath(id: String, replyTo: ActorRef[String]) extends PlanCommand

  final case class RemovePlan(name: String) extends PlanCommand

  final case class StartupSpark() extends PlanCommand

  private val executionSaveFolder = s"$INSTALL_DIRECTORY/execution"
  private val planSaveFolder = s"$INSTALL_DIRECTORY/plan"
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def apply(): Behavior[PlanCommand] = {
    Behaviors.supervise[PlanCommand] {
      Behaviors.receiveMessage {
        case RunPlan(planRunRequest, replyTo) =>
          runPlan(planRunRequest, replyTo)
          Behaviors.same
        case RunPlanDeleteData(planRunRequest, replyTo) =>
          runPlanWithDeleteFlags(planRunRequest, replyTo)
          Behaviors.same
        case SavePlan(planRunRequest) =>
          savePlan(planRunRequest)
          Behaviors.same
        case GetPlans(replyTo) =>
          replyTo ! getPlans
          Behaviors.same
        case GetPlan(name, replyTo) =>
          replyTo ! getPlan(name)
          Behaviors.same
        case GetPlanRunStatus(id, replyTo) =>
          replyTo ! getPlanRunStatus(id)
          Behaviors.same
        case GetPlanRunReportPath(id, replyTo) =>
          replyTo ! getPlanRunReportPath(id)
          Behaviors.same
        case RemovePlan(name) =>
          removePlan(name)
          Behaviors.same
        case GetPlanRuns(replyTo) =>
          replyTo ! getAllPlanExecutions
          Behaviors.same
        case StartupSpark() =>
          startupSpark()
          Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  private def runPlanWithDeleteFlags(
                                      planRunRequest: PlanRunRequest,
                                      replyTo: ActorRef[Response]
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
    runPlan(updatedPlanRunRequest, replyTo, true)
  }

  private def runPlan(
                       planRunRequest: PlanRunRequest,
                       replyTo: ActorRef[Response],
                       isDeleteRun: Boolean = false
                     ): Unit = {
    LOGGER.debug("Received request to run plan, attempting to parse plan request")
    val planRunExecution = PlanRunExecution(planRunRequest.plan.name, planRunRequest.id, STARTED)
    savePlanRunExecution(planRunRequest, planRunExecution, isDeleteRun)

    val tryPlanAsYaml = Try(getPlanAsYaml(planRunRequest))
    tryPlanAsYaml match {
      case Success(planAsYaml) =>
        updatePlanRunExecution(planRunExecution, PARSED_PLAN)
        val runPlanFuture = Future {
          PlanProcessor.determineAndExecutePlan(Some(planAsYaml), DATA_CATERER_INTERFACE_UI)
        }

        runPlanFuture.onComplete {
          case Failure(planException) =>
            updatePlanRunExecution(planRunExecution, FAILED, Some(planException.getMessage))
            replyTo ! KO(planException.getMessage, planException)
          case Success(results) =>
            updatePlanRunExecution(planRunExecution, FINISHED, None, Some(results))
            replyTo ! OK
        }
      case Failure(parseException) =>
        updatePlanRunExecution(planRunExecution, FAILED, Some(parseException.getMessage))
        replyTo ! KO(parseException.getMessage, parseException)
    }

  }

  private def getPlanAsYaml(parsedRequest: PlanRunRequest): YamlPlanRun = {
    val taskToDataSourceMap = parsedRequest.plan.tasks.map(t => t.name -> t.dataSourceName).toMap
    val dataSourceConnectionInfo = getConnectionDetails(taskToDataSourceMap)
      .map(c => {
        val formatMap = Map(FORMAT -> c.`type`)
        c.name -> (c.options ++ formatMap)
      })
      .toMap

    //find tasks and validation using data source connection
    val updatedValidation = validationWithConnectionInfo(parsedRequest, dataSourceConnectionInfo)
    val updatedTasks = tasksWithConnectionInfo(parsedRequest, taskToDataSourceMap, dataSourceConnectionInfo)
    val updatedConfiguration = parsedRequest.configuration
      .map(c => configurationMapping(c, INSTALL_DIRECTORY))
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
      dataSourceConnectionInfo(options(METADATA_SOURCE_NAME))
    } else {
      Map()
    }
  }

  private def getConnectionDetails(taskToDataSourceMap: Map[String, String]): List[Connection] = {
    taskToDataSourceMap.values.map(name => ConnectionRepository.getConnection(name, false)).toList
  }

  private def savePlanRunExecution(
                                    planRunRequest: PlanRunRequest,
                                    planRunExecution: PlanRunExecution,
                                    isDeleteRun: Boolean = false
                                  ): Unit = {
    LOGGER.debug(s"Saving plan run execution details, plan-name=${planRunRequest.plan.name}, plan-run-id=${planRunRequest.id}")
    if (!isDeleteRun) savePlan(planRunRequest)
    val filePath = s"$executionSaveFolder/${planRunExecution.id}.csv"
    try {
      val basePath = Path.of(executionSaveFolder).toFile
      if (!basePath.exists()) basePath.mkdirs()
      val executionFile = Path.of(filePath)
      Files.writeString(
        executionFile,
        planRunExecution.toString,
        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE
      )
    } catch {
      case ex: Exception => throw SaveFileException(filePath, ex)
    }
  }

  private def savePlan(planRunRequest: PlanRunRequest): Unit = {
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

  private def getPlans: PlanRunRequests = {
    LOGGER.debug("Getting all plans")
    val planFolder = Path.of(planSaveFolder)
    if (!planFolder.toFile.exists()) planFolder.toFile.mkdirs()
    val plans = Files.list(planFolder)
      .iterator()
      .asScala
      .map(file => {
        val fileContent = Files.readString(file)
        val tryParse = Try(ObjectMapperUtil.jsonObjectMapper.readValue(fileContent, classOf[PlanRunRequest]))
        tryParse match {
          case Failure(exception) =>
            LOGGER.error(s"Failed to parse plan file, file-name=$file, exception=${exception.getMessage}")
            None
          case Success(value) => Some(value)
        }
      })
      .filter(_.isDefined)
      .map(_.get)
      .toList
    PlanRunRequests(plans)
  }

  private def getPlan(name: String): PlanRunRequest = {
    LOGGER.debug(s"Getting plan details, plan-name=$name")
    val planFile = Path.of(s"$planSaveFolder/$name.json")
    val fileContent = Files.readString(planFile)
    ObjectMapperUtil.jsonObjectMapper.readValue(fileContent, classOf[PlanRunRequest])
  }

  private def updatePlanRunExecution(
                                      planRunExecution: PlanRunExecution,
                                      status: String,
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

  private def getPlanRunStatus(id: String): PlanRunExecution = {
    LOGGER.debug(s"Getting current plan status, plan-run-id=$id")
    val executionFile = Path.of(s"$executionSaveFolder/$id.csv")
    val latestUpdate = Files.readAllLines(executionFile).asScala.last
    PlanRunExecution.fromString(latestUpdate)
  }

  private def getPlanRunReportPath(id: String): String = {
    val planRunExecution = getPlanRunStatus(id)
    LOGGER.debug(s"Report link pathway, id=$id, path=${planRunExecution.reportLink.getOrElse("")}")
    planRunExecution.reportLink.getOrElse(s"/tmp/report/blah")
  }

  private def getAllPlanExecutions: PlanRunExecutionDetails = {
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

  private def removePlan(name: String): Unit = {
    LOGGER.debug(s"Removing plan, plan-name=$name")
    val planFile = Path.of(s"$planSaveFolder/$name.json").toFile
    if (planFile.exists()) {
      planFile.delete()
    } else {
      LOGGER.warn(s"Plan file does not exist, unable to delete, plan-name=$name")
    }
  }

  private def startupSpark(): Response = {
    LOGGER.debug("Starting up Spark")
    try {
      implicit val sparkSession = new SparkProvider(DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG).getSparkSession
      //run some dummy query
      sparkSession.sql("SELECT 1").collect()
      OK
    } catch {
      case ex: Throwable => KO("Failed to start up Spark", ex)
    }
  }
}
