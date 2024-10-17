package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.Constants.{CONFIG_FLAGS_DELETE_GENERATED_RECORDS, CONFIG_FLAGS_GENERATE_DATA, CONFIG_FLAGS_GENERATE_VALIDATIONS, DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG}
import io.github.datacatering.datacaterer.core.exception.SaveFileException
import io.github.datacatering.datacaterer.core.model.Constants.{CONNECTION_GROUP_METADATA_SOURCE, CONNECTION_GROUP_TYPE, CONNECTION_TYPE, FAILED, FINISHED, PARSED_PLAN, STARTED}
import io.github.datacatering.datacaterer.core.model.PlanRunResults
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.mapper.UiMapper
import io.github.datacatering.datacaterer.core.ui.model.{DataSourceRequest, JsonSupport, PlanRunExecution, PlanRunRequest, PlanRunRequests}
import io.github.datacatering.datacaterer.core.ui.plan.PlanResponseHandler.{KO, OK, Response}
import io.github.datacatering.datacaterer.core.util.SparkProvider
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.joda.time.{DateTime, Seconds}
import spray.json._

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


object PlanRepository extends JsonSupport {

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

  private def runPlanWithDeleteFlags(planRunRequest: PlanRunRequest, replyTo: ActorRef[Response]): Unit = {
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

  private def runPlan(planRunRequest: PlanRunRequest, replyTo: ActorRef[Response], isDeleteRun: Boolean = false): Unit = {
    // get connection info
    val dataSourcesWithConnectionInfo = addConnectionDetails(planRunRequest)
    val planRunWithConnectionInfo = planRunRequest.copy(dataSources = dataSourcesWithConnectionInfo)
    val planRunExecution = PlanRunExecution(planRunRequest.name, planRunRequest.id, STARTED)
    // save to file if not a delete data run
    savePlanRunExecution(planRunRequest, planRunExecution, isDeleteRun)

    val tryPlanRun = Try(UiMapper.mapToPlanRun(planRunWithConnectionInfo, INSTALL_DIRECTORY))
    tryPlanRun match {
      case Failure(mapException) =>
        updatePlanRunExecution(planRunExecution, FAILED, Some(mapException.getMessage))
        replyTo ! KO(mapException.getMessage, mapException)
      case Success(planRun) =>
        updatePlanRunExecution(planRunExecution, PARSED_PLAN)
        val runPlanFuture = Future {
          PlanProcessor.determineAndExecutePlan(Some(planRun))
        }

        runPlanFuture.onComplete {
          case Failure(planException) =>
            updatePlanRunExecution(planRunExecution, FAILED, Some(planException.getMessage))
            replyTo ! KO(planException.getMessage, planException)
          case Success(results) =>
            updatePlanRunExecution(planRunExecution, FINISHED, None, Some(results))
            replyTo ! OK
        }
    }
  }

  private def addConnectionDetails(planRunRequest: PlanRunRequest): List[DataSourceRequest] = {
    planRunRequest.dataSources.map(ds => {
      // base data source connection
      val baseConnectionInfo = ConnectionRepository.getConnection(ds.name, false)
      // metadata source connection info could exist in fields
      val fieldsWithMetadataConnection = ds.fields.map(fr => {
        val metadataWithConnectionInfo = fr.optMetadataSource.map(metadataSource => {
          val baseMetadataConnectionInfo = ConnectionRepository.getConnection(metadataSource.name, false)
          val typeMap = Map(
            CONNECTION_TYPE -> baseMetadataConnectionInfo.`type`,
            CONNECTION_GROUP_TYPE -> baseMetadataConnectionInfo.groupType.getOrElse(CONNECTION_GROUP_METADATA_SOURCE)
          )

          metadataSource.copy(overrideOptions = Some(
            baseMetadataConnectionInfo.options ++ metadataSource.overrideOptions.getOrElse(Map()) ++ typeMap
          ))
        })
        fr.copy(optMetadataSource = metadataWithConnectionInfo)
      })
      // metadata source connection info could exist in validation
      val allConnectionOptions = baseConnectionInfo.options ++ ds.options.getOrElse(Map())
      ds.copy(`type` = Some(baseConnectionInfo.`type`), options = Some(allConnectionOptions), fields = fieldsWithMetadataConnection)
    })
  }

  private def savePlanRunExecution(planRunRequest: PlanRunRequest, planRunExecution: PlanRunExecution, isDeleteRun: Boolean = false): Unit = {
    LOGGER.debug(s"Saving plan run execution details, plan-name=${planRunRequest.name}, plan-run-id=${planRunRequest.id}")
    if (!isDeleteRun) savePlan(planRunRequest)
    val filePath = s"$executionSaveFolder/${planRunExecution.id}.csv"
    try {
      val basePath = Path.of(executionSaveFolder).toFile
      if (!basePath.exists()) basePath.mkdirs()
      val executionFile = Path.of(filePath)
      Files.writeString(executionFile, planRunExecution.toString, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    } catch {
      case ex: Exception => throw SaveFileException(filePath, ex)
    }
  }

  private def savePlan(planRunRequest: PlanRunRequest): Unit = {
    LOGGER.debug(s"Saving plan details, plan-name=${planRunRequest.name}")
    val filePath = s"$planSaveFolder/${planRunRequest.name}.json"
    try {
      val basePath = Path.of(planSaveFolder).toFile
      if (!basePath.exists()) basePath.mkdirs()
      val planFile = Path.of(filePath)
      Files.writeString(planFile, planRunRequest.toJson.compactPrint, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
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
        val tryParse = Try(fileContent.parseJson.convertTo[PlanRunRequest])
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
    Files.readString(planFile).parseJson.convertTo[PlanRunRequest]
  }

  private def updatePlanRunExecution(planRunExecution: PlanRunExecution, status: String, failedReason: Option[String] = None,
                                     results: Option[PlanRunResults] = None): Unit = {
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
