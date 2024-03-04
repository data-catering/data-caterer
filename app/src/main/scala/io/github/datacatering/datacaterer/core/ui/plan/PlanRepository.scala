package io.github.datacatering.datacaterer.core.ui.plan

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import io.github.datacatering.datacaterer.core.model.Constants.{FAILED, FINISHED, PARSED_PLAN, STARTED}
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.mapper.UiMapper
import io.github.datacatering.datacaterer.core.ui.model.{JsonSupport, PlanRunExecution, PlanRunRequest, PlanRunRequests}
import io.github.datacatering.datacaterer.core.ui.plan.PlanResponseHandler.{KO, OK, Response}
import org.joda.time.DateTime
import spray.json._

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


object PlanRepository extends JsonSupport {

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

  final case class SavePlan(planRunRequest: PlanRunRequest) extends PlanCommand

  final case class GetPlans(replyTo: ActorRef[PlanRunRequests]) extends PlanCommand

  final case class GetPlan(name: String, replyTo: ActorRef[PlanRunRequest]) extends PlanCommand

  final case class GetPlanRunStatus(id: String, replyTo: ActorRef[PlanRunExecution]) extends PlanCommand

  final case class GetPlanRuns(replyTo: ActorRef[PlanRunExecutionDetails]) extends PlanCommand

  final case class ClearDataFromPlan(name: String) extends PlanCommand

  private val executionSaveFolder = s"$INSTALL_DIRECTORY/execution"
  private val planSaveFolder = s"$INSTALL_DIRECTORY/plan"
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  def apply(): Behavior[PlanCommand] = {
    Behaviors.supervise[PlanCommand] {
      Behaviors.receiveMessage {
        case RunPlan(planRunRequest, replyTo) =>
          // get connection info
          val dataSourcesWithConnectionInfo = planRunRequest.dataSources.map(ds => {
            val connectionInfo = ConnectionRepository.getConnection(ds.name)
            ds.copy(`type` = Some(connectionInfo.`type`), options = Some(connectionInfo.options))
          })
          val planRunWithConnectionInfo = planRunRequest.copy(dataSources = dataSourcesWithConnectionInfo)
          // create new run id
          // save to file
          val planRunExecution = PlanRunExecution(planRunRequest.name, planRunRequest.id, STARTED)
          savePlanRunExecution(planRunRequest, planRunExecution)

          val tryPlanRun = Try(UiMapper.mapToPlanRun(planRunWithConnectionInfo))
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
                  updatePlanRunExecution(planRunExecution, FINISHED)
                  replyTo ! OK
              }
          }
          Behaviors.same
        case SavePlan(planRunRequest) =>
          println(planRunRequest)
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
        case ClearDataFromPlan(_) =>
          Behaviors.same
        case GetPlanRuns(replyTo) =>
          replyTo ! getAllPlanExecutions
          Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  private def savePlanRunExecution(planRunRequest: PlanRunRequest, planRunExecution: PlanRunExecution): Unit = {
    savePlan(planRunRequest)
    try {
      Path.of(executionSaveFolder).toFile.mkdirs()
      val executionFile = Path.of(s"$executionSaveFolder/${planRunExecution.id}.csv")
      Files.writeString(executionFile, planRunExecution.toString, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    } catch {
      case ex: Exception => throw new RuntimeException(s"Failed to save plan execution to file system, plan-name=${planRunRequest.name}", ex)
    }
  }

  private def savePlan(planRunRequest: PlanRunRequest): Unit = {
    try {
      Path.of(planSaveFolder).toFile.mkdirs()
      val planFile = Path.of(s"$planSaveFolder/${planRunRequest.name}.json")
      Files.writeString(planFile, planRunRequest.toJson.compactPrint, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
    } catch {
      case ex: Exception => throw new RuntimeException(s"Failed to save plan to file system, plan-name=${planRunRequest.name}", ex)
    }
  }

  private def getPlans: PlanRunRequests = {
    val planFolder = Path.of(planSaveFolder)
    if (!planFolder.toFile.exists()) planFolder.toFile.mkdirs()
    val plans = Files.list(planFolder)
      .iterator()
      .asScala
      .map(file => {
        val fileContent = Files.readString(file)
        fileContent.parseJson.convertTo[PlanRunRequest]
      })
      .toList
    PlanRunRequests(plans)
  }

  private def getPlan(name: String): PlanRunRequest = {
    val planFile = Path.of(s"$planSaveFolder/$name.json")
    Files.readString(planFile).parseJson.convertTo[PlanRunRequest]
  }

  private def updatePlanRunExecution(planRunExecution: PlanRunExecution, status: String, failedReason: Option[String] = None): Unit = {
    val executionFile = Path.of(s"$executionSaveFolder/${planRunExecution.id}.csv")
    val updatedPlanRun = planRunExecution.copy(status = status, updatedTs = DateTime.now(), failedReason = failedReason)
    Files.writeString(executionFile, updatedPlanRun.toString, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
  }

  private def getPlanRunStatus(id: String): PlanRunExecution = {
    val executionFile = Path.of(s"$executionSaveFolder/$id.csv")
    val latestUpdate = Files.readAllLines(executionFile).asScala.last
    PlanRunExecution.fromString(latestUpdate)
  }

  private def getAllPlanExecutions: PlanRunExecutionDetails = {
    val executionPath = Path.of(executionSaveFolder)
    if (!executionPath.toFile.exists()) executionPath.toFile.mkdirs()
    val allPlanRunExecutions = Files.list(executionPath)
      .iterator()
      .asScala
      .flatMap(execFile => {
        val lines = Files.readAllLines(execFile).asScala
        lines.map(line => PlanRunExecution.fromString(line))
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
}
