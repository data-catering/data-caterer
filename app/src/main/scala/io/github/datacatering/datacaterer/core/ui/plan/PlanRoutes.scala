package io.github.datacatering.datacaterer.core.ui.plan

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes.{BadRequest, InternalServerError}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, Route}
import akka.util.Timeout
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.model.{JsonSupport, PlanRunRequest, SaveConnectionsRequest}
import org.apache.log4j.Logger

import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

class PlanRoutes(
                  planRepository: ActorRef[PlanRepository.PlanCommand],
                  planResponseHandler: ActorRef[PlanResponseHandler.Response],
                  connectionRepository: ActorRef[ConnectionRepository.ConnectionCommand]
                )(implicit system: ActorSystem[_]) extends JsonSupport {

  import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}

  private val LOGGER = Logger.getLogger(getClass.getName)

  // asking someone requires a timeout and a scheduler, if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: Exception =>
        extractUri { uri =>
          complete(HttpResponse(InternalServerError, entity = s"Failed to execute request, uri=$uri, error-message=${ex.getMessage}"))
        }
    }

  lazy val planRoutes: Route = concat(
    path("") {
      get {
        getFromResource("ui/index.html")
      }
    },
    path("connection") {
      get {
        getFromResource("ui/connection/connection.html")
      }
    },
    path("plan") {
      get {
        getFromResource("ui/plan/plan.html")
      }
    },
    path("history") {
      get {
        getFromResource("ui/history/history.html")
      }
    },
    path("ui" / Segments(1, 2)) { fileName =>
      val hasOnlyAlphanumericAndDash = fileName.forall(_.matches("[0-9a-z-]+(\\.(html|css|js))?"))
      if (hasOnlyAlphanumericAndDash) {
        getFromResource(s"ui/${fileName.mkString("/")}")
      } else {
        complete(HttpResponse(BadRequest, entity = s"Unable to fetch resource for request"))
      }
    },
    path("data_catering_transparent.svg") {
      get {
        getFromResource("report/data_catering_transparent.svg")
      }
    },
    pathPrefix("run") {
      concat(
        post {
          entity(as[PlanRunRequest]) { runInfo =>
            planRepository ! PlanRepository.RunPlan(runInfo, planResponseHandler)
            complete("Plan started")
          }
        },
        path("history") {
          get {
            val planRuns = planRepository.ask(PlanRepository.GetPlanRuns)
            rejectEmptyResponse {
              complete(planRuns)
            }
          }
        },
        path("status" / """[a-z0-9-]+""".r) { id =>
          val planStatus = planRepository.ask(x => PlanRepository.GetPlanRunStatus(id, x))
          rejectEmptyResponse {
            complete(planStatus)
          }
        }
      )
    },
    pathPrefix("connection") {
      concat(
        post {
          entity(as[SaveConnectionsRequest]) { connections =>
            connectionRepository ! ConnectionRepository.SaveConnections(connections)
            complete("Saved connections")
          }
        },
        path("""[a-z0-9-]+""".r) { connectionName =>
          concat(
            get {
              val connection = connectionRepository.ask(a => ConnectionRepository.GetConnection(connectionName, a))
              rejectEmptyResponse {
                complete(connection)
              }
            },
            delete {
              connectionRepository ! ConnectionRepository.RemoveConnection(connectionName)
              complete("Removed")
            }
          )
        }
      )
    },
    pathPrefix("connections") {
      val connections = connectionRepository.ask(ConnectionRepository.GetConnections)
      rejectEmptyResponse {
        complete(connections)
      }
    },
    pathPrefix("plan") {
      concat(
        post {
          entity(as[PlanRunRequest]) { runInfo =>
            planRepository ! PlanRepository.SavePlan(runInfo)
            complete("Plan saved")
          }
        },
        path("""[a-z0-9-]+""".r) { planName =>
          val plan = planRepository.ask(PlanRepository.GetPlan(planName, _))
          rejectEmptyResponse {
            complete(plan)
          }
        }
      )
    },
    pathPrefix("plans") {
      val plans = planRepository.ask(PlanRepository.GetPlans)
      rejectEmptyResponse {
        complete(plans)
      }
    },
    pathPrefix("report" / """^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$""".r / Remaining) { (runId, resource) =>
      get {
        getFromFile(s"$INSTALL_DIRECTORY/report/$runId/$resource")
      }
    },
    path("shutdown") {
      system.terminate()
      system.whenTerminated.onComplete {
        case Failure(exception) =>
          System.exit(1)
        case Success(value) =>
          System.exit(0)
      }
      complete("Data Caterer shutdown completed")
    }
  )

}
