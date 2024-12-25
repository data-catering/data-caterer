package io.github.datacatering.datacaterer.core.ui.plan

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.pjfanning.pekkohttpjackson.JacksonSupport
import io.github.datacatering.datacaterer.core.ui.model.{PlanRunRequest, SaveConnectionsRequest}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.model.{HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directives, ExceptionHandler, Route}
import org.apache.pekko.util.Timeout

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

class PlanRoutes(
                  planRepository: ActorRef[PlanRepository.PlanCommand],
                  planResponseHandler: ActorRef[PlanResponseHandler.Response],
                  connectionRepository: ActorRef[ConnectionRepository.ConnectionCommand]
                )(implicit system: ActorSystem[_]) extends Directives with JacksonSupport {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // asking someone requires a timeout and a scheduler, if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val objectMapper: ObjectMapper = ObjectMapperUtil.jsonObjectMapper

  implicit def myExceptionHandler: ExceptionHandler =
    ExceptionHandler {
      case ex: Exception =>
        extractUri { uri =>
          complete(HttpResponse(StatusCodes.InternalServerError, entity = s"Failed to execute request, uri=$uri, error-message=${ex.getMessage}"))
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
        complete(HttpResponse(StatusCodes.BadRequest, entity = s"Unable to fetch resource for request"))
      }
    },
    path("data_catering_transparent.svg") {
      get {
        getFromResource("report/data_catering_transparent.svg")
      }
    },
    pathPrefix("run") {
      concat(
        path("delete-data") {
          post {
            entity(as[PlanRunRequest]) { runInfo =>
              planRepository ! PlanRepository.RunPlanDeleteData(runInfo, planResponseHandler)
              complete("Plan delete data started")
            }
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
        path("status" / """[A-Za-z0-9-_]+""".r) { id =>
          val planStatus = planRepository.ask(x => PlanRepository.GetPlanRunStatus(id, x))
          rejectEmptyResponse {
            complete(planStatus)
          }
        },
        post {
          entity(as[PlanRunRequest]) { runInfo =>
            planRepository ! PlanRepository.RunPlan(runInfo, planResponseHandler)
            complete("Plan started")
          }
        }
      )
    },
    pathPrefix("connection") {
      concat(
        post {
          entity(as[SaveConnectionsRequest]) { connections =>
            connectionRepository ! ConnectionRepository.SaveConnections(connections)
            complete(connections.connections.map(_.name).mkString(", "))
          }
        },
        path("""[A-Za-z0-9-_]+""".r) { connectionName =>
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
      parameter("groupType".optional) { connectionGroupType =>
        val connections = connectionRepository.ask(ConnectionRepository.GetConnections(connectionGroupType, _))
        rejectEmptyResponse {
          complete(connections)
        }
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
        path("""[A-Za-z0-9-_]+""".r) { planName =>
          concat(
            get {
              val plan = planRepository.ask(PlanRepository.GetPlan(planName, _))
              rejectEmptyResponse {
                complete(plan)
              }
            },
            delete {
              planRepository ! PlanRepository.RemovePlan(planName)
              complete("Plan removed")
            }
          )
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
        if (resource.length < 50 && (resource.endsWith(".html") || resource.endsWith(".json") || resource.endsWith(".svg") || resource.endsWith(".css"))) {
          val reportPath = planRepository.ask(PlanRepository.GetPlanRunReportPath(runId, _))
          rejectEmptyResponse {
            val path = Await.result(reportPath, Duration.create(5, TimeUnit.SECONDS))
            getFromFile(s"$path/$resource")
          }
        } else {
          complete("Cannot get resource")
        }
      }
    },
    path("shutdown") {
      system.terminate()
      system.whenTerminated.onComplete {
        case Failure(_) => System.exit(1)
        case Success(_) => System.exit(0)
      }
      complete("Data Caterer shutdown completed")
    }
  )

}
