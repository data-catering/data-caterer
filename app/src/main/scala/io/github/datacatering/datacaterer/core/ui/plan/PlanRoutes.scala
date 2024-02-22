package io.github.datacatering.datacaterer.core.ui.plan

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.util.Timeout
import io.github.datacatering.datacaterer.core.ui.model.{JsonSupport, PlanRunRequest, SaveConnectionsRequest}
import spray.json._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

class PlanRoutes(
                  planRepository: ActorRef[PlanRepository.PlanCommand],
                  planResponseHandler: ActorRef[PlanResponseHandler.Response],
                  connectionRepository: ActorRef[ConnectionRepository.ConnectionCommand]
                )(implicit system: ActorSystem[_]) extends JsonSupport {

  import akka.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}

  // asking someone requires a timeout and a scheduler, if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  lazy val planRoutes: Route = concat(
    path("") {
      get {
        getFromResource("ui/index.html")
      }
    },
    path("connection") {
      get {
        getFromResource("ui/connection.html")
      }
    },
    path("plan") {
      get {
        getFromResource("ui/plan.html")
      }
    },
    path("history") {
      get {
        getFromResource("ui/history.html")
      }
    },
    path("ui" / """[a-z]+\.[a-z]+""".r) { fileName =>
      getFromResource(s"ui/$fileName")
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
        path("create") {
          post {
            entity(as[SaveConnectionsRequest]) { connections =>
              connectionRepository ! ConnectionRepository.SaveConnections(connections)
              complete("Saved connections")
            }
          }
        },
        path("saved" / """[a-z0-9-]+""".r) { connectionName =>
          val connection = connectionRepository.ask(a => ConnectionRepository.GetConnection(connectionName, a))
          rejectEmptyResponse {
            complete(connection)
          }
        },
        path("saved") {
          val connections = connectionRepository.ask(ConnectionRepository.GetConnections)
          rejectEmptyResponse {
            complete(connections)
          }
        }
      )
    },
    pathPrefix("plan") {
      concat(
        path("saved" / """[a-z0-9-]+""".r) { planName =>
          val plan = planRepository.ask(PlanRepository.GetPlan(planName, _))
          rejectEmptyResponse {
            complete(plan)
          }
        },
        path("saved") {
          val plans = planRepository.ask(PlanRepository.GetPlans)
          rejectEmptyResponse {
            complete(plans)
          }
        }
      )
    }
  )

}
