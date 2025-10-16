package io.github.datacatering.datacaterer.core.ui.plan

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.pjfanning.pekkohttpjackson.JacksonSupport
import io.github.datacatering.datacaterer.core.ui.model.{EnhancedPlanRunRequest, PlanRunRequest, SaveConnectionsRequest, SchemaSampleRequest}
import io.github.datacatering.datacaterer.core.ui.sample.FastSampleGenerator
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes, StatusCodes}
import org.apache.pekko.http.scaladsl.server.{Directives, ExceptionHandler, Route}
import org.apache.pekko.util.Timeout

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor}
import scala.util.{Failure, Success}

class PlanRoutes(
                  planRepository: ActorRef[PlanRepository.PlanCommand],
                  planResponseHandler: ActorRef[PlanResponseHandler.Response],
                  connectionRepository: ActorRef[ConnectionRepository.ConnectionCommand],
                )(implicit system: ActorSystem[_]) extends Directives with JacksonSupport {

  import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG}
  import org.apache.spark.sql.SparkSession

  private val LOGGER = Logger.getLogger(getClass.getName)

  // asking someone requires a timeout and a scheduler, if the timeout hits without response
  // the ask is failed with a TimeoutException
  implicit val timeout: Timeout = 3.seconds
  implicit val ec: ExecutionContextExecutor = ExecutionContext.global
  implicit val objectMapper: ObjectMapper = ObjectMapperUtil.jsonObjectMapper

  private def createSparkSession(): SparkSession = {
    new SparkProvider(DEFAULT_MASTER, DEFAULT_RUNTIME_CONFIG).getSparkSession
  }

  /**
   * Helper method to convert format string to Pekko HTTP ContentType
   */
  private def contentTypeFor(format: String): ContentType = {
    FastSampleGenerator.contentTypeForFormat(format) match {
      case "application/json" => ContentType(MediaTypes.`application/json`)
      case "text/csv" => ContentType.WithCharset(MediaTypes.`text/csv`, org.apache.pekko.http.scaladsl.model.HttpCharsets.`UTF-8`)
      case _ => ContentType(MediaTypes.`application/octet-stream`)
    }
  }

  /**
   * Helper method to complete with raw data for single schema responses
   */
  private def completeWithRawData(responseWithDf: FastSampleGenerator.SampleResponseWithDataFrame): Route = {
    if (responseWithDf.response.success && responseWithDf.dataFrame.isDefined && responseWithDf.response.format.isDefined) {
      val format = responseWithDf.response.format.get
      val rawBytes = FastSampleGenerator.dataFrameToRawBytes(responseWithDf.dataFrame.get, format)
      complete(HttpResponse(
        status = StatusCodes.OK,
        entity = HttpEntity(contentTypeFor(format), rawBytes)
      ))
    } else {
      // Error case - return JSON response with error details
      complete(responseWithDf.response)
    }
  }

  /**
   * Helper method to complete with error response
   */
  private def completeWithError(error: io.github.datacatering.datacaterer.core.ui.model.SampleError): Route = {
    complete(HttpResponse(
      status = StatusCodes.BadRequest,
      entity = HttpEntity(ContentType(MediaTypes.`application/json`),
        ObjectMapperUtil.jsonObjectMapper.writeValueAsString(error))
    ))
  }

  /**
   * Helper method to build MultiSchemaSampleResponse from results map
   */
  private def buildMultiSchemaResponse(
    results: Map[String, (io.github.datacatering.datacaterer.api.model.Step, FastSampleGenerator.SampleResponseWithDataFrame)],
    size: Int,
    fast: Boolean
  ): io.github.datacatering.datacaterer.core.ui.model.MultiSchemaSampleResponse = {
    val samplesMap = results.map { case (key, (_, responseWithDf)) =>
      (key, responseWithDf.response.sampleData.getOrElse(List()))
    }

    io.github.datacatering.datacaterer.core.ui.model.MultiSchemaSampleResponse(
      success = true,
      executionId = java.util.UUID.randomUUID().toString.split("-").head,
      samples = samplesMap,
      metadata = Some(io.github.datacatering.datacaterer.core.ui.model.SampleMetadata(
        sampleSize = size,
        actualRecords = samplesMap.values.map(_.size).sum,
        generatedInMs = 0L,
        fastModeEnabled = fast
      ))
    )
  }

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
        // Legacy endpoint - keep for backwards compatibility
        path("delete-data") {
          post {
            entity(as[PlanRunRequest]) { runInfo =>
              planRepository ! PlanRepository.RunPlanDeleteData(runInfo, planResponseHandler)
              complete("Plan delete data started")
            }
          }
        },
        // New hierarchical endpoints for enhanced plan execution
        pathPrefix("plans") {
          concat(
            // Run specific step within a task
            path("""[A-Za-z0-9-_]+""".r / "tasks" / """[A-Za-z0-9-_]+""".r / "steps" / """[A-Za-z0-9-_]+""".r) { (planName, taskName, stepName) =>
              post {
                parameters("source".optional, "mode".optional, "fileName".optional) { (source, mode, fileName) =>
                  val request = EnhancedPlanRunRequest(
                    planName = planName,
                    sourceType = source.getOrElse("auto"),
                    fileName = fileName,
                    taskFilter = Some(taskName),
                    stepFilter = Some(stepName),
                    mode = mode.getOrElse("generate")
                  )
                  planRepository ! PlanRepository.RunEnhancedPlan(request, planResponseHandler)
                  complete(s"Step '$stepName' in task '$taskName' of plan '$planName' started in ${request.mode} mode")
                }
              }
            },
            // Run specific task within a plan
            path("""[A-Za-z0-9-_]+""".r / "tasks" / """[A-Za-z0-9-_]+""".r) { (planName, taskName) =>
              post {
                parameters("source".optional, "mode".optional, "fileName".optional) { (source, mode, fileName) =>
                  val request = EnhancedPlanRunRequest(
                    planName = planName,
                    sourceType = source.getOrElse("auto"),
                    fileName = fileName,
                    taskFilter = Some(taskName),
                    stepFilter = None,
                    mode = mode.getOrElse("generate")
                  )
                  planRepository ! PlanRepository.RunEnhancedPlan(request, planResponseHandler)
                  complete(s"Task '$taskName' in plan '$planName' started in ${request.mode} mode")
                }
              }
            },
            // Run entire plan by name
            path("""[A-Za-z0-9-_]+""".r) { planName =>
              post {
                parameters("source".optional, "mode".optional, "fileName".optional) { (source, mode, fileName) =>
                  val request = EnhancedPlanRunRequest(
                    planName = planName,
                    sourceType = source.getOrElse("auto"),
                    fileName = fileName,
                    taskFilter = None,
                    stepFilter = None,
                    mode = mode.getOrElse("generate")
                  )
                  planRepository ! PlanRepository.RunEnhancedPlan(request, planResponseHandler)
                  complete(s"Plan '$planName' started in ${request.mode} mode")
                }
              }
            }
          )
        },
        // Execution history and status
        path("history") {
          get {
            val planRuns = planRepository.ask(PlanRepository.GetPlanRuns)
            onComplete(planRuns) {
              case Success(value) => complete(value)
              case Failure(_) => complete(StatusCodes.NotFound, "Failed to find execution history")
            }
          }
        },
        path("executions" / """[A-Za-z0-9-_]+""".r) { id =>
          get {
            val planStatus = planRepository.ask(x => PlanRepository.GetPlanRunStatus(id, x))
            onComplete(planStatus) {
              case Success(value) => complete(value)
              case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find status for plan ID: $id")
            }
          }
        },
        // Legacy status endpoint - keep for backwards compatibility
        path("status" / """[A-Za-z0-9-_]+""".r) { id =>
          get {
            val planStatus = planRepository.ask(x => PlanRepository.GetPlanRunStatus(id, x))
            onComplete(planStatus) {
              case Success(value) => complete(value)
              case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find status for plan ID: $id")
            }
          }
        },
        // Legacy endpoint - run from request body
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
              onComplete(connection) {
                case Success(value) => complete(value)
                case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find connection: $connectionName")
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
        onComplete(connections) {
          case Success(value) => complete(value)
          case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find connections for group: ${connectionGroupType.getOrElse("")}")
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
              onComplete(plan) {
                case Success(value) => complete(value)
                case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find plan with name: ${planName}")
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
      onComplete(plans) {
        case Success(value) => complete(value)
        case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find plans")
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
    pathPrefix("sample") {
      concat(
        // Sample from saved plans (new endpoints)
        pathPrefix("plans") {
          concat(
            // Sample from specific step within a task
            path("""[A-Za-z0-9-_]+""".r / "tasks" / """[A-Za-z0-9-_]+""".r / "steps" / """[A-Za-z0-9-_]+""".r) { (planName, taskName, stepName) =>
              get {
                parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional) { (sampleSize, fastMode) =>
                  implicit val sparkSession: SparkSession = createSparkSession()
                  FastSampleGenerator.generateFromPlanStep(planName, taskName, stepName, sampleSize.getOrElse(10), fastMode.getOrElse(true)) match {
                    case Right((_, responseWithDf)) => completeWithRawData(responseWithDf)
                    case Left(error) => completeWithError(error)
                  }
                }
              }
            },
            // Sample from specific task
            path("""[A-Za-z0-9-_]+""".r / "tasks" / """[A-Za-z0-9-_]+""".r) { (planName, taskName) =>
              get {
                parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional) { (sampleSize, fastMode) =>
                  implicit val sparkSession: SparkSession = createSparkSession()
                  val size = sampleSize.getOrElse(10)
                  val fast = fastMode.getOrElse(true)

                  FastSampleGenerator.generateFromPlanTask(planName, taskName, size, fast) match {
                    case Right(results) => complete(buildMultiSchemaResponse(results, size, fast))
                    case Left(error) => completeWithError(error)
                  }
                }
              }
            },
            // Sample from entire plan
            path("""[A-Za-z0-9-_]+""".r) { planName =>
              get {
                parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional) { (sampleSize, fastMode) =>
                  implicit val sparkSession: SparkSession = createSparkSession()
                  val size = sampleSize.getOrElse(10)
                  val fast = fastMode.getOrElse(true)

                  FastSampleGenerator.generateFromPlan(planName, size, fast) match {
                    case Right(results) => complete(buildMultiSchemaResponse(results, size, fast))
                    case Left(error) => completeWithError(error)
                  }
                }
              }
            }
          )
        },
        // Schema-based sample generation
        path("schema") {
          post {
            entity(as[SchemaSampleRequest]) { request =>
              implicit val sparkSession: SparkSession = createSparkSession()
              completeWithRawData(FastSampleGenerator.generateFromSchemaWithDataFrame(request))
            }
          }
        },
        // Task name-based sample generation
        path("tasks" / """[A-Za-z0-9-_]+""".r) { taskName =>
          get {
            parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional) { (sampleSize, fastMode) =>
              implicit val sparkSession: SparkSession = createSparkSession()
              val size = sampleSize.getOrElse(10)
              val fast = fastMode.getOrElse(true)

              FastSampleGenerator.generateFromTaskName(taskName, size, fast) match {
                case Right(results) => complete(buildMultiSchemaResponse(results, size, fast))
                case Left(error) => completeWithError(error)
              }
            }
          }
        },
        // Step name-based sample generation  
        path("steps" / """[A-Za-z0-9-_]+""".r) { stepName =>
          get {
            parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional) { (sampleSize, fastMode) =>
              implicit val sparkSession: SparkSession = createSparkSession()
              val size = sampleSize.getOrElse(10)
              val fast = fastMode.getOrElse(true)

              FastSampleGenerator.generateFromStepName(stepName, size, fast) match {
                case Right((_, responseWithDf)) => completeWithRawData(responseWithDf)
                case Left(error) => completeWithError(error)
              }
            }
          }
        }
      )
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
