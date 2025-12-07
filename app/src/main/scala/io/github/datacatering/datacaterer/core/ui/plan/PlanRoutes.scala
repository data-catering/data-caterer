package io.github.datacatering.datacaterer.core.ui.plan

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.pjfanning.pekkohttpjackson.JacksonSupport
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration
import io.github.datacatering.datacaterer.core.ui.model.{Connection, ConnectionTestResult, EnhancedPlanRunRequest, MultiSchemaSampleResponse, PlanRunRequest, SampleError, SampleMetadata, SaveConnectionsRequest, SchemaSampleRequest, TestConnectionRequest}
import io.github.datacatering.datacaterer.core.ui.service.ConnectionTestService
import io.github.datacatering.datacaterer.core.ui.resource.SparkSessionManager
import io.github.datacatering.datacaterer.core.ui.sample.FastSampleGenerator
import io.github.datacatering.datacaterer.core.util.{ObjectMapperUtil, SparkProvider}
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.AskPattern.{Askable, schedulerFromActorSystem}
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.model.HttpMethods._
import org.apache.pekko.http.scaladsl.model.headers._
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpEntity, HttpResponse, MediaTypes, StatusCodes, HttpHeader}
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

  // Log CORS configuration at startup
  UiConfiguration.Cors.logConfiguration()
  
  /**
   * CORS configuration to allow cross-origin requests.
   * Configurable via environment variables:
   * - DATA_CATERER_CORS_ALLOWED_ORIGINS: Allowed origins (default: "*")
   * - DATA_CATERER_CORS_ALLOWED_METHODS: Allowed methods (default: "GET,POST,PUT,DELETE,OPTIONS")
   * - DATA_CATERER_CORS_ALLOWED_HEADERS: Allowed headers (default: "Content-Type,Authorization,X-Requested-With")
   * - DATA_CATERER_CORS_MAX_AGE: Preflight cache duration in seconds (default: 86400)
   */
  private val corsHeaders: List[HttpHeader] = {
    val corsConfig = UiConfiguration.Cors
    
    val originHeader = if (corsConfig.allowedOrigins == "*") {
      `Access-Control-Allow-Origin`.*
    } else {
      // For specific origins, we'll use the first one as default
      // Note: For multiple origins, you'd need to check the request origin header
      `Access-Control-Allow-Origin`(HttpOrigin(corsConfig.allowedOrigins.split(",").head.trim))
    }
    
    val methodsHeader = `Access-Control-Allow-Methods`(
      corsConfig.allowedMethods.flatMap {
        case "GET" => Some(GET)
        case "POST" => Some(POST)
        case "PUT" => Some(PUT)
        case "DELETE" => Some(DELETE)
        case "OPTIONS" => Some(OPTIONS)
        case "HEAD" => Some(HEAD)
        case "PATCH" => Some(PATCH)
        case _ => None
      }: _*
    )
    
    val headersHeader = `Access-Control-Allow-Headers`(corsConfig.allowedHeaders: _*)
    val maxAgeHeader = `Access-Control-Max-Age`(corsConfig.maxAge)
    
    List(originHeader, methodsHeader, headersHeader, maxAgeHeader)
  }

  /**
   * Directive to add CORS headers to all responses
   */
  private def corsHandler(route: Route): Route = {
    respondWithHeaders(corsHeaders) {
      options {
        // Handle preflight OPTIONS requests
        complete(StatusCodes.OK)
      } ~ route
    }
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
      // Use step from response, or create a default step without transformation
      val step = responseWithDf.step.getOrElse(Step(options = Map("format" -> format)))
      val rawBytes = FastSampleGenerator.dataFrameToRawBytes(responseWithDf.dataFrame.get, format, step)
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
  private def completeWithError(error: SampleError): Route = {
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
    results: Map[String, (Step, FastSampleGenerator.SampleResponseWithDataFrame)],
    size: Int,
    fast: Boolean,
    relationships: Boolean = false
  ): MultiSchemaSampleResponse = {
    val samplesMap = results.map { case (key, (_, responseWithDf)) =>
      (key, responseWithDf.response.sampleData.getOrElse(List()))
    }

    MultiSchemaSampleResponse(
      success = true,
      executionId = java.util.UUID.randomUUID().toString.split("-").head,
      samples = samplesMap,
      metadata = Some(SampleMetadata(
        sampleSize = size,
        actualRecords = samplesMap.values.map(_.size).sum,
        generatedInMs = 0L,
        fastModeEnabled = fast,
        relationshipsEnabled = relationships
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

  lazy val planRoutes: Route = corsHandler(concat(
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
              LOGGER.info(s"Received request to delete data for plan, plan-name=${runInfo.plan.name}, run-id=${runInfo.id}")
              try {
                planRepository ! PlanRepository.RunPlanDeleteData(runInfo, planResponseHandler)
                LOGGER.info(s"Plan delete data initiated, plan-name=${runInfo.plan.name}, run-id=${runInfo.id}")
                complete(s"Plan '${runInfo.plan.name}' delete data started with run ID: ${runInfo.id}")
              } catch {
                case ex: Exception =>
                  LOGGER.error(s"Failed to initiate plan delete data, plan-name=${runInfo.plan.name}", ex)
                  complete(StatusCodes.InternalServerError, s"Failed to start delete data: ${ex.getMessage}")
              }
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
            LOGGER.info(s"Received request to run plan, plan-name=${runInfo.plan.name}, run-id=${runInfo.id}, tasks=${runInfo.tasks.size}")
            try {
              planRepository ! PlanRepository.RunPlan(runInfo, planResponseHandler)
              LOGGER.info(s"Plan run initiated, plan-name=${runInfo.plan.name}, run-id=${runInfo.id}")
              complete(s"Plan '${runInfo.plan.name}' started with run ID: ${runInfo.id}")
            } catch {
              case ex: Exception =>
                LOGGER.error(s"Failed to initiate plan run, plan-name=${runInfo.plan.name}", ex)
                complete(StatusCodes.InternalServerError, s"Failed to start plan: ${ex.getMessage}")
            }
          }
        }
      )
    },
    pathPrefix("connection") {
      concat(
        pathEnd {
          post {
            entity(as[SaveConnectionsRequest]) { connections =>
              LOGGER.info(s"Saving connections via POST /connection, count=${connections.connections.size}")
              connectionRepository ! ConnectionRepository.SaveConnections(connections)
              complete(connections.connections.map(_.name).mkString(", "))
            }
          }
        },
        path("test") {
          post {
            entity(as[Connection]) { connection =>
              LOGGER.info(s"Testing connection, name=${connection.name}, type=${connection.`type`}")
              try {
                val result = ConnectionTestService.testConnection(connection)
                if (result.success) {
                  complete(result)
                } else {
                  complete(StatusCodes.BadRequest, result)
                }
              } catch {
                case ex: Exception =>
                  LOGGER.error(s"Connection test failed with exception, name=${connection.name}", ex)
                  complete(StatusCodes.InternalServerError, ConnectionTestResult(
                    success = false,
                    message = s"Connection test failed: ${ex.getMessage}",
                    details = Some(ex.getClass.getName)
                  ))
              }
            }
          }
        },
        path("""[A-Za-z0-9-_]+""".r / "test") { connectionName =>
          post {
            // Test an existing saved connection by name
            val connectionFuture = connectionRepository.ask(a => ConnectionRepository.GetConnection(connectionName, a))
            onComplete(connectionFuture) {
              case Success(connection) =>
                try {
                  // Get unmasked connection for testing
                  val unmaskedConnection = ConnectionRepository.getConnection(connectionName, masking = false)
                  val result = ConnectionTestService.testConnection(unmaskedConnection)
                  if (result.success) {
                    complete(result)
                  } else {
                    complete(StatusCodes.BadRequest, result)
                  }
                } catch {
                  case ex: Exception =>
                    LOGGER.error(s"Connection test failed with exception, name=$connectionName", ex)
                    complete(StatusCodes.InternalServerError, ConnectionTestResult(
                      success = false,
                      message = s"Connection test failed: ${ex.getMessage}",
                      details = Some(ex.getClass.getName)
                    ))
                }
              case Failure(_) =>
                complete(StatusCodes.NotFound, ConnectionTestResult(
                  success = false,
                  message = s"Connection not found: $connectionName"
                ))
            }
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
              val removeResult = connectionRepository.ask((replyTo: ActorRef[ConnectionRepository.ConnectionResponse]) => 
                ConnectionRepository.RemoveConnection(connectionName, Some(replyTo)))
              onComplete(removeResult) {
                case Success(ConnectionRepository.ConnectionRemoved(name, true)) => 
                  complete(s"Connection '$name' removed successfully")
                case Success(ConnectionRepository.ConnectionRemoved(name, false)) => 
                  complete(StatusCodes.BadRequest, s"Cannot delete connection '$name'. It may be a default connection from application.conf that cannot be deleted via the UI.")
                case Success(_) =>
                  complete(StatusCodes.InternalServerError, s"Unexpected response when removing connection")
                case Failure(ex) => 
                  complete(StatusCodes.InternalServerError, s"Failed to remove connection: ${ex.getMessage}")
              }
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
            LOGGER.info(s"Saving plan, name=${runInfo.plan.name}")
            val saveResult = planRepository.ask((replyTo: ActorRef[PlanRepository.PlanResponse]) => 
              PlanRepository.SavePlan(runInfo, Some(replyTo)))
            onComplete(saveResult) {
              case Success(PlanRepository.PlanSaved(name)) =>
                LOGGER.info(s"Plan saved successfully, name=$name")
                complete(s"Plan '$name' saved successfully")
              case Success(_) =>
                complete(StatusCodes.InternalServerError, "Unexpected response when saving plan")
              case Failure(ex) =>
                LOGGER.error(s"Failed to save plan, name=${runInfo.plan.name}", ex)
                complete(StatusCodes.InternalServerError, s"Failed to save plan: ${ex.getMessage}")
            }
          }
        },
        path("""[A-Za-z0-9-_]+""".r) { planName =>
          concat(
            get {
              val plan = planRepository.ask(PlanRepository.GetPlan(planName, _))
              onComplete(plan) {
                case Success(value) => complete(value)
                case Failure(_) => complete(StatusCodes.NotFound, s"Failed to find plan with name: $planName")
              }
            },
            delete {
              val removeResult = planRepository.ask((replyTo: ActorRef[PlanRepository.PlanResponse]) =>
                PlanRepository.RemovePlan(planName, Some(replyTo)))
              onComplete(removeResult) {
                case Success(PlanRepository.PlanRemoved(name, true)) =>
                  complete(s"Plan '$name' removed successfully")
                case Success(PlanRepository.PlanRemoved(name, false)) =>
                  complete(StatusCodes.NotFound, s"Plan '$name' not found or could not be removed")
                case Success(_) =>
                  complete(StatusCodes.InternalServerError, "Unexpected response when removing plan")
                case Failure(ex) =>
                  complete(StatusCodes.InternalServerError, s"Failed to remove plan: ${ex.getMessage}")
              }
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
                parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional, "enableRelationships".as[Boolean].optional) { (sampleSize, fastMode, enableRelationships) =>
                  implicit val sparkSession: SparkSession = SparkSessionManager.getOrCreate()
                  FastSampleGenerator.generateFromPlanStep(planName, taskName, stepName, sampleSize, fastMode.getOrElse(true)) match {
                    case Right((_, responseWithDf)) => completeWithRawData(responseWithDf)
                    case Left(error) => completeWithError(error)
                  }
                }
              }
            },
            // Sample from specific task
            path("""[A-Za-z0-9-_]+""".r / "tasks" / """[A-Za-z0-9-_]+""".r) { (planName, taskName) =>
              get {
                parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional, "enableRelationships".as[Boolean].optional) { (sampleSize, fastMode, enableRelationships) =>
                  implicit val sparkSession: SparkSession = SparkSessionManager.getOrCreate()
                  val fast = fastMode.getOrElse(true)
                  val relationships = enableRelationships.getOrElse(false)

                  FastSampleGenerator.generateFromPlanTask(planName, taskName, sampleSize, fast, relationships) match {
                    case Right(results) => complete(buildMultiSchemaResponse(results, sampleSize.getOrElse(10), fast, relationships))
                    case Left(error) => completeWithError(error)
                  }
                }
              }
            },
            // Sample from entire plan
            path("""[A-Za-z0-9-_]+""".r) { planName =>
              get {
                parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional, "enableRelationships".as[Boolean].optional) { (sampleSize, fastMode, enableRelationships) =>
                  implicit val sparkSession: SparkSession = SparkSessionManager.getOrCreate()
                  val fast = fastMode.getOrElse(true)
                  val relationships = enableRelationships.getOrElse(false)

                  FastSampleGenerator.generateFromPlan(planName, sampleSize, fast, relationships) match {
                    case Right(results) => complete(buildMultiSchemaResponse(results, sampleSize.getOrElse(10), fast, relationships))
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
              implicit val sparkSession: SparkSession = SparkSessionManager.getOrCreate()
              completeWithRawData(FastSampleGenerator.generateFromSchemaWithDataFrame(request))
            }
          }
        },
        // Task name-based sample generation
        path("tasks" / """[A-Za-z0-9-_]+""".r) { taskName =>
          get {
            parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional, "enableRelationships".as[Boolean].optional) { (sampleSize, fastMode, enableRelationships) =>
              implicit val sparkSession: SparkSession = SparkSessionManager.getOrCreate()
              val fast = fastMode.getOrElse(true)
              val relationships = enableRelationships.getOrElse(false)

              FastSampleGenerator.generateFromTaskName(taskName, sampleSize, fast, relationships) match {
                case Right(results) => complete(buildMultiSchemaResponse(results, sampleSize.getOrElse(10), fast, relationships))
                case Left(error) => completeWithError(error)
              }
            }
          }
        },
        // Step name-based sample generation
        path("steps" / """[A-Za-z0-9-_]+""".r) { stepName =>
          get {
            parameters("sampleSize".as[Int].optional, "fastMode".as[Boolean].optional, "enableRelationships".as[Boolean].optional) { (sampleSize, fastMode, enableRelationships) =>
              implicit val sparkSession: SparkSession = SparkSessionManager.getOrCreate()
              val fast = fastMode.getOrElse(true)

              FastSampleGenerator.generateFromStepName(stepName, sampleSize, fast) match {
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
  ))

}
