package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, PlanRunSummary, Task, ValidationConfiguration}
import io.github.datacatering.datacaterer.core.plan.PrePlanProcessor
import io.github.datacatering.datacaterer.core.util.ManagementUtil.getDataCatererManagementUrl
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class PlanRunPrePlanProcessor(val dataCatererConfiguration: DataCatererConfiguration) extends PrePlanProcessor {

  override val enabled: Boolean = dataCatererConfiguration.flagsConfig.enableTrackActivity

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val dataCatererManagementUrl = getDataCatererManagementUrl
  private val http: AsyncHttpClient = asyncHttpClient

  override def apply(
                      plan: Plan,
                      tasks: List[Task],
                      validations: List[ValidationConfiguration]
                    ): Unit = {
    val planRunSummary = PlanRunSummary(plan, tasks, validations)
    val jsonBody = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(planRunSummary)
    val url = s"$dataCatererManagementUrl/plan/start"
    val prepareRequest = http.prepare("POST", url)
      .setBody(jsonBody)

    val futureResp = prepareRequest.execute().toCompletableFuture.toScala
    futureResp.onComplete {
      case Success(_) =>
        LOGGER.debug(s"Successfully posted run start, url=$url")
        http.close()
      case Failure(exception) =>
        LOGGER.debug(s"Failed to post run start, url=$url", exception)
        http.close()
    }
  }
}
