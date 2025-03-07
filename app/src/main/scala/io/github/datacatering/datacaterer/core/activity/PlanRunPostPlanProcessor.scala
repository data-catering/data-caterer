package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceResult, Plan, PlanResults, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.plan.PostPlanProcessor
import io.github.datacatering.datacaterer.core.util.ManagementUtil.getDataCatererManagementUrl
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class PlanRunPostPlanProcessor(val dataCatererConfiguration: DataCatererConfiguration) extends PostPlanProcessor {

  override val enabled: Boolean = dataCatererConfiguration.flagsConfig.enableTrackActivity

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val dataCatererManagementUrl = getDataCatererManagementUrl
  private val http: AsyncHttpClient = asyncHttpClient

  override def apply(
                      plan: Plan,
                      sparkRecordListener: SparkRecordListener,
                      generationResult: List[DataSourceResult],
                      validationResults: List[ValidationConfigResult]
                    ): Unit = {
    val planResults = PlanResults(plan, generationResult, validationResults)
    val jsonBody = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(planResults)
    val url = s"$dataCatererManagementUrl/plan/finish"
    val prepareRequest = http.prepare("POST", url)
      .setBody(jsonBody)

    val futureResp = prepareRequest.execute().toCompletableFuture.toScala
    futureResp.onComplete {
      case Success(_) =>
        LOGGER.debug(s"Successfully posted run results, url=$url")
        http.close()
      case Failure(exception) =>
        LOGGER.debug(s"Failed to post run results, url=$url", exception)
        http.close()
    }
  }
}
