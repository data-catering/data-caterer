package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.core.util.ManagementUtil.{getApiToken, getApiUser, getDataCatererManagementUrl}
import org.apache.log4j.Logger
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

trait LifecycleManagement {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val apiKey: String = getApiToken
  private val apiUser: String = getApiUser
  private val http: AsyncHttpClient = asyncHttpClient

  val dataCatererManagementUrl: String = getDataCatererManagementUrl

  def sendRequest(url: String, body: String, failOnError: Boolean = false): Unit = {
    val prepareRequest = http.preparePost(url)
      .setBody(body)
      .setHeader("Content-Type", "application/json")
      .setHeader("x-api-key", apiKey)
      .setHeader("x-api-user", apiUser)

    val scalaFuture = prepareRequest.execute().toCompletableFuture.toScala
    val tryFutureResp = Try(scalaFuture)
    tryFutureResp match {
      case Success(futureResp) =>
        futureResp.onComplete {
          case Success(value) =>
            LOGGER.debug(s"Successfully sent request to API server, url=$url")
            if (value.getStatusCode == 429) {
              LOGGER.warn(s"You have reached the quota limit for your plan, ${value.getResponseBody}")
              System.exit(0)
            } else if (value.getStatusCode != 200 && failOnError) {
              LOGGER.error(s"Failed to validate with API server, resp=${value.getResponseBody}")
              http.close()
              System.exit(1)
            }
            http.close()
          case Failure(exception) =>
            LOGGER.debug(s"Failed send request to API server, url=$url", exception)
            http.close()
            if (failOnError) {
              System.exit(1)
            }
        }
      case Failure(ex) =>
        LOGGER.error("Failed to send request to API server", ex)
        http.close()
        if (failOnError) {
          System.exit(1)
        }
    }
    Await.result(scalaFuture, 2.seconds)
  }
}
