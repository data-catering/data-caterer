package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.core.exception.ManagementApiException
import io.github.datacatering.datacaterer.core.util.LifecycleUtil.{isRunningDataCatererUi, isTrackActivity}
import io.github.datacatering.datacaterer.core.util.ManagementUtil.{getApiToken, getApiUser, getDataCatererManagementUrl}
import org.apache.log4j.Logger
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

trait LifecycleManagement {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val apiKey: String = if (isTrackActivity) getApiToken else ""
  private val apiUser: String = if (isTrackActivity) getApiUser else ""
  private val http: AsyncHttpClient = asyncHttpClient
  private val isDataCatererUi: Boolean = isRunningDataCatererUi

  val dataCatererManagementUrl: String = getDataCatererManagementUrl

  def sendRequest(url: String, body: String, failOnError: Boolean = false): Unit = {
    val prepareRequest = http.preparePost(url)
      .setBody(body)
      .setHeader("Content-Type", "application/json")
      .setHeader("x-api-key", apiKey)
      .setHeader("x-api-user", apiUser)

    val scalaFuture = prepareRequest.execute().toCompletableFuture.toScala
    Try(Await.result(scalaFuture, 2.seconds)) match {
      case Success(value) =>
        LOGGER.debug(s"Successfully sent request to API server, url=$url")
        if (value.getStatusCode == 429) {
          LOGGER.warn(s"You have reached the quota limit for your plan, ${value.getResponseBody}")
          //don't exit for UI
          if (!isDataCatererUi) {
            System.exit(0)
          } else {
            throw ManagementApiException(value.getResponseBody)
          }
        } else if (value.getStatusCode != 200 && failOnError) {
          LOGGER.error(s"Failed to validate with API server, resp=${value.getResponseBody}")
          if (!isDataCatererUi) {
            http.close()
            System.exit(1)
          } else {
            throw ManagementApiException(value.getResponseBody)
          }
        }
      case Failure(exception) =>
        LOGGER.debug(s"Failed send request to API server, url=$url", exception)
        if (!isDataCatererUi) {
          http.close()
          if (failOnError) {
            System.exit(1)
          } else {
            throw ManagementApiException(exception.getMessage)
          }
        }
    }
  }
}
