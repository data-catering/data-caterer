package io.github.datacatering.datacaterer.core.ui.security

import io.github.datacatering.datacaterer.core.exception.{InvalidCredentialsException, ManagementApiException, UserNotFoundException}
import io.github.datacatering.datacaterer.core.model.Constants.{DATA_CATERER_API_TOKEN, DATA_CATERER_API_USER}
import io.github.datacatering.datacaterer.core.ui.model.CredentialsRequest
import io.github.datacatering.datacaterer.core.util.ManagementUtil.getDataCatererManagementUrl
import org.apache.log4j.Logger
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.Dsl.asyncHttpClient

import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

object DefaultCredentialsManager extends CredentialsManager {

  private val http: AsyncHttpClient = asyncHttpClient
  private val dataCatererManagementUrl: String = getDataCatererManagementUrl

  override def validateCredentials(credentialsRequest: CredentialsRequest): String = {
    val url = s"$dataCatererManagementUrl/user/${credentialsRequest.userId}"
    val prepareRequest = http.prepareGet(url)
      .setHeader("x-api-token", credentialsRequest.token)
      .setHeader("x-api-user", credentialsRequest.userId)
    val scalaFuture = prepareRequest.execute().toCompletableFuture.toScala
    Try(Await.result(scalaFuture, 2.seconds)) match {
      case Success(response) =>
        if (response.getStatusCode == 200) {
          setUsernameAndToken(credentialsRequest)
          credentialsRequest.userId
        } else {
          if (response.getResponseBody.contains("User not found")) {
            throw UserNotFoundException(credentialsRequest.userId)
          } else {
            throw InvalidCredentialsException(credentialsRequest.userId)
          }
        }
      case Failure(exception) => throw ManagementApiException(exception.getMessage)
    }
  }

  private def setUsernameAndToken(credentialsRequest: CredentialsRequest): Unit = {
    System.setProperty(DATA_CATERER_API_USER, credentialsRequest.userId)
    System.setProperty(DATA_CATERER_API_TOKEN, credentialsRequest.token)
  }

}
