package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.converter.Converters.toScalaList
import io.github.datacatering.datacaterer.api.model.Constants.{PASSWORD, USERNAME}
import io.netty.handler.codec.http.HttpHeaders

import java.util.Base64

object HttpUtil {

  def getAuthHeader(connectionConfig: Map[String, String]): Map[String, String] = {
    if (connectionConfig.contains(USERNAME) && connectionConfig.contains(PASSWORD)) {
      val user = connectionConfig(USERNAME)
      val password = connectionConfig(PASSWORD)
      val encodedUserPassword = Base64.getEncoder.encodeToString(s"$user:$password".getBytes)
      Map("Authorization" -> s"Basic $encodedUserPassword")
    } else {
      Map()
    }
  }

  def getHeadersAsMap(httpHeaders: HttpHeaders): Map[String, String] = {
    toScalaList(httpHeaders.entries())
      .map(m => m.getKey -> m.getValue)
      .toMap
  }
}
