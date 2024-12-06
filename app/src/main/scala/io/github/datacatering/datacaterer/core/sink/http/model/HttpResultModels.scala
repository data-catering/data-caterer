package io.github.datacatering.datacaterer.core.sink.http.model

import io.github.datacatering.datacaterer.core.util.HttpUtil.getHeadersAsMap
import org.asynchttpclient.{Request, Response}

import java.sql.Timestamp
import java.time.Instant

case class HttpRequest(
                        method: String = "",
                        url: String = "",
                        headers: Map[String, String] = Map(),
                        body: String = "",
                        startTime: Timestamp = Timestamp.from(Instant.now())
                      )

object HttpRequest {

  def fromRequest(startTime: Timestamp, request: Request): HttpRequest = {
    HttpRequest(
      request.getMethod,
      request.getUri.toString,
      getHeadersAsMap(request.getHeaders),
      request.getStringData,
      startTime
    )
  }
}

case class HttpResponse(
                         contentType: String = "",
                         headers: Map[String, String] = Map(),
                         body: String = "",
                         statusCode: Int = 200,
                         statusText: String = "",
                         timeTakenMs: Long = 0L
                       )

object HttpResponse {

  def fromResponse(timeTakenMs: Long, response: Response): HttpResponse = {
    //response body contains new line characters
    HttpResponse(
      response.getContentType,
      getHeadersAsMap(response.getHeaders),
      response.getResponseBody,
      response.getStatusCode,
      response.getStatusText,
      timeTakenMs
    )
  }
}

case class HttpResult(request: HttpRequest = HttpRequest(), response: HttpResponse = HttpResponse())

object HttpResult {

  def fromRequestAndResponse(startTime: Timestamp, request: Request, response: Response): HttpResult = {
    val endTime = Timestamp.from(Instant.now())
    val timeTakenMs = endTime.getTime - startTime.getTime
    HttpResult(HttpRequest.fromRequest(startTime, request), HttpResponse.fromResponse(timeTakenMs, response))
  }
}
