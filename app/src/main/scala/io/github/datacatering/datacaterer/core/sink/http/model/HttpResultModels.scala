package io.github.datacatering.datacaterer.core.sink.http.model

import io.github.datacatering.datacaterer.core.util.HttpUtil.getHeadersAsMap
import org.asynchttpclient.{Request, Response}

case class HttpRequest(
                        method: String = "",
                        url: String = "",
                        headers: Map[String, String] = Map(),
                        body: String = ""
                      )

object HttpRequest {

  def fromRequest(request: Request): HttpRequest = {
    HttpRequest(
      request.getMethod,
      request.getUri.toString,
      getHeadersAsMap(request.getHeaders),
      request.getStringData
    )
  }
}

case class HttpResponse(
                         contentType: String = "",
                         headers: Map[String, String] = Map(),
                         body: String = "",
                         statusCode: Int = 200,
                         statusText: String = ""
                       )

object HttpResponse {

  def fromResponse(response: Response): HttpResponse = {
    //response body contains new line characters
    HttpResponse(
      response.getContentType,
      getHeadersAsMap(response.getHeaders),
      response.getResponseBody,
      response.getStatusCode,
      response.getStatusText
    )
  }
}

case class HttpResult(request: HttpRequest = HttpRequest(), response: HttpResponse = HttpResponse())

object HttpResult {

  def fromRequest(request: Request): HttpResult = {
    HttpResult(HttpRequest.fromRequest(request))
  }

  def fromRequestAndResponse(request: Request, response: Response): HttpResult = {
    HttpResult(HttpRequest.fromRequest(request), HttpResponse.fromResponse(response))
  }
}
