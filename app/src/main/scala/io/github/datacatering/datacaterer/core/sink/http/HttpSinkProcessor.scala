package io.github.datacatering.datacaterer.core.sink.http

import io.github.datacatering.datacaterer.api.model.Constants.DEFAULT_REAL_TIME_HEADERS_DATA_TYPE
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.exception.{AddHttpHeaderException, FailedHttpRequestException}
import io.github.datacatering.datacaterer.core.model.Constants.{DEFAULT_HTTP_METHOD, HTTP_HEADER_COL_PREFIX, REAL_TIME_BODY_COL, REAL_TIME_HEADERS_COL, REAL_TIME_METHOD_COL, REAL_TIME_URL_COL}
import io.github.datacatering.datacaterer.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import io.github.datacatering.datacaterer.core.util.HttpUtil.getAuthHeader
import io.github.datacatering.datacaterer.core.util.RowUtil.getRowValue
import io.netty.handler.ssl.SslContextBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StringType
import org.asynchttpclient.Dsl.asyncHttpClient
import org.asynchttpclient.{AsyncHttpClient, DefaultAsyncHttpClientConfig, ListenableFuture, Request, Response}

import java.security.cert.X509Certificate
import javax.net.ssl.X509TrustManager
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object HttpSinkProcessor extends RealTimeSinkProcessor[Unit] with Serializable {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _
  var http: AsyncHttpClient = buildClient

  override val expectedSchema: Map[String, String] = Map(
    REAL_TIME_URL_COL -> StringType.typeName,
    REAL_TIME_BODY_COL -> StringType.typeName,
    REAL_TIME_METHOD_COL -> StringType.typeName,
    REAL_TIME_HEADERS_COL -> DEFAULT_REAL_TIME_HEADERS_DATA_TYPE
  )

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    this.connectionConfig = connectionConfig
    this.step = step
    this
  }

  override def createConnection(connectionConfig: Map[String, String], step: Step): Unit = {}

  def createConnections(connectionConfig: Map[String, String], step: Step, http: AsyncHttpClient): SinkProcessor[_] = {
    this.http = http
    this.connectionConfig = connectionConfig
    this.step = step
    this
  }

  override def close: Unit = {
    close(5)
  }

  @tailrec
  def close(numRetry: Int): Unit = {
    Thread.sleep(1000)
    val activeConnections = http.getClientStats.getTotalActiveConnectionCount
    val idleConnections = http.getClientStats.getTotalIdleConnectionCount
    LOGGER.debug(s"HTTP active connections: $activeConnections, idle connections: $idleConnections")
    if ((activeConnections == 0 && idleConnections == 0) || numRetry == 0) {
      LOGGER.debug(s"Closing HTTP connection now, remaining-retries=$numRetry")
      http.close()
    } else {
      LOGGER.debug(s"Waiting until 0 active and idle connections, remaining-retries=$numRetry")
      close(numRetry - 1)
    }
  }

  override def pushRowToSink(row: Row): Unit = {
    pushRowToSinkFuture(row)
  }

  private def pushRowToSinkFuture(row: Row): Unit = {
    if (http.isClosed) {
      http = buildClient
    }
    val request = createHttpRequest(row)
    Try(http.executeRequest(request)) match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to execute HTTP request, url=${request.getUri}, method=${request.getMethod}", exception)
        throw exception
      case Success(value) => handleResponse(value, request)
    }
  }

  private def handleResponse(value: ListenableFuture[Response], request: Request) = {
    value.toCompletableFuture
      .exceptionally(error => {
        LOGGER.error(s"Failed to send HTTP request, url=${request.getUri}, method=${request.getMethod}", error)
        throw error
      })
      .whenComplete((resp, error) => {
        if (error == null && resp.getStatusCode >= 200 && resp.getStatusCode < 300) {
          LOGGER.debug(s"Successful HTTP request, url=${resp.getUri}, method=${request.getMethod}, status-code=${resp.getStatusCode}, " +
            s"status-text=${resp.getStatusText}, response-body=${resp.getResponseBody}")
          //TODO can save response body along with request in file for validations, save as parquet or json?
          // save request url, request headers, request body, response status code, response status text, response body, response headers, response cookies
        } else {
          LOGGER.error(s"Failed HTTP request, url=${resp.getUri}, method=${request.getMethod}, status-code=${resp.getStatusCode}, " +
            s"status-text=${resp.getStatusText}")
          if (error == null) throw FailedHttpRequestException() else throw error
        }
      }).join()
  }

  def createHttpRequest(row: Row, connectionConfig: Option[Map[String, String]] = None): Request = {
    connectionConfig.foreach(conf => this.connectionConfig = conf)
    val httpUrl = getRowValue[String](row, REAL_TIME_URL_COL)
    val method = getRowValue[String](row, REAL_TIME_METHOD_COL, DEFAULT_HTTP_METHOD)
    val body = getRowValue[String](row, REAL_TIME_BODY_COL, "")

    val basePrepareRequest = http.prepare(method, httpUrl)
      .setBody(body)

    getHeaders(row)
      .foldLeft(basePrepareRequest)((req, header) => {
        val tryAddHeader = Try(req.addHeader(header._1, header._2))
        tryAddHeader match {
          case Failure(exception) =>
            val message = s"Failed to add header to HTTP request, exception=$exception"
            LOGGER.error(message)
            throw AddHttpHeaderException(header._1, exception)
          case Success(value) => value
        }
      })
    basePrepareRequest.build()
  }

  private def getHeaders(row: Row): Map[String, String] = {
    row.schema.fields.filter(_.name.startsWith(HTTP_HEADER_COL_PREFIX))
      .map(field => {
        val headerName = field.metadata.getString(HTTP_HEADER_COL_PREFIX)
        val fieldValue = row.getAs[Any](field.name).toString
        headerName -> fieldValue
      }).toMap ++ getAuthHeader(connectionConfig)
  }

  private def saveToFile(request: Request, response: Response): Unit = {

  }

  private def buildClient: AsyncHttpClient = {
    val trustManager = new X509TrustManager() {
      override def checkClientTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

      override def checkServerTrusted(chain: Array[X509Certificate], authType: String): Unit = {}

      override def getAcceptedIssuers: Array[X509Certificate] = Array()
    }
    val sslContext = SslContextBuilder.forClient().trustManager(trustManager).build()
    val config = new DefaultAsyncHttpClientConfig.Builder().setSslContext(sslContext)
      .setRequestTimeout(5000).build()
    asyncHttpClient(config)
  }
}
