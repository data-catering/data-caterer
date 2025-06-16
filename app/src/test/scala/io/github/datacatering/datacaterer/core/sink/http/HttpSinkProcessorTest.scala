package io.github.datacatering.datacaterer.core.sink.http

import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.model.RealTimeSinkResult
import io.github.datacatering.datacaterer.core.util.SparkSuite
import io.netty.handler.codec.http.DefaultHttpHeaders
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField, StructType}
import org.asynchttpclient.uri.Uri
import org.asynchttpclient.{AsyncHttpClient, BoundRequestBuilder, ClientStats, HostStats, ListenableFuture, Request, Response}
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture

class HttpSinkProcessorTest extends SparkSuite with Matchers with MockFactory {

  val mockHttpClient: AsyncHttpClient = mock[AsyncHttpClient]
  val mockRequest: Request = mock[Request]
  val mockResponse: Response = mock[Response]
  val myHeaderMetadata = new MetadataBuilder().putString("header", "X-my-header").build()
  val schema = StructType(Seq(
    StructField("url", StringType),
    StructField("method", StringType),
    StructField("body", StringType),
    StructField("headerX-my-header", StringType, true, myHeaderMetadata)
  ))

  test("pushRowToSink should return RealTimeSinkResult for successful request") {
    val url = "http://example.com"
    val method = "POST"
    val body = "{\"key\":\"value\"}"
    val header = "hello world"

    val mockBoundRequest = mock[BoundRequestBuilder]
    val mockRequest = mock[Request]
    val mockListenableResponse = mock[ListenableFuture[Response]]

    (() => mockHttpClient.isClosed).expects().once().returns(false)
    (mockHttpClient.prepare(_: String, _: String)).expects(method, url).returns(mockBoundRequest)
    (mockBoundRequest.setBody(_: String)).expects(*).once().returns(mockBoundRequest)
    (mockBoundRequest.addHeader(_: CharSequence, _: String)).expects(*, *).once().returns(mockBoundRequest)
    (() => mockBoundRequest.build()).expects().once().returns(mockRequest)
    (mockHttpClient.executeRequest(_: Request)).expects(mockRequest).once().returns(mockListenableResponse)
    (() => mockListenableResponse.toCompletableFuture).expects().once().returns(CompletableFuture.completedFuture(mockResponse))

    val httpHeaders = new DefaultHttpHeaders()
    (() => mockRequest.getUrl).expects().once().returns(url)
    (() => mockRequest.getUri).expects().once().returns(Uri.create(url))
    (() => mockRequest.getMethod).expects().twice().returns(method)
    (() => mockRequest.getHeaders).expects().once().returns(httpHeaders)
    (() => mockRequest.getStringData).expects().once().returns(body)

    (() => mockResponse.getHeaders).expects().once().returns(httpHeaders)
    (() => mockResponse.getContentType).expects().once().returns("application/json")
    (() => mockResponse.getResponseBody).expects().once().returns("")
    (() => mockResponse.getStatusCode).expects().once().returns(200)
    (() => mockResponse.getStatusText).expects().once().returns("")

    val rdd = sparkSession.sparkContext.parallelize(Seq(Row(url, method, body, header)))
    val row = sparkSession.createDataFrame(rdd, schema).head()

    val processor = HttpSinkProcessor.createConnections(Map.empty, Step(), mockHttpClient)
    val result = processor.pushRowToSink(row)

    result shouldBe a[RealTimeSinkResult]
  }

  test("close should close HTTP client after retries") {
    val clientStats = new ClientStats(java.util.Map.of("", new HostStats(0, 0)))
    (() => mockHttpClient.getClientStats).expects().twice().returns(clientStats)
    (() => mockHttpClient.close()).expects().once()

    val processor = HttpSinkProcessor.createConnections(Map.empty, Step(), mockHttpClient)
    processor.close
  }
}


case class HttpRequestRow(url: String, method: String, body: String)
