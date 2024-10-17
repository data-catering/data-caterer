package io.github.datacatering.datacaterer.core.sink.http

import io.github.datacatering.datacaterer.api.model.Constants.{PASSWORD, USERNAME}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpSinkProcessorTest extends AnyFunSuite with MockFactory {

  private val headerStruct = StructType(Seq(StructField("key", StringType), StructField("value", StringType)))
  private val connectionConfig = Map(USERNAME -> "admin", PASSWORD -> "password")

//  test("Can send HTTP POST request with body and headers") {
//    val mockHttp = mock[Http]
//    val headersArray = new mutable.WrappedArrayBuilder[Row](ClassTag(classOf[Row]))
//    val header1 = new GenericRowWithSchema(Array("id", "id123"), headerStruct)
//    val header2 = new GenericRowWithSchema(Array("type", "account"), headerStruct)
//    headersArray += header1
//    headersArray += header2
//    val mockRow = mock[Row]
//    (mockRow.schema _).expects().anyNumberOfTimes().returns(StructType(Seq(
//      StructField(REAL_TIME_URL_COL, StringType),
//      StructField(REAL_TIME_BODY_COL, StringType),
//      StructField(REAL_TIME_METHOD_COL, StringType),
//      StructField(REAL_TIME_CONTENT_TYPE_COL, StringType),
//      StructField(REAL_TIME_HEADERS_COL, ArrayType(StructType(Seq(
//        StructField("key", StringType),
//        StructField("value", StringType)
//      ))))
//    )))
//    (mockRow.getAs[String](_: String)).expects(REAL_TIME_URL_COL).once().returns("http://localhost:8080/customer")
//    (mockRow.getAs[String](_: String)).expects(REAL_TIME_BODY_COL).once().returns("""{"name":"peter"}""")
//    (mockRow.getAs[String](_: String)).expects(REAL_TIME_METHOD_COL).once().returns("POST")
//    (mockRow.getAs[mutable.WrappedArray[Row]](_: String)).expects(REAL_TIME_HEADERS_COL).once().returns(headersArray.result())
//    (mockRow.getAs[String](_: String)).expects(REAL_TIME_CONTENT_TYPE_COL).once().returns("application/json")
//
//    val res = HttpSinkProcessor.createHttpRequest(mockRow, Some(connectionConfig)).toRequest
//
//    assert(res.getUrl == "http://localhost:8080/customer")
//    assert(res.getMethod == "POST")
//    val headers = res.getHeaders
//    assert(headers.size() == 4)
//    assert(headers.get("id") == "id123")
//    assert(headers.get("type") == "account")
//    assert(headers.get("Authorization") == "Basic YWRtaW46cGFzc3dvcmQ=")
//    assert(headers.get("Content-Type") == "application/json; charset=UTF-8")
//  }
//
//  test("Can push data to HTTP endpoint using defaults") {
//    val mockHttp = mock[Http]
//    val mockRow = mock[Row]
//    val step = Step("step1", "json", Count(), Map(), Schema(None))
//    val httpSinkProcessor = HttpSinkProcessor.createConnections(connectionConfig, step, mockHttp)
//    val mockResp = mock[Response]
//    val futureResp = Future.successful(mockResp)
//
//    (mockRow.schema _).expects().anyNumberOfTimes().returns(StructType(Seq(StructField(REAL_TIME_URL_COL, StringType))))
//    (mockHttp.apply(_: Req)(_: ExecutionContext)).expects(*, *).returns(futureResp).once()
//    (mockRow.getAs[String](_: String)).expects(REAL_TIME_URL_COL).once().returns("http://localhost:8080/help")
//    httpSinkProcessor.pushRowToSink(mockRow)
//  }
//
//  test("Still able to proceed even when exception occurs during HTTP call") {
//    val mockHttp = mock[Http]
//    val mockRow = mock[Row]
//    val step = Step("step1", "json", Count(), Map(), Schema(None))
//    val httpSinkProcessor = HttpSinkProcessor.createConnections(connectionConfig, step, mockHttp)
//    val futureResp = Future.failed(new Throwable())
//
//    (mockRow.schema _).expects().anyNumberOfTimes().returns(StructType(Seq(StructField(REAL_TIME_URL_COL, StringType))))
//    (mockHttp.apply(_: Req)(_: ExecutionContext)).expects(*, *).returns(futureResp).once()
//    (mockRow.getAs[String](_: String)).expects(REAL_TIME_URL_COL).once().returns("http://localhost:8080/help")
//    httpSinkProcessor.pushRowToSink(mockRow)
//  }
}


case class Header(key: String, value: String)
