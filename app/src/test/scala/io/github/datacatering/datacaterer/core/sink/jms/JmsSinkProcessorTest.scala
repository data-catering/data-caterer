package io.github.datacatering.datacaterer.core.sink.jms

import io.github.datacatering.datacaterer.api.model.{Count, Field, Schema, Step}
import io.github.datacatering.datacaterer.core.model.Constants.{REAL_TIME_BODY_COL, REAL_TIME_HEADERS_COL, REAL_TIME_PARTITION_COL, REAL_TIME_URL_COL}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalamock.util.Defaultable
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.util
import javax.jms.{Connection, Message, MessageProducer, Session, TextMessage}
import javax.naming.Context
import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class JmsSinkProcessorTest extends SparkSuite with MockFactory {

  private val mockConnection = mock[Connection]
  private val basicFields = List(Field(REAL_TIME_BODY_COL))
  private val step = Step("step1", "json", Count(), Map(), Schema(Some(basicFields)))
  private val baseFieldsForStruct = Seq(
    StructField(REAL_TIME_BODY_COL, StringType),
    StructField(REAL_TIME_URL_COL, StringType),
    StructField(REAL_TIME_PARTITION_COL, IntegerType)
  )
  private val baseStruct = StructType(baseFieldsForStruct)
  private val headerKeyValueStruct = StructType(Seq(
    StructField("key", StringType),
    StructField("value", BinaryType)
  ))
  private val headerField = StructField(REAL_TIME_HEADERS_COL, ArrayType(headerKeyValueStruct))
  private val structWithHeader = StructType(baseFieldsForStruct ++ Seq(headerField))

  implicit val d: Defaultable[util.Enumeration[_]] = new Defaultable[java.util.Enumeration[_]] {
    override val default: util.Enumeration[_] = null
  }

  test("Push value as a basic text message") {
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = JmsSinkProcessor.createConnections(mockMessageProducer, mockSession, mockConnection, step)

    val mockRow = new GenericRowWithSchema(Array("some_value", "url", 4), baseStruct)
    val mockMessage = mock[TestTextMessage]
    (mockSession.createTextMessage(_: String)).expects("some_value").once().returns(mockMessage)
    (mockMessage.setJMSPriority(_: Int)).expects(4).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Given a partition field defined, set the message priority based on the partition field value") {
    val fields = basicFields ++ List(Field(REAL_TIME_PARTITION_COL))
    val schema = Schema(Some(fields))
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = JmsSinkProcessor.createConnections(mockMessageProducer, mockSession, mockConnection, step.copy(schema = schema))

    val mockRow = new GenericRowWithSchema(Array("some_value", "url", 1), baseStruct)
    val mockMessage = mock[TestTextMessage]
    (mockSession.createTextMessage(_: String)).expects("some_value").once().returns(mockMessage)
    (mockMessage.setJMSPriority(_: Int)).expects(1).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Given a headers field defined, set the message properties") {
    val fields = basicFields ++ List(Field(REAL_TIME_HEADERS_COL))
    val schema = Schema(Some(fields))
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = JmsSinkProcessor.createConnections(mockMessageProducer, mockSession, mockConnection, step.copy(schema = schema))

    val innerRow = new GenericRowWithSchema(Array("account-id", "abc123".getBytes), headerKeyValueStruct)
    val mockRow = new GenericRowWithSchema(Array("some_value", "url", 4, mutable.WrappedArray.make(Array(innerRow))), structWithHeader)
    val mockMessage = mock[TestTextMessage]
    (mockSession.createTextMessage(_: String)).expects("some_value").once().returns(mockMessage)
    (mockMessage.setStringProperty(_: String, _: String)).expects("account-id", "abc123").once()
    (mockMessage.setJMSPriority(_: Int)).expects(4).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Can get all required connection properties for JMS") {
    val connectionConfig = Map(
      "initialContextFactory" -> "com.solacesystems.jndi.SolJNDIInitialContextFactory",
      "url" -> "smf://localhost:55555",
      "vpnName" -> "default",
      "user" -> "admin",
      "password" -> "admin"
    )

    val res = JmsSinkProcessor.getConnectionProperties(connectionConfig)
    assertResult(4)(res.size())
    assert(res.containsKey(Context.INITIAL_CONTEXT_FACTORY))
    assertResult(connectionConfig("initialContextFactory"))(res.getProperty(Context.INITIAL_CONTEXT_FACTORY))
    assert(res.containsKey(Context.SECURITY_PRINCIPAL))
    assertResult(connectionConfig("user") + "@" + connectionConfig("vpnName"))(res.getProperty(Context.SECURITY_PRINCIPAL))
    assert(res.containsKey(Context.SECURITY_CREDENTIALS))
    assertResult(connectionConfig("password"))(res.getProperty(Context.SECURITY_CREDENTIALS))
    assert(res.containsKey(Context.PROVIDER_URL))
    assertResult(connectionConfig("url"))(res.getProperty(Context.PROVIDER_URL))
  }

  test("Throw exception when incomplete connection configuration provided") {
    assertThrows[RuntimeException](JmsSinkProcessor.createConnections(Map(), step))
  }

}

//reference: https://github.com/paulbutcher/ScalaMock/issues/86
trait TestTextMessage extends TextMessage {
  override def isBodyAssignableTo(c: Class[_]): Boolean = this.isBodyAssignableTo(c)
}

case class MockRow(value: String = "some_value", url: String = "base_url", partition: String = "1", headers: Array[(String, Array[Byte])] = Array())
