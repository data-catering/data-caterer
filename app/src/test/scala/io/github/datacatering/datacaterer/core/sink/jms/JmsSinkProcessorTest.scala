package io.github.datacatering.datacaterer.core.sink.jms

import io.github.datacatering.datacaterer.api.model.Constants.{REAL_TIME_BODY_FIELD, REAL_TIME_HEADERS_FIELD, REAL_TIME_PARTITION_FIELD, REAL_TIME_URL_FIELD}
import io.github.datacatering.datacaterer.api.model.{Count, Field, Step}
import io.github.datacatering.datacaterer.core.util.SparkSuite
import jakarta.jms.{Connection, Message, MessageProducer, Session, TextMessage}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, StringType, StructField, StructType}
import org.scalamock.scalatest.MockFactory
import org.scalamock.util.Defaultable

import java.util
import scala.collection.mutable

class JmsSinkProcessorTest extends SparkSuite with MockFactory {

  private val mockConnection = mock[Connection]
  private val basicFields = List(Field(REAL_TIME_BODY_FIELD))
  private val step = Step("step1", "json", Count(), Map(), basicFields)
  private val baseFieldsForStruct = Seq(
    StructField(REAL_TIME_BODY_FIELD, StringType),
    StructField(REAL_TIME_URL_FIELD, StringType),
    StructField(REAL_TIME_PARTITION_FIELD, IntegerType)
  )
  private val baseStruct = StructType(baseFieldsForStruct)
  private val headerKeyValueStruct = StructType(Seq(
    StructField("key", StringType),
    StructField("value", BinaryType)
  ))
  private val headerField = StructField(REAL_TIME_HEADERS_FIELD, ArrayType(headerKeyValueStruct))
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
    val fields = basicFields ++ List(Field(REAL_TIME_PARTITION_FIELD))
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = JmsSinkProcessor.createConnections(mockMessageProducer, mockSession, mockConnection, step.copy(fields = fields))

    val mockRow = new GenericRowWithSchema(Array("some_value", "url", 1), baseStruct)
    val mockMessage = mock[TestTextMessage]
    (mockSession.createTextMessage(_: String)).expects("some_value").once().returns(mockMessage)
    (mockMessage.setJMSPriority(_: Int)).expects(1).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
  }

  test("Given a headers field defined, set the message properties") {
    val fields = basicFields ++ List(Field(REAL_TIME_HEADERS_FIELD))
    val mockSession = mock[Session]
    val mockMessageProducer = mock[MessageProducer]
    val jmsSinkProcessor = JmsSinkProcessor.createConnections(mockMessageProducer, mockSession, mockConnection, step.copy(fields = fields))

    val innerRow = new GenericRowWithSchema(Array("account-id", "abc123".getBytes), headerKeyValueStruct)
    val mockRow = new GenericRowWithSchema(Array("some_value", "url", 4, mutable.WrappedArray.make(Array(innerRow))), structWithHeader)
    val mockMessage = mock[TestTextMessage]
    (mockSession.createTextMessage(_: String)).expects("some_value").once().returns(mockMessage)
    (mockMessage.setStringProperty(_: String, _: String)).expects("account-id", "abc123").once()
    (mockMessage.setJMSPriority(_: Int)).expects(4).once()
    (mockMessageProducer.send(_: Message)).expects(*).once()
    jmsSinkProcessor.pushRowToSink(mockRow)
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
