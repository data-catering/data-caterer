package io.github.datacatering.datacaterer.core.sink.jms

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_RABBITMQ_CONNECTION_FACTORY, DEFAULT_REAL_TIME_HEADERS_DATA_TYPE, DEFAULT_SOLACE_CONNECTION_FACTORY, JMS_CONNECTION_FACTORY, REAL_TIME_BODY_FIELD, REAL_TIME_HEADERS_FIELD, REAL_TIME_PARTITION_FIELD}
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.exception.{FailedJmsMessageCreateException, FailedJmsMessageGetBodyException, FailedJmsMessageSendException}
import io.github.datacatering.datacaterer.core.model.RealTimeSinkResult
import io.github.datacatering.datacaterer.core.sink.jms.jndi.JndiJmsConnection
import io.github.datacatering.datacaterer.core.sink.jms.rabbitmq.RabbitmqJmsConnection
import io.github.datacatering.datacaterer.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import io.github.datacatering.datacaterer.core.util.RowUtil.getRowValue
import jakarta.jms.{Connection, MessageProducer, Session, TextMessage}
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType}

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object JmsSinkProcessor extends RealTimeSinkProcessor[(MessageProducer, Session, Connection)] {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _


  override val expectedSchema: Map[String, String] = Map(
    REAL_TIME_BODY_FIELD -> StringType.typeName,
    REAL_TIME_PARTITION_FIELD -> IntegerType.typeName,
    REAL_TIME_HEADERS_FIELD -> DEFAULT_REAL_TIME_HEADERS_DATA_TYPE
  )

  override def pushRowToSink(row: Row): RealTimeSinkResult = {
    val body = tryGetBody(row)
    val (messageProducer, session, connection) = getConnectionFromPool
    val message = tryCreateMessage(body, messageProducer, session, connection)
    trySendMessage(row, messageProducer, session, connection, message)
    RealTimeSinkResult()
  }

  private def trySendMessage(row: Row, messageProducer: MessageProducer, session: Session, connection: Connection, message: TextMessage): Unit = {
    setAdditionalMessageProperties(row, message)
    Try(messageProducer.send(message)) match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to send JMS message, step-name=${step.name}, step-type=${step.`type`}", exception)
        returnConnectionToPool((messageProducer, session, connection))
        throw FailedJmsMessageSendException(exception)
      case Success(_) => //do nothing
    }
    returnConnectionToPool((messageProducer, session, connection))
  }

  private def tryCreateMessage(body: String, messageProducer: MessageProducer, session: Session, connection: Connection): TextMessage = {
    Try(session.createTextMessage(body)) match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to create JMS text message from $REAL_TIME_BODY_FIELD field, " +
          s"step-name=${step.name}, step-type=${step.`type`}", exception)
        returnConnectionToPool((messageProducer, session, connection))
        throw FailedJmsMessageCreateException(exception)
      case Success(value) => value
    }
  }

  private def tryGetBody(row: Row): String = {
    Try(row.getAs[String](REAL_TIME_BODY_FIELD)) match {
      case Failure(exception) =>
        LOGGER.error(s"Required field name not defined in schema, required-field=$REAL_TIME_BODY_FIELD")
        throw FailedJmsMessageGetBodyException(exception)
      case Success(value) => value
    }
  }

  private def setAdditionalMessageProperties(row: Row, message: TextMessage): Unit = {
    val jmsPriority = getRowValue(row, REAL_TIME_PARTITION_FIELD, 4)
    message.setJMSPriority(jmsPriority)
    val properties = getRowValue[mutable.WrappedArray[Row]](row, REAL_TIME_HEADERS_FIELD, mutable.WrappedArray.empty[Row])
      .map(row => {
        row.getAs[String]("key") -> row.getAs[Array[Byte]]("value")
      })
    properties.foreach(property => message.setStringProperty(property._1, new String(property._2, StandardCharsets.UTF_8)))
  }

  override def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    this.connectionConfig = connectionConfig
    this.step = step
    init(connectionConfig, step)
    this
  }

  def createConnections(messageProducer: MessageProducer, session: Session, connection: Connection, step: Step): SinkProcessor[_] = {
    connectionPool.clear()
    connectionPool.put((messageProducer, session, connection))
    this.step = step
    this
  }

  def createConnection(connectionConfig: Map[String, String], step: Step): (MessageProducer, Session, Connection) = {
    val jmsConnection = connectionConfig(JMS_CONNECTION_FACTORY) match {
      case DEFAULT_SOLACE_CONNECTION_FACTORY => new JndiJmsConnection(connectionConfig)
      case DEFAULT_RABBITMQ_CONNECTION_FACTORY => new RabbitmqJmsConnection(connectionConfig)
      case _ => throw new IllegalArgumentException(s"Unsupported JMS connection factory, factory=${connectionConfig(JMS_CONNECTION_FACTORY)}")
    }
    val connection = jmsConnection.createConnection()
    val messageProducer = jmsConnection.createMessageProducer(connection, step)
    (messageProducer, jmsConnection.session, connection)
  }

  def close: Unit = {
    //TODO hack to wait for all producers to be finished, hard to know as connections are used across all partitions
    //    Thread.sleep(1000)
    //    while (connectionPool.size() > 0) {
    //      val (messageProducer, session, connection) = connectionPool.take()
    //      messageProducer.close()
    //      connection.close()
    //      session.close()
    //    }
  }
}