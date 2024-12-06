package io.github.datacatering.datacaterer.core.sink.jms

import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_REAL_TIME_HEADERS_DATA_TYPE, JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, PASSWORD, URL, USERNAME}
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.exception.{FailedJmsMessageCreateException, FailedJmsMessageGetBodyException, FailedJmsMessageSendException}
import io.github.datacatering.datacaterer.core.model.Constants.{REAL_TIME_BODY_COL, REAL_TIME_HEADERS_COL, REAL_TIME_PARTITION_COL}
import io.github.datacatering.datacaterer.core.model.RealTimeSinkResult
import io.github.datacatering.datacaterer.core.sink.http.model.{HttpRequest, HttpResult}
import io.github.datacatering.datacaterer.core.sink.{RealTimeSinkProcessor, SinkProcessor}
import io.github.datacatering.datacaterer.core.util.RowUtil.getRowValue
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType}

import java.nio.charset.StandardCharsets
import java.util.Properties
import javax.jms.{Connection, ConnectionFactory, Destination, MessageProducer, Session, TextMessage}
import javax.naming.{Context, InitialContext}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object JmsSinkProcessor extends RealTimeSinkProcessor[(MessageProducer, Session, Connection)] {

  private val LOGGER = Logger.getLogger(getClass.getName)

  var connectionConfig: Map[String, String] = _
  var step: Step = _


  override val expectedSchema: Map[String, String] = Map(
    REAL_TIME_BODY_COL -> StringType.typeName,
    REAL_TIME_PARTITION_COL -> IntegerType.typeName,
    REAL_TIME_HEADERS_COL -> DEFAULT_REAL_TIME_HEADERS_DATA_TYPE
  )

  override def pushRowToSink(row: Row): RealTimeSinkResult = {
    val body = tryGetBody(row)
    val (messageProducer, session, connection) = getConnectionFromPool
    val message = tryCreateMessage(body, messageProducer, session, connection)
    trySendMessage(row, messageProducer, session, connection, message)
    RealTimeSinkResult("")
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
        LOGGER.error(s"Failed to create JMS text message from $REAL_TIME_BODY_COL column, " +
          s"step-name=${step.name}, step-type=${step.`type`}", exception)
        returnConnectionToPool((messageProducer, session, connection))
        throw FailedJmsMessageCreateException(exception)
      case Success(value) => value
    }
  }

  private def tryGetBody(row: Row): String = {
    Try(row.getAs[String](REAL_TIME_BODY_COL)) match {
      case Failure(exception) =>
        LOGGER.error(s"Required column name not defined in schema, required-column=$REAL_TIME_BODY_COL")
        throw FailedJmsMessageGetBodyException(exception)
      case Success(value) => value
    }
  }

  private def setAdditionalMessageProperties(row: Row, message: TextMessage): Unit = {
    val jmsPriority = getRowValue(row, REAL_TIME_PARTITION_COL, 4)
    message.setJMSPriority(jmsPriority)
    val properties = getRowValue[mutable.WrappedArray[Row]](row, REAL_TIME_HEADERS_COL, mutable.WrappedArray.empty[Row])
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
    val (connection, context) = createInitialConnection(connectionConfig)
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = context.lookup(s"${step.options(JMS_DESTINATION_NAME)}").asInstanceOf[Destination]
    val messageProducer = session.createProducer(destination)
    (messageProducer, session, connection)
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

  protected def createInitialConnection(connectionConfig: Map[String, String]): (Connection, InitialContext) = {
    val properties: Properties = getConnectionProperties(connectionConfig)
    val context = new InitialContext(properties)
    val cf = context.lookup(connectionConfig(JMS_CONNECTION_FACTORY)).asInstanceOf[ConnectionFactory]
    (cf.createConnection(), context)
  }

  def getConnectionProperties(connectionConfig: Map[String, String]): Properties = {
    val properties = new Properties()
    properties.put(Context.INITIAL_CONTEXT_FACTORY, connectionConfig(JMS_INITIAL_CONTEXT_FACTORY))
    properties.put(Context.PROVIDER_URL, connectionConfig(URL))
    properties.put(Context.SECURITY_PRINCIPAL, s"${connectionConfig(USERNAME)}@${connectionConfig(JMS_VPN_NAME)}")
    properties.put(Context.SECURITY_CREDENTIALS, connectionConfig(PASSWORD))
    val addedConfig = List(JMS_INITIAL_CONTEXT_FACTORY, URL, USERNAME, PASSWORD, JMS_VPN_NAME)
    connectionConfig.filter(x => !addedConfig.contains(x._1))
      .foreach(c => properties.put(c._1, c._2))
    properties
  }
}