package io.github.datacatering.datacaterer.core.sink.jms.jndi

import io.github.datacatering.datacaterer.api.model.Constants.{JMS_CONNECTION_FACTORY, JMS_DESTINATION_NAME, JMS_INITIAL_CONTEXT_FACTORY, JMS_VPN_NAME, PASSWORD, URL, USERNAME}
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.sink.jms.JmsConnection
import jakarta.jms.{Connection, ConnectionFactory, Destination, MessageProducer, Session}

import java.util.Properties
import javax.naming.{Context, InitialContext}

class JndiJmsConnection(override val connectionConfig: Map[String, String]) extends JmsConnection {

  override lazy val connection: Connection = createConnection()
  override lazy val session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

  private var context: InitialContext = _

  override def createConnection(): Connection = {
    val (connection, context) = createInitialConnection(connectionConfig)
    this.context = context
    connection.start()
    connection
  }

  override def createMessageProducer(connection: Connection, step: Step): MessageProducer = {
    val destination = context.lookup(step.options(JMS_DESTINATION_NAME)).asInstanceOf[Destination]
    session.createProducer(destination)
  }

  private def createInitialConnection(connectionConfig: Map[String, String]): (Connection, InitialContext) = {
    val properties: Properties = getConnectionProperties
    val context = new InitialContext(properties)
    val cf = context.lookup(connectionConfig(JMS_CONNECTION_FACTORY)).asInstanceOf[ConnectionFactory]
    (cf.createConnection(), context)
  }

  def getConnectionProperties: Properties = {
    val properties = new Properties()
    properties.put(Context.INITIAL_CONTEXT_FACTORY, connectionConfig(JMS_INITIAL_CONTEXT_FACTORY))
    properties.put(Context.PROVIDER_URL, connectionConfig(URL))
    if (connectionConfig.contains(JMS_VPN_NAME)) {
      properties.put(Context.SECURITY_PRINCIPAL, s"${connectionConfig(USERNAME)}@${connectionConfig(JMS_VPN_NAME)}")
    } else {
      properties.put(Context.SECURITY_PRINCIPAL, connectionConfig(USERNAME))
    }
    properties.put(Context.SECURITY_CREDENTIALS, connectionConfig(PASSWORD))
    val addedConfig = List(JMS_INITIAL_CONTEXT_FACTORY, URL, USERNAME, PASSWORD, JMS_VPN_NAME)
    connectionConfig.filter(x => !addedConfig.contains(x._1))
      .foreach(c => properties.put(c._1, c._2))
    properties
  }
}
