package io.github.datacatering.datacaterer.core.sink.jms.rabbitmq

import com.rabbitmq.jms.admin.RMQConnectionFactory
import io.github.datacatering.datacaterer.api.model.Constants.{JMS_DESTINATION_NAME, JMS_VIRTUAL_HOST, PASSWORD, URL, USERNAME}
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.sink.jms.JmsConnection
import jakarta.jms.{Connection, MessageProducer, Session}

class RabbitmqJmsConnection(override val connectionConfig: Map[String, String]) extends JmsConnection {

  override def createConnection(): Connection = {
    val factory = createFactory
    val connection = factory.createConnection()
    connection.start()
    connection
  }

  override def createMessageProducer(connection: Connection, session: Session, step: Step): MessageProducer = {
    val jmsDestination = session.createQueue(step.options(JMS_DESTINATION_NAME))
    session.createProducer(jmsDestination)
  }

  def createFactory: RMQConnectionFactory = {
    val factory = new RMQConnectionFactory()
    factory.setUsername(connectionConfig(USERNAME))
    factory.setPassword(connectionConfig(PASSWORD))
    factory.setVirtualHost(connectionConfig(JMS_VIRTUAL_HOST))
    val url = connectionConfig(URL)
    //url in format amqp://localhost:5672
    val splitUrl = url.split(":")
    if (splitUrl.length != 3) {
      throw new IllegalArgumentException(s"Invalid URL format for RabbitMQ connection, expected format: amqp://localhost:5672, url=$url")
    }
    val host = splitUrl(1).replace("//", "")
    val port = splitUrl(2)
    factory.setHost(host)
    factory.setPort(port.toInt)
    factory
  }
}
