package io.github.datacatering.datacaterer.core.sink.jms.rabbitmq

import io.github.datacatering.datacaterer.api.model.Constants.{JMS_DESTINATION_NAME, JMS_VIRTUAL_HOST, PASSWORD, URL, USERNAME}
import io.github.datacatering.datacaterer.api.model.Step
import jakarta.jms.{Connection, MessageProducer, Queue, Session}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RabbitmqJmsConnectionTest extends AnyFlatSpec with MockFactory with Matchers {

  "createConnection" should "create a valid connection with correct configuration" in {
    val connectionConfig = Map(
      USERNAME -> "user",
      PASSWORD -> "pass",
      JMS_VIRTUAL_HOST -> "/",
      URL -> "amqp://localhost:5672"
    )
    val rabbitmqJmsConnection = new RabbitmqJmsConnection(connectionConfig)
    val factory = rabbitmqJmsConnection.createFactory

    factory.getUsername shouldEqual "user"
    factory.getPassword shouldEqual "pass"
    factory.getVirtualHost shouldEqual "/"
    factory.getHost shouldEqual "localhost"
    factory.getPort shouldEqual 5672
  }

  it should "throw IllegalArgumentException for invalid URL format" in {
    val connectionConfig = Map(
      USERNAME -> "user",
      PASSWORD -> "pass",
      JMS_VIRTUAL_HOST -> "/",
      URL -> "invalid_url"
    )
    val rabbitmqJmsConnection = new RabbitmqJmsConnection(connectionConfig)

    an[IllegalArgumentException] should be thrownBy rabbitmqJmsConnection.createConnection()
  }

  "createMessageProducer" should "create a message producer for the given step" in {
    val connectionConfig = Map(
      USERNAME -> "user",
      PASSWORD -> "pass",
      JMS_VIRTUAL_HOST -> "/",
      URL -> "amqp://localhost:5672"
    )
    val rabbitmqJmsConnection = new RabbitmqJmsConnection(connectionConfig)
    val connection = mock[Connection]
    val session = mock[Session]
    val destination = mock[Queue]
    val step = Step(options = Map(JMS_DESTINATION_NAME -> "queueName"))

    (session.createQueue(_: String)).expects("queueName").once().returns(destination)
    (session.createProducer(_: Queue)).expects(destination).once().returns(mock[MessageProducer])

    val producer = rabbitmqJmsConnection.createMessageProducer(connection, session, step)

    producer should not be null
  }
}
