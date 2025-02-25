package io.github.datacatering.datacaterer.core.sink.jms

import io.github.datacatering.datacaterer.api.model.Step
import jakarta.jms.{Connection, MessageProducer, Session}

trait JmsConnection {

  val connectionConfig: Map[String, String]

  def createConnection(): Connection

  def createMessageProducer(connection: Connection, session: Session, step: Step): MessageProducer

  def createSession(connection: Connection): Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

}
