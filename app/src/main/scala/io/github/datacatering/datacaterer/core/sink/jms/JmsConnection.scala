package io.github.datacatering.datacaterer.core.sink.jms

import io.github.datacatering.datacaterer.api.model.Step
import jakarta.jms.{Connection, MessageProducer, Session}

trait JmsConnection {

  val connectionConfig: Map[String, String]
  val connection: Connection
  val session: Session

  def createConnection(): Connection

  def createMessageProducer(connection: Connection, step: Step): MessageProducer

}
