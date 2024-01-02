package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Step
import org.apache.spark.sql.Row

trait SinkProcessor[T] {

  var connectionConfig: Map[String, String]
  var step: Step

  def createConnection(connectionConfig: Map[String, String], step: Step): T

  def pushRowToSink(row: Row): Unit

  def close: Unit
}
