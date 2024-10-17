package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.api.model.Constants.{HTTP, JMS}
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.exception.UnsupportedRealTimeDataSourceFormat
import io.github.datacatering.datacaterer.core.sink.http.HttpSinkProcessor
import io.github.datacatering.datacaterer.core.sink.jms.JmsSinkProcessor
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import java.util.concurrent.LinkedBlockingQueue

trait SinkProcessor[T] {

  var connectionConfig: Map[String, String]
  var step: Step

  def createConnection(connectionConfig: Map[String, String], step: Step): T

  def pushRowToSink(row: Row): Unit

  def close: Unit
}

trait RealTimeSinkProcessor[T] extends SinkProcessor[T] with Serializable {

  private var hasInit: Boolean = false
  lazy val connectionPool: LinkedBlockingQueue[T] = new LinkedBlockingQueue[T]()
  lazy val maxPoolSize: Int = 2

  val expectedSchema: Map[String, String]

  def validate(schema: StructType): Unit = {
    schema.fields.foreach(field => {
      expectedSchema.get(field.name).foreach(fieldType =>
        assert(field.dataType.sql.equalsIgnoreCase(fieldType), s"Unexpected field type for real time data source, " +
          s"name=${field.name}, type=${field.dataType.sql.toLowerCase}, expected-type=$fieldType")
      )
    })
  }

  def init(connectionConfig: Map[String, String], step: Step): Unit = {
    while (connectionPool.size() < maxPoolSize) {
      connectionPool.put(createConnection(connectionConfig, step))
    }
    hasInit = true
  }

  def createConnections(connectionConfig: Map[String, String], step: Step): SinkProcessor[_]

  def getConnectionFromPool: T = {
    if (connectionPool.size() == 0 && !hasInit) {
      init(connectionConfig, step)
    }
    connectionPool.take()
  }

  def returnConnectionToPool(connection: T): Unit = {
    connectionPool.put(connection)
  }

}

object SinkProcessor {

  def getConnection(format: String, connectionConfig: Map[String, String], step: Step): SinkProcessor[_] = {
    format match {
      case HTTP => HttpSinkProcessor.createConnections(connectionConfig, step)
      case JMS => JmsSinkProcessor.createConnections(connectionConfig, step)
      case x => throw UnsupportedRealTimeDataSourceFormat(x)
    }
  }

  def validateSchema(format: String, schema: StructType): Unit = {
    format match {
      case HTTP => HttpSinkProcessor.validate(schema)
      case JMS => JmsSinkProcessor.validate(schema)
      case _ => //do nothing
    }
  }
}