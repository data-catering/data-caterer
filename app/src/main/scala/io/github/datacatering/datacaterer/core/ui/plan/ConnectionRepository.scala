package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.model.{Connection, GetConnectionsResponse, JsonSupport, SaveConnectionsRequest}
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.Try


object ConnectionRepository extends JsonSupport {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /*
  Create and get connection details
   */
  sealed trait ConnectionCommand

  final case class SaveConnections(saveConnectionsRequest: SaveConnectionsRequest) extends ConnectionCommand

  final case class GetConnection(name: String, replyTo: ActorRef[Connection]) extends ConnectionCommand

  final case class GetConnections(optConnectionGroupType: Option[String], replyTo: ActorRef[GetConnectionsResponse]) extends ConnectionCommand

  final case class RemoveConnection(name: String) extends ConnectionCommand

  private val connectionSaveFolder = s"$INSTALL_DIRECTORY/connection"

  def apply(): Behavior[ConnectionCommand] = {
    Behaviors.supervise[ConnectionCommand] {
      Behaviors.receiveMessage {
        case SaveConnections(saveConnectionsRequest) =>
          saveConnectionsRequest.connections.foreach(saveConnection)
          Behaviors.same
        case GetConnection(name, replyTo) =>
          replyTo ! getConnection(name)
          Behaviors.same
        case GetConnections(optConnectionGroupType, replyTo) =>
          replyTo ! getAllConnections(optConnectionGroupType)
          Behaviors.same
        case RemoveConnection(name) =>
          removeConnection(name)
          Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  private def saveConnection(connection: Connection): Unit = {
    LOGGER.debug(s"Saving connection, connection-name=${connection.name}, connection-type=${connection.`type`}," +
      s"connection-group=${connection.groupType}")
    val basePath = Path.of(connectionSaveFolder).toFile
    if (!basePath.exists()) basePath.mkdirs()
    val connectionFile = Path.of(s"$connectionSaveFolder/${connection.name}.csv")
    Files.writeString(connectionFile, connection.toString, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def getConnection(name: String, masking: Boolean = true): Connection = {
    LOGGER.debug(s"Getting connection details, connection-name=$name")
    val connectionFile = Path.of(s"$connectionSaveFolder/$name.csv")
    val connection = Connection.fromString(Files.readString(connectionFile), name, masking)
    connection.copy(options = connection.options)
  }

  private def getAllConnections(optConnectionGroupType: Option[String], masking: Boolean = true): GetConnectionsResponse = {
    LOGGER.debug(s"Getting all connection details, connection-group=${optConnectionGroupType.getOrElse("")}")
    val connectionPath = Path.of(connectionSaveFolder)
    if (!connectionPath.toFile.exists()) connectionPath.toFile.mkdirs()
    val connections = Files.list(connectionPath)
      .iterator()
      .asScala
      .map(file => {
        val tryParse = Try(Connection.fromString(Files.readString(file), file.toFile.getName, masking))
        if (tryParse.isFailure) {
          LOGGER.error(s"Failed to parse connection details from file, file=$file, error=${tryParse.failed.get.getMessage}")
        }
        tryParse
      })
      .filter(_.isSuccess)
      .map(_.get)
      .toList
      .filter(conn => optConnectionGroupType.forall(conn.groupType.contains))
      .sortBy(_.name)
    GetConnectionsResponse(connections)
  }

  private def removeConnection(connectionName: String): Boolean = {
    LOGGER.warn(s"Removing connections details from file system, connection-name=$connectionName")
    val connectionFile = Path.of(s"$connectionSaveFolder/$connectionName.csv").toFile
    if (connectionFile.exists()) {
      connectionFile.delete()
    } else {
      LOGGER.warn(s"Connection file does not exist, unable to delete, connection-name=$connectionName")
      false
    }
  }
}
