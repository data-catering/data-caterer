package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.api.model.Constants.FORMAT
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.model.{Connection, GetConnectionsResponse, SaveConnectionsRequest}
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}

import java.nio.file.{Files, Path, StandardOpenOption}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.{Failure, Success, Try}


object ConnectionRepository {

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
    
    // First try to get from saved connections file
    val connectionFile = Path.of(s"$connectionSaveFolder/$name.csv")
    val tryConnectionFromFile = if (connectionFile.toFile.exists()) {
      Try(Connection.fromString(Files.readString(connectionFile), name, masking))
    } else {
      Failure(new IllegalArgumentException(s"Connection file not found: $name"))
    }
    
    tryConnectionFromFile match {
      case Success(connection) => connection.copy(options = connection.options)
      case Failure(fileException) =>
        // If not found in file, try to get from application.conf
        LOGGER.debug(s"Connection not found in file, checking application.conf, connection-name=$name")
        Try(getConnectionFromConfig(name, masking)) match {
          case Success(conn) => conn
          case Failure(configException) =>
            // If not found in either location, throw the original exception
            throw new IllegalArgumentException(s"Connection not found: $name", fileException)
        }
    }
  }
  
  /**
   * Get connection details from application.conf
   */
  private def getConnectionFromConfig(name: String, masking: Boolean = true): Connection = {
    val connectionConfigs = ConfigParser.connectionConfigsByName
    connectionConfigs.get(name) match {
      case Some(config) =>
        val format = config.getOrElse(FORMAT, "unknown")
        val options = if (masking) {
          config.map {
            case (key, value) if key.contains("password") || key.contains("token") => (key, "***")
            case (key, value) => (key, value)
          }
        } else {
          config
        }
        Connection(name, format, Some("data-source"), options - FORMAT)
      case None =>
        throw new IllegalArgumentException(s"Connection not found in application.conf: $name")
    }
  }

  private def getAllConnections(optConnectionGroupType: Option[String], masking: Boolean = true): GetConnectionsResponse = {
    LOGGER.debug(s"Getting all connection details, connection-group=${optConnectionGroupType.getOrElse("")}")
    
    // Get connections from files
    val connectionPath = Path.of(connectionSaveFolder)
    if (!connectionPath.toFile.exists()) connectionPath.toFile.mkdirs()
    val fileConnections = Files.list(connectionPath)
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
    
    // Get connections from application.conf
    val configConnections = getConnectionsFromConfig(masking)
    
    // Merge connections, with file connections taking priority (deduplicating by name)
    val allConnections = (fileConnections ++ configConnections)
      .groupBy(_.name)
      .map(_._2.head) // Take first occurrence (file connection if exists, otherwise config)
      .toList
      .filter(conn => optConnectionGroupType.forall(conn.groupType.contains))
      .sortBy(_.name)
    
    GetConnectionsResponse(allConnections)
  }
  
  /**
   * Get all connections from application.conf
   */
  private def getConnectionsFromConfig(masking: Boolean = true): List[Connection] = {
    val connectionConfigs = ConfigParser.connectionConfigsByName
    connectionConfigs.map { case (name, config) =>
      val format = config.getOrElse(FORMAT, "unknown")
      val options = if (masking) {
        config.map {
          case (key, value) if key.contains("password") || key.contains("token") => (key, "***")
          case (key, value) => (key, value)
        }
      } else {
        config
      }
      Connection(name, format, Some("data-source"), options - FORMAT)
    }.toList
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
