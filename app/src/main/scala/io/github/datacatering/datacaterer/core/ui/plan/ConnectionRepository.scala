package io.github.datacatering.datacaterer.core.ui.plan

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import io.github.datacatering.datacaterer.core.model.Constants.{FAILED, FINISHED, PARSED_PLAN, STARTED}
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration.INSTALL_DIRECTORY
import io.github.datacatering.datacaterer.core.ui.mapper.UiMapper
import io.github.datacatering.datacaterer.core.ui.model.{Connection, GetConnectionsResponse, JsonSupport, PlanRunExecution, PlanRunRequest, SaveConnectionsRequest}
import io.github.datacatering.datacaterer.core.ui.plan.PlanResponseHandler.{KO, OK, Response}
import org.apache.log4j.Logger
import org.joda.time.DateTime

import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID
import scala.collection.JavaConverters.{asScalaIteratorConverter, iterableAsScalaIterableConverter}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}


object ConnectionRepository extends JsonSupport {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /*
  Create and get connection details
   */
  sealed trait ConnectionCommand

  final case class SaveConnections(saveConnectionsRequest: SaveConnectionsRequest) extends ConnectionCommand

  final case class GetConnection(name: String, replyTo: ActorRef[Connection]) extends ConnectionCommand

  final case class GetConnections(replyTo: ActorRef[GetConnectionsResponse]) extends ConnectionCommand

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
        case GetConnections(replyTo) =>
          replyTo ! getAllConnections
          Behaviors.same
        case RemoveConnection(name) =>
          removeConnection(name)
          Behaviors.same
      }
    }.onFailure(SupervisorStrategy.restart)
  }

  private def saveConnection(connection: Connection): Unit = {
    LOGGER.debug(s"Saving connection, connection-name=${connection.name}, connection-type=${connection.`type`}")
    Path.of(connectionSaveFolder).toFile.mkdirs()
    val connectionFile = Path.of(s"$connectionSaveFolder/${connection.name}.csv")
    Files.writeString(connectionFile, connection.toString, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def getConnection(name: String): Connection = {
    LOGGER.debug(s"Getting connection details, connection-name=$name")
    val connectionFile = Path.of(s"$connectionSaveFolder/$name.csv")
    val connection = Connection.fromString(Files.readString(connectionFile))
    val mappedPw = connection.options.map(o => if (o._1 == "password") (o._1, "***") else o)
    connection.copy(options = mappedPw)
  }

  private def getAllConnections: GetConnectionsResponse = {
    LOGGER.debug(s"Getting all connection details")
    val connectionPath = Path.of(connectionSaveFolder)
    if (!connectionPath.toFile.exists()) connectionPath.toFile.mkdirs()
    val connections = Files.list(connectionPath)
      .iterator()
      .asScala
      .map(file => {
        val tryParse = Try(Connection.fromString(Files.readString(file)))
        if (tryParse.isFailure) {
          LOGGER.error(s"Failed to parse connection details from file, file=$file, error=${tryParse.failed.get.getMessage}")
        }
        tryParse
      })
      .filter(_.isSuccess)
      .map(_.get)
      .toList
      .sortBy(_.name)
    GetConnectionsResponse(connections)
  }

  private def removeConnection(connectionName: String): Boolean = {
    LOGGER.warn(s"Removing connections details from file system, connection-name=$connectionName")
    val connectionFile = Path.of(s"$connectionSaveFolder/$connectionName.csv").toFile
    if (connectionFile.exists()) connectionFile.delete() else false
  }
}
