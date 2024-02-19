package io.github.datacatering.datacaterer.core.ui.plan

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, Behavior}
import io.github.datacatering.datacaterer.core.model.Constants.{FAILED, FINISHED, PARSED_PLAN, STARTED}
import io.github.datacatering.datacaterer.core.plan.PlanProcessor
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

  private val connectionSaveFolder = "/tmp/data-caterer/connection"

  def apply(): Behavior[ConnectionCommand] = Behaviors.receiveMessage {
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
      Behaviors.same
  }

  private def saveConnection(connection: Connection): Unit = {
    Path.of(connectionSaveFolder).toFile.mkdirs()
    val connectionFile = Path.of(s"$connectionSaveFolder/${connection.name}.csv")
    Files.writeString(connectionFile, connection.toString, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
  }

  def getConnection(name: String): Connection = {
    val connectionFile = Path.of(s"$connectionSaveFolder/$name.csv")
    Connection.fromString(Files.readString(connectionFile))
  }

  private def getAllConnections: GetConnectionsResponse = {
    val connectionPath = Path.of(connectionSaveFolder)
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
}
