package io.github.datacatering.datacaterer.core.ui.plan

import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration
import org.apache.log4j.Logger
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorSystem, Behavior, PostStop}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding

import java.awt.Desktop
import java.net.URI
import scala.util.{Failure, Success}

object PlanServer {

  sealed trait Message

  private final case class StartFailed(cause: Throwable) extends Message

  private final case class Started(binding: ServerBinding) extends Message

  case object Stop extends Message


  private val LOGGER = Logger.getLogger(getClass.getName)

  def apply(installDir: String = UiConfiguration.INSTALL_DIRECTORY, openBrowser: Boolean = true): Behavior[Message] = Behaviors.setup { ctx =>
    implicit val system: ActorSystem[Nothing] = ctx.system

    LOGGER.info(s"Starting server with install directory: $installDir")

    // Use unique actor names to avoid conflicts when multiple tests run concurrently
    val uniqueSuffix = java.util.UUID.randomUUID().toString.take(8)
    val planRepository = ctx.spawn(PlanRepository(installDir), s"PlanRepository-$uniqueSuffix")
    val planResponseHandler = ctx.spawn(PlanResponseHandler(), s"PlanResponseHandler-$uniqueSuffix")
    val connectionRepository = ctx.spawn(ConnectionRepository(installDir), s"ConnectionRepository-$uniqueSuffix")
    val routes = new PlanRoutes(planRepository, planResponseHandler, connectionRepository)

    // Use configurable port with fallback to 9898
    val port = Option(System.getProperty("datacaterer.ui.port"))
      .map(_.toInt)
      .getOrElse(9898)
    
    //TODO should check if port is available, try other ports if not available
    val server = Http().newServerAt("0.0.0.0", port).bind(routes.planRoutes)

    ctx.pipeToSelf(server) {
      case Failure(exception) => StartFailed(exception)
      case Success(value) => Started(value)
    }

    def running(binding: ServerBinding): Behavior[Message] =
      Behaviors.receiveMessagePartial[Message] {
        case Stop =>
          ctx.log.info("Stopping server http://{}:{}/", binding.localAddress.getHostString, binding.localAddress.getPort)
          Behaviors.stopped
      }.receiveSignal {
        case (_, PostStop) =>
          binding.unbind()
          Behaviors.same
      }

    def starting(wasStopped: Boolean): Behaviors.Receive[Message] =
      Behaviors.receiveMessage[Message] {
        case StartFailed(cause) =>
          throw new RuntimeException("Server failed to start", cause)
        case Started(binding) =>
          val server = s"http://localhost:${binding.localAddress.getPort}/"
          ctx.log.info("Server online at {}", server)
          if (openBrowser && Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Desktop.Action.BROWSE)) {
            Desktop.getDesktop.browse(new URI(server))
          }
          if (wasStopped) ctx.self ! Stop
          //startup spark with defaults
          planRepository.tell(PlanRepository.StartupSpark())
          running(binding)
        case Stop =>
          // we got a stop message but haven't completed starting yet,
          // we cannot stop until starting has completed
          starting(wasStopped = true)
      }

    starting(wasStopped = false)
  }

}
