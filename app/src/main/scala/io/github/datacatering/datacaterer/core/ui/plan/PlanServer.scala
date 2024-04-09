package io.github.datacatering.datacaterer.core.ui.plan

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{Behavior, PostStop}
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


  def apply(): Behavior[Message] = Behaviors.setup { ctx =>
    implicit val system = ctx.system

    val planRepository = ctx.spawn(PlanRepository(), "PlanRepository")
    val planResponseHandler = ctx.spawn(PlanResponseHandler(), "PlanResponseHandler")
    val connectionRepository = ctx.spawn(ConnectionRepository(), "ConnectionRepository")
    val routes = new PlanRoutes(planRepository, planResponseHandler, connectionRepository)

    //TODO should check if port 9898 is available, try other ports if not available
    val server = Http().newServerAt("0.0.0.0", 9898).bind(routes.planRoutes)

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
          if (Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Desktop.Action.BROWSE)) {
            Desktop.getDesktop.browse(new URI(server))
          }
          if (wasStopped) ctx.self ! Stop
          running(binding)
        case Stop =>
          // we got a stop message but haven't completed starting yet,
          // we cannot stop until starting has completed
          starting(wasStopped = true)
      }

    starting(wasStopped = false)
  }

}
