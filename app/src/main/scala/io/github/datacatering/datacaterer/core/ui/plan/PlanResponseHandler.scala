package io.github.datacatering.datacaterer.core.ui.plan

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object PlanResponseHandler {

  // Trait defining successful and failure responses
  sealed trait Response

  case object OK extends Response

  final case class KO(reason: String, throwable: Throwable) extends Response

  def apply(): Behavior[Response] = Behaviors.receive {
    case (ctx, OK) =>
      ctx.log.info("Plan is successful")
      Behaviors.same
    case (ctx, KO(reason, throwable)) =>
      throwable.printStackTrace()
      ctx.log.error(s"Plan failed, reason=$reason")
      Behaviors.same
  }

}
