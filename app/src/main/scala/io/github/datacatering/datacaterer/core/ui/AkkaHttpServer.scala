package io.github.datacatering.datacaterer.core.ui

import akka.actor.typed.ActorSystem
import io.github.datacatering.datacaterer.core.ui.plan.PlanServer

object AkkaHttpServer extends App {

  val system: ActorSystem[PlanServer.Message] = ActorSystem(PlanServer(), "BuildPlanServer")

}

