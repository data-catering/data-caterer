package io.github.datacatering.datacaterer.core.ui

import io.github.datacatering.datacaterer.core.ui.plan.PlanServer
import org.apache.pekko.actor.typed.ActorSystem

object DataCatererUI extends App {

  val system: ActorSystem[PlanServer.Message] = ActorSystem(PlanServer(), "BuildPlanServer")

}

