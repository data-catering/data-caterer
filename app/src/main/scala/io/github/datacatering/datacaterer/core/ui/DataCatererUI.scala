package io.github.datacatering.datacaterer.core.ui

import io.github.datacatering.datacaterer.core.ui.config.UiConfiguration
import io.github.datacatering.datacaterer.core.ui.plan.PlanServer
import org.apache.pekko.actor.typed.ActorSystem

object DataCatererUI extends App {

  // Parse command-line arguments
  // Usage: DataCatererUI [--install-dir <path>] [--no-browser]
  val installDir = args.indexOf("--install-dir") match {
    case idx if idx >= 0 && idx + 1 < args.length => args(idx + 1)
    case _ => UiConfiguration.INSTALL_DIRECTORY
  }

  val openBrowser = !args.contains("--no-browser")

  val system: ActorSystem[PlanServer.Message] = ActorSystem(PlanServer(installDir, openBrowser), "BuildPlanServer")

}

