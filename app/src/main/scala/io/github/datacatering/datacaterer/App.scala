package io.github.datacatering.datacaterer

import io.github.datacatering.datacaterer.core.plan.PlanProcessor
import org.apache.log4j.Logger

import java.time.{Duration, LocalDateTime}

object App {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val startTime = LocalDateTime.now()
    LOGGER.info("Starting Data Caterer")
    try {
      PlanProcessor.determineAndExecutePlan()
    } catch {
      case t: Throwable =>
        LOGGER.error("An error occurred while processing the plan", t)
        System.exit(1)
    } finally {
      val endTime = LocalDateTime.now()
      val duration = Duration.between(startTime, endTime)
      LOGGER.info(s"Completed in ${duration.toSeconds}s")
      System.exit(0)
    }
  }
}
