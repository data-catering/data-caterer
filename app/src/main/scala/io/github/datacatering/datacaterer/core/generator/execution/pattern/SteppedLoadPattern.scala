package io.github.datacatering.datacaterer.core.generator.execution.pattern

import io.github.datacatering.datacaterer.api.model.LoadPatternStep
import io.github.datacatering.datacaterer.core.util.GeneratorUtil

/**
 * Stepped load pattern increases the load in discrete steps.
 * Each step has a specific rate and duration, useful for incremental capacity planning.
 *
 * Example: 50 req/s for 2min, then 100 req/s for 2min, then 200 req/s for 2min
 *
 * @param steps A list of load pattern steps, each with a rate and duration
 */
case class SteppedLoadPattern(steps: List[LoadPatternStep]) extends LoadPattern {

  // Pre-calculate cumulative durations in seconds for efficient lookup
  private lazy val cumulativeDurations: List[(Double, Int)] = {
    steps.foldLeft((0.0, List[(Double, Int)]())) { case ((cumulative, acc), step) =>
      val durationSeconds = GeneratorUtil.parseDurationToSeconds(step.duration)
      val newCumulative = cumulative + durationSeconds
      (newCumulative, acc :+ (newCumulative, step.rate))
    }._2
  }

  override def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int = {
    // Find the step that contains the current elapsed time
    cumulativeDurations.find { case (cumulative, _) => elapsedSeconds < cumulative } match {
      case Some((_, rate)) => rate
      case None => cumulativeDurations.lastOption.map(_._2).getOrElse(1)
    }
  }

  override def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (steps.isEmpty) {
      errors += "Stepped load pattern must have at least one step"
    } else {
      steps.zipWithIndex.foreach { case (step, index) =>
        if (step.rate <= 0) {
          errors += s"Stepped load pattern step ${index + 1} rate must be positive, got: ${step.rate}"
        }
        val durationSeconds = GeneratorUtil.parseDurationToSeconds(step.duration)
        if (durationSeconds <= 0) {
          errors += s"Stepped load pattern step ${index + 1} duration must be positive, got: ${step.duration}"
        }
      }
    }

    errors.toList
  }
}
