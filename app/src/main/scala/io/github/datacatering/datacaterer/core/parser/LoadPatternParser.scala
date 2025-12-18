package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.{LoadPattern => LoadPatternModel}
import io.github.datacatering.datacaterer.core.generator.execution.pattern._
import io.github.datacatering.datacaterer.core.util.GeneratorUtil
import org.apache.log4j.Logger

/**
 * Parser for converting YAML LoadPattern model to executable LoadPattern implementations.
 */
object LoadPatternParser {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Parse a LoadPattern model from YAML/API into an executable LoadPattern instance.
   *
   * @param patternModel The LoadPattern model from YAML/API configuration
   * @return Either a validated LoadPattern instance or a list of error messages
   */
  def parse(patternModel: LoadPatternModel): Either[List[String], LoadPattern] = {
    val pattern = patternModel.`type`.toLowerCase match {
      case "constant" =>
        parseConstant(patternModel)

      case "ramp" =>
        parseRamp(patternModel)

      case "spike" =>
        parseSpike(patternModel)

      case "stepped" | "step" =>
        parseStepped(patternModel)

      case "wave" | "sinusoidal" =>
        parseWave(patternModel)

      case "breakingpoint" | "breaking_point" =>
        parseBreakingPoint(patternModel)

      case unknown =>
        Left(List(s"Unknown load pattern type: $unknown. Supported types: constant, ramp, spike, stepped, wave, breakingPoint"))
    }

    // Validate the pattern if successfully created
    pattern match {
      case Right(p) =>
        val validationErrors = p.validate()
        if (validationErrors.isEmpty) {
          LOGGER.debug(s"Successfully parsed load pattern: ${patternModel.`type`}")
          Right(p)
        } else {
          LOGGER.warn(s"Load pattern validation failed: ${validationErrors.mkString(", ")}")
          Left(validationErrors)
        }
      case Left(errors) =>
        LOGGER.warn(s"Failed to parse load pattern: ${errors.mkString(", ")}")
        Left(errors)
    }
  }

  private def parseConstant(model: LoadPatternModel): Either[List[String], ConstantLoadPattern] = {
    model.baseRate match {
      case Some(rate) => Right(ConstantLoadPattern(rate))
      case None => Left(List("Constant load pattern requires 'baseRate' parameter"))
    }
  }

  private def parseRamp(model: LoadPatternModel): Either[List[String], RampLoadPattern] = {
    (model.startRate, model.endRate) match {
      case (Some(start), Some(end)) => Right(RampLoadPattern(start, end))
      case (None, _) => Left(List("Ramp load pattern requires 'startRate' parameter"))
      case (_, None) => Left(List("Ramp load pattern requires 'endRate' parameter"))
    }
  }

  private def parseSpike(model: LoadPatternModel): Either[List[String], SpikeLoadPattern] = {
    (model.baseRate, model.spikeRate, model.spikeStart, model.spikeDuration) match {
      case (Some(base), Some(spike), Some(start), Some(duration)) =>
        Right(SpikeLoadPattern(base, spike, start, duration))
      case _ =>
        val missing = List(
          if (model.baseRate.isEmpty) Some("baseRate") else None,
          if (model.spikeRate.isEmpty) Some("spikeRate") else None,
          if (model.spikeStart.isEmpty) Some("spikeStart") else None,
          if (model.spikeDuration.isEmpty) Some("spikeDuration") else None
        ).flatten
        Left(List(s"Spike load pattern requires: ${missing.mkString(", ")}"))
    }
  }

  private def parseStepped(model: LoadPatternModel): Either[List[String], SteppedLoadPattern] = {
    model.steps match {
      case Some(steps) if steps.nonEmpty => Right(SteppedLoadPattern(steps))
      case Some(_) => Left(List("Stepped load pattern requires at least one step"))
      case None => Left(List("Stepped load pattern requires 'steps' parameter"))
    }
  }

  private def parseWave(model: LoadPatternModel): Either[List[String], WaveLoadPattern] = {
    (model.baseRate, model.amplitude, model.frequency) match {
      case (Some(base), Some(amp), Some(freq)) =>
        Right(WaveLoadPattern(base, amp, freq))
      case _ =>
        val missing = List(
          if (model.baseRate.isEmpty) Some("baseRate") else None,
          if (model.amplitude.isEmpty) Some("amplitude") else None,
          if (model.frequency.isEmpty) Some("frequency") else None
        ).flatten
        Left(List(s"Wave load pattern requires: ${missing.mkString(", ")}"))
    }
  }

  private def parseBreakingPoint(model: LoadPatternModel): Either[List[String], BreakingPointPattern] = {
    (model.startRate, model.rateIncrement, model.incrementInterval) match {
      case (Some(start), Some(increment), Some(interval)) =>
        val intervalSeconds = GeneratorUtil.parseDurationToSeconds(interval)
        Right(BreakingPointPattern(start, increment, intervalSeconds, model.maxRate))
      case _ =>
        val missing = List(
          if (model.startRate.isEmpty) Some("startRate") else None,
          if (model.rateIncrement.isEmpty) Some("rateIncrement") else None,
          if (model.incrementInterval.isEmpty) Some("incrementInterval") else None
        ).flatten
        Left(List(s"Breaking point pattern requires: ${missing.mkString(", ")}"))
    }
  }
}
