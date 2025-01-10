package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.{DataSourceResult, ValidationConfigResult}

case class PlanRunResults(
                           generationResults: List[DataSourceResult] = List(),
                           validationResults: List[ValidationConfigResult] = List(),
                           optReportPath: Option[String] = None
                         ) {

  def summariseGenerationResults: (List[List[String]], Long) = {
    val timeTaken = generationResults.map(_.sinkResult.durationInSeconds).max
    val dataSourceSummary = generationResults.map(_.summarise)
    (dataSourceSummary, timeTaken)
  }

  def summariseValidationResults: (List[List[String]], Long) = {
    val timeTaken = validationResults.map(_.durationInSeconds).sum
    val validationSummary = validationResults.map(_.summarise)
    (validationSummary, timeTaken)
  }
}

case class RealTimeSinkResult(result: String = "")
