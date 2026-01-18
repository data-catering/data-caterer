package io.github.datacatering.datacaterer.core.validator.metric

import io.github.datacatering.datacaterer.api.model.{BetweenFieldValidation, EqualFieldValidation, FieldValidation, GreaterThanFieldValidation, InFieldValidation, LessThanFieldValidation, MetricValidation}
import io.github.datacatering.datacaterer.api.model.PerformanceMetrics
import org.apache.log4j.Logger

/**
 * Validates performance metrics against configured thresholds
 */
class MetricValidator(performanceMetrics: PerformanceMetrics) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def validate(metricValidation: MetricValidation): MetricValidationResult = {
    val metricName = metricValidation.metric
    val metricValue = getMetricValue(metricName)

    LOGGER.debug(s"Validating metric: metric=$metricName, value=$metricValue, validations=${metricValidation.validation.size}")

    val validationResults = metricValidation.validation.map { fieldValidation =>
      val result = evaluateValidation(metricName, metricValue, fieldValidation)
      if (!result.isValid) {
        LOGGER.warn(s"Metric validation failed: metric=$metricName, value=$metricValue, " +
          s"validation-type=${fieldValidation.`type`}, expected=${getExpectedValue(fieldValidation)}")
      }
      result
    }

    val allPassed = validationResults.forall(_.isValid)
    MetricValidationResult(metricName, metricValue, allPassed, validationResults)
  }

  private def getMetricValue(metricName: String): Double = {
    metricName.toLowerCase match {
      case "throughput" => performanceMetrics.averageThroughput
      case "latency_p50" => performanceMetrics.latencyP50
      case "latency_p75" => performanceMetrics.latencyP75
      case "latency_p90" => performanceMetrics.latencyP90
      case "latency_p95" => performanceMetrics.latencyP95
      case "latency_p99" => performanceMetrics.latencyP99
      case "error_rate" => performanceMetrics.errorRate
      case "records_generated" => performanceMetrics.totalRecords.toDouble
      case "duration_seconds" => performanceMetrics.totalDurationSeconds.toDouble
      case "max_throughput" => performanceMetrics.maxThroughput
      case "min_throughput" => performanceMetrics.minThroughput
      case _ =>
        LOGGER.warn(s"Unknown metric: $metricName, returning 0.0")
        0.0
    }
  }

  private def evaluateValidation(metricName: String, metricValue: Double, validation: FieldValidation): FieldValidationResult = {
    val isValid = validation match {
      case GreaterThanFieldValidation(value, strictly) =>
        val threshold = parseDouble(value)
        if (strictly) metricValue > threshold else metricValue >= threshold

      case LessThanFieldValidation(value, strictly) =>
        val threshold = parseDouble(value)
        if (strictly) metricValue < threshold else metricValue <= threshold

      case EqualFieldValidation(value, negate) =>
        val threshold = parseDouble(value)
        val eq = math.abs(metricValue - threshold) < 0.0001
        if (negate) !eq else eq

      case BetweenFieldValidation(min, max, negate) =>
        val between = metricValue >= min && metricValue <= max
        if (negate) !between else between

      case InFieldValidation(values, negate) =>
        val thresholds = values.map(parseDouble)
        val inSet = thresholds.exists(t => math.abs(metricValue - t) < 0.0001)
        if (negate) !inSet else inSet

      case _ =>
        LOGGER.warn(s"Unsupported validation type for metric: ${validation.`type`}")
        false
    }

    FieldValidationResult(validation.`type`, isValid, getExpectedValue(validation))
  }

  private def parseDouble(value: Any): Double = {
    value match {
      case d: Double => d
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case s: String => s.toDouble
      case _ => value.toString.toDouble
    }
  }

  private def getExpectedValue(validation: FieldValidation): String = {
    validation match {
      case GreaterThanFieldValidation(value, strictly) =>
        if (strictly) s"> $value" else s">= $value"
      case LessThanFieldValidation(value, strictly) =>
        if (strictly) s"< $value" else s"<= $value"
      case EqualFieldValidation(value, negate) =>
        if (negate) s"!= $value" else s"== $value"
      case BetweenFieldValidation(min, max, negate) =>
        if (negate) s"not between $min and $max" else s"between $min and $max"
      case InFieldValidation(values, negate) =>
        if (negate) s"not in ${values.mkString("[", ",", "]")}" else s"in ${values.mkString("[", ",", "]")}"
      case _ => validation.`type`
    }
  }
}

case class MetricValidationResult(
                                    metricName: String,
                                    metricValue: Double,
                                    isValid: Boolean,
                                    fieldValidations: List[FieldValidationResult]
                                  )

case class FieldValidationResult(
                                   validationType: String,
                                   isValid: Boolean,
                                   expectedValue: String
                                 )
