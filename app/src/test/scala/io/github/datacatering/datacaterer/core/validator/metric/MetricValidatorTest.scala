package io.github.datacatering.datacaterer.core.validator.metric

import io.github.datacatering.datacaterer.api.model._
import io.github.datacatering.datacaterer.core.generator.metrics.{BatchMetrics, PerformanceMetrics}
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class MetricValidatorTest extends AnyFunSuite {

  val sampleMetrics = PerformanceMetrics(
    batchMetrics = List(
      BatchMetrics(1, LocalDateTime.now(), LocalDateTime.now(), 100, 1000),
      BatchMetrics(2, LocalDateTime.now(), LocalDateTime.now(), 100, 1000)
    ),
    startTime = Some(LocalDateTime.now().minusSeconds(2)),
    endTime = Some(LocalDateTime.now())
  )

  val validator = new MetricValidator(sampleMetrics)

  test("Validate throughput greater than threshold - pass") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(GreaterThanFieldValidation(value = 50.0, strictly = true))
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
    assert(result.metricValue == 100.0) // 200 records / 2 seconds
  }

  test("Validate throughput greater than threshold - fail") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(GreaterThanFieldValidation(value = 150.0, strictly = true))
    )

    val result = validator.validate(metricValidation)

    assert(!result.isValid)
  }

  test("Validate latency less than threshold - pass") {
    val metricValidation = MetricValidation(
      metric = "latency_p95",
      validation = List(LessThanFieldValidation(value = 2000.0, strictly = true))
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
  }

  test("Validate latency less than threshold - fail") {
    val metricValidation = MetricValidation(
      metric = "latency_p95",
      validation = List(LessThanFieldValidation(value = 500.0, strictly = true))
    )

    val result = validator.validate(metricValidation)

    assert(!result.isValid)
  }

  test("Validate records generated equal to value") {
    val metricValidation = MetricValidation(
      metric = "records_generated",
      validation = List(EqualFieldValidation(value = 200.0, negate = false))
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
    assert(result.metricValue == 200.0)
  }

  test("Validate metric between range - pass") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(BetweenFieldValidation(min = 50.0, max = 150.0, negate = false))
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
  }

  test("Validate metric between range - fail") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(BetweenFieldValidation(min = 200.0, max = 300.0, negate = false))
    )

    val result = validator.validate(metricValidation)

    assert(!result.isValid)
  }

  test("Validate metric in set - pass") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(InFieldValidation(values = List(50.0, 100.0, 150.0), negate = false))
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
  }

  test("Validate metric in set - fail") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(InFieldValidation(values = List(50.0, 75.0, 125.0), negate = false))
    )

    val result = validator.validate(metricValidation)

    assert(!result.isValid)
  }

  test("Validate multiple conditions - all pass") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(
        GreaterThanFieldValidation(value = 50.0, strictly = true),
        LessThanFieldValidation(value = 150.0, strictly = true)
      )
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
    assert(result.fieldValidations.size == 2)
    assert(result.fieldValidations.forall(_.isValid))
  }

  test("Validate multiple conditions - one fails") {
    val metricValidation = MetricValidation(
      metric = "throughput",
      validation = List(
        GreaterThanFieldValidation(value = 50.0, strictly = true),
        LessThanFieldValidation(value = 80.0, strictly = true)
      )
    )

    val result = validator.validate(metricValidation)

    assert(!result.isValid)
    assert(result.fieldValidations.size == 2)
    assert(result.fieldValidations.count(_.isValid) == 1)
  }

  test("Handle unknown metric gracefully") {
    val metricValidation = MetricValidation(
      metric = "unknown_metric",
      validation = List(GreaterThanFieldValidation(value = 0.0, strictly = true))
    )

    val result = validator.validate(metricValidation)

    // Should return 0.0 for unknown metric and fail validation
    assert(result.metricValue == 0.0)
    assert(!result.isValid)
  }

  test("Validate duration seconds") {
    val metricValidation = MetricValidation(
      metric = "duration_seconds",
      validation = List(GreaterThanFieldValidation(value = 0.0, strictly = true))
    )

    val result = validator.validate(metricValidation)

    assert(result.isValid)
    assert(result.metricValue >= 0.0)
  }
}
