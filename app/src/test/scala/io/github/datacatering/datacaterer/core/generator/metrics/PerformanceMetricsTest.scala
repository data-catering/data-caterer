package io.github.datacatering.datacaterer.core.generator.metrics

import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class PerformanceMetricsTest extends AnyFunSuite {

  test("Calculate total records from batch metrics") {
    val batch1 = BatchMetrics(1, LocalDateTime.now(), LocalDateTime.now(), 100, 1000)
    val batch2 = BatchMetrics(2, LocalDateTime.now(), LocalDateTime.now(), 150, 1500)
    val batch3 = BatchMetrics(3, LocalDateTime.now(), LocalDateTime.now(), 200, 2000)

    val metrics = PerformanceMetrics(batchMetrics = List(batch1, batch2, batch3))

    assert(metrics.totalRecords == 450)
  }

  test("Calculate average throughput") {
    val batch1 = BatchMetrics(1, LocalDateTime.now(), LocalDateTime.now(), 100, 1000)
    val batch2 = BatchMetrics(2, LocalDateTime.now(), LocalDateTime.now(), 200, 2000)

    val metrics = PerformanceMetrics(batchMetrics = List(batch1, batch2))

    // (100 + 200) / (1000 + 2000) * 1000 = 100 records/sec
    assert(metrics.averageThroughput == 100.0)
  }

  test("Calculate batch throughput") {
    val batch = BatchMetrics(1, LocalDateTime.now(), LocalDateTime.now(), 100, 1000)

    // 100 records in 1000ms = 100 records/sec
    assert(batch.throughput == 100.0)
  }

  test("Calculate max and min throughput") {
    val batch1 = BatchMetrics(1, LocalDateTime.now(), LocalDateTime.now(), 100, 1000) // 100/s
    val batch2 = BatchMetrics(2, LocalDateTime.now(), LocalDateTime.now(), 200, 1000) // 200/s
    val batch3 = BatchMetrics(3, LocalDateTime.now(), LocalDateTime.now(), 50, 1000)  // 50/s

    val metrics = PerformanceMetrics(batchMetrics = List(batch1, batch2, batch3))

    assert(metrics.maxThroughput == 200.0)
    assert(metrics.minThroughput == 50.0)
  }

  test("Calculate latency percentiles") {
    val batches = (1 to 100).map { i =>
      BatchMetrics(i, LocalDateTime.now(), LocalDateTime.now(), 10, i.toLong)
    }.toList

    val metrics = PerformanceMetrics(batchMetrics = batches)

    assert(metrics.latencyP50 > 0)
    assert(metrics.latencyP95 > metrics.latencyP50)
    assert(metrics.latencyP99 > metrics.latencyP95)
  }

  test("Add batch metric updates metrics") {
    val startTime = LocalDateTime.now()
    val endTime = startTime.plusSeconds(1)
    val batch = BatchMetrics(1, startTime, endTime, 100, 1000)

    val metrics = PerformanceMetrics()
    val updatedMetrics = metrics.addBatchMetric(batch)

    assert(updatedMetrics.batchMetrics.size == 1)
    assert(updatedMetrics.totalRecords == 100)
    assert(updatedMetrics.startTime.isDefined)
    assert(updatedMetrics.endTime.isDefined)
  }

  test("Handle empty metrics gracefully") {
    val metrics = PerformanceMetrics()

    assert(metrics.totalRecords == 0)
    assert(metrics.averageThroughput == 0.0)
    assert(metrics.maxThroughput == 0.0)
    assert(metrics.minThroughput == 0.0)
    assert(metrics.latencyP50 == 0.0)
  }

  test("Calculate total duration seconds") {
    val start = LocalDateTime.of(2025, 1, 1, 12, 0, 0)
    val end = LocalDateTime.of(2025, 1, 1, 12, 5, 30)

    val metrics = PerformanceMetrics(
      startTime = Some(start),
      endTime = Some(end)
    )

    assert(metrics.totalDurationSeconds == 330) // 5 minutes 30 seconds
  }
}
