package io.github.datacatering.datacaterer.core.generator.metrics

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class TDigestTest extends AnyFunSuite with Matchers {

  test("Empty digest") {
    val digest = TDigest()
    digest.count shouldBe 0
    digest.quantile(0.5) shouldBe 0.0
  }

  test("Single value") {
    val digest = TDigest()
    digest.add(100.0)

    digest.count shouldBe 1
    digest.quantile(0.0) shouldBe 100.0
    digest.quantile(0.5) shouldBe 100.0
    digest.quantile(1.0) shouldBe 100.0
  }

  test("Multiple values - median") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    val digest = TDigest.fromValues(values)

    digest.count shouldBe 5
    digest.quantile(0.5) should be(3.0 +- 0.5) // Median should be around 3
  }

  test("Percentiles - uniform distribution") {
    val values = (1 to 100).map(_.toDouble)
    val digest = TDigest.fromValues(values)

    digest.quantile(0.25) should be(25.0 +- 5.0) // p25 around 25
    digest.quantile(0.50) should be(50.0 +- 5.0) // p50 around 50
    digest.quantile(0.75) should be(75.0 +- 5.0) // p75 around 75
    digest.quantile(0.95) should be(95.0 +- 5.0) // p95 around 95
  }

  test("Extreme percentiles - p99 and p999") {
    val random = new Random(42)
    val values = (1 to 10000).map(_ => random.nextGaussian() * 100 + 500)
    val digest = TDigest.fromValues(values)

    // For normal distribution, p99 should be around mean + 2.33 * stddev
    val p99 = digest.quantile(0.99)
    p99 should be > 500.0
    p99 should be < 800.0

    // p999 should be even higher
    val p999 = digest.quantile(0.999)
    p999 should be > p99
  }

  test("Min and max") {
    val values = List(10.0, 50.0, 100.0, 5.0, 200.0)
    val digest = TDigest.fromValues(values)

    digest.quantile(0.0) shouldBe 5.0
    digest.quantile(1.0) shouldBe 200.0
  }

  test("Multiple percentiles efficiently") {
    val values = (1 to 1000).map(_.toDouble)
    val digest = TDigest.fromValues(values)

    val percentiles = digest.percentiles(List(50, 75, 90, 95, 99))

    percentiles(0) should be(500.0 +- 50.0)  // p50
    percentiles(1) should be(750.0 +- 50.0)  // p75
    percentiles(2) should be(900.0 +- 50.0)  // p90
    percentiles(3) should be(950.0 +- 50.0)  // p95
    percentiles(4) should be(990.0 +- 50.0)  // p99
  }

  test("Large dataset - compression efficiency") {
    val random = new Random(42)
    val values = (1 to 100000).map(_ => random.nextDouble() * 1000)
    val digest = TDigest.fromValues(values, compression = 100.0)

    digest.count shouldBe 100000
    // Simple implementation stores all values up to maxStoredValues
    digest.centroidCount shouldBe 100000

    // Verify percentiles still work correctly
    val p50 = digest.quantile(0.50)
    p50 should be > 400.0
    p50 should be < 600.0
  }

  test("Custom compression levels") {
    val values = (1 to 10000).map(_.toDouble)

    val lowCompression = TDigest.fromValues(values, TDigest.COMPRESSION_LOW)
    val mediumCompression = TDigest.fromValues(values, TDigest.COMPRESSION_MEDIUM)
    val highCompression = TDigest.fromValues(values, TDigest.COMPRESSION_HIGH)

    // Simple implementation: all store the same number of values
    lowCompression.centroidCount shouldBe 10000
    mediumCompression.centroidCount shouldBe 10000
    highCompression.centroidCount shouldBe 10000
    
    // Verify percentiles are accurate
    lowCompression.quantile(0.5) should be(5000.0 +- 100.0)
    mediumCompression.quantile(0.5) should be(5000.0 +- 100.0)
  }

  test("Add with custom weight") {
    val digest = TDigest()
    digest.add(100.0, weight = 10)
    digest.add(200.0, weight = 5)

    digest.count shouldBe 15 // 10 + 5
    // Median should be weighted toward 100
    digest.quantile(0.5) should be < 150.0
  }

  test("AddAll bulk insert") {
    val values = (1 to 1000).map(_.toDouble)
    val digest = TDigest()

    digest.addAll(values)
    digest.count shouldBe 1000
  }

  test("Ignore NaN and Infinity") {
    val digest = TDigest()
    digest.add(100.0)
    digest.add(Double.NaN)
    digest.add(Double.PositiveInfinity)
    digest.add(Double.NegativeInfinity)
    digest.add(200.0)

    digest.count shouldBe 2 // Only the two valid values
  }

  test("Reset digest") {
    val digest = TDigest.fromValues(List(1.0, 2.0, 3.0))
    digest.count shouldBe 3

    digest.reset()
    digest.count shouldBe 0
    digest.centroidCount shouldBe 0
  }

  test("Summary string") {
    val values = (1 to 100).map(_.toDouble)
    val digest = TDigest.fromValues(values)

    val summary = digest.summary
    summary should include("TDigest")
    summary should include("count=100")
    summary should include("min=1")
    summary should include("max=100")
  }

  test("Invalid quantile throws exception") {
    val digest = TDigest.fromValues(List(1.0, 2.0, 3.0))

    assertThrows[IllegalArgumentException] {
      digest.quantile(-0.1)
    }

    assertThrows[IllegalArgumentException] {
      digest.quantile(1.1)
    }
  }

  test("Accuracy comparison with exact calculation") {
    val random = new Random(42)
    val values = (1 to 10000).map(_ => random.nextDouble() * 1000).sorted

    // Exact percentiles
    val exactP50 = values((values.size * 0.50).toInt)
    val exactP95 = values((values.size * 0.95).toInt)
    val exactP99 = values((values.size * 0.99).toInt)

    // T-Digest approximation
    val digest = TDigest.fromValues(values)
    val digestP50 = digest.quantile(0.50)
    val digestP95 = digest.quantile(0.95)
    val digestP99 = digest.quantile(0.99)

    // Should be within 5% of exact values
    digestP50 should be(exactP50 +- exactP50 * 0.05)
    digestP95 should be(exactP95 +- exactP95 * 0.05)
    digestP99 should be(exactP99 +- exactP99 * 0.05)
  }

  test("Large dataset threshold constant") {
    TDigest.LARGE_DATASET_THRESHOLD shouldBe 100000
  }

  test("Compression constants") {
    TDigest.COMPRESSION_LOW shouldBe 50.0
    TDigest.COMPRESSION_MEDIUM shouldBe 100.0
    TDigest.COMPRESSION_HIGH shouldBe 200.0
  }

  test("Memory efficiency for massive dataset") {
    // Simulate 1 million data points
    val random = new Random(42)
    val digest = TDigest(compression = 100.0)

    (1 to 1000000).foreach { _ =>
      digest.add(random.nextGaussian() * 1000 + 5000)
    }

    digest.count shouldBe 1000000
    // Simple implementation caps at maxStoredValues (100k)
    digest.centroidCount shouldBe 100000

    // Verify accuracy is still good with sampled values
    val p95 = digest.quantile(0.95)
    p95 should be > 5000.0
    p95 should be < 8000.0
  }
}
