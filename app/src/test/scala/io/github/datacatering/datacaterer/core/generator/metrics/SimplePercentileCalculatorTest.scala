package io.github.datacatering.datacaterer.core.generator.metrics

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Random

class SimplePercentileCalculatorTest extends AnyFunSuite with Matchers {

  test("Empty calculator") {
    val calc = SimplePercentileCalculator()
    calc.count shouldBe 0
    calc.quantile(0.5) shouldBe 0.0
  }

  test("Single value") {
    val calc = SimplePercentileCalculator()
    calc.add(100.0)

    calc.count shouldBe 1
    calc.quantile(0.0) shouldBe 100.0
    calc.quantile(0.5) shouldBe 100.0
    calc.quantile(1.0) shouldBe 100.0
  }

  test("Multiple values - median") {
    val values = List(1.0, 2.0, 3.0, 4.0, 5.0)
    val calc = SimplePercentileCalculator.fromValues(values)

    calc.count shouldBe 5
    calc.quantile(0.5) should be(3.0 +- 0.5) // Median should be around 3
  }

  test("Percentiles - uniform distribution") {
    val values = (1 to 100).map(_.toDouble)
    val calc = SimplePercentileCalculator.fromValues(values)

    calc.quantile(0.25) should be(25.0 +- 5.0) // p25 around 25
    calc.quantile(0.50) should be(50.0 +- 5.0) // p50 around 50
    calc.quantile(0.75) should be(75.0 +- 5.0) // p75 around 75
    calc.quantile(0.95) should be(95.0 +- 5.0) // p95 around 95
  }

  test("Extreme percentiles - p99 and p999") {
    val random = new Random(42)
    val values = (1 to 10000).map(_ => random.nextGaussian() * 100 + 500)
    val calc = SimplePercentileCalculator.fromValues(values)

    // For normal distribution, p99 should be around mean + 2.33 * stddev
    val p99 = calc.quantile(0.99)
    p99 should be > 500.0
    p99 should be < 800.0

    // p999 should be even higher
    val p999 = calc.quantile(0.999)
    p999 should be > p99
  }

  test("Min and max") {
    val values = List(10.0, 50.0, 100.0, 5.0, 200.0)
    val calc = SimplePercentileCalculator.fromValues(values)

    calc.quantile(0.0) shouldBe 5.0
    calc.quantile(1.0) shouldBe 200.0
  }

  test("Multiple percentiles efficiently") {
    val values = (1 to 1000).map(_.toDouble)
    val calc = SimplePercentileCalculator.fromValues(values)

    val percentiles = calc.percentiles(List(50, 75, 90, 95, 99))

    percentiles(0) should be(500.0 +- 50.0)  // p50
    percentiles(1) should be(750.0 +- 50.0)  // p75
    percentiles(2) should be(900.0 +- 50.0)  // p90
    percentiles(3) should be(950.0 +- 50.0)  // p95
    percentiles(4) should be(990.0 +- 50.0)  // p99
  }

  test("Large dataset - memory bounded") {
    val random = new Random(42)
    val values = (1 to 100000).map(_ => random.nextDouble() * 1000)
    val calc = SimplePercentileCalculator.fromValues(values, compression = 100.0)

    calc.count shouldBe 100000
    // Simple implementation stores all values up to maxStoredValues
    calc.storedCount shouldBe 100000

    // Verify percentiles still work correctly
    val p50 = calc.quantile(0.50)
    p50 should be > 400.0
    p50 should be < 600.0
  }

  test("Custom compression levels") {
    val values = (1 to 10000).map(_.toDouble)

    val lowCompression = SimplePercentileCalculator.fromValues(values, SimplePercentileCalculator.COMPRESSION_LOW)
    val mediumCompression = SimplePercentileCalculator.fromValues(values, SimplePercentileCalculator.COMPRESSION_MEDIUM)
    val highCompression = SimplePercentileCalculator.fromValues(values, SimplePercentileCalculator.COMPRESSION_HIGH)

    // Simple implementation: all store the same number of values
    lowCompression.storedCount shouldBe 10000
    mediumCompression.storedCount shouldBe 10000
    highCompression.storedCount shouldBe 10000

    // Verify percentiles are accurate
    lowCompression.quantile(0.5) should be(5000.0 +- 100.0)
    mediumCompression.quantile(0.5) should be(5000.0 +- 100.0)
  }

  test("Add with custom weight") {
    val calc = SimplePercentileCalculator()
    calc.add(100.0, weight = 10)
    calc.add(200.0, weight = 5)

    calc.count shouldBe 15 // 10 + 5
    // Median should be weighted toward 100
    calc.quantile(0.5) should be < 150.0
  }

  test("AddAll bulk insert") {
    val values = (1 to 1000).map(_.toDouble)
    val calc = SimplePercentileCalculator()

    calc.addAll(values)
    calc.count shouldBe 1000
  }

  test("Ignore NaN and Infinity") {
    val calc = SimplePercentileCalculator()
    calc.add(100.0)
    calc.add(Double.NaN)
    calc.add(Double.PositiveInfinity)
    calc.add(Double.NegativeInfinity)
    calc.add(200.0)

    calc.count shouldBe 2 // Only the two valid values
  }

  test("Reset calculator") {
    val calc = SimplePercentileCalculator.fromValues(List(1.0, 2.0, 3.0))
    calc.count shouldBe 3

    calc.reset()
    calc.count shouldBe 0
    calc.storedCount shouldBe 0
  }

  test("Summary string") {
    val values = (1 to 100).map(_.toDouble)
    val calc = SimplePercentileCalculator.fromValues(values)

    val summary = calc.summary
    summary should include("SimplePercentileCalculator")
    summary should include("count=100")
    summary should include("min=1")
    summary should include("max=100")
  }

  test("Invalid quantile throws exception") {
    val calc = SimplePercentileCalculator.fromValues(List(1.0, 2.0, 3.0))

    assertThrows[IllegalArgumentException] {
      calc.quantile(-0.1)
    }

    assertThrows[IllegalArgumentException] {
      calc.quantile(1.1)
    }
  }

  test("Accuracy comparison with exact calculation") {
    val random = new Random(42)
    val values = (1 to 10000).map(_ => random.nextDouble() * 1000).sorted

    // Exact percentiles
    val exactP50 = values((values.size * 0.50).toInt)
    val exactP95 = values((values.size * 0.95).toInt)
    val exactP99 = values((values.size * 0.99).toInt)

    // SimplePercentileCalculator approximation
    val calc = SimplePercentileCalculator.fromValues(values)
    val calcP50 = calc.quantile(0.50)
    val calcP95 = calc.quantile(0.95)
    val calcP99 = calc.quantile(0.99)

    // Should be within 5% of exact values
    calcP50 should be(exactP50 +- exactP50 * 0.05)
    calcP95 should be(exactP95 +- exactP95 * 0.05)
    calcP99 should be(exactP99 +- exactP99 * 0.05)
  }

  test("Large dataset threshold constant") {
    SimplePercentileCalculator.LARGE_DATASET_THRESHOLD shouldBe 100000
  }

  test("Compression constants") {
    SimplePercentileCalculator.COMPRESSION_LOW shouldBe 50.0
    SimplePercentileCalculator.COMPRESSION_MEDIUM shouldBe 100.0
    SimplePercentileCalculator.COMPRESSION_HIGH shouldBe 200.0
  }

  test("Memory efficiency for massive dataset") {
    // Simulate 1 million data points
    val random = new Random(42)
    val calc = SimplePercentileCalculator(compression = 100.0)

    (1 to 1000000).foreach { _ =>
      calc.add(random.nextGaussian() * 1000 + 5000)
    }

    calc.count shouldBe 1000000
    // Simple implementation caps at maxStoredValues (100k)
    calc.storedCount shouldBe 100000

    // Verify accuracy is still good with sampled values
    val p95 = calc.quantile(0.95)
    p95 should be > 5000.0
    p95 should be < 8000.0
  }

  test("Deprecated TDigest alias works") {
    // Test that the deprecated TDigest alias still works for backwards compatibility
    val calc = TDigest()
    calc.add(100.0)
    calc.count shouldBe 1

    val calc2 = TDigest.fromValues(List(1.0, 2.0, 3.0))
    calc2.count shouldBe 3

    TDigest.LARGE_DATASET_THRESHOLD shouldBe 100000
  }
}
