package io.github.datacatering.datacaterer.core.util.timing

/**
 * Analysis of batch interval distribution for rate consistency
 */
case class IntervalAnalysis(
  intervals: List[Long],
  expectedIntervalMs: Double,
  tolerancePercent: Double = 15.0
) {
  require(intervals.nonEmpty, "Intervals list cannot be empty")

  val avgMs: Double = intervals.sum.toDouble / intervals.size
  val minMs: Long = intervals.min
  val maxMs: Long = intervals.max
  val stdDev: Double = {
    val mean = avgMs
    val variance = intervals.map(i => Math.pow(i - mean, 2)).sum / intervals.size
    Math.sqrt(variance)
  }
  val coefficientOfVariation: Double = (stdDev / avgMs) * 100.0
  val p50Ms: Long = calculatePercentile(0.5)
  val p90Ms: Long = calculatePercentile(0.9)
  val p95Ms: Long = calculatePercentile(0.95)

  val deviationFromExpectedPercent: Double = Math.abs(avgMs - expectedIntervalMs) / expectedIntervalMs * 100.0
  val isConsistent: Boolean = coefficientOfVariation <= tolerancePercent

  private def calculatePercentile(percentile: Double): Long = {
    val sorted = intervals.sorted
    val index = (percentile * sorted.size).toInt
    sorted(Math.min(index, sorted.size - 1))
  }

  override def toString: String = {
    val status = if (isConsistent) "✓ PASS" else "✗ FAIL"
    s"""Interval Analysis [$status]
       |  Expected interval: ${"%.2f".format(expectedIntervalMs)}ms
       |  Average: ${"%.2f".format(avgMs)}ms
       |  Min: ${minMs}ms
       |  Max: ${maxMs}ms
       |  Median (p50): ${p50Ms}ms
       |  p90: ${p90Ms}ms
       |  p95: ${p95Ms}ms
       |  Std Dev: ${"%.2f".format(stdDev)}ms
       |  Coefficient of Variation: ${"%.2f".format(coefficientOfVariation)}%
       |  Tolerance: <${tolerancePercent}%
       |  Sample size: ${intervals.size} intervals
       |""".stripMargin
  }
}

object IntervalAnalysis {
  def apply(intervals: List[Long], expectedIntervalMs: Double): IntervalAnalysis = {
    new IntervalAnalysis(intervals, expectedIntervalMs)
  }

  def apply(intervals: List[Double]): IntervalAnalysis = {
    val intervalsLong = intervals.map(_.toLong)
    val expectedMs = intervals.sum / intervals.size
    new IntervalAnalysis(intervalsLong, expectedMs)
  }
}
