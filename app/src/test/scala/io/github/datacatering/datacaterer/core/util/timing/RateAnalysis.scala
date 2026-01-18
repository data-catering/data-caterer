package io.github.datacatering.datacaterer.core.util.timing

/**
 * Analysis of actual rate compared to target rate
 */
case class RateAnalysis(
  actualRate: Double,
  targetRate: Double,
  tolerancePercent: Double = 10.0
) {
  val deviationPercent: Double = Math.abs(actualRate - targetRate) / targetRate * 100.0
  val isWithinTolerance: Boolean = deviationPercent <= tolerancePercent

  override def toString: String = {
    val status = if (isWithinTolerance) "✓ PASS" else "✗ FAIL"
    s"""Rate Analysis [$status]
       |  Target: ${"%.2f".format(targetRate)} records/sec
       |  Actual: ${"%.2f".format(actualRate)} records/sec
       |  Deviation: ${"%.2f".format(deviationPercent)}%
       |  Tolerance: ±${tolerancePercent}%
       |""".stripMargin
  }
}

object RateAnalysis {
  def apply(actualRate: Double, targetRate: Double): RateAnalysis = {
    new RateAnalysis(actualRate, targetRate)
  }
}
