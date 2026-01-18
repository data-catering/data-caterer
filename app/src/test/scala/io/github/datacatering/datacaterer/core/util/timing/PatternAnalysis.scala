package io.github.datacatering.datacaterer.core.util.timing

/**
 * Sample point for pattern analysis
 */
case class PatternSample(
  elapsedSec: Double,
  expectedRate: Double,
  actualRate: Double,
  tolerancePercent: Double = 15.0
) {
  val deviationPercent: Double = Math.abs(actualRate - expectedRate) / expectedRate * 100.0
  val isWithinTolerance: Boolean = deviationPercent <= tolerancePercent

  override def toString: String = {
    val status = if (isWithinTolerance) "✓" else "✗"
    s"  [$status] t=${"%.1f".format(elapsedSec)}s: expected=${"%.1f".format(expectedRate)}/s, actual=${"%.1f".format(actualRate)}/s, deviation=${"%.1f".format(deviationPercent)}%"
  }
}

/**
 * Analysis of pattern conformance over time
 */
case class PatternAnalysis(
  samples: List[PatternSample]
) {
  require(samples.nonEmpty, "Pattern samples cannot be empty")

  val samplesWithinTolerance: Int = samples.count(_.isWithinTolerance)
  val conformancePercent: Double = samplesWithinTolerance.toDouble / samples.size * 100.0
  val avgDeviationPercent: Double = samples.map(_.deviationPercent).sum / samples.size
  val maxDeviationPercent: Double = samples.map(_.deviationPercent).max
  val isPassing: Boolean = conformancePercent >= 80.0 // At least 80% of samples within tolerance

  override def toString: String = {
    val status = if (isPassing) "✓ PASS" else "✗ FAIL"
    val sampleDetails = samples.map(_.toString).mkString("\n")
    s"""Pattern Analysis [$status]
       |  Samples within tolerance: $samplesWithinTolerance/${samples.size} (${"%.1f".format(conformancePercent)}%)
       |  Average deviation: ${"%.1f".format(avgDeviationPercent)}%
       |  Max deviation: ${"%.1f".format(maxDeviationPercent)}%
       |  Passing threshold: ≥80% samples within tolerance
       |
       |Sample Details:
       |$sampleDetails
       |""".stripMargin
  }
}

object PatternAnalysis {
  def apply(samples: List[PatternSample]): PatternAnalysis = {
    new PatternAnalysis(samples)
  }
}
