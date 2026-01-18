package io.github.datacatering.datacaterer.core.util.timing

/**
 * Analysis of execution duration compared to expected duration
 */
case class DurationAnalysis(
  actualMs: Long,
  expectedMs: Long,
  tolerancePercent: Double = 5.0
) {
  val deviationMs: Long = actualMs - expectedMs
  val deviationPercent: Double = Math.abs(deviationMs).toDouble / expectedMs * 100.0
  val isWithinTolerance: Boolean = deviationPercent <= tolerancePercent

  override def toString: String = {
    val status = if (isWithinTolerance) "✓ PASS" else "✗ FAIL"
    s"""Duration Analysis [$status]
       |  Expected: ${expectedMs}ms
       |  Actual: ${actualMs}ms
       |  Deviation: ${deviationMs}ms (${"%.2f".format(deviationPercent)}%)
       |  Tolerance: ±${tolerancePercent}%
       |""".stripMargin
  }
}

object DurationAnalysis {
  def apply(actualMs: Long, expectedMs: Long): DurationAnalysis = {
    new DurationAnalysis(actualMs, expectedMs)
  }
}
