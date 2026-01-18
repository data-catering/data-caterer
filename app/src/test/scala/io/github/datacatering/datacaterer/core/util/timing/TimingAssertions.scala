package io.github.datacatering.datacaterer.core.util.timing

import org.scalatest.matchers.{MatchResult, Matcher}

/**
 * Custom ScalaTest matchers for timing validations
 */
trait TimingAssertions {

  /**
   * Matcher for duration within tolerance
   * Usage: analysis should haveDurationWithin(5000, 5.0)
   */
  def haveDurationWithin(expectedMs: Long, tolerancePercent: Double): Matcher[DurationAnalysis] = {
    Matcher { analysis =>
      MatchResult(
        analysis.isWithinTolerance && Math.abs(analysis.expectedMs - expectedMs) < 1,
        s"Duration ${analysis.actualMs}ms was not within ±${tolerancePercent}% of ${expectedMs}ms (deviation: ${"%.2f".format(analysis.deviationPercent)}%)",
        s"Duration ${analysis.actualMs}ms was within ±${tolerancePercent}% of ${expectedMs}ms"
      )
    }
  }

  /**
   * Matcher for rate within tolerance
   * Usage: analysis should haveRateWithin(50.0, 10.0)
   */
  def haveRateWithin(targetRate: Double, tolerancePercent: Double): Matcher[RateAnalysis] = {
    Matcher { analysis =>
      MatchResult(
        analysis.isWithinTolerance && Math.abs(analysis.targetRate - targetRate) < 0.01,
        s"Rate ${"%.2f".format(analysis.actualRate)}/s was not within ±${tolerancePercent}% of ${targetRate}/s (deviation: ${"%.2f".format(analysis.deviationPercent)}%)",
        s"Rate ${"%.2f".format(analysis.actualRate)}/s was within ±${tolerancePercent}% of ${targetRate}/s"
      )
    }
  }

  /**
   * Matcher for interval consistency
   * Usage: analysis should haveConsistentIntervals(15.0)
   */
  def haveConsistentIntervals(tolerancePercent: Double): Matcher[IntervalAnalysis] = {
    Matcher { analysis =>
      MatchResult(
        analysis.isConsistent,
        s"Interval coefficient of variation ${"%.2f".format(analysis.coefficientOfVariation)}% exceeds tolerance ${tolerancePercent}%",
        s"Interval coefficient of variation ${"%.2f".format(analysis.coefficientOfVariation)}% is within tolerance ${tolerancePercent}%"
      )
    }
  }

  /**
   * Matcher for pattern conformance
   * Usage: analysis should conformToPattern(80.0)
   */
  def conformToPattern(minConformancePercent: Double = 80.0): Matcher[PatternAnalysis] = {
    Matcher { analysis =>
      MatchResult(
        analysis.conformancePercent >= minConformancePercent,
        s"Pattern conformance ${"%.1f".format(analysis.conformancePercent)}% is below threshold ${minConformancePercent}% (${analysis.samplesWithinTolerance}/${analysis.samples.size} samples within tolerance)",
        s"Pattern conformance ${"%.1f".format(analysis.conformancePercent)}% meets threshold ${minConformancePercent}%"
      )
    }
  }
}

object TimingAssertions extends TimingAssertions
