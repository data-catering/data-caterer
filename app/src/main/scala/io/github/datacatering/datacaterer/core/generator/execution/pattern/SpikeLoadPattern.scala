package io.github.datacatering.datacaterer.core.generator.execution.pattern

/**
 * Spike load pattern simulates a sudden surge in traffic.
 * The load remains at a base rate, then spikes to a higher rate for a brief period.
 *
 * This is useful for testing how systems handle sudden traffic increases (e.g., Black Friday sales).
 *
 * @param baseRate The normal baseline rate in records per second
 * @param spikeRate The elevated rate during the spike in records per second
 * @param spikeStart The point in time when the spike starts (0.0 to 1.0, as fraction of total duration)
 * @param spikeDuration The duration of the spike (0.0 to 1.0, as fraction of total duration)
 */
case class SpikeLoadPattern(
  baseRate: Int,
  spikeRate: Int,
  spikeStart: Double,
  spikeDuration: Double
) extends LoadPattern {

  override def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int = {
    if (totalDurationSeconds <= 0) return baseRate

    val progress = elapsedSeconds / totalDurationSeconds
    val spikeEnd = spikeStart + spikeDuration

    if (progress >= spikeStart && progress < spikeEnd) {
      spikeRate
    } else {
      baseRate
    }
  }

  override def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (baseRate <= 0) errors += s"Spike load pattern baseRate must be positive, got: $baseRate"
    if (spikeRate <= 0) errors += s"Spike load pattern spikeRate must be positive, got: $spikeRate"
    if (spikeRate <= baseRate) errors += s"Spike load pattern spikeRate ($spikeRate) must be greater than baseRate ($baseRate)"
    if (spikeStart < 0.0 || spikeStart > 1.0) errors += s"Spike load pattern spikeStart must be between 0.0 and 1.0, got: $spikeStart"
    if (spikeDuration <= 0.0 || spikeDuration > 1.0) errors += s"Spike load pattern spikeDuration must be between 0.0 and 1.0, got: $spikeDuration"
    if (spikeStart + spikeDuration > 1.0) errors += s"Spike load pattern spikeStart + spikeDuration must not exceed 1.0, got: ${spikeStart + spikeDuration}"

    errors.toList
  }
}
