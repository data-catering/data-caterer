package io.github.datacatering.datacaterer.core.generator.execution.pattern

/**
 * Wave load pattern simulates periodic fluctuations in traffic.
 * The load oscillates around a base rate in a sinusoidal pattern.
 *
 * This is useful for testing systems under cyclical load patterns (e.g., daily traffic patterns).
 *
 * Formula: rate = baseRate + amplitude * sin(2π * frequency * time)
 *
 * @param baseRate The average baseline rate in records per second
 * @param amplitude The amplitude of the wave (peak deviation from base rate)
 * @param frequency The number of complete waves per test duration (e.g., 2.0 = 2 complete cycles)
 */
case class WaveLoadPattern(
  baseRate: Int,
  amplitude: Int,
  frequency: Double
) extends LoadPattern {

  override def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int = {
    if (totalDurationSeconds <= 0) return baseRate

    val progress = elapsedSeconds / totalDurationSeconds
    val radians = 2 * math.Pi * frequency * progress
    val wave = math.sin(radians)
    val rate = baseRate + (amplitude * wave)

    math.max(1, rate.toInt)
  }

  override def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (baseRate <= 0) errors += s"Wave load pattern baseRate must be positive, got: $baseRate"
    if (amplitude < 0) errors += s"Wave load pattern amplitude must be non-negative, got: $amplitude"
    if (amplitude >= baseRate) errors += s"Wave load pattern amplitude ($amplitude) should be less than baseRate ($baseRate) to avoid negative rates"
    if (frequency <= 0) errors += s"Wave load pattern frequency must be positive, got: $frequency"

    errors.toList
  }
}
