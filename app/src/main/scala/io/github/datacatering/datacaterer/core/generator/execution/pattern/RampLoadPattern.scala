package io.github.datacatering.datacaterer.core.generator.execution.pattern

/**
 * Ramp load pattern gradually increases the load from a start rate to an end rate.
 * This is useful for finding the breaking point or capacity of a system under gradual load increase.
 *
 * The rate increases linearly over time from startRate to endRate.
 *
 * @param startRate The initial rate in records per second
 * @param endRate The final rate in records per second
 */
case class RampLoadPattern(startRate: Int, endRate: Int) extends LoadPattern {

  override def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int = {
    if (totalDurationSeconds <= 0) return startRate

    val progress = math.min(elapsedSeconds / totalDurationSeconds, 1.0)
    val rate = startRate + ((endRate - startRate) * progress)
    math.max(1, rate.toInt)
  }

  override def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (startRate <= 0) errors += s"Ramp load pattern startRate must be positive, got: $startRate"
    if (endRate <= 0) errors += s"Ramp load pattern endRate must be positive, got: $endRate"
    if (startRate >= endRate) errors += s"Ramp load pattern startRate ($startRate) must be less than endRate ($endRate)"

    errors.toList
  }
}
