package io.github.datacatering.datacaterer.core.generator.execution.pattern

/**
 * Constant load pattern maintains a steady rate throughout the test duration.
 * This is the simplest pattern and is useful for baseline performance testing.
 *
 * @param rate The constant rate in records per second
 */
case class ConstantLoadPattern(rate: Int) extends LoadPattern {

  override def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int = rate

  override def validate(): List[String] = {
    if (rate <= 0) List(s"Constant load pattern rate must be positive, got: $rate")
    else List()
  }
}
