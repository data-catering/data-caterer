package io.github.datacatering.datacaterer.core.generator.execution.pattern

/**
 * Breaking point load pattern automatically increases the load until a breaking condition is met.
 * This is useful for finding the maximum capacity of a system.
 *
 * The rate starts at startRate and increases by rateIncrement every incrementInterval seconds
 * until either maxRate is reached or a breaking condition (like error rate threshold) is triggered.
 *
 * Note: Full breaking point functionality (automatic stopping on threshold breach) is implemented in Phase 3.
 * This class provides the basic rate progression for Phase 2.
 *
 * @param startRate The initial rate in records per second
 * @param rateIncrement How much to increase the rate at each interval
 * @param incrementInterval Duration in seconds between rate increases
 * @param maxRate Maximum rate cap (optional)
 */
case class BreakingPointPattern(
  startRate: Int,
  rateIncrement: Int,
  incrementInterval: Double,
  maxRate: Option[Int] = None
) extends LoadPattern {

  override def getRateAt(elapsedSeconds: Double, totalDurationSeconds: Double): Int = {
    if (incrementInterval <= 0) return startRate

    val intervals = (elapsedSeconds / incrementInterval).toInt
    val rate = startRate + (intervals * rateIncrement)

    maxRate match {
      case Some(max) => math.min(rate, max)
      case None => rate
    }
  }

  override def validate(): List[String] = {
    val errors = scala.collection.mutable.ListBuffer[String]()

    if (startRate <= 0) errors += s"Breaking point pattern startRate must be positive, got: $startRate"
    if (rateIncrement <= 0) errors += s"Breaking point pattern rateIncrement must be positive, got: $rateIncrement"
    if (incrementInterval <= 0) errors += s"Breaking point pattern incrementInterval must be positive, got: $incrementInterval"
    maxRate.foreach { max =>
      if (max <= startRate) errors += s"Breaking point pattern maxRate ($max) must be greater than startRate ($startRate)"
    }

    errors.toList
  }

  /**
   * Parse duration string to seconds.
   * Supports formats like: "30s", "5m", "1h"
   */
  def parseDuration(duration: String): Double = {
    val pattern = """(\d+)([smh])""".r
    val matches = pattern.findAllMatchIn(duration.toLowerCase)

    matches.foldLeft(0.0) { (total, m) =>
      val value = m.group(1).toDouble
      val unit = m.group(2)
      val seconds = unit match {
        case "s" => value
        case "m" => value * 60
        case "h" => value * 3600
        case _ => 0.0
      }
      total + seconds
    }
  }
}
