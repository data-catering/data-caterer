package io.github.datacatering.datacaterer.core.generator.execution.rate

import org.apache.log4j.Logger

import java.time.{Duration, LocalDateTime}

/**
 * Tracks duration-based execution timing
 */
class DurationTracker(durationString: String) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val targetDuration: Duration = parseDuration(durationString)
  private var startTime: Option[LocalDateTime] = None
  private var endTime: Option[LocalDateTime] = None

  def start(): Unit = {
    startTime = Some(LocalDateTime.now())
    endTime = Some(startTime.get.plus(targetDuration))
    LOGGER.info(s"Duration tracker started: duration=$durationString (${targetDuration.getSeconds}s), " +
      s"start-time=$startTime, end-time=$endTime")
  }

  def hasTimeRemaining: Boolean = {
    (startTime, endTime) match {
      case (Some(_), Some(end)) =>
        val now = LocalDateTime.now()
        val remaining = now.isBefore(end)
        if (LOGGER.isDebugEnabled && !remaining) {
          LOGGER.debug(s"Duration tracker finished: target-duration=$durationString, current-time=$now, end-time=$end")
        }
        remaining
      case _ =>
        LOGGER.warn("Duration tracker not started")
        false
    }
  }

  def getRemainingTimeMs: Long = {
    endTime match {
      case Some(end) =>
        val now = LocalDateTime.now()
        val remaining = Duration.between(now, end).toMillis
        math.max(0, remaining)
      case None => 0L
    }
  }

  def getElapsedTimeMs: Long = {
    startTime match {
      case Some(start) =>
        Duration.between(start, LocalDateTime.now()).toMillis
      case None => 0L
    }
  }

  private def parseDuration(durationStr: String): Duration = {
    // Parse duration strings like "5m", "30s", "1h", "2h30m", "100ms"
    // Match ms first to avoid confusion with m+s
    val pattern = """(\d+)(ms|[smh])""".r
    val matches = pattern.findAllMatchIn(durationStr).toList

    if (matches.isEmpty) {
      throw new IllegalArgumentException(s"Invalid duration format: $durationStr. " +
        s"Expected format: <number><unit> where unit is ms (milliseconds), s (seconds), m (minutes), or h (hours). " +
        s"Examples: '30s', '5m', '1h', '2h30m', '100ms'")
    }

    matches.foldLeft(Duration.ZERO) { case (duration, m) =>
      val value = m.group(1).toLong
      val unit = m.group(2)
      val toAdd = unit match {
        case "ms" => Duration.ofMillis(value)
        case "s" => Duration.ofSeconds(value)
        case "m" => Duration.ofMinutes(value)
        case "h" => Duration.ofHours(value)
        case _ => throw new IllegalArgumentException(s"Invalid duration unit: $unit")
      }
      duration.plus(toAdd)
    }
  }
}
