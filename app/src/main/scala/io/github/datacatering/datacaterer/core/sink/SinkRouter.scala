package io.github.datacatering.datacaterer.core.sink

import io.github.datacatering.datacaterer.core.generator.execution.GenerationMode
import org.apache.log4j.Logger

/**
 * Routes data to appropriate sink writers based on format, generation mode, and configuration.
 * Centralizes routing logic that was previously scattered across BatchDataProcessor, SinkFactory, and sink writers.
 */
class SinkRouter {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Real-time formats that support row-by-row streaming
  private val REAL_TIME_FORMATS = Set(
    "http", "jms", "kafka", 
    "jdbc", "cassandra", "mongodb", 
    "postgresql", "mysql"
  )

  /**
   * Determine which sink strategy to use based on:
   * - Data format (file vs real-time source)
   * - Generation mode (batched vs all-upfront)
   * - Step configuration (rate control)
   * 
   * @param format Data format (e.g., "json", "http", "kafka")
   * @param generationMode How data was generated (Batched, AllUpfront, Progressive)
   * @param stepOptions Step configuration options
   * @return SinkStrategy indicating which sink writer to use
   */
  def determineSinkStrategy(
    format: String,
    generationMode: GenerationMode,
    stepOptions: Map[String, String]
  ): SinkStrategy = {
    val isRealTimeFormat = REAL_TIME_FORMATS.contains(format.toLowerCase)
    val hasRateControl = stepOptions.contains("rowsPerSecond") || stepOptions.get("hasRateControl").contains("true")

    val strategy = (isRealTimeFormat, generationMode, hasRateControl) match {
      case (true, GenerationMode.AllUpfront, true) =>
        // Real-time sink with all data generated upfront and rate control configured
        LOGGER.debug(s"Using StreamingSink for format=$format with rate control")
        SinkStrategy.StreamingSink

      case (false, GenerationMode.AllUpfront, true) =>
        // File-based sink - rate control doesn't apply, just write all data
        LOGGER.debug(s"Using BatchSink for format=$format (rate control ignored)")
        SinkStrategy.BatchSink

      case _ =>
        // Default to batch sink for all other scenarios
        LOGGER.debug(s"Using BatchSink for format=$format, generationMode=$generationMode")
        SinkStrategy.BatchSink
    }

    strategy
  }
}

/**
 * Sink strategy determines which writer to use for pushing data
 */
sealed trait SinkStrategy

object SinkStrategy {
  /**
   * Use batch writer - writes all data at once using Spark DataFrameWriter
   * Suitable for: files (CSV, JSON, Parquet), databases without rate control
   */
  case object BatchSink extends SinkStrategy

  /**
   * Use Pekko streaming writer with throttling
   * Suitable for: HTTP, JMS, Kafka with all data generated upfront and rate control
   */
  case object StreamingSink extends SinkStrategy
}

