package io.github.datacatering.datacaterer.core.generator.result

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.github.datacatering.datacaterer.core.generator.metrics.PerformanceMetrics
import io.github.datacatering.datacaterer.core.util.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import java.time.format.DateTimeFormatter

/**
 * Exports performance metrics to CSV and JSON formats for external analysis.
 *
 * CSV Export:
 * - Batch-level metrics for time-series analysis
 * - Compatible with spreadsheet tools, R, Python pandas
 * - Easy to import into BI tools
 *
 * JSON Export:
 * - Complete performance data structure
 * - Summary metrics + batch details
 * - Machine-readable format for automation
 *
 * Phase 3 feature (deferred from Phase 2).
 */
class PerformanceMetricsExporter(implicit sparkSession: SparkSession) {

  private val LOGGER = Logger.getLogger(getClass.getName)
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  /**
   * Export performance metrics to CSV format
   */
  def exportToCsv(metrics: PerformanceMetrics, outputPath: String): Unit = {
    try {
      val csv = generateCsv(metrics)
      val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      FileUtil.writeStringToFile(fileSystem, outputPath, csv)
      LOGGER.info(s"Performance metrics exported to CSV: $outputPath")
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to export performance metrics to CSV: $outputPath", ex)
    }
  }

  /**
   * Export performance metrics to JSON format
   */
  def exportToJson(metrics: PerformanceMetrics, outputPath: String): Unit = {
    try {
      val json = generateJson(metrics)
      val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      FileUtil.writeStringToFile(fileSystem, outputPath, json)
      LOGGER.info(s"Performance metrics exported to JSON: $outputPath")
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to export performance metrics to JSON: $outputPath", ex)
    }
  }

  /**
   * Generate CSV content from performance metrics
   * Format: batch_number, start_time, end_time, records_generated, duration_ms, throughput, phase
   */
  private def generateCsv(metrics: PerformanceMetrics): String = {
    val header = "batch_number,start_time,end_time,records_generated,duration_ms,throughput_rps,phase\n"

    val rows = metrics.batchMetrics.map { batch =>
      s"${batch.batchNumber}," +
        s"${batch.startTime.format(timestampFormatter)}," +
        s"${batch.endTime.format(timestampFormatter)}," +
        s"${batch.recordsGenerated}," +
        s"${batch.batchDurationMs}," +
        s"${batch.throughput}," +
        s"${batch.phase}"
    }.mkString("\n")

    header + rows
  }

  /**
   * Generate JSON content from performance metrics
   * Includes summary statistics and batch-level details
   */
  private def generateJson(metrics: PerformanceMetrics): String = {
    val jsonStructure = Map(
      "summary" -> Map(
        "total_records" -> metrics.totalRecords,
        "total_duration_seconds" -> metrics.totalDurationSeconds,
        "average_throughput" -> metrics.averageThroughput,
        "max_throughput" -> metrics.maxThroughput,
        "min_throughput" -> metrics.minThroughput,
        "latency_p50_ms" -> metrics.latencyP50,
        "latency_p75_ms" -> metrics.latencyP75,
        "latency_p90_ms" -> metrics.latencyP90,
        "latency_p95_ms" -> metrics.latencyP95,
        "latency_p99_ms" -> metrics.latencyP99,
        "latency_p999_ms" -> metrics.latencyP999,
        "error_rate" -> metrics.errorRate,
        "start_time" -> metrics.startTime.map(_.format(timestampFormatter)).orNull,
        "end_time" -> metrics.endTime.map(_.format(timestampFormatter)).orNull
      ),
      "batches" -> metrics.batchMetrics.map { batch =>
        Map(
          "batch_number" -> batch.batchNumber,
          "start_time" -> batch.startTime.format(timestampFormatter),
          "end_time" -> batch.endTime.format(timestampFormatter),
          "records_generated" -> batch.recordsGenerated,
          "duration_ms" -> batch.batchDurationMs,
          "throughput_rps" -> batch.throughput,
          "phase" -> batch.phase
        )
      }
    )

    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonStructure)
  }

  /**
   * Generate summary CSV with only aggregate metrics
   */
  def exportSummaryToCsv(metrics: PerformanceMetrics, outputPath: String): Unit = {
    try {
      val csv = generateSummaryCsv(metrics)
      val fileSystem = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
      FileUtil.writeStringToFile(fileSystem, outputPath, csv)
      LOGGER.info(s"Performance summary exported to CSV: $outputPath")
    } catch {
      case ex: Exception =>
        LOGGER.error(s"Failed to export performance summary to CSV: $outputPath", ex)
    }
  }

  /**
   * Generate summary-only CSV
   */
  private def generateSummaryCsv(metrics: PerformanceMetrics): String = {
    val header = "metric,value\n"

    val rows = List(
      s"total_records,${metrics.totalRecords}",
      s"total_duration_seconds,${metrics.totalDurationSeconds}",
      s"average_throughput_rps,${metrics.averageThroughput}",
      s"max_throughput_rps,${metrics.maxThroughput}",
      s"min_throughput_rps,${metrics.minThroughput}",
      s"latency_p50_ms,${metrics.latencyP50}",
      s"latency_p75_ms,${metrics.latencyP75}",
      s"latency_p90_ms,${metrics.latencyP90}",
      s"latency_p95_ms,${metrics.latencyP95}",
      s"latency_p99_ms,${metrics.latencyP99}",
      s"latency_p999_ms,${metrics.latencyP999}",
      s"error_rate,${metrics.errorRate}",
      s"start_time,${metrics.startTime.map(_.format(timestampFormatter)).getOrElse("")}",
      s"end_time,${metrics.endTime.map(_.format(timestampFormatter)).getOrElse("")}"
    ).mkString("\n")

    header + rows
  }

  /**
   * Export all formats (CSV, summary CSV, JSON) to a directory
   */
  def exportAll(metrics: PerformanceMetrics, reportFolder: String): Unit = {
    exportToCsv(metrics, s"$reportFolder/performance_metrics.csv")
    exportSummaryToCsv(metrics, s"$reportFolder/performance_summary.csv")
    exportToJson(metrics, s"$reportFolder/performance_metrics.json")
  }
}

object PerformanceMetricsExporter {

  def apply()(implicit sparkSession: SparkSession): PerformanceMetricsExporter = {
    new PerformanceMetricsExporter()
  }
}
