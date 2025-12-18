package io.github.datacatering.datacaterer.core.generator.result

import io.github.datacatering.datacaterer.core.generator.metrics.PerformanceMetrics
import scalatags.Text.all._
import scalatags.Text.tags2

/**
 * HTML writer for performance testing reports with charts and visualizations.
 * Generates interactive performance reports using Chart.js.
 */
class PerformanceHtmlWriter {

  /**
   * Generate performance report section as HTML
   */
  def performanceSection(metrics: PerformanceMetrics): String = {
    div(cls := "container-fluid mt-4",
      h2(cls := "mb-4", i(cls := "bi bi-speedometer2 me-2"), "Performance Metrics"),

      // Summary cards
      summaryCards(metrics),

      // Charts
      div(cls := "row mt-4",
        div(cls := "col-lg-6 mb-4",
          chartCard("throughputChart", "Throughput Over Time", "Records per second throughout the test execution")
        ),
        div(cls := "col-lg-6 mb-4",
          chartCard("latencyChart", "Latency Percentiles", "Batch processing latency distribution")
        )
      ),

      div(cls := "row",
        div(cls := "col-lg-12 mb-4",
          chartCard("timelineChart", "Execution Timeline", "Batch execution timeline showing records generated")
        )
      ),

      // Batch details table
      batchDetailsTable(metrics),

      // Chart initialization scripts
      chartScripts(metrics)
    ).render
  }

  /**
   * Generate summary cards with key metrics
   */
  private def summaryCards(metrics: PerformanceMetrics) = {
    div(cls := "row mb-4",
      metricCard("Total Records", metrics.totalRecords.toString, "bi-database", "primary"),
      metricCard("Avg Throughput", f"${metrics.averageThroughput}%.2f rec/s", "bi-speedometer", "success"),
      metricCard("P95 Latency", f"${metrics.latencyP95}%.2f ms", "bi-clock-history", "info"),
      metricCard("Duration", s"${metrics.totalDurationSeconds}s", "bi-hourglass-split", "warning"),
      metricCard("Error Rate", f"${metrics.errorRate * 100}%.2f%%", "bi-exclamation-triangle", if (metrics.errorRate > 0.01) "danger" else "success"),
      metricCard("Total Batches", metrics.batchMetrics.size.toString, "bi-layers", "secondary")
    )
  }

  /**
   * Generate a single metric card
   */
  private def metricCard(title: String, value: String, icon: String, colorClass: String) = {
    div(cls := "col-md-2 mb-3",
      div(cls := s"card border-$colorClass",
        div(cls := "card-body text-center",
          i(cls := s"bi $icon text-$colorClass", style := "font-size: 2rem;"),
          h6(cls := "card-title mt-2 text-muted", title),
          h4(cls := s"card-text text-$colorClass", value)
        )
      )
    )
  }

  /**
   * Generate chart card container
   */
  private def chartCard(canvasId: String, title: String, description: String) = {
    div(cls := "card",
      div(cls := "card-header",
        h5(cls := "mb-0", title),
        small(cls := "text-muted", description)
      ),
      div(cls := "card-body",
        canvas(id := canvasId, style := "max-height: 300px;")
      )
    )
  }

  /**
   * Generate batch details table
   */
  private def batchDetailsTable(metrics: PerformanceMetrics) = {
    if (metrics.batchMetrics.isEmpty) {
      div()
    } else {
      div(cls := "card mt-4",
        div(cls := "card-header",
          h5(cls := "mb-0", "Batch Details")
        ),
        div(cls := "card-body p-0",
          div(cls := "table-responsive",
            table(cls := "table table-sm table-hover mb-0",
              thead(cls := "table-light",
                tr(
                  th("Batch #"),
                  th("Start Time"),
                  th("Records"),
                  th("Duration (ms)"),
                  th("Throughput (rec/s)")
                )
              ),
              tbody(
                metrics.batchMetrics.take(100).map { batch =>
                  tr(
                    td(batch.batchNumber.toString),
                    td(batch.startTime.toString.split("T")(1).split("\\.").head),
                    td(batch.recordsGenerated.toString),
                    td(f"${batch.batchDurationMs}%,d"),
                    td(f"${batch.throughput}%.2f")
                  )
                }
              )
            )
          ),
          if (metrics.batchMetrics.size > 100) {
            div(cls := "card-footer text-muted text-center",
              small(s"Showing first 100 of ${metrics.batchMetrics.size} batches")
            )
          } else {
            div()
          }
        )
      )
    }
  }

  /**
   * Generate Chart.js initialization scripts
   */
  private def chartScripts(metrics: PerformanceMetrics) = {
    if (metrics.batchMetrics.isEmpty) {
      script()
    } else {
      val throughputData = metrics.batchMetrics.map(_.throughput).mkString(",")
      val batchNumbers = metrics.batchMetrics.map(_.batchNumber).mkString(",")
      val recordsData = metrics.batchMetrics.map(_.recordsGenerated).mkString(",")
      val latencyData = s"${metrics.latencyP50},${metrics.latencyP75},${metrics.latencyP90},${metrics.latencyP95},${metrics.latencyP99}"

      script(raw(
        s"""
        |// Wait for Chart.js to load
        |if (typeof Chart !== 'undefined') {
        |  initializeCharts();
        |} else {
        |  window.addEventListener('load', initializeCharts);
        |}
        |
        |function initializeCharts() {
        |  // Throughput chart
        |  const throughputCtx = document.getElementById('throughputChart');
        |  if (throughputCtx) {
        |    new Chart(throughputCtx, {
        |      type: 'line',
        |      data: {
        |        labels: [$batchNumbers],
        |        datasets: [{
        |          label: 'Throughput (rec/s)',
        |          data: [$throughputData],
        |          borderColor: 'rgb(75, 192, 192)',
        |          backgroundColor: 'rgba(75, 192, 192, 0.1)',
        |          tension: 0.1,
        |          fill: true
        |        }]
        |      },
        |      options: {
        |        responsive: true,
        |        maintainAspectRatio: false,
        |        plugins: {
        |          legend: { display: false },
        |          tooltip: {
        |            callbacks: {
        |              label: function(context) {
        |                return 'Throughput: ' + context.parsed.y.toFixed(2) + ' rec/s';
        |              }
        |            }
        |          }
        |        },
        |        scales: {
        |          x: { title: { display: true, text: 'Batch Number' } },
        |          y: { title: { display: true, text: 'Records/Second' }, beginAtZero: true }
        |        }
        |      }
        |    });
        |  }
        |
        |  // Latency percentiles chart
        |  const latencyCtx = document.getElementById('latencyChart');
        |  if (latencyCtx) {
        |    new Chart(latencyCtx, {
        |      type: 'bar',
        |      data: {
        |        labels: ['P50', 'P75', 'P90', 'P95', 'P99'],
        |        datasets: [{
        |          label: 'Latency (ms)',
        |          data: [$latencyData],
        |          backgroundColor: [
        |            'rgba(54, 162, 235, 0.8)',
        |            'rgba(75, 192, 192, 0.8)',
        |            'rgba(255, 206, 86, 0.8)',
        |            'rgba(255, 159, 64, 0.8)',
        |            'rgba(255, 99, 132, 0.8)'
        |          ]
        |        }]
        |      },
        |      options: {
        |        responsive: true,
        |        maintainAspectRatio: false,
        |        plugins: {
        |          legend: { display: false },
        |          tooltip: {
        |            callbacks: {
        |              label: function(context) {
        |                return 'Latency: ' + context.parsed.y.toFixed(2) + ' ms';
        |              }
        |            }
        |          }
        |        },
        |        scales: {
        |          x: { title: { display: true, text: 'Percentile' } },
        |          y: { title: { display: true, text: 'Milliseconds' }, beginAtZero: true }
        |        }
        |      }
        |    });
        |  }
        |
        |  // Timeline chart
        |  const timelineCtx = document.getElementById('timelineChart');
        |  if (timelineCtx) {
        |    new Chart(timelineCtx, {
        |      type: 'bar',
        |      data: {
        |        labels: [$batchNumbers],
        |        datasets: [{
        |          label: 'Records Generated',
        |          data: [$recordsData],
        |          backgroundColor: 'rgba(153, 102, 255, 0.6)',
        |          borderColor: 'rgba(153, 102, 255, 1)',
        |          borderWidth: 1
        |        }]
        |      },
        |      options: {
        |        responsive: true,
        |        maintainAspectRatio: false,
        |        plugins: {
        |          legend: { display: false }
        |        },
        |        scales: {
        |          x: { title: { display: true, text: 'Batch Number' } },
        |          y: { title: { display: true, text: 'Records' }, beginAtZero: true }
        |        }
        |      }
        |    });
        |  }
        |}
        |""".stripMargin
      ))
    }
  }

  /**
   * Generate standalone performance report page
   */
  def performanceReport(metrics: PerformanceMetrics): String = {
    html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        tags2.title("Performance Report - Data Caterer"),
        link(rel := "stylesheet", href := "https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css"),
        link(rel := "stylesheet", href := "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/bootstrap-icons.min.css"),
        script(src := "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.js")
      ),
      body(
        tags2.nav(cls := "navbar navbar-dark bg-dark",
          div(cls := "container-fluid",
            span(cls := "navbar-brand", "Performance Report")
          )
        ),
        performanceSection(metrics)
      )
    ).render
  }
}
