package io.github.datacatering.datacaterer.core.generator.result

import io.github.datacatering.datacaterer.api.model.{DataSourceResult, DataSourceResultSummary, FlagsConfig, Plan, Step, StepResultSummary, TaskResultSummary, Validation, ValidationConfig, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.listener.{SparkRecordListener, SparkTaskRecordSummary}
import io.github.datacatering.datacaterer.core.model.Constants.{REPORT_DATA_SOURCES_HTML, REPORT_FIELDS_HTML, REPORT_HOME_HTML, REPORT_VALIDATIONS_HTML}
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import io.github.datacatering.datacaterer.core.util.PlanImplicits.CountOps
import org.joda.time.DateTime
import scalatags.Text.all._
import scalatags.Text.tags2
import scalatags.generic.AttrPair

import scala.math.BigDecimal.RoundingMode

/**
 * Modern HTML report generator using Scalatags for better maintainability
 * and modern UI frameworks (Bootstrap 5, Chart.js 4, Alpine.js)
 */
class ModernHtmlWriter {

  private val BOOTSTRAP_VERSION = "5.3.2"
  private val CHARTJS_VERSION = "4.4.1"
  private val ALPINEJS_VERSION = "3.13.3"

  // Custom attributes
  private val integrity = attr("integrity")
  private val crossorigin = attr("crossorigin")
  private val defer = attr("defer")
  private val ariaValuenow = attr("aria-valuenow")
  private val ariaValuemin = attr("aria-valuemin")
  private val ariaValuemax = attr("aria-valuemax")

  /**
   * Generate the main index page with overview
   */
  def index(plan: Plan, stepResultSummary: List[StepResultSummary], taskResultSummary: List[TaskResultSummary],
            dataSourceResultSummary: List[DataSourceResultSummary], validationResults: List[ValidationConfigResult],
            flagsConfig: FlagsConfig, sparkRecordListener: SparkRecordListener): String = {
    html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        tags2.title("Data Caterer - Report"),
        link(rel := "icon", href := "data_catering_transparent.svg"),
        externalDependencies,
        customStyles
      ),
      body(
        topNavBar,
        div(cls := "container-fluid mt-3",
          overview(plan, stepResultSummary, taskResultSummary, dataSourceResultSummary, validationResults, flagsConfig, sparkRecordListener)
        ),
        bodyScripts
      )
    ).render
  }

  /**
   * External CSS and JS dependencies
   */
  private def externalDependencies: Seq[Modifier] = Seq(
    // Bootstrap 5
    link(
      href := s"https://cdn.jsdelivr.net/npm/bootstrap@$BOOTSTRAP_VERSION/dist/css/bootstrap.min.css",
      rel := "stylesheet",
      integrity := "sha384-T3c6CoIi6uLrA9TneNEoa7RxnatzjcDSCmG1MXxSR1GAsXEV/Dwwykc2MPK8M2HN",
      crossorigin := "anonymous"
    ),
    // Bootstrap Icons
    link(
      rel := "stylesheet",
      href := "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.2/font/bootstrap-icons.min.css"
    ),
    // Chart.js 4
    script(src := s"https://cdn.jsdelivr.net/npm/chart.js@$CHARTJS_VERSION/dist/chart.umd.js")
  )

  /**
   * Custom CSS styles including dark mode support
   */
  private def customStyles = tag("style")(
    """
      |:root {
      |  --dc-primary: #ff6e42;
      |  --dc-primary-dark: #ff9100;
      |  --dc-bg-light: #ffffff;
      |  --dc-bg-dark: #1a1a1a;
      |  --dc-text-light: #212529;
      |  --dc-text-dark: #e9ecef;
      |  --dc-card-light: #ffffff;
      |  --dc-card-dark: #2d2d2d;
      |  --dc-border-light: #dee2e6;
      |  --dc-border-dark: #495057;
      |}
      |
      |body {
      |  transition: background-color 0.3s, color 0.3s;
      |  background-color: var(--dc-bg-light);
      |  color: var(--dc-text-light);
      |}
      |
      |body.dark {
      |  background-color: var(--dc-bg-dark);
      |  color: var(--dc-text-dark);
      |}
      |
      |.dark .card {
      |  background-color: var(--dc-card-dark);
      |  border-color: var(--dc-border-dark);
      |}
      |
      |.dark .table {
      |  color: var(--dc-text-dark);
      |  --bs-table-bg: var(--dc-card-dark);
      |  --bs-table-striped-bg: rgba(255, 255, 255, 0.05);
      |}
      |
      |.dark .navbar {
      |  background-color: var(--dc-card-dark) !important;
      |  border-bottom: 1px solid var(--dc-border-dark);
      |}
      |
      |.top-banner {
      |  background: linear-gradient(135deg, var(--dc-primary) 0%, var(--dc-primary-dark) 100%);
      |  padding: 1rem;
      |  box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      |}
      |
      |.top-banner .logo {
      |  height: 40px;
      |  transition: transform 0.2s;
      |}
      |
      |.top-banner .logo:hover {
      |  transform: scale(1.05);
      |}
      |
      |.stat-card {
      |  border-left: 4px solid var(--dc-primary);
      |  transition: transform 0.2s, box-shadow 0.2s;
      |}
      |
      |.stat-card:hover {
      |  transform: translateY(-2px);
      |  box-shadow: 0 4px 8px rgba(0,0,0,0.1);
      |}
      |
      |.stat-card .stat-icon {
      |  font-size: 2rem;
      |  color: var(--dc-primary);
      |}
      |
      |.stat-card .stat-value {
      |  font-size: 1.75rem;
      |  font-weight: 600;
      |}
      |
      |.stat-card .stat-label {
      |  color: #6c757d;
      |  font-size: 0.875rem;
      |  text-transform: uppercase;
      |  letter-spacing: 0.5px;
      |}
      |
      |.dark .stat-card .stat-label {
      |  color: #adb5bd;
      |}
      |
      |.chart-container {
      |  position: relative;
      |  height: 300px;
      |  margin: 1rem 0;
      |}
      |
      |.badge-custom {
      |  padding: 0.5em 0.75em;
      |  border-radius: 0.25rem;
      |}
      |
      |.table-responsive {
      |  border-radius: 0.5rem;
      |  overflow: hidden;
      |}
      |
      |.dark-mode-toggle {
      |  cursor: pointer;
      |  font-size: 1.25rem;
      |  color: white;
      |  transition: color 0.2s;
      |}
      |
      |.dark-mode-toggle:hover {
      |  color: var(--dc-primary-dark);
      |}
      |
      |.progress {
      |  height: 24px;
      |  border-radius: 0.5rem;
      |  background-color: #e9ecef;
      |}
      |
      |.dark .progress {
      |  background-color: #495057;
      |}
      |
      |.progress-bar {
      |  border-radius: 0.5rem;
      |  transition: width 0.6s ease;
      |}
      |""".stripMargin
  )

  /**
   * Top navigation bar with logo and dark mode toggle
   */
  private def topNavBar: Seq[Modifier] = Seq(
    div(cls := "top-banner",
      div(cls := "container-fluid d-flex justify-content-between align-items-center",
        div(cls := "d-flex align-items-center",
          a(href := REPORT_HOME_HTML, cls := "text-decoration-none",
            img(src := "data_catering_transparent.svg", cls := "logo", alt := "Data Caterer Logo")
          ),
          h4(cls := "text-white mb-0 ms-3", "Data Caterer")
        ),
        div(cls := "d-flex align-items-center gap-3",
          span(cls := "text-white",
            small(s"Generated: ${DateTime.now().toString("yyyy-MM-dd HH:mm:ss")}")
          )
        )
      )
    ),
    tags2.nav(cls := "navbar navbar-expand-lg navbar-light bg-white shadow-sm",
      div(cls := "container-fluid",
        div(cls := "navbar-nav",
          a(href := REPORT_HOME_HTML, cls := "nav-link", i(cls := "bi bi-house-door me-1"), "Overview"),
          a(href := REPORT_DATA_SOURCES_HTML, cls := "nav-link", i(cls := "bi bi-database me-1"), "Data Sources"),
          a(href := REPORT_FIELDS_HTML, cls := "nav-link", i(cls := "bi bi-list-columns me-1"), "Fields"),
          a(href := REPORT_VALIDATIONS_HTML, cls := "nav-link", i(cls := "bi bi-check-circle me-1"), "Validations")
        )
      )
    )
  )

  /**
   * Overview section with summary statistics and visualizations
   */
  def overview(plan: Plan, stepResultSummary: List[StepResultSummary], taskResultSummary: List[TaskResultSummary],
               dataSourceResultSummary: List[DataSourceResultSummary], validationResults: List[ValidationConfigResult],
               flagsConfig: FlagsConfig, sparkRecordListener: SparkRecordListener): Seq[Modifier] = {
    val totalRecords = stepResultSummary.map(_.numRecords).sum
    val isSuccess = stepResultSummary.forall(_.isSuccess)
    val successRate = if (stepResultSummary.nonEmpty) {
      BigDecimal((stepResultSummary.count(_.isSuccess).toDouble / stepResultSummary.size) * 100)
        .setScale(1, RoundingMode.HALF_UP)
    } else BigDecimal(0)

    Seq(
      // Summary Statistics Cards
      div(cls := "row g-3 mb-4",
        statCard("bi-file-earmark-text", plan.name, "Plan Name", "primary"),
        statCard("bi-database", totalRecords.toString, "Total Records", "success"),
        statCard("bi-list-task", taskResultSummary.size.toString, "Tasks", "info"),
        statCard("bi-bar-chart", stepResultSummary.size.toString, "Steps", "warning"),
        statCard("bi-check-circle", s"$successRate%", "Success Rate", if (isSuccess) "success" else "danger"),
        statCard("bi-shield-check", dataSourceResultSummary.size.toString, "Data Sources", "secondary")
      ),

      // Plan Details Table
      h3(cls := "mt-4 mb-3", i(cls := "bi bi-file-earmark-text me-2"), "Plan Details"),
      div(cls := "card shadow-sm mb-4",
        div(cls := "card-body",
          planDetailsModern(plan, stepResultSummary, taskResultSummary, dataSourceResultSummary)
        )
      ),

      // Flags Configuration
      h3(cls := "mt-4 mb-3", i(cls := "bi bi-toggles me-2"), "Configuration Flags"),
      div(cls := "card shadow-sm mb-4",
        div(cls := "card-body",
          flagsSummaryModern(flagsConfig)
        )
      ),

      // Throughput Chart
      h3(cls := "mt-4 mb-3", i(cls := "bi bi-graph-up me-2"), "Output Throughput"),
      div(cls := "card shadow-sm mb-4",
        div(cls := "card-body",
          createLineGraphModern("outputRowsPerSecond", sparkRecordListener.outputRows.toList)
        )
      ),

      // Tasks Summary
      h3(cls := "mt-4 mb-3", i(cls := "bi bi-list-task me-2"), "Tasks Summary"),
      div(cls := "card shadow-sm mb-4",
        div(cls := "card-body",
          tasksSummaryModern(taskResultSummary)
        )
      ),

      // Validations Summary
      if (validationResults.nonEmpty) {
        Seq(
          h3(cls := "mt-4 mb-3", i(cls := "bi bi-check-circle me-2"), "Validations Summary"),
          div(cls := "card shadow-sm mb-4",
            div(cls := "card-body",
              validationSummaryModern(validationResults)
            )
          )
        )
      } else frag()
    )
  }

  /**
   * Create a statistic card
   */
  private def statCard(icon: String, value: String, label: String, variant: String) =
    div(cls := "col-md-4 col-lg-2",
      div(cls := "card stat-card shadow-sm h-100",
        div(cls := "card-body d-flex flex-column justify-content-between",
          div(cls := "d-flex justify-content-between align-items-start mb-2",
            i(cls := s"bi $icon stat-icon"),
            span(cls := s"badge bg-$variant badge-custom", label)
          ),
          div(
            div(cls := "stat-value", value),
            div(cls := "stat-label mt-1", label)
          )
        )
      )
    )

  /**
   * Plan details table with comprehensive information
   */
  private def planDetailsModern(plan: Plan, stepResultSummary: List[StepResultSummary],
                                 taskResultSummary: List[TaskResultSummary],
                                 dataSourceResultSummary: List[DataSourceResultSummary]) = {
    val totalRecords = stepResultSummary.map(_.numRecords).sum
    val isSuccess = stepResultSummary.forall(_.isSuccess)
    val foreignKeys = plan.sinkOptions.map(_.foreignKeys).getOrElse(Map())

    div(cls := "table-responsive",
      table(cls := "table table-bordered",
        thead(cls := "table-light",
          tr(
            th("Plan Name"),
            th("Num Records"),
            th("Success"),
            th("Tasks"),
            th("Steps"),
            th("Data Sources"),
            th("Foreign Keys")
          )
        ),
        tbody(
          tr(
            td(strong(plan.name)),
            td(span(cls := "badge bg-info", totalRecords.toString)),
            td(
              if (isSuccess)
                span(cls := "badge bg-success", i(cls := "bi bi-check-circle-fill me-1"), "Success")
              else
                span(cls := "badge bg-danger", i(cls := "bi bi-x-circle-fill me-1"), "Failed")
            ),
            td(taskResultSummary.size.toString),
            td(stepResultSummary.size.toString),
            td(dataSourceResultSummary.size.toString),
            td(
              if (foreignKeys.nonEmpty)
                pre(cls := "mb-0 small", ObjectMapperUtil.jsonObjectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(foreignKeys))
              else
                em(cls := "text-muted", "None")
            )
          )
        )
      )
    )
  }

  /**
   * Modern flags summary using badges
   */
  private def flagsSummaryModern(flagsConfig: FlagsConfig) = {
    val flags = List(
      ("Generate Metadata", flagsConfig.enableGeneratePlanAndTasks),
      ("Generate Data", flagsConfig.enableGenerateData),
      ("Record Tracking", flagsConfig.enableRecordTracking),
      ("Delete Data", flagsConfig.enableDeleteGeneratedRecords),
      ("Calculate Metadata", flagsConfig.enableSinkMetadata),
      ("Validate Data", flagsConfig.enableValidation),
      ("Unique Check", flagsConfig.enableUniqueCheck)
    )

    div(cls := "d-flex flex-wrap gap-2",
      flags.map { case (name, enabled) =>
        span(
          cls := s"badge ${if (enabled) "bg-success" else "bg-secondary"} badge-custom",
          i(cls := s"bi ${if (enabled) "bi-check-circle-fill" else "bi-x-circle-fill"} me-1"),
          name
        )
      }
    )
  }

  /**
   * Modern tasks summary table
   */
  private def tasksSummaryModern(taskResultSummary: List[TaskResultSummary]) = {
    if (taskResultSummary.isEmpty) {
      div(cls := "alert alert-info", i(cls := "bi bi-info-circle me-2"), "No tasks found")
    } else {
      div(cls := "table-responsive",
        table(cls := "table table-hover table-striped",
          thead(cls := "table-dark",
            tr(
              th("Task Name"),
              th("Records"),
              th("Status"),
              th("Steps")
            )
          ),
          tbody(
            taskResultSummary.map(taskResult =>
              tr(
                td(
                  a(href := s"tasks.html#${taskResult.task.name}", cls := "text-decoration-none",
                    i(cls := "bi bi-box me-1"),
                    taskResult.task.name
                  )
                ),
                td(
                  span(cls := "badge bg-info", taskResult.numRecords.toString)
                ),
                td(
                  if (taskResult.isSuccess)
                    span(cls := "badge bg-success", i(cls := "bi bi-check-circle-fill me-1"), "Success")
                  else
                    span(cls := "badge bg-danger", i(cls := "bi bi-x-circle-fill me-1"), "Failed")
                ),
                td(
                  taskResult.task.steps.map(step =>
                    a(
                      href := s"$REPORT_FIELDS_HTML#${step.name}",
                      cls := "badge bg-secondary text-decoration-none me-1",
                      step.name
                    )
                  )
                )
              )
            )
          )
        )
      )
    }
  }

  /**
   * Modern validation summary
   */
  private def validationSummaryModern(validationResults: List[ValidationConfigResult]) = {
    div(cls := "table-responsive",
      table(cls := "table table-hover table-striped",
        thead(cls := "table-dark",
          tr(
            th("Name"),
            th("Data Sources"),
            th("Description"),
            th("Success Rate")
          )
        ),
        tbody(
          validationResults.map(validationConfRes => {
            val resultsForDataSource = validationConfRes.dataSourceValidationResults.flatMap(_.validationResults)
            val numSuccess = resultsForDataSource.count(_.isSuccess)
            val total = resultsForDataSource.size
            val percent = if (total > 0) BigDecimal((numSuccess.toDouble / total) * 100).setScale(1, RoundingMode.HALF_UP) else BigDecimal(0)

            tr(
              td(
                a(href := s"$REPORT_VALIDATIONS_HTML#${validationConfRes.name}", cls := "text-decoration-none",
                  i(cls := "bi bi-shield-check me-1"),
                  validationConfRes.name
                )
              ),
              td(
                validationConfRes.dataSourceValidationResults.map(_.dataSourceName).distinct.map(dsName =>
                  a(
                    href := s"$REPORT_DATA_SOURCES_HTML#$dsName",
                    cls := "badge bg-secondary text-decoration-none me-1",
                    dsName
                  )
                )
              ),
              td(validationConfRes.description),
              td(
                div(cls := "progress",
                  div(
                    cls := s"progress-bar ${if (percent >= 100) "bg-success" else if (percent >= 50) "bg-warning" else "bg-danger"}",
                    role := "progressbar",
                    style := s"width: $percent%",
                    ariaValuenow := percent.toString(),
                    ariaValuemin := "0",
                    ariaValuemax := "100",
                    s"$numSuccess/$total ($percent%)"
                  )
                )
              )
            )
          })
        )
      )
    )
  }

  /**
   * Create modern line graph using Chart.js 4
   */
  private def createLineGraphModern(chartId: String, recordSummary: List[SparkTaskRecordSummary]): Frag = {
    if (recordSummary.isEmpty) {
      div(cls := "alert alert-info", i(cls := "bi bi-info-circle me-2"), "No throughput data available")
    } else {
      val sumRowsPerFinishTime = recordSummary
        .map(x => {
          val roundFinishTimeToSecond = x.finishTime - (x.finishTime % 1000) + 1000
          (roundFinishTimeToSecond, x.numRecords)
        })
        .groupBy(_._1)
        .map(t => (t._1, t._2.map(_._2).sum))

      val sortedSumRows = sumRowsPerFinishTime.toList.sortBy(_._1)
      val timeSeriesValues = (sortedSumRows.head._1 to sortedSumRows.last._1 by 1000)
        .map(t => (t, sumRowsPerFinishTime.getOrElse(t, 0L)))
        .toList

      val labels = timeSeriesValues.map(x => new DateTime(x._1).toString("HH:mm:ss"))
      val dataValues = timeSeriesValues.map(_._2)

      Seq(
        div(cls := "chart-container",
          canvas(id := chartId)
        ),
        script(raw(
          s"""
             |const ctx${chartId} = document.getElementById('$chartId').getContext('2d');
             |new Chart(ctx${chartId}, {
             |  type: 'line',
             |  data: {
             |    labels: ${labels.map(l => s"'$l'").mkString("[", ",", "]")},
             |    datasets: [{
             |      label: 'Rows/Second',
             |      data: ${dataValues.mkString("[", ",", "]")},
             |      borderColor: 'rgb(255, 110, 66)',
             |      backgroundColor: 'rgba(255, 110, 66, 0.1)',
             |      borderWidth: 2,
             |      fill: true,
             |      tension: 0.4
             |    }]
             |  },
             |  options: {
             |    responsive: true,
             |    maintainAspectRatio: false,
             |    plugins: {
             |      legend: {
             |        display: true,
             |        position: 'top'
             |      },
             |      tooltip: {
             |        mode: 'index',
             |        intersect: false
             |      }
             |    },
             |    scales: {
             |      y: {
             |        beginAtZero: true,
             |        grid: {
             |          color: 'rgba(0, 0, 0, 0.1)'
             |        }
             |      },
             |      x: {
             |        grid: {
             |          display: false
             |        }
             |      }
             |    }
             |  }
             |});
             |""".stripMargin
        ))
      )
    }
  }

  /**
   * Body scripts for Bootstrap
   */
  private def bodyScripts = script(
    src := s"https://cdn.jsdelivr.net/npm/bootstrap@$BOOTSTRAP_VERSION/dist/js/bootstrap.bundle.min.js",
    integrity := "sha384-C6RzsynM9kWDrMNeT87bh95OGNyZPhcTNXj1NW7RuBCsyN/o0jlpcV8Qyq46cDfL",
    crossorigin := "anonymous"
  )

  /**
   * SVG logo (same as before)
   */
  def dataCateringSvg: String = {
    """<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="1080" zoomAndPan="magnify" viewBox="0 0 810 809.999993" height="1080" preserveAspectRatio="xMidYMid meet" version="1.0"><defs><g/><clipPath id="d7af0a96c1"><path d="M 43.5 10 L 767.203125 10 L 767.203125 800 L 43.5 800 Z M 43.5 10 " clip-rule="nonzero"/></clipPath><clipPath id="0d6c067e67"><path d="M 43.5 281.675781 L 43.5 528.097656 C 43.5 580.964844 71.75 629.835938 117.597656 656.269531 L 331.273438 779.472656 C 377.121094 805.90625 433.621094 805.90625 479.464844 779.472656 L 693.144531 656.269531 C 738.988281 629.835938 767.238281 580.964844 767.238281 528.097656 L 767.238281 281.675781 C 767.238281 228.808594 738.988281 179.9375 693.144531 153.503906 L 479.464844 30.300781 C 433.621094 3.867188 377.121094 3.867188 331.273438 30.300781 L 117.597656 153.503906 C 71.75 179.957031 43.5 228.808594 43.5 281.675781 Z M 43.5 281.675781 " clip-rule="nonzero"/></clipPath></defs><g clip-path="url(#d7af0a96c1)"><g clip-path="url(#0d6c067e67)"><path fill="#36699f" d="M 43.5 803.773438 L 43.5 6.226562 L 767.203125 6.226562 L 767.203125 803.773438 Z M 43.5 803.773438 " fill-opacity="1" fill-rule="nonzero"/></g></g><g fill="#ff00ff" fill-opacity="1"><g transform="translate(206.24592, 640.079332)"><g><path d="M 197.644531 0.652344 C 345.714844 0 407.03125 -73.710938 407.03125 -232.214844 C 407.03125 -391.375 345.714844 -452.039062 197.644531 -452.039062 L 105.671875 -452.039062 C 69.796875 -452.039062 50.226562 -431.816406 50.226562 -393.984375 C 50.226562 -196.339844 49.574219 -113.5 48.921875 -54.140625 C 48.921875 -18.917969 68.492188 0 104.367188 0 Z M 171.554688 -90.015625 C 170.902344 -112.195312 170.902344 -136.328125 170.902344 -159.8125 C 170.902344 -271.355469 170.902344 -317.015625 171.554688 -362.023438 L 195.035156 -362.023438 C 265.484375 -362.023438 286.355469 -326.144531 286.355469 -226.34375 C 286.355469 -138.9375 262.875 -90.015625 195.035156 -90.015625 Z M 171.554688 -90.015625 "/></g></g></g><g fill="#00ffff" fill-opacity="1"><g transform="translate(181.784409, 640.079332)"><g><path d="M 197.644531 0.652344 C 345.714844 0 407.03125 -73.710938 407.03125 -232.214844 C 407.03125 -391.375 345.714844 -452.039062 197.644531 -452.039062 L 105.671875 -452.039062 C 69.796875 -452.039062 50.226562 -431.816406 50.226562 -393.984375 C 50.226562 -196.339844 49.574219 -113.5 48.921875 -54.140625 C 48.921875 -18.917969 68.492188 0 104.367188 0 Z M 171.554688 -90.015625 C 170.902344 -112.195312 170.902344 -136.328125 170.902344 -159.8125 C 170.902344 -271.355469 170.902344 -317.015625 171.554688 -362.023438 L 195.035156 -362.023438 C 265.484375 -362.023438 286.355469 -326.144531 286.355469 -226.34375 C 286.355469 -138.9375 262.875 -90.015625 195.035156 -90.015625 Z M 171.554688 -90.015625 "/></g></g></g><g fill="#fbcccc" fill-opacity="1"><g transform="translate(194.015166, 640.079332)"><g><path d="M 197.644531 0.652344 C 345.714844 0 407.03125 -73.710938 407.03125 -232.214844 C 407.03125 -391.375 345.714844 -452.039062 197.644531 -452.039062 L 105.671875 -452.039062 C 69.796875 -452.039062 50.226562 -431.816406 50.226562 -393.984375 C 50.226562 -196.339844 49.574219 -113.5 48.921875 -54.140625 C 48.921875 -18.917969 68.492188 0 104.367188 0 Z M 171.554688 -90.015625 C 170.902344 -112.195312 170.902344 -136.328125 170.902344 -159.8125 C 170.902344 -271.355469 170.902344 -317.015625 171.554688 -362.023438 L 195.035156 -362.023438 C 265.484375 -362.023438 286.355469 -326.144531 286.355469 -226.34375 C 286.355469 -138.9375 262.875 -90.015625 195.035156 -90.015625 Z M 171.554688 -90.015625 "/></g></g></g></svg>"""
  }

  /**
   * Generate empty CSS file (all styles are inline or from CDN)
   */
  def mainCss: String = ""

  /**
   * Generate task details page
   */
  def taskDetails(taskResultSummary: List[TaskResultSummary]): String = {
    html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        tags2.title("Task Details - Data Caterer"),
        link(rel := "icon", href := "data_catering_transparent.svg"),
        externalDependencies,
        customStyles
      ),
      body(
        topNavBar,
        div(cls := "container-fluid mt-3",
          h1(cls := "mb-4", i(cls := "bi bi-list-task me-2"), "Task Details"),
          if (taskResultSummary.isEmpty) {
            div(cls := "alert alert-info", i(cls := "bi bi-info-circle me-2"), "No tasks found")
          } else {
            div(cls := "card shadow-sm",
              div(cls := "card-body",
                div(cls := "table-responsive",
                  table(cls := "table table-hover",
                    thead(cls := "table-dark",
                      tr(
                        th("Task Name"),
                        th("Steps"),
                        th("Total Records"),
                        th("Status")
                      )
                    ),
                    tbody(
                      taskResultSummary.map(taskResult =>
                        tr(id := taskResult.task.name,
                          td(
                            i(cls := "bi bi-box me-2"),
                            strong(taskResult.task.name)
                          ),
                          td(
                            taskResult.task.steps.map(step =>
                              a(
                                href := s"$REPORT_FIELDS_HTML#${step.name}",
                                cls := "badge bg-secondary text-decoration-none me-1",
                                step.name
                              )
                            )
                          ),
                          td(
                            span(cls := "badge bg-info", taskResult.numRecords.toString)
                          ),
                          td(
                            if (taskResult.isSuccess)
                              span(cls := "badge bg-success", i(cls := "bi bi-check-circle-fill me-1"), "Success")
                            else
                              span(cls := "badge bg-danger", i(cls := "bi bi-x-circle-fill me-1"), "Failed")
                          )
                        )
                      )
                    )
                  )
                )
              )
            )
          }
        ),
        bodyScripts
      )
    ).render
  }

  /**
   * Generate step details page
   */
  def stepDetails(stepResultSummary: List[StepResultSummary]): String = {
    html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        tags2.title("Step/Field Details - Data Caterer"),
        link(rel := "icon", href := "data_catering_transparent.svg"),
        externalDependencies,
        customStyles,
        tag("style")(
          """|.field-details-panel {
             |  max-height: 400px;
             |  overflow-y: auto;
             |}
             |""".stripMargin
        )
      ),
      body(
        topNavBar,
        div(cls := "container-fluid mt-3",
          h1(cls := "mb-4", i(cls := "bi bi-list-columns me-2"), "Step & Field Details"),
          if (stepResultSummary.isEmpty) {
            div(cls := "alert alert-info", i(cls := "bi bi-info-circle me-2"), "No steps found")
          } else {
            div(cls := "row",
              // Steps table
              div(cls := "col-12 mb-4",
                div(cls := "card shadow-sm",
                  div(cls := "card-body",
                    div(cls := "table-responsive",
                      table(cls := "table table-hover table-sm",
                        thead(cls := "table-dark",
                          tr(
                            th("Step Name"),
                            th("Type"),
                            th("Enabled"),
                            th("Records"),
                            th("Status"),
                            th("Batches"),
                            th("Time (s)"),
                            th("Options"),
                            th("Count Config"),
                            th("Fields")
                          )
                        ),
                        tbody(
                          stepResultSummary.zipWithIndex.flatMap { case (result, idx) =>
                            val stepOptions = if (result.dataSourceResults.nonEmpty) {
                              result.dataSourceResults.head.sinkResult.options
                            } else {
                              result.step.options
                            }
                            Seq(
                              tr(id := result.step.name,
                                td(strong(result.step.name)),
                                td(span(cls := "badge bg-info", result.step.`type`)),
                                td(
                                  if (result.step.enabled)
                                    i(cls := "bi bi-check-circle text-success")
                                  else
                                    i(cls := "bi bi-x-circle text-danger")
                                ),
                                td(result.numRecords.toString),
                                td(
                                  if (result.isSuccess)
                                    span(cls := "badge bg-success", i(cls := "bi bi-check"))
                                  else
                                    span(cls := "badge bg-danger", i(cls := "bi bi-x"))
                                ),
                                td(result.dataSourceResults.map(_.batchNum).max.toString),
                                td(result.dataSourceResults.map(_.sinkResult.durationInSeconds).sum.toString),
                                td(
                                  if (stepOptions.nonEmpty)
                                    button(
                                      cls := "btn btn-sm btn-outline-secondary",
                                      data("bs-toggle") := "collapse",
                                      data("bs-target") := s"#options-${idx}",
                                      i(cls := "bi bi-gear me-1"),
                                      s"${stepOptions.size} options"
                                    )
                                  else
                                    em(cls := "text-muted small", "None")
                                ),
                                td(
                                  button(
                                    cls := "btn btn-sm btn-outline-info",
                                    data("bs-toggle") := "collapse",
                                    data("bs-target") := s"#count-${idx}",
                                    i(cls := "bi bi-123 me-1"),
                                    "View"
                                  )
                                ),
                                td(
                                  a(
                                    href := s"#field-metadata-${result.step.name}",
                                    cls := "btn btn-sm btn-outline-primary",
                                    i(cls := "bi bi-eye me-1"),
                                    s"${result.step.fields.size} fields"
                                  )
                                )
                              )
                            ) ++ (
                              // Options collapse row
                              if (stepOptions.nonEmpty) {
                                Seq(tr(
                                  td(attr("colspan") := "10",
                                    div(
                                      cls := "collapse",
                                      id := s"options-${idx}",
                                      div(cls := "card card-body bg-light",
                                        h6(cls := "mb-2", "Step Options:"),
                                        keyValueTableModern(stepOptions.map(x => List(x._1, x._2)).toList)
                                      )
                                    )
                                  )
                                ))
                              } else Seq()
                            ) ++ Seq(
                              // Count configuration collapse row
                              tr(
                                td(attr("colspan") := "10",
                                  div(
                                    cls := "collapse",
                                    id := s"count-${idx}",
                                    div(cls := "card card-body bg-light",
                                      h6(cls := "mb-2", "Count Configuration:"),
                                      keyValueTableModern(result.step.count.numRecordsString._2)
                                    )
                                  )
                                )
                              )
                            )
                          }
                        )
                      )
                    )
                  )
                )
              ),
              // Field details panel
              div(cls := "col-12",
                stepResultSummary.map { result =>
                  div(
                    id := s"field-metadata-${result.step.name}",
                    cls := "card shadow-sm mt-3",
                    div(cls := "card-header bg-primary text-white",
                      h5(cls := "mb-0",
                        i(cls := "bi bi-list-columns me-2"),
                        s"Field Details: ${result.step.name}"
                      )
                    ),
                    div(cls := "card-body field-details-panel",
                      fieldMetadataModern(result.step, result.dataSourceResults)
                    )
                  )
                }
              )
            )
          }
        ),
        bodyScripts
      )
    ).render
  }

  /**
   * Helper function to render key-value pairs as a modern table
   */
  private def keyValueTableModern(keyValues: List[List[String]], optHeader: Option[List[String]] = None): Frag = {
    if (keyValues.isEmpty) {
      em(cls := "text-muted", "No data")
    } else {
      table(cls := "table table-sm table-bordered mb-0",
        optHeader.map(headers =>
          thead(
            tr(
              headers.map(header => th(header))
            )
          )
        ).getOrElse(frag()),
        tbody(
          keyValues.map(kv =>
            tr(
              if (kv.size == 1) {
                td(attr("colspan") := "2", kv.head)
              } else {
                frag(
                  td(strong(kv.head)),
                  kv.tail.map(kvt => td(kvt))
                )
              }
            )
          )
        )
      )
    }
  }

  /**
   * Modern field metadata display
   */
  private def fieldMetadataModern(step: Step, dataSourceResults: List[DataSourceResult]): Frag = {
    if (dataSourceResults.isEmpty || dataSourceResults.head.sinkResult.generatedMetadata.isEmpty) {
      div(cls := "alert alert-warning", i(cls := "bi bi-exclamation-triangle me-2"), "No field metadata available")
    } else {
      val originalFields = step.fields
      val generatedFields = dataSourceResults.head.sinkResult.generatedMetadata

      div(cls := "table-responsive",
        table(cls := "table table-hover table-sm",
          thead(cls := "table-secondary",
            tr(
              th("Field Name"),
              th("Type"),
              th("Nullable"),
              th("Metadata Comparison")
            )
          ),
          tbody(
            originalFields.map(field => {
              val optGenField = generatedFields.find(f => f.name == field.name)
              val genMetadata = optGenField.map(_.options).getOrElse(Map())
              val originalMetadata = field.options

              tr(
                td(
                  i(cls := "bi bi-record-circle me-1"),
                  strong(field.name)
                ),
                td(
                  span(cls := "badge bg-secondary", field.`type`.getOrElse("string"):String)
                ),
                td(
                  if (field.nullable)
                    i(cls := "bi bi-check-circle text-success")
                  else
                    i(cls := "bi bi-x-circle text-danger")
                ),
                td(
                  if (originalMetadata.nonEmpty || genMetadata.nonEmpty) {
                    val allKeys = (originalMetadata.keys ++ genMetadata.keys).toList.distinct.filter(_ != "histogram")
                    div(cls := "small",
                      table(cls := "table table-sm table-bordered mb-0",
                        thead(
                          tr(
                            th("Property"),
                            th("Original"),
                            th("Generated")
                          )
                        ),
                        tbody(
                          allKeys.map(key =>
                            tr(
                              td(code(key)),
                              td(originalMetadata.getOrElse(key, "-").toString),
                              td(
                                span(genMetadata.getOrElse(key, "-").toString),
                                if (originalMetadata.getOrElse(key, "") != genMetadata.getOrElse(key, "")) {
                                  i(cls := "bi bi-exclamation-triangle text-warning ms-1", title := "Value differs")
                                } else {
                                  frag()
                                }
                              )
                            )
                          )
                        )
                      )
                    ):Modifier
                  } else {
                    em("No metadata"):Modifier
                  }
                )
              )
            })
          )
        )
      )
    }
  }

  /**
   * Generate data source details page
   */
  def dataSourceDetails(dataSourceResults: List[DataSourceResult]): String = {
    val resByDataSource = dataSourceResults.groupBy(_.sinkResult.name)

    html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        tags2.title("Data Source Details - Data Caterer"),
        link(rel := "icon", href := "data_catering_transparent.svg"),
        externalDependencies,
        customStyles
      ),
      body(
        topNavBar,
        div(cls := "container-fluid mt-3",
          h1(cls := "mb-4", i(cls := "bi bi-database me-2"), "Data Source Details"),
          if (resByDataSource.isEmpty) {
            div(cls := "alert alert-info", i(cls := "bi bi-info-circle me-2"), "No data sources found")
          } else {
            div(cls := "card shadow-sm",
              div(cls := "card-body",
                div(cls := "table-responsive",
                  table(cls := "table table-hover",
                    thead(cls := "table-dark",
                      tr(
                        th("Name"),
                        th("Format"),
                        th("Records"),
                        th("Status"),
                        th("Options")
                      )
                    ),
                    tbody(
                      resByDataSource.zipWithIndex.flatMap { case ((name, results), idx) =>
                        val numRecords = results.map(_.sinkResult.count).sum
                        val success = results.forall(_.sinkResult.isSuccess)
                        val formats = results.map(_.sinkResult.format).distinct
                        val options = if (results.nonEmpty) results.head.sinkResult.options else Map()

                        Seq(
                          tr(id := name,
                            td(
                              i(cls := "bi bi-database me-2"),
                              strong(name)
                            ),
                            td(
                              formats.map(fmt =>
                                span(cls := "badge bg-info me-1", fmt)
                              )
                            ),
                            td(
                              span(cls := "badge bg-secondary", numRecords.toString)
                            ),
                            td(
                              if (success)
                                span(cls := "badge bg-success", i(cls := "bi bi-check-circle-fill me-1"), "Success")
                              else
                                span(cls := "badge bg-danger", i(cls := "bi bi-x-circle-fill me-1"), "Failed")
                            ),
                            td(
                              if (options.nonEmpty) {
                                button(
                                  cls := "btn btn-sm btn-outline-secondary",
                                  data("bs-toggle") := "collapse",
                                  data("bs-target") := s"#ds-options-${idx}",
                                  i(cls := "bi bi-gear me-1"),
                                  s"${options.size} options"
                                )
                              } else {
                                em(cls := "text-muted", "No options")
                              }
                            )
                          )
                        ) ++ (
                          if (options.nonEmpty) {
                            Seq(tr(
                              td(attr("colspan") := "5",
                                div(
                                  cls := "collapse",
                                  id := s"ds-options-${idx}",
                                  div(cls := "card card-body bg-light",
                                    h6(cls := "mb-2", "Data Source Options:"),
                                    keyValueTableModern(options.toList.sortBy(_._1).map(x => List(x._1, x._2)))
                                  )
                                )
                              )
                            ))
                          } else Seq()
                        )
                      }.toSeq
                    )
                  )
                )
              )
            )
          }
        ),
        bodyScripts
      )
    ).render
  }

  /**
   * Generate validations page
   */
  def validations(validationResults: List[ValidationConfigResult], validationConfig: ValidationConfig): String = {
    html(
      head(
        meta(charset := "utf-8"),
        meta(name := "viewport", content := "width=device-width, initial-scale=1"),
        tags2.title("Validations - Data Caterer"),
        link(rel := "icon", href := "data_catering_transparent.svg"),
        externalDependencies,
        customStyles
      ),
      body(
        topNavBar,
        div(cls := "container-fluid mt-3",
          h1(cls := "mb-4", i(cls := "bi bi-check-circle me-2"), "Validation Results"),
          if (validationResults.isEmpty) {
            div(cls := "alert alert-info", i(cls := "bi bi-info-circle me-2"), "No validation results found")
          } else {
            Seq(
              // Summary section
              div(cls := "card shadow-sm mb-4",
                div(cls := "card-header bg-primary text-white",
                  h5(cls := "mb-0", "Validation Summary")
                ),
                div(cls := "card-body",
                  validationSummaryModern(validationResults)
                )
              ),
              // Detailed results
              div(cls := "card shadow-sm",
                div(cls := "card-header bg-primary text-white",
                  h5(cls := "mb-0", "Detailed Validation Results")
                ),
                div(cls := "card-body",
                  validationDetailsModern(validationResults)
                )
              )
            )
          }
        ),
        bodyScripts
      )
    ).render
  }

  /**
   * Detailed validation results
   */
  private def validationDetailsModern(validationResults: List[ValidationConfigResult]): Frag = {
    div(cls := "table-responsive",
      table(cls := "table table-hover table-sm",
        thead(cls := "table-dark",
          tr(
            th("Description"),
            th("Data Source"),
            th("Options"),
            th("Success Rate"),
            th("Within Threshold"),
            th("Validation Details"),
            th("Error Samples")
          )
        ),
        tbody(
          validationResults.flatMap(validationConfRes =>
            validationConfRes.dataSourceValidationResults.flatMap(dataSourceValidationRes =>
              dataSourceValidationRes.validationResults.map(validationRes => {
                val numSuccess = validationRes.total - validationRes.numErrors
                val successPercent = if (validationRes.total > 0) {
                  BigDecimal((numSuccess.toDouble / validationRes.total) * 100)
                    .setScale(1, RoundingMode.HALF_UP)
                } else BigDecimal(0)

                tr(
                  td(validationRes.validation.description.getOrElse("Validation"):String),
                  td(
                    a(
                      href := s"$REPORT_DATA_SOURCES_HTML#${dataSourceValidationRes.dataSourceName}",
                      cls := "text-decoration-none",
                      dataSourceValidationRes.dataSourceName
                    )
                  ),
                  td(
                    if (dataSourceValidationRes.options.nonEmpty) {
                      div(cls := "small",
                        dataSourceValidationRes.options.toList.sortBy(_._1).take(3).map { case (key, value) =>
                          div(code(cls := "me-1", key), ": ", value)
                        }
                      ):Modifier
                    } else {
                      em(cls := "text-muted", "None"):Modifier
                    }
                  ),
                  td(
                    div(cls := "progress mb-1", style := "height: 20px;",
                      div(
                        cls := s"progress-bar ${if (validationRes.isSuccess) "bg-success" else "bg-danger"}",
                        style := s"width: $successPercent%",
                        ariaValuenow := successPercent.toString,
                        ariaValuemin := "0",
                        ariaValuemax := "100",
                        s"$successPercent%"
                      )
                    ),
                    div(cls := "small text-center",
                      s"$numSuccess / ${validationRes.total}"
                    )
                  ),
                  td(
                    if (validationRes.isSuccess)
                      span(cls := "badge bg-success", i(cls := "bi bi-check-circle-fill me-1"), "Pass")
                    else
                      span(cls := "badge bg-danger", i(cls := "bi bi-x-circle-fill me-1"), "Fail")
                  ),
                  td(
                    if (getValidationOptions(validationRes.validation).nonEmpty) {
                      keyValueTableModern(
                        getValidationOptions(validationRes.validation),
                        Some(List("Property", "Value"))
                      ):Modifier
                    } else {
                      em(cls := "text-muted", "None"):Modifier
                    }
                  ),
                  td(
                    if (!validationRes.isSuccess && validationRes.sampleErrorValues.isDefined) {
                      div(cls := "small",
                        validationRes.sampleErrorValues.get.take(5).map(errorValue =>
                          div(cls := "alert alert-danger alert-sm mb-1 p-2",
                            code(cls := "small", ObjectMapperUtil.jsonObjectMapper.writeValueAsString(errorValue))
                          )
                        )
                      ):Modifier
                    } else {
                      span(cls := "badge bg-success", "No errors"):Modifier
                    }
                  )
                )
              })
            )
          )
        )
      )
    )
  }

  /**
   * Get validation options as list of strings
   */
  private def getValidationOptions(validation: Validation): List[List[String]] = {
    validation.toOptions.filter(_.forall(_.nonEmpty))
  }
}
