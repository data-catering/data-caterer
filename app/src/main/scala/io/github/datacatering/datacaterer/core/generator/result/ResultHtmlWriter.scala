package io.github.datacatering.datacaterer.core.generator.result

import io.github.datacatering.datacaterer.api.model.Constants.HISTOGRAM
import io.github.datacatering.datacaterer.api.model.{FlagsConfig, Plan, Step, Validation, ValidationConfig}
import io.github.datacatering.datacaterer.core.listener.{SparkRecordListener, SparkTaskRecordSummary}
import io.github.datacatering.datacaterer.core.model.Constants.{REPORT_DATA_SOURCES_HTML, REPORT_FIELDS_HTML, REPORT_HOME_HTML, REPORT_VALIDATIONS_HTML}
import io.github.datacatering.datacaterer.core.model.{DataSourceResult, DataSourceResultSummary, StepResultSummary, TaskResultSummary, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.util.PlanImplicits.CountOps
import org.joda.time.DateTime

import scala.math.BigDecimal.RoundingMode
import scala.xml.{Node, NodeBuffer, NodeSeq}

class ResultHtmlWriter {

  def index(plan: Plan, stepResultSummary: List[StepResultSummary], taskResultSummary: List[TaskResultSummary],
            dataSourceResultSummary: List[DataSourceResultSummary], validationResults: List[ValidationConfigResult],
            flagsConfig: FlagsConfig, sparkRecordListener: SparkRecordListener): Node = {
    <html>
      <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
        <title id='title'>Data Caterer</title>{plugins}
      </head>
      <body>
        {topNavBar}{overview(plan, stepResultSummary, taskResultSummary, dataSourceResultSummary, validationResults, flagsConfig, sparkRecordListener)}
      </body>{bodyScripts}
    </html>
  }

  def overview(plan: Plan, stepResultSummary: List[StepResultSummary], taskResultSummary: List[TaskResultSummary],
               dataSourceResultSummary: List[DataSourceResultSummary], validationResults: List[ValidationConfigResult],
               flagsConfig: FlagsConfig, sparkRecordListener: SparkRecordListener): Node = {
    <div>
      <h1>Data Caterer Summary</h1>
      <h2>Flags</h2>{flagsSummary(flagsConfig)}<h2>Plan</h2>{planSummary(plan, stepResultSummary, taskResultSummary, dataSourceResultSummary)}<h2>Tasks</h2>{tasksSummary(taskResultSummary)}<h2>Validations</h2>{validationSummary(validationResults)}<h2>Output Rows Per Second</h2>{createLineGraph("outputRowsPerSecond", sparkRecordListener.outputRows.toList)}<div>
      Generated at
      {DateTime.now()}
    </div>
    </div>
  }

  def topNavBar: NodeBuffer = {
    <div class="top-banner">
      <a class="logo" href={REPORT_HOME_HTML}>
        <img src="data_catering_transparent.svg" alt="logo"/>
      </a>
      <span>
        <b>Data Caterer</b>
      </span>
    </div>
      <nav class="topnav">
        <a href={REPORT_HOME_HTML}>Overview</a>
        <a href={REPORT_DATA_SOURCES_HTML}>Data Source</a>
        <a href={REPORT_FIELDS_HTML}>Field</a>
        <a href={REPORT_VALIDATIONS_HTML}>Validation</a>
      </nav>
  }

  def flagsSummary(flagsConfig: FlagsConfig): Node = {
    <table class="table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Generate Metadata</th>
          <th>Generate Data</th>
          <th>Record Tracking</th>
          <th>Delete Data</th>
          <th>Calculate Generated Records Metadata</th>
          <th>Validate Data</th>
          <th>Unique Check</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            {checkMark(flagsConfig.enableGeneratePlanAndTasks)}
          </td>
          <td>
            {checkMark(flagsConfig.enableGenerateData)}
          </td>
          <td>
            {checkMark(flagsConfig.enableRecordTracking)}
          </td>
          <td>
            {checkMark(flagsConfig.enableDeleteGeneratedRecords)}
          </td>
          <td>
            {checkMark(flagsConfig.enableSinkMetadata)}
          </td>
          <td>
            {checkMark(flagsConfig.enableValidation)}
          </td>
          <td>
            {checkMark(flagsConfig.enableUniqueCheck)}
          </td>
        </tr>
      </tbody>
    </table>
  }

  def planSummary(plan: Plan, stepResultSummary: List[StepResultSummary],
                  taskResultSummary: List[TaskResultSummary], dataSourceResultSummary: List[DataSourceResultSummary]): Node = {
    val totalRecords = stepResultSummary.map(_.numRecords).sum
    val isSuccess = stepResultSummary.forall(_.isSuccess)
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Plan Name</th>
          <th>Num Records</th>
          <th>Success</th>
          <th>Tasks</th>
          <th>Steps</th>
          <th>Data Sources</th>
          <th>Foreign Keys</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td>
            {plan.name}
          </td>
          <td>
            {totalRecords}
          </td>
          <td>
            {checkMark(isSuccess)}
          </td>
          <td>
            {taskResultSummary.size}
          </td>
          <td>
            {stepResultSummary.size}
          </td>
          <td>
            {dataSourceResultSummary.size}
          </td>
          <td>
            {plan.sinkOptions.map(_.foreignKeys).getOrElse(Map())}
          </td>
        </tr>
      </tbody>
    </table>
  }

  def tasksSummary(taskResultSummary: List[TaskResultSummary]): Node = {
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Name</th>
          <th>Num Records</th>
          <th>Success</th>
          <th>Steps</th>
        </tr>
      </thead>
      <tbody>
        {taskResultSummary.map(res => {
        val taskRef = s"tasks.html#${res.task.name}"
        <tr>
          <td>
            <a href={taskRef}>
              {res.task.name}
            </a>
          </td>
          <td>
            {res.numRecords}
          </td>
          <td>
            {checkMark(res.isSuccess)}
          </td>
          <td>
            {toStepLinks(res.task.steps)}
          </td>
        </tr>
      })}
      </tbody>
    </table>
  }

  def taskDetails(taskResultSummary: List[TaskResultSummary]): Node = {
    <html>
      <head>
        <title>
          Task Details - Data Caterer
        </title>{plugins}
      </head>
      <body>
        {topNavBar}<h1>Tasks</h1>
        <table class="tablesorter table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>Name</th>
              <th>Steps</th>
            </tr>
          </thead>
          <tbody>
            {taskResultSummary.map(res => {
            <tr id={res.task.name}>
              <td>
                {res.task.name}
              </td>
              <td>
                {toStepLinks(res.task.steps)}
              </td>
            </tr>
          })}
          </tbody>
        </table>
      </body>{bodyScripts}
    </html>
  }

  def stepsSummary(stepResultSummary: List[StepResultSummary]): Node = {
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Name</th>
          <th>Num Records</th>
          <th>Success</th>
          <th>Options</th>
          <th>Num Batches</th>
          <th>Time Taken (s)</th>
        </tr>
      </thead>
      <tbody>
        {stepResultSummary.map(res => {
        val stepLink = s"$REPORT_FIELDS_HTML#${res.step.name}"
        <tr>
          <td>
            <a href={stepLink}>
              {res.step.name}
            </a>
          </td>
          <td>
            {res.numRecords}
          </td>
          <td>
            {checkMark(res.isSuccess)}
          </td>
          <td>
            {optionsString(res)}
          </td>
          <td>
            {res.dataSourceResults.map(_.batchNum).max}
          </td>
          <td>
            {res.dataSourceResults.map(_.sinkResult.durationInSeconds).sum}
          </td>
        </tr>
      })}
      </tbody>
    </table>
  }

  def stepDetails(stepResultSummary: List[StepResultSummary]): Node = {
    <html>
      <head>
        <title>
          Step Details - Data Caterer
        </title>{plugins}
      </head>
      <body>
        {topNavBar}<div class="outer-container">
        <div class="top-container">
          <h1>Steps</h1>
          <table class="tablesorter table table-striped" style="font-size: 13px">
            <thead>
              <tr>
                <th>Name</th>
                <th>Num Records</th>
                <th>Success</th>
                <th>Type</th>
                <th>Enabled</th>
                <th>Options</th>
                <th>Count</th>
                <th>Fields</th>
              </tr>
            </thead>
            <tbody>
              {stepResultSummary.map(res => {
              val fieldMetadataOnClick = s"showFieldMetadata('${res.step.name}', this)"
              <tr id={res.step.name}>
                <td>
                  {res.step.name}
                </td>
                <td>
                  {res.numRecords}
                </td>
                <td>
                  {checkMark(res.isSuccess)}
                </td>
                <td>
                  {res.step.`type`}
                </td>
                <td>
                  {checkMark(res.step.enabled)}
                </td>
                <td>
                  {optionsString(res)}
                </td>
                <td>
                  {keyValueTable(res.step.count.numRecordsString._2)}
                </td>
                <td>
                  <button id="field-metadata-button" onclick={fieldMetadataOnClick}>Fields</button>
                  <div style="display: none;">
                    {fieldMetadata(res.step, res.dataSourceResults)}
                  </div>
                </td>
              </tr>
            })}
            </tbody>
          </table>
        </div>
        <div class="slider">...</div>{if (stepResultSummary.nonEmpty) {
          <div class="bottom-container" id="current-field-metadata">
            {fieldMetadata(stepResultSummary.head.step, stepResultSummary.head.dataSourceResults)}
          </div>
        }}
      </div>
      </body>{bodyScripts}
    </html>
  }

  def fieldMetadata(step: Step, dataSourceResults: List[DataSourceResult]): Node = {
    val originalFields = step.fields
    val generatedFields = dataSourceResults.head.sinkResult.generatedMetadata
    val metadataMatch = originalFields.map(field => {
      val optGenField = generatedFields.find(f => f.name == field.name)
      val genMetadata = optGenField.map(_.options).getOrElse(Map())
      val originalMetadata = field.options
      val metadataCompare = (originalMetadata.keys ++ genMetadata.keys).filter(_ != HISTOGRAM).toList.distinct
        .map(key => {
          List(key, originalMetadata.getOrElse(key, "").toString, genMetadata.getOrElse(key, "").toString)
        })
      (field.name, metadataCompare)
    }).toMap
    val fieldMetadataId = s"field-metadata-${step.name}"

    <div id={fieldMetadataId}>
      <h3>Field Details:
        {step.name}
      </h3>
      <table class="tablesorter table table-striped" style="font-size: 13px">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Nullable</th>
            <th>Generated Records Metadata Comparison</th>
          </tr>
        </thead>
        <tbody>
          {originalFields.map(field => {
          <tr>
            <td>
              {field.name}
            </td>
            <td>
              {field.`type`.getOrElse("string")}
            </td>
            <td>
              {checkMark(field.nullable)}
            </td>
            <td>
              {keyValueTable(metadataMatch(field.name), Some(List("Metadata Field", "Original Value", "Generated Value")), true)}
            </td>
          </tr>
        })}
        </tbody>
      </table>
    </div>
  }

  def dataSourceDetails(dataSourceResults: List[DataSourceResult]): Node = {
    val resByDataSource = dataSourceResults.groupBy(_.sinkResult.name)
    <html>
      <head>
        <title>
          Data Source Details - Data Caterer
        </title>{plugins}
      </head>
      <body>
        {topNavBar}<h1>Data Sources</h1>
        <table class="tablesorter table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>Name</th>
              <th>Num Records</th>
              <th>Success</th>
              <th>Format</th>
              <th>Options</th>
            </tr>
          </thead>
          <tbody>
            {resByDataSource.map(ds => {
            val numRecords = ds._2.map(_.sinkResult.count).sum
            val success = ds._2.forall(_.sinkResult.isSuccess)
            <tr id={ds._1}>
              <td>
                {ds._1}
              </td>
              <td>
                {numRecords}
              </td>
              <td>
                {checkMark(success)}
              </td>
              <td>
                {ds._2.map(_.sinkResult.format).distinct.mkString("\n")}
              </td>
              <td>
                {keyValueTable(ds._2.flatMap(x => x.sinkResult.options.map(y => List(y._1, y._2))))}
              </td>
            </tr>
          })}
          </tbody>
        </table>
      </body>{bodyScripts}
    </html>
  }

  def validations(validationResults: List[ValidationConfigResult], validationConfig: ValidationConfig): Node = {
    <html>
      <head>
        <title>
          Validations - Data Caterer
        </title>{plugins}
      </head>
      <body>
        {topNavBar}<h1>Validations</h1>{validationSummary(validationResults)}<h2>Details</h2>
        <table class="tablesorter table table-striped" style="font-size: 13px">
          <thead>
            <tr>
              <th>Description</th>
              <th>Data Source</th>
              <th>Options</th>
              <th>Success</th>
              <th>Within Error Threshold</th>
              <th>Validation</th>
              <th>Error Sample</th>
            </tr>
          </thead>
          <tbody>
            {validationResults.flatMap(validationConfRes => {
            validationConfRes.dataSourceValidationResults.flatMap(dataSourceValidationRes => {
              val dataSourceLink = s"$REPORT_DATA_SOURCES_HTML#${dataSourceValidationRes.dataSourceName}"
              dataSourceValidationRes.validationResults.map(validationRes => {
                val numSuccess = validationRes.total - validationRes.numErrors
                <tr>
                  <td>
                    {validationRes.validation.description.getOrElse("Validate")}
                  </td>
                  <td>
                    <a href={dataSourceLink}>
                      {dataSourceValidationRes.dataSourceName}
                    </a>
                  </td>
                  <td>
                    {formatOptions(dataSourceValidationRes.options)}
                  </td>
                  <td>
                    {progressBar(numSuccess, validationRes.total)}
                  </td>
                  <td>
                    {checkMark(validationRes.isSuccess)}
                  </td>
                  <td>
                    {keyValueTable(getValidationOptions(validationRes.validation))}
                  </td>
                  <td>
                    {if (validationRes.isSuccess) "" else keyValueTable(validationRes.sampleErrorValues.get.take(validationConfig.numSampleErrorRecords).map(e => List(e.json)).toList)}
                  </td>
                </tr>
              })
            })
          })}
          </tbody>
        </table>
      </body>{bodyScripts}
    </html>
  }

  def validationSummary(validationResults: List[ValidationConfigResult]): Node = {
    <table class="tablesorter table table-striped" style="font-size: 13px">
      <thead>
        <tr>
          <th>Name</th>
          <th>Data Sources</th>
          <th>Description</th>
          <th>Success</th>
        </tr>
      </thead>
      <tbody>
        {validationResults.map(validationConfRes => {
        val validationLink = s"$REPORT_VALIDATIONS_HTML#${validationConfRes.name}"
        val resultsForDataSource = validationConfRes.dataSourceValidationResults.flatMap(_.validationResults)
        val numSuccess = resultsForDataSource.count(_.isSuccess)
        <tr>
          <td>
            <a href={validationLink}>
              {validationConfRes.name}
            </a>
          </td>
          <td>
            {toDataSourceLinks(validationConfRes.dataSourceValidationResults.map(_.dataSourceName).distinct)}
          </td>
          <td>
            {validationConfRes.description}
          </td>
          <td>
            {progressBar(numSuccess, resultsForDataSource.size)}
          </td>
        </tr>
      })}
      </tbody>
    </table>
  }

  def createLineGraph(name: String, recordSummary: List[SparkTaskRecordSummary]): NodeBuffer = {
    if (recordSummary.nonEmpty) {
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

      val xValues = timeSeriesValues.map(x => new DateTime(x._1).toString("HH:mm:ss")).map(s => "\"" + s + "\"")
      val yValues = timeSeriesValues.map(_._2)
      val (yMin, yMax) = if (yValues.nonEmpty) (Math.max(0, yValues.min - 2), yValues.max + 2) else (0, 0)
      createChart(name, xValues, yValues, yMin, yMax)
    } else {
      <div>
        <p>No data found</p>
      </div>
        <p></p>
    }
  }

  private def createChart[T, K](name: String, xValues: List[T], yValues: List[K], minY: K, maxY: K): NodeBuffer = {
    val xValuesStr = s"[${xValues.mkString(",")}]"
    val yValuesStr = s"[${yValues.mkString(",")}]"
    <canvas id={name} style="width:100%;max-width:700px"></canvas>
      <script type="text/javascript">
        {xml.Unparsed(
        s"""new Chart($name, {
           |    type: "line",
           |    data: {
           |      labels: $xValuesStr,
           |      datasets: [{
           |        backgroundColor: "rgba(0,0,255,1.0)",
           |        borderColor: "rgba(0,0,255,0.1)",
           |        data: $yValuesStr
           |      }]
           |    },
           |    options: {
           |      legend: {display: false},
           |      scales: {
           |        yAxes: [{ticks: {min: $minY, max: $maxY}}],
           |      },
           |      responsive: true
           |    }
           |  });
           |""".stripMargin)}
      </script>
  }

  private def checkMark(isSuccess: Boolean): NodeSeq = if (isSuccess) xml.EntityRef("#9989") else xml.EntityRef("#10060")

  private def progressBar(success: Long, total: Long): NodeBuffer = {
    val percent = if (success > 0 && total > 0) BigDecimal(success.toDouble / total * 100).setScale(2, RoundingMode.HALF_UP).toString() else "0"
    val width = s"width:$percent%"
    val progressBarText = s"$success/$total ($percent%)"
    <div class="progress">
      <div class="progress-bar progress-bar-success" role="progressbar" aria-valuenow={percent} aria-valuemin="0" aria-valuemax="100" style={width}></div>
    </div>
      <div>
        {progressBarText}
      </div>
  }

  private def keyValueTable(keyValues: List[List[String]], optHeader: Option[List[String]] = None, isCollapsible: Boolean = false): Node = {
    val baseTable =
      <table class="table table-striped" style="font-size: 13px">
        {optHeader.map(headers => {
        <thead>
          <tr>
            {headers.map(header => {
            <th>
              {header}
            </th>
          })}
          </tr>
        </thead>
      }).getOrElse(List())}<tbody>
        {keyValues.map(kv => {
          <tr>
            <td>
              {if (kv.size == 1) {
              {
                kv.head
              }
            } else {
              <b>
                {kv.head}
              </b>
            }}
            </td>{kv.tail.map(kvt => {
            <td>
              {kvt}
            </td>
          })}
          </tr>
        })}
      </tbody>
      </table>

    if (isCollapsible) {
      {
        xml.Group(Seq(
          <button class="collapsible">Expand</button>,
          <div class="table-collapsible">
            {baseTable}
          </div>
        ))
      }
    } else {
      {
        baseTable
      }
    }
  }

  private def optionsString(res: StepResultSummary): Node = {
    val dataSourceResult = res.dataSourceResults
    val baseOptions = if (dataSourceResult.nonEmpty) {
      dataSourceResult.head.sinkResult.options
    } else {
      res.step.options
    }
    val optionsToList = baseOptions.map(x => List(x._1, x._2)).toList
    keyValueTable(optionsToList)
  }

  private def formatOptions(options: Map[String, String]): String = options.map(s => s"${s._1} -> ${s._2}").mkString("\n")

  private def toStepLinks(steps: List[Step]): Node = {
    {
      xml.Group(steps.map(step => {
        val stepLink = s"$REPORT_FIELDS_HTML#${step.name}"
        <a href={stepLink}>
          {s"${step.name}"}
        </a>
      }))
    }
  }

  private def toDataSourceLinks(dataSourceNames: List[String]): Node = {
    {
      xml.Group(dataSourceNames.map(dataSource => {
        val dataSourceLink = s"$REPORT_DATA_SOURCES_HTML#$dataSource"
        <a href={dataSourceLink}>
          {dataSource}
        </a>
      }))
    }
  }

  private def getValidationOptions(validation: Validation): List[List[String]] = {
    validation.toOptions.filter(_.forall(_.nonEmpty))
  }

  def bodyScripts: NodeBuffer = {
    <script type="text/javascript">
      {xml.Unparsed(
      s"""
         |function showFieldMetadata(step, e) {
         |  var newFieldMetadata = document.getElementById('field-metadata-' + step).cloneNode(true);
         |  var currentFieldMetadata = document.getElementById('current-field-metadata');
         |  collapseOnClick(newFieldMetadata);
         |  currentFieldMetadata.replaceChild(newFieldMetadata, currentFieldMetadata.children[0])
         |
         |  var closestCell = e.parentElement,
         |      activeCell = document.getElementsByClassName('selected-row');
         |
         |  if (activeCell !== null && activeCell.length !== 0) {
         |    activeCell[0].classList.remove('selected-row');
         |  }
         |  closestCell.classList.add('selected-row');
         |}
         |
         |let block = document.querySelector(".top-container"),
         |  slider = document.querySelector(".slider");
         |
         |slider.onmousedown = function dragMouseDown(e) {
         |  let dragX = e.clientY;
         |  document.onmousemove = function onMouseMove(e) {
         |    block.style.height = block.offsetHeight + e.clientY - dragX + "px";
         |    dragX = e.clientY;
         |  }
         |  document.onmouseup = () => document.onmousemove = document.onmouseup = null;
         |}
         |""".stripMargin)}
    </script>
      <script type="text/javascript">
        {xml.Unparsed(
        """
          |$(document).ready(function() {$(".tablesorter").tablesorter();});
          |
          |function collapseOnClick(element) {
          |  var coll = element.getElementsByClassName("collapsible");
          |  var i;
          |
          |  for (i = 0; i < coll.length; i++) {
          |    coll[i].addEventListener("click", function() {
          |      this.classList.toggle("active");
          |       var content = this.nextElementSibling;
          |       if (content.style.maxHeight){
          |         content.style.maxHeight = null;
          |       } else {
          |         content.style.maxHeight = content.scrollHeight + "px";
          |       }
          |    });
          |  }
          |};
          |
          |collapseOnClick(document);
          |""".stripMargin
      )}
      </script>
  }

  def mainCss: String = {
    """.box-iframe {
      |    float: left;
      |    margin-right: 10px;
      |}
      |
      |body {
      |    margin: 0;
      |}
      |
      |.top-banner {
      |    height: fit-content;
      |    background-color: #ff6e42;
      |    padding: 0 .2rem;
      |    display: flex;
      |}
      |
      |.top-banner span {
      |    color: #f2f2f2;
      |    font-size: 17px;
      |    padding: 5px 6px;
      |    display: flex;
      |    align-items: center;
      |}
      |
      |.logo {
      |    padding: 5px;
      |    height: 45px;
      |    width: auto;
      |    display: flex;
      |    align-items: center;
      |    justify-content: center;
      |}
      |
      |.logo:hover {
      |    background-color: #ff9100;
      |    color: black;
      |}
      |
      |.top-banner img {
      |    height: 35px;
      |    width: auto;
      |    display: flex;
      |    justify-content: center;
      |    vertical-align: middle;
      |}
      |
      |.topnav {
      |    overflow: hidden;
      |    background-color: #ff6e42;
      |}
      |
      |.topnav a {
      |    float: left;
      |    color: #f2f2f2;
      |    text-align: center;
      |    padding: 8px 10px;
      |    text-decoration: none;
      |    font-size: 17px;
      |}
      |
      |.topnav a:hover {
      |    background-color: #ff9100;
      |    color: black;
      |}
      |
      |.topnav a.active {
      |    color: black;
      |}
      |
      |table {
      |    overflow: hidden;
      |    transition: max-height 0.2s ease-out;
      |}
      |
      |table.codegrid {
      |    font-family: monospace;
      |    font-size: 12px;
      |    width: auto !important;
      |}
      |
      |table.statementlist {
      |    width: auto !important;
      |    font-size: 13px;
      |}
      |
      |table.codegrid td {
      |    padding: 0 !important;
      |    border: 0 !important
      |}
      |
      |table td.linenumber {
      |    width: 40px !important;
      |}
      |
      |td {
      |    white-space: normal
      |}
      |
      |.table thead th {
      |    position: sticky;
      |    top: 0;
      |    z-index: 1;
      |}
      |
      |table, tr, td, th {
      |    border-collapse: collapse;
      |}
      |
      |.table-collapsible {
      |    max-height: 0;
      |    overflow: hidden;
      |    transition: max-height 0.2s ease-out;
      |}
      |
      |.collapsible {
      |    background-color: lightgray;
      |    color: black;
      |    cursor: pointer;
      |    width: 100%;
      |    border: none;
      |    text-align: left;
      |    outline: none;
      |}
      |
      |.collapsible:after {
      |    content: "\02795"; /* Unicode character for "plus" sign (+) */
      |    color: white;
      |    float: right;
      |}
      |
      |.active:after {
      |    content: "\2796"; /* Unicode character for "minus" sign (-) */
      |}
      |
      |.outer-container {
      |    display: flex;
      |    flex-direction: field;
      |    height: 100vh;
      |}
      |
      |.top-container {
      |    height: 50%;
      |    overflow: auto;
      |    resize: vertical;
      |}
      |
      |.bottom-container {
      |    flex: 1;
      |    min-height: 0;
      |    height: 50%;
      |    overflow: auto;
      |    resize: vertical;
      |}
      |
      |.slider {
      |    text-align: center;
      |    background-color: #dee2e6;
      |    cursor: row-resize;
      |    user-select: none;
      |}
      |
      |.selected-row {
      |    background-color: #ff6e42 !important;
      |}
      |
      |.progress {
      |    white-space: normal;
      |    background-color: #d9534f;
      |}
      |
      |.progress-bar {
      |    color: black;
      |}
      |""".stripMargin
  }

  def plugins: NodeBuffer = {
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.10.2/jquery.min.js"></script>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.20.1/css/theme.default.min.css" type="text/css"/>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.20.1/js/jquery.tablesorter.min.js"></script>
        <link rel="stylesheet" href="https://netdna.bootstrapcdn.com/bootstrap/3.0.3/css/bootstrap.min.css" type="text/css"/>
      <script src="https://netdna.bootstrapcdn.com/bootstrap/3.0.3/js/bootstrap.min.js"></script>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/2.9.4/Chart.js"></script>
        <link rel="stylesheet" href="main.css" type="text/css"/>
        <link rel="icon" href="data_catering_transparent.svg"/>
  }

  def dataCateringSvg: String = "<svg xmlns=\"http://www.w3.org/2000/svg\" xmlns:xlink=\"http://www.w3.org/1999/xlink\" width=\"1080\" zoomAndPan=\"magnify\" viewBox=\"0 0 810 809.999993\" height=\"1080\" preserveAspectRatio=\"xMidYMid meet\" version=\"1.0\"><defs><g/><clipPath id=\"d7af0a96c1\"><path d=\"M 43.5 10 L 767.203125 10 L 767.203125 800 L 43.5 800 Z M 43.5 10 \" clip-rule=\"nonzero\"/></clipPath><clipPath id=\"0d6c067e67\"><path d=\"M 43.5 281.675781 L 43.5 528.097656 C 43.5 580.964844 71.75 629.835938 117.597656 656.269531 L 331.273438 779.472656 C 377.121094 805.90625 433.621094 805.90625 479.464844 779.472656 L 693.144531 656.269531 C 738.988281 629.835938 767.238281 580.964844 767.238281 528.097656 L 767.238281 281.675781 C 767.238281 228.808594 738.988281 179.9375 693.144531 153.503906 L 479.464844 30.300781 C 433.621094 3.867188 377.121094 3.867188 331.273438 30.300781 L 117.597656 153.503906 C 71.75 179.957031 43.5 228.808594 43.5 281.675781 Z M 43.5 281.675781 \" clip-rule=\"nonzero\"/></clipPath></defs><g clip-path=\"url(#d7af0a96c1)\"><g clip-path=\"url(#0d6c067e67)\"><path fill=\"#36699f\" d=\"M 43.5 803.773438 L 43.5 6.226562 L 767.203125 6.226562 L 767.203125 803.773438 Z M 43.5 803.773438 \" fill-opacity=\"1\" fill-rule=\"nonzero\"/></g></g><g fill=\"#ff00ff\" fill-opacity=\"1\"><g transform=\"translate(206.24592, 640.079332)\"><g><path d=\"M 197.644531 0.652344 C 345.714844 0 407.03125 -73.710938 407.03125 -232.214844 C 407.03125 -391.375 345.714844 -452.039062 197.644531 -452.039062 L 105.671875 -452.039062 C 69.796875 -452.039062 50.226562 -431.816406 50.226562 -393.984375 C 50.226562 -196.339844 49.574219 -113.5 48.921875 -54.140625 C 48.921875 -18.917969 68.492188 0 104.367188 0 Z M 171.554688 -90.015625 C 170.902344 -112.195312 170.902344 -136.328125 170.902344 -159.8125 C 170.902344 -271.355469 170.902344 -317.015625 171.554688 -362.023438 L 195.035156 -362.023438 C 265.484375 -362.023438 286.355469 -326.144531 286.355469 -226.34375 C 286.355469 -138.9375 262.875 -90.015625 195.035156 -90.015625 Z M 171.554688 -90.015625 \"/></g></g></g><g fill=\"#00ffff\" fill-opacity=\"1\"><g transform=\"translate(181.784409, 640.079332)\"><g><path d=\"M 197.644531 0.652344 C 345.714844 0 407.03125 -73.710938 407.03125 -232.214844 C 407.03125 -391.375 345.714844 -452.039062 197.644531 -452.039062 L 105.671875 -452.039062 C 69.796875 -452.039062 50.226562 -431.816406 50.226562 -393.984375 C 50.226562 -196.339844 49.574219 -113.5 48.921875 -54.140625 C 48.921875 -18.917969 68.492188 0 104.367188 0 Z M 171.554688 -90.015625 C 170.902344 -112.195312 170.902344 -136.328125 170.902344 -159.8125 C 170.902344 -271.355469 170.902344 -317.015625 171.554688 -362.023438 L 195.035156 -362.023438 C 265.484375 -362.023438 286.355469 -326.144531 286.355469 -226.34375 C 286.355469 -138.9375 262.875 -90.015625 195.035156 -90.015625 Z M 171.554688 -90.015625 \"/></g></g></g><g fill=\"#fbcccc\" fill-opacity=\"1\"><g transform=\"translate(194.015166, 640.079332)\"><g><path d=\"M 197.644531 0.652344 C 345.714844 0 407.03125 -73.710938 407.03125 -232.214844 C 407.03125 -391.375 345.714844 -452.039062 197.644531 -452.039062 L 105.671875 -452.039062 C 69.796875 -452.039062 50.226562 -431.816406 50.226562 -393.984375 C 50.226562 -196.339844 49.574219 -113.5 48.921875 -54.140625 C 48.921875 -18.917969 68.492188 0 104.367188 0 Z M 171.554688 -90.015625 C 170.902344 -112.195312 170.902344 -136.328125 170.902344 -159.8125 C 170.902344 -271.355469 170.902344 -317.015625 171.554688 -362.023438 L 195.035156 -362.023438 C 265.484375 -362.023438 286.355469 -326.144531 286.355469 -226.34375 C 286.355469 -138.9375 262.875 -90.015625 195.035156 -90.015625 Z M 171.554688 -90.015625 \"/></g></g></g></svg>"
}
