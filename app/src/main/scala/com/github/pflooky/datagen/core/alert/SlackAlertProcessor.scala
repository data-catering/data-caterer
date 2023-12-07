package com.github.pflooky.datagen.core.alert

import com.github.pflooky.datacaterer.api.model.SlackAlertConfig
import com.github.pflooky.datagen.core.model.Constants.REPORT_HOME_HTML
import com.github.pflooky.datagen.core.model.{DataSourceResult, ValidationConfigResult}
import com.slack.api.Slack
import com.slack.api.methods.MethodsClient
import com.slack.api.methods.request.chat.ChatPostMessageRequest
import org.apache.log4j.Logger

import scala.math.BigDecimal.RoundingMode
import scala.util.{Failure, Success, Try}

class SlackAlertProcessor(slackAlertConfig: SlackAlertConfig) {

  private lazy val LOGGER = Logger.getLogger(getClass.getName)

  def sendAlerts(
                  generationResult: List[DataSourceResult],
                  validationResults: List[ValidationConfigResult],
                  optReportFolderPath: Option[String] = None
                ): Unit = {
    val slack = Slack.getInstance()
    val methods = slack.methods(slackAlertConfig.token)

    slackAlertConfig.channels.foreach(channel => {
      val messageHeader = "*Data Caterer Run Results*"
      val dataGenResultStr = dataGenerationResultText(generationResult)
      val validationResStr = validationResultText(validationResults)
      val reportFooter = optReportFolderPath
        .map(p => s"For more details, check the report generated here: `file:///$p/$REPORT_HOME_HTML` (copy and paste into browser URL)")
        .getOrElse("No report generated. Please enable via: `plan.enableSaveReports(true)` if you want to find more details.")

      val messageText = s"$messageHeader\n$dataGenResultStr\n$validationResStr\n---\n$reportFooter"
      sendMessage(methods, channel, messageText)
    })
  }

  private def sendMessage(methods: MethodsClient, channel: String, messageText: String): Unit = {
    val request = ChatPostMessageRequest.builder()
      .channel(channel)
      .text(messageText)
      .mrkdwn(true)
      .build()

    Try(methods.chatPostMessage(request)) match {
      case Failure(exception) =>
        LOGGER.error(s"Failed to post message to Slack channel, channel=$channel", exception)
      case Success(value) =>
        if (value.isOk) {
          LOGGER.debug(s"Successfully posted message to Slack channel, channel=$channel")
        } else {
          LOGGER.error(s"Sent message to Slack channel but received back error, channel=$channel, error-from-slack=${value.getError}")
        }
    }
  }

  private def dataGenerationResultText(generationResult: List[DataSourceResult]): String = {
    if (generationResult.nonEmpty) {
      val timeTaken = generationResult.map(_.sinkResult.durationInSeconds).max
      val startGenerationText = s"*Data Generation Summary*\nTime taken: ${timeTaken}s"
      val mappedResults = generationResult.map(dataSourceResult => {
        val name = dataSourceResult.name
        val format = dataSourceResult.sinkResult.format
        val isSuccess = getSuccessSymbol(dataSourceResult.sinkResult.isSuccess)
        val numRecords = dataSourceResult.sinkResult.count.toString
        List(name, format, isSuccess, numRecords)
      })
      val header = List(List("Data Source Name", "Format", "Success", "Num Records"))
      startGenerationText + "\n" + formatTable(header ++ mappedResults)
    } else {
      "*No data generated*"
    }
  }

  private def validationResultText(validationResults: List[ValidationConfigResult]): String = {
    if (validationResults.nonEmpty) {
      val timeTaken = validationResults.map(_.durationInSeconds).sum
      val startValidationText = s"*Data Validation Summary*\nTime taken: ${timeTaken}s"
      val mappedResults = validationResults.map(validationConfigResult => {
        val validationName = validationConfigResult.name
        val validationDesc = validationConfigResult.description
        val validationRes = validationConfigResult.dataSourceValidationResults.flatMap(_.validationResults)
        val validationSuccess = validationRes.map(_.isSuccess)
        val numSuccess = validationSuccess.count(x => x)
        val successRate = BigDecimal(numSuccess.toDouble / validationRes.size * 100).setScale(2, RoundingMode.HALF_UP)
        val isSuccess = getSuccessSymbol(validationSuccess.forall(x => x))
        val successRateVisual = s"$numSuccess/${validationRes.size} ($successRate%)"

        List(validationName, validationDesc, isSuccess, successRateVisual)
      })
      val header = List(List("Name", "Description", "Success", "Success Rate"))
      startValidationText + "\n" + formatTable(header ++ mappedResults)
    } else {
      "*No data validations*"
    }
  }

  private def getSuccessSymbol(isSuccess: Boolean): String = {
    if (isSuccess) "✅" else "❌"
  }

  private def formatTable(table: Seq[Seq[Any]], hasHeader: Boolean = true): String = {
    if (table.isEmpty) ""
    else {
      val colWidths = table.transpose.map(_.map(cell => if (cell == null) 0 else cell.toString.length).max + 2)
      val rows = table.map(_.zip(colWidths)
        .map { case (item, size) => {
          val minus = if (item == "✅" || item == "❌") 2 else 1
          (" %-" + (size - minus) + "s").format(item)
        }
        }
        .mkString("|", "|", "|"))
      val separator = colWidths.map("-" * _).mkString("+", "+", "+")
      val combined = if (hasHeader) separator +: rows.head +: separator +: rows.tail :+ separator else separator +: rows :+ separator
      "```\n" + combined.mkString("\n") + "\n```"
    }
  }
}
