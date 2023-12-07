package com.github.pflooky.datagen.core.alert

import com.github.pflooky.datacaterer.api.model.Constants.{ALERT_TRIGGER_ON_ALL, ALERT_TRIGGER_ON_FAILURE, ALERT_TRIGGER_ON_GENERATION_FAILURE, ALERT_TRIGGER_ON_GENERATION_SUCCESS, ALERT_TRIGGER_ON_SUCCESS, ALERT_TRIGGER_ON_VALIDATION_FAILURE, ALERT_TRIGGER_ON_VALIDATION_SUCCESS}
import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, Plan}
import com.github.pflooky.datagen.core.listener.SparkRecordListener
import com.github.pflooky.datagen.core.model.{DataSourceResult, ValidationConfigResult}
import com.github.pflooky.datagen.core.plan.PostPlanProcessor
import org.apache.log4j.Logger

class AlertProcessor(val dataCatererConfiguration: DataCatererConfiguration) extends PostPlanProcessor {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override val enabled: Boolean = dataCatererConfiguration.flagsConfig.enableAlerts

  override def apply(plan: Plan, sparkRecordListener: SparkRecordListener, generationResult: List[DataSourceResult],
                     validationResults: List[ValidationConfigResult]): Unit = {
    sendAlerts(generationResult, validationResults)
  }

  def sendAlerts(
                  generationResult: List[DataSourceResult],
                  validationResults: List[ValidationConfigResult]
                ): Unit = {
    if (shouldTriggerAlert(generationResult, validationResults)) {
      LOGGER.info("Sending data generation and validation summary as alerts to Slack")
      val slackAlertProcessor = new SlackAlertProcessor(dataCatererConfiguration.alertConfig.slackAlertConfig)
      val optReportFolderPath = if (dataCatererConfiguration.flagsConfig.enableSaveReports) Some(dataCatererConfiguration.foldersConfig.generatedReportsFolderPath) else None
      slackAlertProcessor.sendAlerts(generationResult, validationResults, optReportFolderPath)
    } else {
      LOGGER.warn(s"Alerts not triggered due to trigger on condition not met, trigger-on=${dataCatererConfiguration.alertConfig.triggerOn}")
    }
  }

  def shouldTriggerAlert(
                          generationResult: List[DataSourceResult],
                          validationResults: List[ValidationConfigResult]
                        ): Boolean = {
    val generationSuccess = generationResult.forall(_.sinkResult.isSuccess)
    val validationSuccess = validationResults.flatMap(v => v.dataSourceValidationResults.flatMap(d => d.validationResults.map(_.isSuccess))).forall(x => x)
    dataCatererConfiguration.alertConfig.triggerOn match {
      case ALERT_TRIGGER_ON_ALL => true
      case ALERT_TRIGGER_ON_FAILURE => !generationSuccess && !validationSuccess
      case ALERT_TRIGGER_ON_SUCCESS => generationSuccess && validationSuccess
      case ALERT_TRIGGER_ON_GENERATION_FAILURE => !generationSuccess
      case ALERT_TRIGGER_ON_GENERATION_SUCCESS => generationSuccess
      case ALERT_TRIGGER_ON_VALIDATION_FAILURE => !validationSuccess
      case ALERT_TRIGGER_ON_VALIDATION_SUCCESS => validationSuccess
      case _ => false
    }
  }

}
