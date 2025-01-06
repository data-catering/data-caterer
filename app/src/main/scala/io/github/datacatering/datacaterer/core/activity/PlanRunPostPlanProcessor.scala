package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.api.model.Constants.{PLAN_STAGE_EXCEPTION_MESSAGE_LENGTH, PLAN_STAGE_FINISHED, PLAN_STATUS_FAILED, PLAN_STATUS_SUCCESS}
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceResult, Plan, PlanResults, ValidationConfigResult}
import io.github.datacatering.datacaterer.api.util.ResultWriterUtil.{getGenerationStatus, getValidationStatus}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.plan.PostPlanProcessor
import io.github.datacatering.datacaterer.core.util.ManagementUtil.isTrackActivity
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger

class PlanRunPostPlanProcessor(val dataCatererConfiguration: DataCatererConfiguration) extends PostPlanProcessor with LifecycleManagement {

  private val LOGGER = Logger.getLogger(getClass.getName)

  override val enabled: Boolean = isTrackActivity

  override def apply(
                      plan: Plan,
                      sparkRecordListener: SparkRecordListener,
                      generationResult: List[DataSourceResult],
                      validationResults: List[ValidationConfigResult]
                    ): Unit = {
    notifyPlanFailed(plan, generationResult, validationResults)
  }

  def notifyPlanFailed(
                        plan: Plan,
                        generationResult: List[DataSourceResult],
                        validationResults: List[ValidationConfigResult],
                        stage: String = PLAN_STAGE_FINISHED,
                        optException: Option[Exception] = None
                      ): Unit = {
    val exceptionMessage = optException.map(ex => ex.getMessage.substring(0, Math.min(PLAN_STAGE_EXCEPTION_MESSAGE_LENGTH, ex.getMessage.length)))
    val isGenerationSuccess = getGenerationStatus(generationResult)
    val isValidationSuccess = getValidationStatus(validationResults)
    val planResults = PlanResults(plan, generationResult, validationResults, isGenerationSuccess, isValidationSuccess, stage, exceptionMessage)
    val body = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(planResults)
    val status = if (stage == PLAN_STAGE_FINISHED) PLAN_STATUS_SUCCESS else PLAN_STATUS_FAILED
    val url = s"$dataCatererManagementUrl/plan/finish?stage=$stage&status=$status"
    LOGGER.debug(s"Sending HTTP request, url=$url, message=$exceptionMessage")
    sendRequest(url, body)
  }
}
