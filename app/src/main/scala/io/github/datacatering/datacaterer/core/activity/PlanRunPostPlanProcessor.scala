package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.api.model.Constants.{PLAN_STAGE_EXCEPTION_MESSAGE_LENGTH, PLAN_STAGE_FINISHED, PLAN_STATUS_FAILED, PLAN_STATUS_SUCCESS}
import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceResult, Plan, PlanResults, ValidationConfigResult}
import io.github.datacatering.datacaterer.api.util.ResultWriterUtil.{getGenerationStatus, getValidationStatus}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.plan.PostPlanProcessor
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil
import org.apache.log4j.Logger

class PlanRunPostPlanProcessor(val dataCatererConfiguration: DataCatererConfiguration) extends PostPlanProcessor with LifecycleManagement {

  private val LOGGER = Logger.getLogger(getClass.getName)
  
  override def apply(
                      plan: Plan,
                      sparkRecordListener: SparkRecordListener,
                      generationResult: List[DataSourceResult],
                      validationResults: List[ValidationConfigResult]
                    ): Unit = {
    notifyPlanResult(plan, generationResult, validationResults)
  }

  def notifyPlanResult(
                        plan: Plan,
                        generationResult: List[DataSourceResult],
                        validationResults: List[ValidationConfigResult],
                        stage: String = PLAN_STAGE_FINISHED,
                        optException: Option[Exception] = None
                      ): Unit = {
    if (enabled) {
      val exceptionMessage = optException.map(ex => ex.getMessage.substring(0, Math.min(PLAN_STAGE_EXCEPTION_MESSAGE_LENGTH, ex.getMessage.length)))
      val isGenerationSuccess = getGenerationStatus(generationResult)
      val isValidationSuccess = getValidationStatus(validationResults)
      val generationSummary = generationResult.map(_.jsonSummary)
      val validationSummary = validationResults.map(_.jsonSummary(false))

      val planResults = PlanResults(plan, generationSummary, validationSummary, isGenerationSuccess, isValidationSuccess, stage, exceptionMessage)
      val planResultsJson = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(planResults)

      val status = if (stage == PLAN_STAGE_FINISHED && optException.isEmpty) PLAN_STATUS_SUCCESS else PLAN_STATUS_FAILED

      if (optException.isDefined) {
        LOGGER.error(s"Plan failed, stage=$stage, status=$status, message=${exceptionMessage.getOrElse("")}")
        LOGGER.debug(s"Failed plan details, results=$planResultsJson")
      } else {
        LOGGER.debug(s"Plan finished, stage=$stage, status=$status")
      }
    }
  }
}
