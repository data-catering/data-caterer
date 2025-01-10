package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, PlanRunSummary, Task, ValidationConfiguration}
import io.github.datacatering.datacaterer.api.util.ConfigUtil.cleanseOptions
import io.github.datacatering.datacaterer.core.plan.PrePlanProcessor
import io.github.datacatering.datacaterer.core.util.LifecycleUtil.isTrackActivity
import io.github.datacatering.datacaterer.core.util.ObjectMapperUtil

class PlanRunPrePlanProcessor(val dataCatererConfiguration: DataCatererConfiguration) extends PrePlanProcessor with LifecycleManagement {

  override val enabled: Boolean = isTrackActivity

  override def apply(
                      plan: Plan,
                      tasks: List[Task],
                      validations: List[ValidationConfiguration]
                    ): Unit = {
    val planRunSummary = PlanRunSummary(plan, tasks, validations)
    val cleansedPlanRunSummary = cleanseOptions(planRunSummary)
    val body = ObjectMapperUtil.jsonObjectMapper.writeValueAsString(cleansedPlanRunSummary)
    val url = s"$dataCatererManagementUrl/plan/start"
    sendRequest(url, body, true)
  }
}
