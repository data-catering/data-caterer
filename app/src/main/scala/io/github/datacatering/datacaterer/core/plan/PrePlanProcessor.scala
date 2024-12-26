package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan, Task, ValidationConfiguration}

trait PrePlanProcessor {

  val dataCatererConfiguration: DataCatererConfiguration
  val enabled: Boolean

  def apply(plan: Plan, tasks: List[Task], validations: List[ValidationConfiguration]): Unit

}
