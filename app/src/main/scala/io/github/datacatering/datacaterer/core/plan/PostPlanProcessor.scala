package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, DataSourceResult, Plan, ValidationConfigResult}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener

trait PostPlanProcessor {

  val dataCatererConfiguration: DataCatererConfiguration
  val enabled: Boolean

  def apply(plan: Plan, sparkRecordListener: SparkRecordListener, generationResult: List[DataSourceResult],
            validationResults: List[ValidationConfigResult]): Unit

}
