package io.github.datacatering.datacaterer.core.plan

import io.github.datacatering.datacaterer.api.model.{DataCatererConfiguration, Plan}
import io.github.datacatering.datacaterer.core.listener.SparkRecordListener
import io.github.datacatering.datacaterer.core.model.{DataSourceResult, ValidationConfigResult}

trait PostPlanProcessor {

  val dataCatererConfiguration: DataCatererConfiguration
  val enabled: Boolean

  def apply(plan: Plan, sparkRecordListener: SparkRecordListener, generationResult: List[DataSourceResult],
            validationResults: List[ValidationConfigResult]): Unit

}
