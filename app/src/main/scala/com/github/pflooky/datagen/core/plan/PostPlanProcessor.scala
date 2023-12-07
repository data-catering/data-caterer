package com.github.pflooky.datagen.core.plan

import com.github.pflooky.datacaterer.api.model.{DataCatererConfiguration, Plan}
import com.github.pflooky.datagen.core.listener.SparkRecordListener
import com.github.pflooky.datagen.core.model.{DataSourceResult, ValidationConfigResult}

trait PostPlanProcessor {

  val dataCatererConfiguration: DataCatererConfiguration
  val enabled: Boolean

  def apply(plan: Plan, sparkRecordListener: SparkRecordListener, generationResult: List[DataSourceResult],
            validationResults: List[ValidationConfigResult]): Unit

}
