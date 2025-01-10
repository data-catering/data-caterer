package io.github.datacatering.datacaterer.api.util

import io.github.datacatering.datacaterer.api.model.{DataSourceResult, ValidationConfigResult}

object ResultWriterUtil {

  def getSuccessSymbol(isSuccess: Boolean): String = {
    if (isSuccess) "✅" else "❌"
  }

  def getGenerationStatus(generationResult: List[DataSourceResult]): Boolean = {
    generationResult.forall(_.sinkResult.isSuccess)
  }

  def getValidationStatus(validationResults: List[ValidationConfigResult]): Boolean = {
    validationResults
      .flatMap(v => v.dataSourceValidationResults.flatMap(d => d.validationResults.map(_.isSuccess)))
      .forall(x => x)
  }
}
