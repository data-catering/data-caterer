package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.ValidationBuilder
import org.apache.spark.sql.types.StructField

trait ValidationPredictionCheck {

  def check(fields: Array[StructField]): List[ValidationBuilder]

  def check(field: StructField): List[ValidationBuilder]
}
