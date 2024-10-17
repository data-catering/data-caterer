package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.IS_PRIMARY_KEY
import org.apache.spark.sql.types.StructField

class PrimaryKeyValidationPredictionCheck extends ValidationPredictionCheck {
  override def check(fields: Array[StructField]): List[ValidationBuilder] = {
    val primaryKeys = fields.filter(f => f.metadata.contains(IS_PRIMARY_KEY) && f.metadata.getString(IS_PRIMARY_KEY) == "true").toList
    if (primaryKeys.nonEmpty) {
      val uniqueCheck = ValidationBuilder().unique(primaryKeys.map(_.name): _*)
      List(uniqueCheck) ++ primaryKeys.flatMap(check)
    } else {
      List()
    }
  }

  override def check(field: StructField): List[ValidationBuilder] = {
    List(ValidationBuilder().col(field.name).isNotNull)
  }
}
