package io.github.datacatering.datacaterer.core.generator.metadata.validation

import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, FAKER_EXPR_COUNTRY_CODE, FAKER_EXPR_CURRENCY_CODE, FAKER_EXPR_IPV4, FAKER_EXPR_IPV6}
import org.apache.spark.sql.types.StructField

class ExpressionValidationPredictionCheck extends ValidationPredictionCheck {
  override def check(fields: Array[StructField]): List[ValidationBuilder] = {
    val exprFields = fields.filter(_.metadata.contains(EXPRESSION))
    exprFields.flatMap(check).toList
  }

  override def check(field: StructField): List[ValidationBuilder] = {
    //TODO get all possible values from faker library if expression can only take certain values
    field.metadata.getString(EXPRESSION) match {
      case FAKER_EXPR_COUNTRY_CODE =>
        List(new ValidationBuilder().col(field.name).matches("^[A-Z]{2,3}$"))
      case FAKER_EXPR_CURRENCY_CODE =>
        List(new ValidationBuilder().col(field.name).matches("^[A-Z]{3}$"))
      case FAKER_EXPR_IPV4 =>
        List(new ValidationBuilder().col(field.name).matches("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$"))
      case FAKER_EXPR_IPV6 =>
        List(new ValidationBuilder().col(field.name).matches("(?:^|(?<=\\s))(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))(?=\\s|$)"))
      case _ => List()
    }
  }
}
