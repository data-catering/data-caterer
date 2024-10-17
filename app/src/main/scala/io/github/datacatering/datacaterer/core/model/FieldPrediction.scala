package io.github.datacatering.datacaterer.core.model

import io.github.datacatering.datacaterer.api.model.Constants.{EXPRESSION, FIELD_LABEL, IS_PII}

case class FieldPrediction(fakerExpression: String, label: String, isPII: Boolean, additionalMetadata: Map[String, String] = Map()) {

  def toMap: Map[String, String] = {
    val baseMap = Map(
      EXPRESSION -> fakerExpression,
      FIELD_LABEL -> label,
      IS_PII -> isPII.toString
    ) ++ additionalMetadata
    baseMap.filter(m => m._2.nonEmpty && m._2 != "#{}")
  }
}
