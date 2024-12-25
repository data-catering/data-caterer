package io.github.datacatering.datacaterer.core.util

object ValidationUtil {

  def cleanValidationIdentifier(identifier: String): String = identifier.replaceAll("[{}]", "")

  def cleanFieldName(field: String): String = field.split("\\.").map(c => s"`$c`").mkString(".")
}
