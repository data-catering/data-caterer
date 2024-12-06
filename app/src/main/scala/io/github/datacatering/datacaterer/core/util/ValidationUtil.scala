package io.github.datacatering.datacaterer.core.util

object ValidationUtil {

  def cleanValidationIdentifier(identifier: String): String = identifier.replaceAll("[{}]", "")

  def cleanColumnName(column: String): String = column.split("\\.").map(c => s"`$c`").mkString(".")

}
