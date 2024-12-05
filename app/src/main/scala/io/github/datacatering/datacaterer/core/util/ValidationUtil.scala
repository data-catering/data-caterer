package io.github.datacatering.datacaterer.core.util

object ValidationUtil {

  def cleanValidationIdentifier(identifier: String): String = identifier.replaceAll("[{}]", "")

}
