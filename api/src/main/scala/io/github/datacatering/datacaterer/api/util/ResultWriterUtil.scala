package io.github.datacatering.datacaterer.api.util

object ResultWriterUtil {

  def getSuccessSymbol(isSuccess: Boolean): String = {
    if (isSuccess) "✅" else "❌"
  }

}
