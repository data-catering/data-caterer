package io.github.datacatering.datacaterer.core.util

object ResultWriterUtil {

  def getSuccessSymbol(isSuccess: Boolean): String = {
    if (isSuccess) "✅" else "❌"
  }

}
