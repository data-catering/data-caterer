package io.github.datacatering.datacaterer.core.util

object ManagementUtil {

  def getDataCatererManagementUrl: String = {
    val envVar = System.getenv("DATA_CATERER_MANAGEMENT_URL")
    if (envVar == null) "http://localhost:8082" else envVar
  }
}
