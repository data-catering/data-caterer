package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.core.exception.MissingApiEnvVarException
import io.github.datacatering.datacaterer.core.model.Constants.{DATA_CATERER_API_TOKEN, DATA_CATERER_API_USER, DATA_CATERER_MANAGEMENT_URL}

object ManagementUtil {

  def getDataCatererManagementUrl: String = {
    val envVar = System.getenv(DATA_CATERER_MANAGEMENT_URL)
    if (envVar == null) "http://localhost:8082/v1" else envVar
  }

  def getApiToken: String = getEnvOrProperty(DATA_CATERER_API_TOKEN)

  def getApiUser: String = getEnvOrProperty(DATA_CATERER_API_USER)

  def getEnvOrProperty(key: String): String = {
    val envVar = System.getenv(key)
    val propertyVar = System.getProperty(key)
    if (envVar != null && !envVar.isBlank) {
      envVar
    } else if (propertyVar != null && !propertyVar.isBlank) {
      propertyVar
    } else {
      throw MissingApiEnvVarException(key)
    }
  }
}
