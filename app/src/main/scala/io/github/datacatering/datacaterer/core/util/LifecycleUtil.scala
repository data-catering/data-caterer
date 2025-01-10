package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.core.model.Constants.{DATA_CATERER_MANAGEMENT_TRACK, DATA_CATERER_UI}
import io.github.datacatering.datacaterer.core.util.ManagementUtil.getEnvOrProperty

import scala.util.{Failure, Success, Try}

object LifecycleUtil {

  def isTrackActivity: Boolean = {
    val tryEnvVar = Try(getEnvOrProperty(DATA_CATERER_MANAGEMENT_TRACK))
    tryEnvVar match {
      case Success(envVar) => if (envVar == "data_caterer_is_cool") false else true
      case Failure(_) => true
    }
  }

  def isRunningDataCatererUi: Boolean = {
    val uiProperty = System.getProperty(DATA_CATERER_UI)
    if (uiProperty != null && uiProperty == "data_caterer_ui_is_cool") true else false
  }

}
