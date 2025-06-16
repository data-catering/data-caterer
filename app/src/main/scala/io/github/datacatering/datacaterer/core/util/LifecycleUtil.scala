package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.core.model.Constants.DATA_CATERER_UI

object LifecycleUtil {

  def isRunningDataCatererUi: Boolean = {
    val uiProperty = System.getProperty(DATA_CATERER_UI)
    if (uiProperty != null) true else false
  }

}
