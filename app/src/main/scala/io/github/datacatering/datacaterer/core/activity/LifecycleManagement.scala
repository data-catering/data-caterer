package io.github.datacatering.datacaterer.core.activity

import io.github.datacatering.datacaterer.core.util.LifecycleUtil.isRunningDataCatererUi

trait LifecycleManagement {

  private val isDataCatererUi: Boolean = isRunningDataCatererUi

}
