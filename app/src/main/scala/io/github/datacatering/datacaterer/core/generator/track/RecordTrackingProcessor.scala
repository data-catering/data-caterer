package io.github.datacatering.datacaterer.core.generator.track

import io.github.datacatering.datacaterer.api.model.Constants.{IS_PRIMARY_KEY, PATH}
import io.github.datacatering.datacaterer.api.model.Step
import io.github.datacatering.datacaterer.core.model.Constants.RECORD_TRACKING_VALIDATION_FORMAT
import io.github.datacatering.datacaterer.core.util.MetadataUtil.getSubDataSourcePath
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

class RecordTrackingProcessor(recordTrackingFolderPath: String) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  def trackRecords(df: DataFrame, dataSourceName: String, planName: String, step: Step): Unit = {
    val subDataSourcePath = getSubDataSourcePath(dataSourceName, planName, step, recordTrackingFolderPath)
    LOGGER.debug(s"Generated record tracking is enabled, data-source-name=$dataSourceName, plan-name=$planName, save-path=$subDataSourcePath")
    if (df.isEmpty || df.schema.isEmpty) {
      LOGGER.debug("Unable to save records for record tracking due to 0 records found or empty schema")
    } else {
      getFieldsToTrack(df, step).write
        .format(RECORD_TRACKING_VALIDATION_FORMAT)
        .mode(SaveMode.Append)
        .option(PATH, subDataSourcePath)
        .save()
    }
  }

  def getFieldsToTrack(df: DataFrame, step: Step): DataFrame = {
    val primaryKeys = step.fields
      .filter(f => f.options.contains(IS_PRIMARY_KEY) && f.options(IS_PRIMARY_KEY).toString == "true")
      .map(_.name)
    LOGGER.debug(s"Found primary keys for data, primary-keys=${primaryKeys.mkString(",")}")
    if (primaryKeys.isEmpty) df else df.selectExpr(primaryKeys: _*)
  }
}
