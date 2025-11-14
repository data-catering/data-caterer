package io.github.datacatering.datacaterer.core.foreignkey.validator

import io.github.datacatering.datacaterer.core.exception.MissingDataSourceFromForeignKeyException
import io.github.datacatering.datacaterer.core.foreignkey.util.NestedFieldUtil
import io.github.datacatering.datacaterer.core.model.ForeignKeyWithGenerateAndDelete
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Validates foreign key relationships and data compatibility.
 */
object ForeignKeyValidator {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Validate that a foreign key relationship is valid and enabled.
   *
   * @param generatedDataMap Map of data frame name to DataFrame
   * @param enabledSources List of enabled data source names
   * @param fkr Foreign key relationship to validate
   * @return true if relationship is valid, false otherwise
   */
  def isValidForeignKeyRelation(
    generatedDataMap: Map[String, DataFrame],
    enabledSources: List[String],
    fkr: ForeignKeyWithGenerateAndDelete
  ): Boolean = {
    val isMainForeignKeySourceEnabled = enabledSources.contains(fkr.source.dataSource)
    val subForeignKeySources = fkr.generationLinks.map(_.dataSource)
    val isSubForeignKeySourceEnabled = subForeignKeySources.forall(enabledSources.contains)
    val disabledSubSources = subForeignKeySources.filter(s => !enabledSources.contains(s))

    val sourceDfName = s"${fkr.source.dataSource}.${fkr.source.step}"
    if (!generatedDataMap.contains(sourceDfName)) {
      throw MissingDataSourceFromForeignKeyException(sourceDfName)
    }

    val mainDfFields = generatedDataMap(sourceDfName).schema.fields
    val fieldExistsMain = fkr.source.fields.forall(c => NestedFieldUtil.hasDfContainField(c, mainDfFields))

    if (!isMainForeignKeySourceEnabled) {
      LOGGER.warn(s"Foreign key data source is not enabled. Data source needs to be enabled for foreign key relationship " +
        s"to exist from generated data, data-source-name=${fkr.source.dataSource}")
    }
    if (!isSubForeignKeySourceEnabled) {
      LOGGER.warn(s"Sub data sources within foreign key relationship are not enabled, disabled-task=${disabledSubSources.mkString(",")}")
    }
    if (!fieldExistsMain) {
      LOGGER.warn(s"Main field for foreign key references is not created, data-source-name=${fkr.source.dataSource}, field=${fkr.source.fields}")
    }

    isMainForeignKeySourceEnabled && isSubForeignKeySourceEnabled && fieldExistsMain
  }

  /**
   * Validate that target DataFrame contains all required foreign key fields.
   *
   * @param targetFields List of target field names
   * @param targetDf Target DataFrame
   * @return true if all fields exist, false otherwise
   */
  def targetContainsAllFields(targetFields: List[String], targetDf: DataFrame): Boolean = {
    targetFields.forall(field => NestedFieldUtil.hasDfContainField(field, targetDf.schema.fields))
  }

  /**
   * Validate field mapping compatibility.
   *
   * @param sourceFields List of source field names
   * @param targetFields List of target field names
   * @throws IllegalArgumentException if field counts don't match
   */
  def validateFieldMapping(sourceFields: List[String], targetFields: List[String]): Unit = {
    require(sourceFields.length == targetFields.length,
      s"Source and target field counts must match: source=${sourceFields.length}, target=${targetFields.length}")
  }
}
