package io.github.datacatering.datacaterer.core.foreignkey.model

import io.github.datacatering.datacaterer.api.model.{PerFieldCount, Plan, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import org.apache.spark.sql.DataFrame

/**
 * Context object containing all inputs needed for foreign key processing.
 *
 * @param plan The data generation plan
 * @param generatedData Map of data source name to generated DataFrame
 * @param executableTasks Optional list of task summaries and tasks for accessing step configurations
 * @param config Foreign key configuration
 */
case class ForeignKeyContext(
  plan: Plan,
  generatedData: Map[String, DataFrame],
  executableTasks: Option[List[(TaskSummary, Task)]],
  config: ForeignKeyConfig
)

/**
 * Result of foreign key processing.
 *
 * @param dataFrames List of (dataSourceName, DataFrame) tuples with foreign keys applied
 * @param insertOrder Ordered list of data source names for proper FK insertion sequence
 */
case class ForeignKeyResult(
  dataFrames: List[(String, DataFrame)],
  insertOrder: List[String]
)

/**
 * Field mapping for foreign key relationships.
 *
 * @param sourceField Source field name (can be nested with dot notation)
 * @param targetField Target field name (can be nested with dot notation)
 */
case class FieldMapping(
  sourceField: String,
  targetField: String
) {
  def isNested: Boolean = targetField.contains(".")
}

object FieldMapping {
  def from(sourceFields: List[String], targetFields: List[String]): List[FieldMapping] = {
    sourceFields.zip(targetFields).map { case (src, tgt) => FieldMapping(src, tgt) }
  }
}

/**
 * Enhanced relationship information with additional context.
 *
 * @param sourceDataFrameName Source data frame name
 * @param sourceFields List of source field names
 * @param targetDataFrameName Target data frame name
 * @param targetFields List of target field names
 * @param config Foreign key configuration for this relationship
 * @param targetPerFieldCount Optional perField count configuration from target step
 */
case class EnhancedForeignKeyRelation(
  sourceDataFrameName: String,
  sourceFields: List[String],
  targetDataFrameName: String,
  targetFields: List[String],
  config: ForeignKeyConfig,
  targetPerFieldCount: Option[PerFieldCount]
) {
  def fieldMappings: List[FieldMapping] = FieldMapping.from(sourceFields, targetFields)

  def hasNestedFields: Boolean = targetFields.exists(_.contains("."))

  def hasFlatFields: Boolean = targetFields.exists(!_.contains("."))

  def hasMixedFields: Boolean = hasNestedFields && hasFlatFields
}
