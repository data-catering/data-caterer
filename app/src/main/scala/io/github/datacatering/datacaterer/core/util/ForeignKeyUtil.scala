package io.github.datacatering.datacaterer.core.util

import io.github.datacatering.datacaterer.api.model.{Plan, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.foreignkey.ForeignKeyProcessor
import io.github.datacatering.datacaterer.core.foreignkey.config.ForeignKeyConfig
import io.github.datacatering.datacaterer.core.foreignkey.model.ForeignKeyContext
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
 * Compatibility wrapper for ForeignKeyUtil.
 *
 * This object provides the old public API while delegating to the new
 * foreign key architecture (ForeignKeyProcessor and strategies).
 *
 * @deprecated Use ForeignKeyProcessor directly for new code.
 *             This wrapper is provided for backward compatibility only.
 */
object ForeignKeyUtil {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Apply foreign key relationships to generated DataFrames.
   *
   * @param plan The execution plan containing foreign key definitions
   * @param generatedData Map of dataframe names to DataFrames
   * @param executableTasks Optional list of (TaskSummary, Task) pairs for perField count extraction
   * @return List of (dataframe name, DataFrame) with foreign keys applied, ordered by dependency
   */
  def getDataFramesWithForeignKeys(
    plan: Plan,
    generatedData: Seq[(String, DataFrame)],
    executableTasks: Option[List[(TaskSummary, Task)]] = None
  ): List[(String, DataFrame)] = {

    LOGGER.debug(s"Applying foreign keys using new architecture (ForeignKeyProcessor)")

    // Create context
    val context = ForeignKeyContext(
      plan = plan,
      generatedData = generatedData.toMap,
      executableTasks = executableTasks,
      config = ForeignKeyConfig.default
    )

    // Use processor
    val processor = ForeignKeyProcessor()
    val result = processor.process(context)

    // Return dataframes in insertion order
    result.dataFrames
  }
}
