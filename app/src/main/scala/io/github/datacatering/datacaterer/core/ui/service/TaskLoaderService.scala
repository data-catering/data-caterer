package io.github.datacatering.datacaterer.core.ui.service

import io.github.datacatering.datacaterer.api.model.Task
import io.github.datacatering.datacaterer.core.config.ConfigParser
import io.github.datacatering.datacaterer.core.ui.resource.YamlResourceCache
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Centralized service for loading tasks from YAML files
 *
 * This service consolidates task loading logic that was previously duplicated across
 * PlanRepository and FastSampleGenerator, leveraging YamlResourceCache for performance.
 *
 * Features:
 * - Uses YamlResourceCache for efficient task lookups
 * - Provides task-by-name and bulk loading operations
 * - Supports custom task folder paths for testing
 * - Consistent error handling and logging
 */
object TaskLoaderService {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Find a task by name using cached task folder parsing
   *
   * @param taskName Name of the task to find
   * @param taskFolderPath Optional custom task folder path (defaults to configured path)
   * @return Task if found
   * @throws IllegalArgumentException if task not found
   */
  def findTaskByName(
    taskName: String,
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): Task = {
    val effectiveTaskFolderPath = taskFolderPath.getOrElse(ConfigParser.foldersConfig.taskFolderPath)

    LOGGER.debug(s"Finding task by name, task-name=$taskName, folder=$effectiveTaskFolderPath")

    // Use cached task folder parsing for performance
    YamlResourceCache.findTaskByName(taskName, effectiveTaskFolderPath).getOrElse {
      // Cache miss - invalidate folder cache and try once more
      // This handles cases where new task files were added after cache was populated
      LOGGER.debug(s"Task not found in cache, invalidating folder cache and retrying, task-name=$taskName")
      YamlResourceCache.invalidateTaskFolder(effectiveTaskFolderPath)

      YamlResourceCache.findTaskByName(taskName, effectiveTaskFolderPath).getOrElse {
        // Still not found - get all tasks for better error message
        val allTasks = getAllTasksFromFolder(Some(effectiveTaskFolderPath))
        val availableTaskNames = allTasks.map(_.name).mkString(", ")
        throw new IllegalArgumentException(
          s"Task '$taskName' not found in task folder: $effectiveTaskFolderPath. " +
            s"Available tasks: $availableTaskNames"
        )
      }
    }
  }

  /**
   * Find tasks by names (bulk operation)
   *
   * @param taskNames List of task names to find
   * @param taskFolderPath Optional custom task folder path
   * @return List of found tasks (silently skips missing tasks)
   */
  def findTasksByNames(
    taskNames: List[String],
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): List[Task] = {
    val effectiveTaskFolderPath = taskFolderPath.getOrElse(ConfigParser.foldersConfig.taskFolderPath)

    LOGGER.debug(s"Finding tasks by names, task-names=${taskNames.mkString(", ")}, folder=$effectiveTaskFolderPath")

    taskNames.flatMap(taskName => {
      try {
        Some(findTaskByName(taskName, taskFolderPath))
      } catch {
        case ex: IllegalArgumentException =>
          LOGGER.warn(s"Task not found, skipping, task-name=$taskName, error=${ex.getMessage}")
          None
      }
    })
  }

  /**
   * Get all tasks from a folder using cached parsing
   *
   * @param taskFolderPath Optional custom task folder path
   * @return Array of all tasks in the folder
   */
  def getAllTasksFromFolder(
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): Array[Task] = {
    val effectiveTaskFolderPath = taskFolderPath.getOrElse(ConfigParser.foldersConfig.taskFolderPath)

    LOGGER.debug(s"Loading all tasks from folder, folder=$effectiveTaskFolderPath")

    YamlResourceCache.getTasksFromFolder(effectiveTaskFolderPath)
  }

  /**
   * Get all tasks from a folder and return as List
   */
  def getAllTasksFromFolderAsList(
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): List[Task] = {
    getAllTasksFromFolder(taskFolderPath).toList
  }

  /**
   * Check if a task exists by name
   *
   * @param taskName Name of the task to check
   * @param taskFolderPath Optional custom task folder path
   * @return true if task exists, false otherwise
   */
  def taskExists(
    taskName: String,
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): Boolean = {
    val effectiveTaskFolderPath = taskFolderPath.getOrElse(ConfigParser.foldersConfig.taskFolderPath)

    YamlResourceCache.findTaskByName(taskName, effectiveTaskFolderPath).isDefined
  }

  /**
   * Find a specific step within a task
   *
   * @param taskName Name of the task containing the step
   * @param stepName Name of the step to find
   * @param taskFolderPath Optional custom task folder path
   * @return Step if found
   * @throws IllegalArgumentException if task or step not found
   */
  def findStepInTask(
    taskName: String,
    stepName: String,
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): io.github.datacatering.datacaterer.api.model.Step = {
    val task = findTaskByName(taskName, taskFolderPath)

    task.steps.find(_.name == stepName) match {
      case Some(step) =>
        LOGGER.debug(s"Found step in task, task-name=$taskName, step-name=$stepName")
        step
      case None =>
        val availableStepNames = task.steps.map(_.name).mkString(", ")
        throw new IllegalArgumentException(
          s"Step '$stepName' not found in task '$taskName'. Available steps: $availableStepNames"
        )
    }
  }

  /**
   * Get all steps from a task
   *
   * @param taskName Name of the task
   * @param taskFolderPath Optional custom task folder path
   * @return List of all steps in the task
   */
  def getAllStepsFromTask(
    taskName: String,
    taskFolderPath: Option[String] = None
  )(implicit sparkSession: SparkSession): List[io.github.datacatering.datacaterer.api.model.Step] = {
    val task = findTaskByName(taskName, taskFolderPath)
    task.steps
  }

  /**
   * Invalidate the task cache for a specific folder
   * Useful after task files are modified
   *
   * @param taskFolderPath Task folder path to invalidate (defaults to configured path)
   */
  def invalidateCache(taskFolderPath: Option[String] = None): Unit = {
    val effectiveTaskFolderPath = taskFolderPath.getOrElse(ConfigParser.foldersConfig.taskFolderPath)
    LOGGER.info(s"Invalidating task cache, folder=$effectiveTaskFolderPath")
    YamlResourceCache.invalidateTaskFolder(effectiveTaskFolderPath)
  }

  /**
   * Get cache statistics for monitoring
   */
  def getCacheStats: io.github.datacatering.datacaterer.core.ui.resource.YamlCacheStats = {
    YamlResourceCache.getCacheStats
  }
}
