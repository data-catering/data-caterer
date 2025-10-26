package io.github.datacatering.datacaterer.core.ui.resource

import io.github.datacatering.datacaterer.api.model.{Plan, Task}
import io.github.datacatering.datacaterer.core.parser.PlanParser
import io.github.datacatering.datacaterer.core.ui.cache.{CacheStats, FileBackedCache}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Centralized YAML resource caching
 * Provides cached access to parsed YAML plans and tasks with automatic invalidation
 */
object YamlResourceCache {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Cache for parsed YAML plans
  private val planCache = new FileBackedCache[Plan](
    maxSize = 500,
    ttlSeconds = 300,
    checkModificationTime = true
  )

  // Cache for parsed YAML tasks
  private val taskCache = new FileBackedCache[Task](
    maxSize = 1000,
    ttlSeconds = 300,
    checkModificationTime = true
  )

  // Cache for task folder contents (to avoid repeated directory scans)
  // Uses very short TTL (2 seconds) to ensure tests see newly created files
  // and dynamic environments get fresh data quickly
  private val taskFolderCache = new FileBackedCache[Array[Task]](
    maxSize = 10,
    ttlSeconds = 2,
    checkModificationTime = false  // Folder modification time doesn't reflect content changes
  )

  /**
   * Load plan from file with caching
   * Automatically caches the parsed plan and invalidates if file is modified
   *
   * @param planFilePath Absolute path to plan YAML file
   * @param sparkSession Implicit SparkSession for parsing
   * @return Parsed Plan object
   */
  def getPlan(planFilePath: String)(implicit sparkSession: SparkSession): Plan = {
    planCache.getOrLoad(planFilePath, { path =>
      LOGGER.debug(s"Cache miss for plan, loading from file: $path")
      PlanParser.parsePlan(path)
    })
  }

  /**
   * Load task from file with caching
   * Automatically caches the parsed task and invalidates if file is modified
   *
   * @param taskFilePath Absolute path to task YAML file
   * @param sparkSession Implicit SparkSession for parsing
   * @return Parsed Task object
   */
  def getTask(taskFilePath: String)(implicit sparkSession: SparkSession): Task = {
    taskCache.getOrLoad(taskFilePath, { path =>
      LOGGER.debug(s"Cache miss for task, loading from file: $path")
      PlanParser.parseTaskFile(path)
    })
  }

  /**
   * Load all tasks from folder with caching
   * Caches the entire folder scan result to avoid repeated directory traversals
   *
   * @param taskFolderPath Path to folder containing task YAML files
   * @param sparkSession Implicit SparkSession for parsing
   * @return Array of parsed Task objects
   */
  def getTasksFromFolder(taskFolderPath: String)(implicit sparkSession: SparkSession): Array[Task] = {
    taskFolderCache.getOrLoad(taskFolderPath, { folderPath =>
      LOGGER.debug(s"Cache miss for task folder, scanning: $folderPath")
      PlanParser.parseTasksFromFolder(folderPath)
    })
  }

  /**
   * Find task by name with caching
   * Uses cached folder scan results to find the task
   *
   * @param taskName Name of the task to find
   * @param taskFolderPath Path to folder containing task YAML files
   * @param sparkSession Implicit SparkSession for parsing
   * @return Optional Task if found
   */
  def findTaskByName(taskName: String, taskFolderPath: String)(implicit sparkSession: SparkSession): Option[Task] = {
    getTasksFromFolder(taskFolderPath).find(_.name == taskName)
  }

  /**
   * Invalidate plan cache entry
   * Should be called when a plan file is modified externally
   *
   * @param planFilePath Path to plan file to invalidate
   */
  def invalidatePlan(planFilePath: String): Unit = {
    planCache.invalidate(planFilePath)
    LOGGER.info(s"Invalidated plan cache entry: $planFilePath")
  }

  /**
   * Invalidate task cache entry
   * Should be called when a task file is modified externally
   *
   * @param taskFilePath Path to task file to invalidate
   */
  def invalidateTask(taskFilePath: String): Unit = {
    taskCache.invalidate(taskFilePath)
    LOGGER.info(s"Invalidated task cache entry: $taskFilePath")
  }

  /**
   * Invalidate task folder cache
   * Should be called when task files are added/removed from folder
   *
   * @param taskFolderPath Path to task folder to invalidate
   */
  def invalidateTaskFolder(taskFolderPath: String): Unit = {
    taskFolderCache.invalidate(taskFolderPath)
    LOGGER.info(s"Invalidated task folder cache: $taskFolderPath")
  }

  /**
   * Invalidate all caches
   * Should be called for full cache reset (e.g., on configuration reload)
   */
  def invalidateAll(): Unit = {
    planCache.invalidateAll()
    taskCache.invalidateAll()
    taskFolderCache.invalidateAll()
    LOGGER.info("Invalidated all YAML caches")
  }

  /**
   * Get cache statistics for monitoring
   * Useful for tracking cache performance and hit rates
   *
   * @return YamlCacheStats with statistics for all caches
   */
  def getCacheStats: YamlCacheStats = {
    val stats = YamlCacheStats(
      planCache = planCache.stats,
      taskCache = taskCache.stats,
      taskFolderCache = taskFolderCache.stats
    )
    LOGGER.debug(s"Cache statistics:\n$stats")
    stats
  }

  /**
   * Log cache statistics at INFO level
   * Useful for periodic monitoring and debugging
   */
  def logCacheStats(): Unit = {
    val stats = getCacheStats
    LOGGER.info(s"YAML Cache Statistics:\n$stats")
  }
}

/**
 * Combined cache statistics for all YAML caches
 *
 * @param planCache Statistics for plan cache
 * @param taskCache Statistics for task cache
 * @param taskFolderCache Statistics for task folder cache
 */
case class YamlCacheStats(
  planCache: CacheStats,
  taskCache: CacheStats,
  taskFolderCache: CacheStats
) {
  override def toString: String = {
    val planHitRate = "%.2f".format(planCache.hitRate * 100)
    val planAvgLoadTime = "%.2f".format(planCache.averageLoadTime)
    val taskHitRate = "%.2f".format(taskCache.hitRate * 100)
    val taskAvgLoadTime = "%.2f".format(taskCache.averageLoadTime)
    val folderHitRate = "%.2f".format(taskFolderCache.hitRate * 100)
    val folderAvgLoadTime = "%.2f".format(taskFolderCache.averageLoadTime)

    s"""YAML Cache Statistics:
       |  Plan Cache:
       |    - Hits: ${planCache.hitCount}, Misses: ${planCache.missCount}
       |    - Hit Rate: $planHitRate%
       |    - Avg Load Time: ${planAvgLoadTime}ms
       |    - Size: ${planCache.loadCount}, Evictions: ${planCache.evictionCount}
       |  Task Cache:
       |    - Hits: ${taskCache.hitCount}, Misses: ${taskCache.missCount}
       |    - Hit Rate: $taskHitRate%
       |    - Avg Load Time: ${taskAvgLoadTime}ms
       |    - Size: ${taskCache.loadCount}, Evictions: ${taskCache.evictionCount}
       |  Task Folder Cache:
       |    - Hits: ${taskFolderCache.hitCount}, Misses: ${taskFolderCache.missCount}
       |    - Hit Rate: $folderHitRate%
       |    - Avg Load Time: ${folderAvgLoadTime}ms
       |    - Size: ${taskFolderCache.loadCount}, Evictions: ${taskFolderCache.evictionCount}
       |""".stripMargin
  }
}
