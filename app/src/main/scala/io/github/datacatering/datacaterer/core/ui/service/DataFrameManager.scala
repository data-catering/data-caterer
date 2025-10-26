package io.github.datacatering.datacaterer.core.ui.service

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Manages DataFrame lifecycle to prevent memory leaks
 *
 * This service addresses the memory leak issue identified in FastSampleGenerator
 * where DataFrames are cached with df.cache() but never unpersisted, leading to
 * memory accumulation over time and potential OOM errors.
 *
 * Features:
 * - Automatic tracking of cached DataFrames
 * - Explicit unpersist operations
 * - Bulk cleanup operations
 * - Memory usage monitoring
 * - Thread-safe operations
 *
 * Usage pattern:
 * {{{
 *   val df = generateData()
 *   val cachedDf = DataFrameManager.cacheDataFrame("my-key", df)
 *   // ... use cachedDf ...
 *   DataFrameManager.unpersist("my-key")
 * }}}
 */
object DataFrameManager {

  private val LOGGER = Logger.getLogger(getClass.getName)

  // Thread-safe tracking of cached DataFrames
  private val cachedDataFrames = new ConcurrentHashMap[String, DataFrame]()

  // Statistics
  @volatile private var totalCached: Long = 0
  @volatile private var totalUnpersisted: Long = 0

  /**
   * Cache a DataFrame and track it for later cleanup
   *
   * @param key Unique identifier for this DataFrame
   * @param dataFrame DataFrame to cache
   * @param storageLevel Storage level for caching (default: MEMORY_AND_DISK)
   * @return Cached DataFrame
   */
  def cacheDataFrame(
    key: String,
    dataFrame: DataFrame,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK
  ): DataFrame = {
    LOGGER.debug(s"Caching DataFrame, key=$key, storage-level=$storageLevel")

    // Unpersist previous DataFrame with same key if exists
    if (cachedDataFrames.containsKey(key)) {
      LOGGER.warn(s"DataFrame already cached with key=$key, unpersisting old version")
      unpersist(key)
    }

    val cached = dataFrame.persist(storageLevel)
    cachedDataFrames.put(key, cached)
    totalCached += 1

    LOGGER.debug(s"DataFrame cached successfully, key=$key, total-cached=${cachedDataFrames.size()}")
    cached
  }

  /**
   * Unpersist a specific DataFrame by key
   *
   * @param key Unique identifier of the DataFrame to unpersist
   * @param blocking Whether to wait for unpersist to complete (default: true)
   * @return true if DataFrame was found and unpersisted, false otherwise
   */
  def unpersist(key: String, blocking: Boolean = true): Boolean = {
    Option(cachedDataFrames.get(key)) match {
      case Some(df) =>
        LOGGER.debug(s"Unpersisting DataFrame, key=$key, blocking=$blocking")
        df.unpersist(blocking)
        cachedDataFrames.remove(key)
        totalUnpersisted += 1
        LOGGER.debug(s"DataFrame unpersisted successfully, key=$key, remaining-cached=${cachedDataFrames.size()}")
        true
      case None =>
        LOGGER.debug(s"DataFrame not found in cache, key=$key")
        false
    }
  }

  /**
   * Unpersist multiple DataFrames by keys
   *
   * @param keys List of DataFrame keys to unpersist
   * @param blocking Whether to wait for unpersist to complete
   * @return Number of DataFrames successfully unpersisted
   */
  def unpersistMultiple(keys: List[String], blocking: Boolean = true): Int = {
    LOGGER.debug(s"Unpersisting multiple DataFrames, count=${keys.size}")
    keys.count(key => unpersist(key, blocking))
  }

  /**
   * Unpersist all cached DataFrames
   *
   * @param blocking Whether to wait for unpersist to complete (default: true)
   * @return Number of DataFrames unpersisted
   */
  def unpersistAll(blocking: Boolean = true): Int = {
    LOGGER.info(s"Unpersisting all DataFrames, count=${cachedDataFrames.size()}")
    val keys = cachedDataFrames.keySet().asScala.toList
    val count = unpersistMultiple(keys, blocking)
    LOGGER.info(s"All DataFrames unpersisted, count=$count")
    count
  }

  /**
   * Get a cached DataFrame without modifying cache state
   *
   * @param key Unique identifier of the DataFrame
   * @return Optionally the cached DataFrame if it exists
   */
  def getCachedDataFrame(key: String): Option[DataFrame] = {
    Option(cachedDataFrames.get(key))
  }

  /**
   * Check if a DataFrame is currently cached
   *
   * @param key Unique identifier to check
   * @return true if DataFrame is cached, false otherwise
   */
  def isCached(key: String): Boolean = {
    cachedDataFrames.containsKey(key)
  }

  /**
   * Get the number of currently cached DataFrames
   *
   * @return Count of cached DataFrames
   */
  def getCachedCount: Int = {
    cachedDataFrames.size()
  }

  /**
   * Get all currently cached DataFrame keys
   *
   * @return List of all cached DataFrame keys
   */
  def getCachedKeys: List[String] = {
    cachedDataFrames.keySet().asScala.toList
  }

  /**
   * Get statistics about DataFrame caching
   *
   * @return DataFrameCacheStats with current statistics
   */
  def getStats: DataFrameCacheStats = {
    DataFrameCacheStats(
      currentCachedCount = cachedDataFrames.size(),
      totalCached = totalCached,
      totalUnpersisted = totalUnpersisted,
      cachedKeys = getCachedKeys
    )
  }

  /**
   * Clear all tracking without unpersisting
   * WARNING: Use only if DataFrames have been unpersisted externally
   */
  def clearTracking(): Unit = {
    LOGGER.warn("Clearing DataFrame tracking without unpersisting - ensure DataFrames are already unpersisted")
    cachedDataFrames.clear()
  }

  /**
   * Execute a block of code with automatic DataFrame cleanup
   *
   * @param key Unique identifier for cached DataFrames in this block
   * @param block Code block that may cache DataFrames
   * @tparam T Return type of the block
   * @return Result of the block execution
   */
  def withAutoCleanup[T](key: String)(block: => T): T = {
    try {
      block
    } finally {
      unpersist(key, blocking = false)
    }
  }

  /**
   * Execute a block with multiple DataFrames and automatic cleanup
   *
   * @param keys List of keys to clean up after block execution
   * @param block Code block that may cache DataFrames
   * @tparam T Return type of the block
   * @return Result of the block execution
   */
  def withAutoCleanupMultiple[T](keys: List[String])(block: => T): T = {
    try {
      block
    } finally {
      unpersistMultiple(keys, blocking = false)
    }
  }
}

/**
 * Statistics about DataFrame caching
 */
case class DataFrameCacheStats(
  currentCachedCount: Int,
  totalCached: Long,
  totalUnpersisted: Long,
  cachedKeys: List[String]
) {
  def activeLeaks: Long = totalCached - totalUnpersisted - currentCachedCount

  override def toString: String = {
    s"""DataFrame Cache Statistics:
       |  Currently Cached: $currentCachedCount
       |  Total Cached: $totalCached
       |  Total Unpersisted: $totalUnpersisted
       |  Active Leaks: $activeLeaks
       |  Cached Keys: ${cachedKeys.mkString(", ")}
       |""".stripMargin
  }
}
