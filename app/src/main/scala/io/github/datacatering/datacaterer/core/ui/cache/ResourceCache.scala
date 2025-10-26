package io.github.datacatering.datacaterer.core.ui.cache

/**
 * Generic cache interface for resources
 */
trait ResourceCache[K, V] {
  /**
   * Get value from cache if present and valid
   */
  def get(key: K): Option[V]

  /**
   * Get value from cache or load using provided loader function
   */
  def getOrLoad(key: K, loader: K => V): V

  /**
   * Invalidate a single cache entry
   */
  def invalidate(key: K): Unit

  /**
   * Invalidate all cache entries
   */
  def invalidateAll(): Unit

  /**
   * Current cache size
   */
  def size: Int

  /**
   * Cache statistics
   */
  def stats: CacheStats
}

/**
 * Cache statistics for monitoring
 */
case class CacheStats(
  hitCount: Long,
  missCount: Long,
  evictionCount: Long,
  totalLoadTime: Double,
  loadCount: Long
) {
  def hitRate: Double = {
    val total = hitCount + missCount
    if (total == 0) 0.0 else hitCount.toDouble / total
  }

  def averageLoadTime: Double = {
    if (loadCount == 0) 0.0 else totalLoadTime / loadCount
  }
}

/**
 * Thread-safe mutable cache statistics
 */
private[cache] class CacheStatsImpl {
  @volatile private var hits: Long = 0
  @volatile private var misses: Long = 0
  @volatile private var evictions: Long = 0
  @volatile private var totalLoadTimeMs: Double = 0.0
  @volatile private var loads: Long = 0

  def recordHit(): Unit = {
    hits += 1
  }

  def recordMiss(): Unit = {
    misses += 1
  }

  def recordEviction(): Unit = {
    evictions += 1
  }

  def recordLoad(loadTimeMs: Double): Unit = {
    totalLoadTimeMs += loadTimeMs
    loads += 1
  }

  def snapshot(): CacheStats = {
    CacheStats(hits, misses, evictions, totalLoadTimeMs, loads)
  }
}
