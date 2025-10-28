package io.github.datacatering.datacaterer.core.ui.cache

import org.apache.log4j.Logger

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Thread-safe in-memory cache with TTL and LRU eviction
 *
 * @param maxSize Maximum number of entries to cache
 * @param ttlSeconds Time-to-live in seconds for cache entries
 */
class InMemoryCache[K, V](
  maxSize: Int = 1000,
  ttlSeconds: Long = 300
) extends ResourceCache[K, V] {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private case class CacheEntry(
    value: V,
    loadedAt: Long,
    lastAccessedAt: Long
  )

  private val cache = new ConcurrentHashMap[K, CacheEntry]()
  private val statsImpl = new CacheStatsImpl()

  override def get(key: K): Option[V] = {
    Option(cache.get(key)).flatMap { entry =>
      if (isValid(entry)) {
        // Update last accessed time for LRU
        val updated = entry.copy(lastAccessedAt = System.currentTimeMillis())
        cache.put(key, updated)
        statsImpl.recordHit()
        Some(entry.value)
      } else {
        cache.remove(key)
        statsImpl.recordMiss()
        None
      }
    }.orElse {
      statsImpl.recordMiss()
      None
    }
  }

  override def getOrLoad(key: K, loader: K => V): V = {
    get(key).getOrElse {
      val startTime = System.nanoTime()

      try {
        val value = loader(key)
        val loadTime = (System.nanoTime() - startTime) / 1000000.0

        val now = System.currentTimeMillis()
        val entry = CacheEntry(value, now, now)

        // Evict if at capacity
        if (cache.size() >= maxSize) {
          evictLRU()
        }

        cache.put(key, entry)
        statsImpl.recordLoad(loadTime)

        LOGGER.debug(s"Loaded and cached resource, key=$key, load-time=${loadTime}ms, cache-size=${cache.size()}")
        value
      } catch {
        case ex: Exception =>
          val loadTime = (System.nanoTime() - startTime) / 1000000.0
          LOGGER.error(s"Failed to load resource, key=$key, load-time=${loadTime}ms", ex)
          throw ex
      }
    }
  }

  override def invalidate(key: K): Unit = {
    cache.remove(key)
    LOGGER.debug(s"Invalidated cache entry, key=$key")
  }

  override def invalidateAll(): Unit = {
    val size = cache.size()
    cache.clear()
    LOGGER.info(s"Invalidated all cache entries, count=$size")
  }

  override def size: Int = cache.size()

  override def stats: CacheStats = statsImpl.snapshot()

  private def isValid(entry: CacheEntry): Boolean = {
    val age = System.currentTimeMillis() - entry.loadedAt
    val isValid = age <= ttlSeconds * 1000

    if (!isValid) {
      LOGGER.debug(s"Cache entry expired due to TTL, age=${age}ms, ttl=${ttlSeconds * 1000}ms")
    }

    isValid
  }

  private def evictLRU(): Unit = {
    val entries = cache.entrySet().asScala.toSeq
    val lru = if (entries.nonEmpty) Some(entries.minBy(_.getValue.lastAccessedAt)) else None

    lru.foreach { entry =>
      cache.remove(entry.getKey)
      statsImpl.recordEviction()
      LOGGER.debug(s"Evicted LRU cache entry, key=${entry.getKey}, cache-size=${cache.size()}")
    }
  }
}
