package io.github.datacatering.datacaterer.core.ui.cache

import org.apache.log4j.Logger

import java.io.File
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Thread-safe file-backed cache with modification time tracking
 * Automatically invalidates entries when source files are modified
 *
 * @param maxSize Maximum number of entries to cache
 * @param ttlSeconds Time-to-live in seconds for cache entries
 * @param checkModificationTime Whether to check file modification times for invalidation
 */
class FileBackedCache[V](
  maxSize: Int = 1000,
  ttlSeconds: Long = 300,
  checkModificationTime: Boolean = true
) extends ResourceCache[String, V] {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private case class CacheEntry(
    value: V,
    loadedAt: Long,
    fileModifiedAt: Long
  )

  private val cache = new ConcurrentHashMap[String, CacheEntry]()
  private val statsImpl = new CacheStatsImpl()

  override def get(key: String): Option[V] = {
    Option(cache.get(key)).flatMap { entry =>
      if (isValid(key, entry)) {
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

  override def getOrLoad(key: String, loader: String => V): V = {
    get(key).getOrElse {
      val startTime = System.nanoTime()

      try {
        val value = loader(key)
        val loadTime = (System.nanoTime() - startTime) / 1000000.0

        val fileModTime = if (checkModificationTime) {
          val file = new File(key)
          if (file.exists()) file.lastModified() else 0L
        } else {
          0L
        }

        val entry = CacheEntry(value, System.currentTimeMillis(), fileModTime)

        // Evict if at capacity
        if (cache.size() >= maxSize) {
          evictOldest()
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

  override def invalidate(key: String): Unit = {
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

  private def isValid(key: String, entry: CacheEntry): Boolean = {
    val now = System.currentTimeMillis()
    val age = now - entry.loadedAt

    // Check TTL
    if (age > ttlSeconds * 1000) {
      LOGGER.debug(s"Cache entry expired due to TTL, key=$key, age=${age}ms, ttl=${ttlSeconds * 1000}ms")
      return false
    }

    // Check file modification time
    if (checkModificationTime) {
      val file = new File(key)
      if (!file.exists()) {
        LOGGER.debug(s"Cache entry invalidated, file no longer exists, key=$key")
        return false
      }
      val currentModTime = file.lastModified()
      if (currentModTime != entry.fileModifiedAt) {
        LOGGER.debug(s"Cache entry invalidated due to file modification, key=$key, " +
          s"cached-mod-time=${entry.fileModifiedAt}, current-mod-time=$currentModTime")
        return false
      }
    }

    true
  }

  private def evictOldest(): Unit = {
    val entries = cache.entrySet().asScala.toSeq
    val oldest = if (entries.nonEmpty) Some(entries.minBy(_.getValue.loadedAt)) else None

    oldest.foreach { entry =>
      cache.remove(entry.getKey)
      statsImpl.recordEviction()
      LOGGER.debug(s"Evicted oldest cache entry, key=${entry.getKey}, cache-size=${cache.size()}")
    }
  }
}
