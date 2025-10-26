package io.github.datacatering.datacaterer.core.ui.http

import org.apache.log4j.Logger

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.concurrent.duration.FiniteDuration

/**
 * Simple token bucket rate limiter
 *
 * Features:
 * - Token bucket algorithm for smooth rate limiting
 * - Per-client rate limiting based on identifier (IP, API key, etc.)
 * - Configurable rate and burst capacity
 * - Thread-safe concurrent access
 * - Automatic token refill
 */
class RateLimiter(
  tokensPerSecond: Double = 10.0,
  bucketCapacity: Int = 20,
  refillInterval: FiniteDuration = FiniteDuration(100, TimeUnit.MILLISECONDS)
) {

  private val LOGGER = Logger.getLogger(getClass.getName)

  private case class TokenBucket(
    var tokens: Double,
    var lastRefillTime: Long
  )

  private val buckets = new ConcurrentHashMap[String, TokenBucket]()

  /**
   * Check if a request is allowed for the given client
   * @param clientId Unique identifier for the client (IP address, API key, etc.)
   * @param tokensRequired Number of tokens required (default 1)
   * @return true if request is allowed, false if rate limit exceeded
   */
  def allowRequest(clientId: String, tokensRequired: Double = 1.0): Boolean = {
    val bucket = buckets.computeIfAbsent(clientId, _ => {
      LOGGER.debug(s"Creating new rate limit bucket for client: $clientId")
      TokenBucket(bucketCapacity.toDouble, System.currentTimeMillis())
    })

    synchronized {
      refillBucket(bucket)

      if (bucket.tokens >= tokensRequired) {
        bucket.tokens -= tokensRequired
        LOGGER.debug(s"Request allowed for $clientId, tokens remaining: ${bucket.tokens}")
        true
      } else {
        LOGGER.debug(s"Request denied for $clientId, insufficient tokens: ${bucket.tokens}")
        false
      }
    }
  }

  /**
   * Refill tokens based on time elapsed since last refill
   */
  private def refillBucket(bucket: TokenBucket): Unit = {
    val now = System.currentTimeMillis()
    val timeSinceLastRefill = now - bucket.lastRefillTime
    val intervalsElapsed = timeSinceLastRefill.toDouble / refillInterval.toMillis

    if (intervalsElapsed >= 1.0) {
      val tokensToAdd = intervalsElapsed * tokensPerSecond * (refillInterval.toMillis / 1000.0)
      bucket.tokens = math.min(bucketCapacity.toDouble, bucket.tokens + tokensToAdd)
      bucket.lastRefillTime = now
      LOGGER.trace(s"Refilled bucket: added $tokensToAdd tokens, current: ${bucket.tokens}")
    }
  }

  /**
   * Get current token count for a client (for monitoring)
   */
  def getTokenCount(clientId: String): Double = {
    Option(buckets.get(clientId)) match {
      case Some(bucket) =>
        synchronized {
          refillBucket(bucket)
          bucket.tokens
        }
      case None => bucketCapacity.toDouble
    }
  }

  /**
   * Reset rate limit for a specific client
   */
  def reset(clientId: String): Unit = {
    buckets.remove(clientId)
    LOGGER.info(s"Rate limit reset for client: $clientId")
  }

  /**
   * Reset all rate limits
   */
  def resetAll(): Unit = {
    buckets.clear()
    LOGGER.info("All rate limits reset")
  }

  /**
   * Get statistics for monitoring
   */
  def getStats: RateLimiterStats = {
    val clientCount = buckets.size()
    val avgTokens = if (clientCount > 0) {
      import scala.jdk.CollectionConverters._
      buckets.values().asScala.map(_.tokens).sum / clientCount
    } else {
      0.0
    }

    RateLimiterStats(
      activeClients = clientCount,
      averageTokens = avgTokens,
      tokensPerSecond = tokensPerSecond,
      bucketCapacity = bucketCapacity
    )
  }

  /**
   * Clean up old buckets that haven't been used recently
   * Call periodically to prevent memory leaks
   */
  def cleanup(inactiveThresholdMs: Long = 3600000): Unit = {
    import scala.jdk.CollectionConverters._
    val now = System.currentTimeMillis()
    val toRemove = buckets.entrySet().asScala.filter { entry =>
      val bucket = entry.getValue
      (now - bucket.lastRefillTime) > inactiveThresholdMs
    }.map(_.getKey).toList

    toRemove.foreach { key =>
      buckets.remove(key)
      LOGGER.debug(s"Removed inactive rate limit bucket: $key")
    }

    if (toRemove.nonEmpty) {
      LOGGER.info(s"Cleaned up ${toRemove.size} inactive rate limit buckets")
    }
  }
}

/**
 * Rate limiter statistics
 */
case class RateLimiterStats(
  activeClients: Int,
  averageTokens: Double,
  tokensPerSecond: Double,
  bucketCapacity: Int
) {
  override def toString: String = {
    f"""Rate Limiter Stats:
       |  Active Clients: $activeClients
       |  Average Tokens: $averageTokens%.2f
       |  Rate: $tokensPerSecond%.1f tokens/sec
       |  Capacity: $bucketCapacity tokens
       |""".stripMargin
  }
}

/**
 * Global rate limiter instance for the application
 */
object GlobalRateLimiter {
  private val LOGGER = Logger.getLogger(getClass.getName)

  // Default: 100 requests per second per client, burst up to 200
  private val defaultLimiter = new RateLimiter(
    tokensPerSecond = 100.0,
    bucketCapacity = 200
  )

  // Strict limiter for expensive operations
  private val strictLimiter = new RateLimiter(
    tokensPerSecond = 10.0,
    bucketCapacity = 20
  )

  /**
   * Get the default rate limiter
   */
  def default: RateLimiter = defaultLimiter

  /**
   * Get the strict rate limiter for expensive operations
   */
  def strict: RateLimiter = strictLimiter

  /**
   * Start background cleanup task
   */
  def startCleanupTask(): Unit = {
    val cleanupThread = new Thread(() => {
      while (true) {
        try {
          Thread.sleep(600000) // 10 minutes
          LOGGER.debug("Running rate limiter cleanup task")
          defaultLimiter.cleanup()
          strictLimiter.cleanup()
        } catch {
          case _: InterruptedException =>
            LOGGER.info("Rate limiter cleanup task interrupted")
            return
          case ex: Exception =>
            LOGGER.error("Error in rate limiter cleanup task", ex)
        }
      }
    })
    cleanupThread.setDaemon(true)
    cleanupThread.setName("RateLimiter-Cleanup")
    cleanupThread.start()
    LOGGER.info("Rate limiter cleanup task started")
  }
}

/**
 * Helper methods for extracting client identifiers from Pekko HTTP requests
 */
object ClientIdentifier {

  import org.apache.pekko.http.scaladsl.model.HttpHeader

  /**
   * Extract client IP address from headers
   * Checks X-Forwarded-For, X-Real-IP, and remote address
   */
  def fromHeaders(headers: Seq[HttpHeader], remoteAddr: Option[String] = None): String = {
    // Check X-Forwarded-For header (proxy/load balancer)
    headers.find(_.is("x-forwarded-for"))
      .map(_.value().split(",").head.trim)
      .orElse {
        // Check X-Real-IP header
        headers.find(_.is("x-real-ip")).map(_.value())
      }
      .orElse(remoteAddr)
      .getOrElse("unknown")
  }

  /**
   * Extract API key from Authorization header
   */
  def fromApiKey(headers: Seq[HttpHeader]): Option[String] = {
    headers.find(_.is("authorization"))
      .map(_.value())
      .filter(_.startsWith("Bearer "))
      .map(_.substring(7))
  }

  /**
   * Use API key if present, otherwise fall back to IP address
   */
  def fromRequest(headers: Seq[HttpHeader], remoteAddr: Option[String] = None): String = {
    fromApiKey(headers).getOrElse(fromHeaders(headers, remoteAddr))
  }
}
