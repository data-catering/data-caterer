package io.github.datacatering.datacaterer.core.ui.http

import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, EntityTag, `Cache-Control`, `ETag`, `Last-Modified`}
import org.apache.pekko.http.scaladsl.model.{DateTime, HttpHeader, HttpResponse, StatusCodes}

import java.security.MessageDigest
import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}

/**
 * Utilities for HTTP caching headers for Pekko HTTP
 *
 * Supports:
 * - ETag generation from content or cache keys
 * - Cache-Control directives for different resource types
 * - Last-Modified timestamp tracking
 * - 304 Not Modified responses
 */
object PekkoCacheHeaders {

  /**
   * Generate an ETag from string content using MD5 hash
   */
  def generateETag(content: String): `ETag` = {
    val md5 = MessageDigest.getInstance("MD5")
    val hash = md5.digest(content.getBytes("UTF-8"))
    val hexString = hash.map("%02x".format(_)).mkString
    `ETag`(EntityTag(hexString))
  }

  /**
   * Generate an ETag from multiple components
   */
  def generateETag(components: String*): `ETag` = {
    generateETag(components.mkString(":"))
  }

  /**
   * Create Cache-Control header for static resources (long-lived)
   * Example: Cache-Control: public, max-age=31536000
   */
  def staticResourceCacheControl(maxAge: FiniteDuration = 365.days): `Cache-Control` = {
    `Cache-Control`(
      CacheDirectives.public,
      CacheDirectives.`max-age`(maxAge.toSeconds)
    )
  }

  /**
   * Create Cache-Control header for dynamic resources (short-lived, revalidate)
   * Example: Cache-Control: public, max-age=300, must-revalidate
   */
  def dynamicResourceCacheControl(maxAge: FiniteDuration = 5.minutes): `Cache-Control` = {
    `Cache-Control`(
      CacheDirectives.public,
      CacheDirectives.`max-age`(maxAge.toSeconds),
      CacheDirectives.`must-revalidate`
    )
  }

  /**
   * Create Cache-Control header for no caching
   * Example: Cache-Control: no-cache, no-store, must-revalidate
   */
  def noCacheControl: `Cache-Control` = {
    `Cache-Control`(
      CacheDirectives.`no-cache`,
      CacheDirectives.`no-store`,
      CacheDirectives.`must-revalidate`
    )
  }

  /**
   * Create Last-Modified header from Instant
   */
  def lastModified(instant: Instant): `Last-Modified` = {
    `Last-Modified`(DateTime(instant.toEpochMilli))
  }

  /**
   * Create Last-Modified header from timestamp in milliseconds
   */
  def lastModified(timestampMillis: Long): `Last-Modified` = {
    `Last-Modified`(DateTime(timestampMillis))
  }

  /**
   * Create Last-Modified header for current time
   */
  def lastModifiedNow: `Last-Modified` = {
    `Last-Modified`(DateTime.now)
  }

  /**
   * Add caching headers to a response
   */
  def withCacheHeaders(
    response: HttpResponse,
    etag: Option[`ETag`] = None,
    cacheControl: Option[`Cache-Control`] = None,
    lastModified: Option[`Last-Modified`] = None
  ): HttpResponse = {
    val headers = Seq(
      etag,
      cacheControl,
      lastModified
    ).flatten

    response.withHeaders(response.headers ++ headers)
  }

  /**
   * Add standard caching for YAML resources
   * Uses 5-minute cache with ETag and Last-Modified
   */
  def withYamlResourceCache(
    response: HttpResponse,
    resourceKey: String,
    lastModifiedTime: Long
  ): HttpResponse = {
    withCacheHeaders(
      response,
      etag = Some(generateETag(resourceKey, lastModifiedTime.toString)),
      cacheControl = Some(dynamicResourceCacheControl()),
      lastModified = Some(lastModified(lastModifiedTime))
    )
  }

  /**
   * Add standard caching for connection resources
   * Uses 10-minute cache with ETag
   */
  def withConnectionCache(
    response: HttpResponse,
    connectionName: String,
    version: String = "1"
  ): HttpResponse = {
    withCacheHeaders(
      response,
      etag = Some(generateETag(connectionName, version)),
      cacheControl = Some(dynamicResourceCacheControl(10.minutes))
    )
  }

  /**
   * Add no-cache headers for dynamic/real-time data
   */
  def withNoCache(response: HttpResponse): HttpResponse = {
    withCacheHeaders(
      response,
      cacheControl = Some(noCacheControl)
    )
  }

  /**
   * Helper to create 304 Not Modified response
   */
  def notModifiedResponse(
    etag: Option[`ETag`] = None,
    cacheControl: Option[`Cache-Control`] = None,
    lastModified: Option[`Last-Modified`] = None
  ): HttpResponse = {
    val response = HttpResponse(status = StatusCodes.NotModified)
    withCacheHeaders(response, etag, cacheControl, lastModified)
  }

  /**
   * Extract If-None-Match header from request headers
   */
  def getIfNoneMatch(headers: Seq[HttpHeader]): Option[String] = {
    headers.collectFirst {
      case h if h.is("if-none-match") => h.value()
    }
  }

  /**
   * Extract If-Modified-Since header from request headers
   */
  def getIfModifiedSince(headers: Seq[HttpHeader]): Option[DateTime] = {
    headers.collectFirst {
      case h if h.is("if-modified-since") =>
        try {
          DateTime.fromIsoDateTimeString(h.value()).getOrElse(DateTime.now)
        } catch {
          case _: Exception => DateTime.now
        }
    }
  }

  /**
   * Check if ETag matches If-None-Match header
   */
  def matchesETag(headers: Seq[HttpHeader], etag: `ETag`): Boolean = {
    getIfNoneMatch(headers).exists { ifNoneMatch =>
      ifNoneMatch == etag.etag.tag || ifNoneMatch == s""""${etag.etag.tag}""""
    }
  }

  /**
   * Check if resource is not modified since If-Modified-Since header
   */
  def notModifiedSince(headers: Seq[HttpHeader], lastModified: `Last-Modified`): Boolean = {
    getIfModifiedSince(headers).exists { ifModifiedSince =>
      lastModified.date <= ifModifiedSince
    }
  }
}
