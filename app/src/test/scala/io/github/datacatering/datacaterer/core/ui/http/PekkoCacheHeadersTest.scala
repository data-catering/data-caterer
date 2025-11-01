package io.github.datacatering.datacaterer.core.ui.http

import org.apache.pekko.http.scaladsl.model.headers.{CacheDirectives, RawHeader, `Cache-Control`, `Last-Modified`}
import org.apache.pekko.http.scaladsl.model.{DateTime, HttpHeader, HttpResponse, StatusCodes}
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class PekkoCacheHeadersTest extends AnyFunSuiteLike with Matchers {

  test("generateETag should create consistent ETag from content") {
    val content = "test-content"
    val etag1 = PekkoCacheHeaders.generateETag(content)
    val etag2 = PekkoCacheHeaders.generateETag(content)

    etag1 shouldEqual etag2
    etag1.etag.tag should not be empty
  }

  test("generateETag should create different ETags for different content") {
    val etag1 = PekkoCacheHeaders.generateETag("content1")
    val etag2 = PekkoCacheHeaders.generateETag("content2")

    etag1.etag.tag should not equal etag2.etag.tag
  }

  test("generateETag should handle multiple components") {
    val etag = PekkoCacheHeaders.generateETag("resource-name", "version-1", "12345")
    etag.etag.tag should not be empty

    // Should be same as concatenated
    val etag2 = PekkoCacheHeaders.generateETag("resource-name:version-1:12345")
    etag.etag.tag shouldEqual etag2.etag.tag
  }

  test("staticResourceCacheControl should include public and max-age") {
    val cacheControl = PekkoCacheHeaders.staticResourceCacheControl()

    cacheControl.directives should contain(CacheDirectives.public)
    cacheControl.directives.exists(_.toString.contains("max-age")) shouldBe true
  }

  test("staticResourceCacheControl should use custom max-age") {
    val cacheControl = PekkoCacheHeaders.staticResourceCacheControl(maxAge = 1.day)

    val maxAgeDirective = cacheControl.directives.find(_.toString.contains("max-age"))
    maxAgeDirective shouldBe defined
    maxAgeDirective.get.toString should include("86400") // 1 day in seconds
  }

  test("dynamicResourceCacheControl should include public, max-age, and must-revalidate") {
    val cacheControl = PekkoCacheHeaders.dynamicResourceCacheControl()

    cacheControl.directives should contain(CacheDirectives.public)
    cacheControl.directives should contain(CacheDirectives.`must-revalidate`)
    cacheControl.directives.exists(_.toString.contains("max-age")) shouldBe true
  }

  test("dynamicResourceCacheControl should use custom max-age") {
    val cacheControl = PekkoCacheHeaders.dynamicResourceCacheControl(maxAge = 10.minutes)

    val maxAgeDirective = cacheControl.directives.find(_.toString.contains("max-age"))
    maxAgeDirective shouldBe defined
    maxAgeDirective.get.toString should include("600") // 10 minutes in seconds
  }

  test("noCacheControl should include no-cache, no-store, must-revalidate") {
    val cacheControl = PekkoCacheHeaders.noCacheControl

    cacheControl.directives should contain(CacheDirectives.`no-cache`)
    cacheControl.directives should contain(CacheDirectives.`no-store`)
    cacheControl.directives should contain(CacheDirectives.`must-revalidate`)
  }

  test("lastModified should create Last-Modified header from timestamp") {
    val timestamp = System.currentTimeMillis()
    val lastMod = PekkoCacheHeaders.lastModified(timestamp)

    lastMod shouldBe a[`Last-Modified`]
    // DateTime may truncate to seconds, so check within 1 second
    math.abs(lastMod.date.clicks - timestamp) should be < 1000L
  }

  test("lastModifiedNow should create Last-Modified header for current time") {
    val before = System.currentTimeMillis()
    val lastMod = PekkoCacheHeaders.lastModifiedNow
    val after = System.currentTimeMillis()

    // Should be within a reasonable time window (DateTime may truncate, so allow 2 seconds)
    math.abs(lastMod.date.clicks - before) should be < 2000L
    math.abs(lastMod.date.clicks - after) should be < 2000L
  }

  test("withCacheHeaders should add ETag to response") {
    val response = HttpResponse(status = StatusCodes.OK)
    val etag = PekkoCacheHeaders.generateETag("test")

    val withHeaders = PekkoCacheHeaders.withCacheHeaders(response, etag = Some(etag))

    withHeaders.headers should contain(etag)
  }

  test("withCacheHeaders should add Cache-Control to response") {
    val response = HttpResponse(status = StatusCodes.OK)
    val cacheControl = PekkoCacheHeaders.dynamicResourceCacheControl()

    val withHeaders = PekkoCacheHeaders.withCacheHeaders(response, cacheControl = Some(cacheControl))

    withHeaders.headers should contain(cacheControl)
  }

  test("withCacheHeaders should add Last-Modified to response") {
    val response = HttpResponse(status = StatusCodes.OK)
    val lastMod = PekkoCacheHeaders.lastModifiedNow

    val withHeaders = PekkoCacheHeaders.withCacheHeaders(response, lastModified = Some(lastMod))

    withHeaders.headers should contain(lastMod)
  }

  test("withCacheHeaders should add multiple headers at once") {
    val response = HttpResponse(status = StatusCodes.OK)
    val etag = PekkoCacheHeaders.generateETag("test")
    val cacheControl = PekkoCacheHeaders.dynamicResourceCacheControl()
    val lastMod = PekkoCacheHeaders.lastModifiedNow

    val withHeaders = PekkoCacheHeaders.withCacheHeaders(
      response,
      etag = Some(etag),
      cacheControl = Some(cacheControl),
      lastModified = Some(lastMod)
    )

    withHeaders.headers should contain allOf (etag, cacheControl, lastMod)
  }

  test("withYamlResourceCache should add appropriate headers") {
    val response = HttpResponse(status = StatusCodes.OK)
    val timestamp = System.currentTimeMillis()

    val withCache = PekkoCacheHeaders.withYamlResourceCache(response, "test-resource", timestamp)

    withCache.headers.exists(_.is("etag")) shouldBe true
    withCache.headers.exists(_.is("cache-control")) shouldBe true
    withCache.headers.exists(_.is("last-modified")) shouldBe true
  }

  test("withConnectionCache should add appropriate headers") {
    val response = HttpResponse(status = StatusCodes.OK)

    val withCache = PekkoCacheHeaders.withConnectionCache(response, "my-connection", "v1")

    withCache.headers.exists(_.is("etag")) shouldBe true
    withCache.headers.exists(_.is("cache-control")) shouldBe true
  }

  test("withNoCache should add no-cache headers") {
    val response = HttpResponse(status = StatusCodes.OK)

    val withCache = PekkoCacheHeaders.withNoCache(response)

    val cacheControl = withCache.headers.collectFirst {
      case cc: `Cache-Control` => cc
    }
    cacheControl shouldBe defined
    cacheControl.get.directives should contain(CacheDirectives.`no-cache`)
  }

  test("notModifiedResponse should create 304 response") {
    val response = PekkoCacheHeaders.notModifiedResponse()

    response.status shouldBe StatusCodes.NotModified
  }

  test("notModifiedResponse should include ETag if provided") {
    val etag = PekkoCacheHeaders.generateETag("test")
    val response = PekkoCacheHeaders.notModifiedResponse(etag = Some(etag))

    response.headers should contain(etag)
  }

  test("getIfNoneMatch should extract If-None-Match header") {
    val headers = Seq(RawHeader("If-None-Match", "test-etag-value"))

    val ifNoneMatch = PekkoCacheHeaders.getIfNoneMatch(headers)

    ifNoneMatch shouldBe Some("test-etag-value")
  }

  test("getIfNoneMatch should return None when header is absent") {
    val headers = Seq.empty[HttpHeader]

    val ifNoneMatch = PekkoCacheHeaders.getIfNoneMatch(headers)

    ifNoneMatch shouldBe None
  }

  test("matchesETag should return true when ETags match") {
    val etag = PekkoCacheHeaders.generateETag("test")
    val headers = Seq(RawHeader("If-None-Match", etag.etag.tag))

    PekkoCacheHeaders.matchesETag(headers, etag) shouldBe true
  }

  test("matchesETag should return true when ETags match with quotes") {
    val etag = PekkoCacheHeaders.generateETag("test")
    val headers = Seq(RawHeader("If-None-Match", s""""${etag.etag.tag}""""))

    PekkoCacheHeaders.matchesETag(headers, etag) shouldBe true
  }

  test("matchesETag should return false when ETags do not match") {
    val etag = PekkoCacheHeaders.generateETag("test")
    val headers = Seq(RawHeader("If-None-Match", "different-etag"))

    PekkoCacheHeaders.matchesETag(headers, etag) shouldBe false
  }

  test("matchesETag should return false when header is absent") {
    val etag = PekkoCacheHeaders.generateETag("test")
    val headers = Seq.empty[HttpHeader]

    PekkoCacheHeaders.matchesETag(headers, etag) shouldBe false
  }

  test("notModifiedSince should return true when resource not modified") {
    val lastMod = PekkoCacheHeaders.lastModified(System.currentTimeMillis() - 10000) // 10 seconds ago
    val ifModifiedSince = DateTime.now // Now is after last modification

    val headers = Seq(RawHeader("If-Modified-Since", ifModifiedSince.toIsoDateTimeString()))

    PekkoCacheHeaders.notModifiedSince(headers, lastMod) shouldBe true
  }

  test("notModifiedSince should return false when resource was modified") {
    val lastMod = PekkoCacheHeaders.lastModifiedNow // Modified now
    val ifModifiedSince = DateTime(System.currentTimeMillis() - 10000) // 10 seconds ago

    val headers = Seq(RawHeader("If-Modified-Since", ifModifiedSince.toIsoDateTimeString()))

    PekkoCacheHeaders.notModifiedSince(headers, lastMod) shouldBe false
  }

  test("notModifiedSince should return false when header is absent") {
    val lastMod = PekkoCacheHeaders.lastModifiedNow
    val headers = Seq.empty[HttpHeader]

    PekkoCacheHeaders.notModifiedSince(headers, lastMod) shouldBe false
  }

  test("ETag generation should be deterministic for same input") {
    val input = "consistent-data"
    val iterations = 100

    val etags = (1 to iterations).map(_ => PekkoCacheHeaders.generateETag(input))

    etags.distinct should have size 1
  }

  test("Cache headers should work with different resource types") {
    val response = HttpResponse(status = StatusCodes.OK)

    // Test YAML resource
    val yamlCached = PekkoCacheHeaders.withYamlResourceCache(response, "plan-abc", 123456789L)
    yamlCached.headers.size should be > response.headers.size

    // Test connection resource
    val connCached = PekkoCacheHeaders.withConnectionCache(response, "conn-xyz", "v2")
    connCached.headers.size should be > response.headers.size

    // Test no-cache
    val noCached = PekkoCacheHeaders.withNoCache(response)
    noCached.headers.size should be > response.headers.size
  }
}
