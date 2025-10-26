package io.github.datacatering.datacaterer.core.ui.http

import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class RateLimiterTest extends AnyFunSuiteLike with Matchers with BeforeAndAfterEach {

  var rateLimiter: RateLimiter = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    rateLimiter = new RateLimiter(tokensPerSecond = 10.0, bucketCapacity = 20)
  }

  test("allowRequest should allow requests within rate limit") {
    val clientId = "client1"

    // Should allow multiple requests up to bucket capacity
    (1 to 20).foreach { _ =>
      rateLimiter.allowRequest(clientId) shouldBe true
    }
  }

  test("allowRequest should deny requests exceeding rate limit") {
    val clientId = "client2"

    // Fill the bucket
    (1 to 20).foreach { _ =>
      rateLimiter.allowRequest(clientId)
    }

    // Next request should be denied
    rateLimiter.allowRequest(clientId) shouldBe false
  }

  test("allowRequest should refill tokens over time") {
    val clientId = "client3"

    // Fill the bucket
    (1 to 20).foreach { _ =>
      rateLimiter.allowRequest(clientId)
    }

    // Should be denied immediately
    rateLimiter.allowRequest(clientId) shouldBe false

    // Wait for refill (200ms should add ~2 tokens at 10 tokens/sec)
    Thread.sleep(200)

    // Should allow some requests now
    rateLimiter.allowRequest(clientId) shouldBe true
  }

  test("allowRequest should track separate limits per client") {
    val client1 = "client-a"
    val client2 = "client-b"

    // Fill bucket for client1
    (1 to 20).foreach { _ =>
      rateLimiter.allowRequest(client1)
    }

    // client1 should be denied
    rateLimiter.allowRequest(client1) shouldBe false

    // client2 should still be allowed
    rateLimiter.allowRequest(client2) shouldBe true
  }

  test("allowRequest should support custom token requirements") {
    val clientId = "client4"

    // Request 5 tokens at once
    rateLimiter.allowRequest(clientId, tokensRequired = 5.0) shouldBe true

    // Should have 15 tokens remaining
    rateLimiter.allowRequest(clientId, tokensRequired = 15.0) shouldBe true

    // Should have 0 tokens remaining
    rateLimiter.allowRequest(clientId, tokensRequired = 1.0) shouldBe false
  }

  test("getTokenCount should return current token count") {
    val clientId = "client5"

    // Initial token count should be bucket capacity
    rateLimiter.getTokenCount(clientId) shouldBe 20.0 +- 0.1

    // Use 5 tokens
    rateLimiter.allowRequest(clientId, tokensRequired = 5.0)

    // Should have 15 tokens remaining
    rateLimiter.getTokenCount(clientId) shouldBe 15.0 +- 0.1
  }

  test("reset should reset rate limit for specific client") {
    val clientId = "client6"

    // Fill the bucket
    (1 to 20).foreach { _ =>
      rateLimiter.allowRequest(clientId)
    }

    // Should be denied
    rateLimiter.allowRequest(clientId) shouldBe false

    // Reset the client
    rateLimiter.reset(clientId)

    // Should be allowed again
    rateLimiter.allowRequest(clientId) shouldBe true
  }

  test("resetAll should reset all rate limits") {
    val client1 = "client-x"
    val client2 = "client-y"

    // Fill buckets for both clients
    (1 to 20).foreach { _ =>
      rateLimiter.allowRequest(client1)
      rateLimiter.allowRequest(client2)
    }

    // Both should be denied
    rateLimiter.allowRequest(client1) shouldBe false
    rateLimiter.allowRequest(client2) shouldBe false

    // Reset all
    rateLimiter.resetAll()

    // Both should be allowed again
    rateLimiter.allowRequest(client1) shouldBe true
    rateLimiter.allowRequest(client2) shouldBe true
  }

  test("getStats should return current statistics") {
    rateLimiter.allowRequest("client-a")
    rateLimiter.allowRequest("client-b")
    rateLimiter.allowRequest("client-c")

    val stats = rateLimiter.getStats

    stats.activeClients shouldBe 3
    stats.tokensPerSecond shouldBe 10.0
    stats.bucketCapacity shouldBe 20
    stats.averageTokens should be > 0.0
  }

  test("cleanup should remove inactive clients") {
    // Create a fresh rate limiter to avoid interference from other tests
    val cleanupTestLimiter = new RateLimiter(tokensPerSecond = 10.0, bucketCapacity = 20)
    val clientId = "inactive-client"

    cleanupTestLimiter.allowRequest(clientId)

    // Client should exist
    cleanupTestLimiter.getStats.activeClients shouldBe 1

    // Wait a bit to ensure time has passed
    Thread.sleep(50)

    // Cleanup with very short threshold (10ms)
    cleanupTestLimiter.cleanup(inactiveThresholdMs = 10)

    // Client should be removed
    cleanupTestLimiter.getStats.activeClients shouldBe 0
  }

  test("cleanup should not remove recently active clients") {
    val clientId = "active-client"

    rateLimiter.allowRequest(clientId)

    // Cleanup with long threshold
    rateLimiter.cleanup(inactiveThresholdMs = 3600000) // 1 hour

    // Client should still exist
    rateLimiter.getStats.activeClients shouldBe 1
  }

  test("RateLimiter should handle concurrent requests") {
    val clientId = "concurrent-client"
    val threads = (1 to 10).map { i =>
      new Thread(() => {
        (1 to 5).foreach { _ =>
          rateLimiter.allowRequest(clientId)
          Thread.sleep(10)
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Should have processed all requests without errors
    rateLimiter.getTokenCount(clientId) should be >= 0.0
  }

  test("GlobalRateLimiter should provide default limiter") {
    val limiter = GlobalRateLimiter.default

    limiter shouldBe a[RateLimiter]
    limiter.allowRequest("test-client") shouldBe true
  }

  test("GlobalRateLimiter should provide strict limiter") {
    val limiter = GlobalRateLimiter.strict

    limiter shouldBe a[RateLimiter]
    limiter.allowRequest("test-client") shouldBe true
  }

  test("ClientIdentifier should extract IP from X-Forwarded-For header") {
    val headers = Seq(RawHeader("X-Forwarded-For", "192.168.1.1, 10.0.0.1"))

    val clientId = ClientIdentifier.fromHeaders(headers)

    clientId shouldBe "192.168.1.1"
  }

  test("ClientIdentifier should extract IP from X-Real-IP header") {
    val headers = Seq(RawHeader("X-Real-IP", "192.168.1.2"))

    val clientId = ClientIdentifier.fromHeaders(headers)

    clientId shouldBe "192.168.1.2"
  }

  test("ClientIdentifier should prefer X-Forwarded-For over X-Real-IP") {
    val headers = Seq(
      RawHeader("X-Forwarded-For", "192.168.1.1"),
      RawHeader("X-Real-IP", "192.168.1.2")
    )

    val clientId = ClientIdentifier.fromHeaders(headers)

    clientId shouldBe "192.168.1.1"
  }

  test("ClientIdentifier should use remote address as fallback") {
    val headers = Seq.empty
    val remoteAddr = Some("192.168.1.3")

    val clientId = ClientIdentifier.fromHeaders(headers, remoteAddr)

    clientId shouldBe "192.168.1.3"
  }

  test("ClientIdentifier should return unknown when no identifier available") {
    val headers = Seq.empty

    val clientId = ClientIdentifier.fromHeaders(headers, None)

    clientId shouldBe "unknown"
  }

  test("ClientIdentifier should extract API key from Authorization header") {
    val headers = Seq(RawHeader("Authorization", "Bearer my-api-key-12345"))

    val apiKey = ClientIdentifier.fromApiKey(headers)

    apiKey shouldBe Some("my-api-key-12345")
  }

  test("ClientIdentifier should return None for non-Bearer auth") {
    val headers = Seq(RawHeader("Authorization", "Basic dXNlcjpwYXNz"))

    val apiKey = ClientIdentifier.fromApiKey(headers)

    apiKey shouldBe None
  }

  test("ClientIdentifier should return None when Authorization header absent") {
    val headers = Seq.empty

    val apiKey = ClientIdentifier.fromApiKey(headers)

    apiKey shouldBe None
  }

  test("ClientIdentifier fromRequest should prefer API key over IP") {
    val headers = Seq(
      RawHeader("Authorization", "Bearer api-key-123"),
      RawHeader("X-Forwarded-For", "192.168.1.1")
    )

    val clientId = ClientIdentifier.fromRequest(headers)

    clientId shouldBe "api-key-123"
  }

  test("ClientIdentifier fromRequest should fall back to IP when no API key") {
    val headers = Seq(RawHeader("X-Forwarded-For", "192.168.1.1"))

    val clientId = ClientIdentifier.fromRequest(headers)

    clientId shouldBe "192.168.1.1"
  }

  test("RateLimiter should handle zero tokens remaining") {
    val clientId = "zero-tokens-client"

    // Use all tokens
    rateLimiter.allowRequest(clientId, tokensRequired = 20.0) shouldBe true

    // Should have exactly 0 tokens
    rateLimiter.getTokenCount(clientId) shouldBe 0.0 +- 0.01

    // Should deny next request
    rateLimiter.allowRequest(clientId) shouldBe false
  }

  test("RateLimiter should handle fractional tokens") {
    val clientId = "fractional-client"

    // Use 0.5 tokens
    rateLimiter.allowRequest(clientId, tokensRequired = 0.5) shouldBe true

    // Should have 19.5 tokens remaining
    rateLimiter.getTokenCount(clientId) shouldBe 19.5 +- 0.01
  }

  test("RateLimiterStats toString should format correctly") {
    val stats = RateLimiterStats(
      activeClients = 5,
      averageTokens = 15.75,
      tokensPerSecond = 10.0,
      bucketCapacity = 20
    )

    val str = stats.toString

    str should include("Active Clients: 5")
    str should include("Average Tokens: 15.75")
    str should include("Rate: 10.0 tokens/sec")
    str should include("Capacity: 20 tokens")
  }
}
