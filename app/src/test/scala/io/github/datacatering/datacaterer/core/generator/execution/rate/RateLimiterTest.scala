package io.github.datacatering.datacaterer.core.generator.execution.rate

import org.scalatest.funsuite.AnyFunSuite

class RateLimiterTest extends AnyFunSuite {

  test("Parse rate unit in seconds") {
    val limiter = new RateLimiter(100, "1s")
    val sleepTime = limiter.calculateSleepTime(100, 500)
    // Expected: 1000ms for 100 records at 100/s, took 500ms, so sleep 500ms
    assert(sleepTime == 500)
  }

  test("Parse rate unit in milliseconds") {
    val limiter = new RateLimiter(100, "100ms")
    val sleepTime = limiter.calculateSleepTime(100, 50)
    // Expected: 100ms for 100 records at 100/100ms = 1000/s, took 50ms, so sleep 50ms
    assert(sleepTime == 50)
  }

  test("Parse rate unit in minutes") {
    val limiter = new RateLimiter(6000, "1m")
    val sleepTime = limiter.calculateSleepTime(100, 500)
    // Expected: 1000ms for 100 records at 100/s (6000/60s), took 500ms, so sleep 500ms
    assert(sleepTime == 500)
  }

  test("No sleep when behind schedule") {
    val limiter = new RateLimiter(100, "1s")
    val sleepTime = limiter.calculateSleepTime(100, 2000)
    // Took 2000ms but expected 1000ms at 100/s, so no sleep needed
    assert(sleepTime == 0)
  }

  test("Sleep when ahead of schedule") {
    val limiter = new RateLimiter(100, "1s")
    val sleepTime = limiter.calculateSleepTime(100, 200)
    // Expected 1000ms for 100 records, took 200ms, sleep 800ms
    assert(sleepTime == 800)
  }

  test("No sleep when exactly on schedule") {
    val limiter = new RateLimiter(100, "1s")
    val sleepTime = limiter.calculateSleepTime(100, 1000)
    // Expected 1000ms, took 1000ms, no sleep needed
    assert(sleepTime == 0)
  }

  test("Calculate sleep time for partial batch") {
    val limiter = new RateLimiter(100, "1s")
    val sleepTime = limiter.calculateSleepTime(50, 200)
    // Expected 500ms for 50 records at 100/s, took 200ms, sleep 300ms
    assert(sleepTime == 300)
  }

  test("Zero records generates no sleep") {
    val limiter = new RateLimiter(100, "1s")
    val sleepTime = limiter.calculateSleepTime(0, 1000)
    assert(sleepTime == 0)
  }

  test("Fail on invalid rate unit format") {
    assertThrows[IllegalArgumentException] {
      new RateLimiter(100, "invalid")
    }
  }

  test("Fail on invalid rate unit type") {
    assertThrows[IllegalArgumentException] {
      new RateLimiter(100, "1x")
    }
  }

  test("High throughput rate limiting") {
    val limiter = new RateLimiter(10000, "1s")
    val sleepTime = limiter.calculateSleepTime(1000, 50)
    // Expected 100ms for 1000 records at 10000/s, took 50ms, sleep 50ms
    assert(sleepTime == 50)
  }
}
