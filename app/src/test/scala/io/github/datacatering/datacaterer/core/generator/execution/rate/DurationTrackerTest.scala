package io.github.datacatering.datacaterer.core.generator.execution.rate

import org.scalatest.funsuite.AnyFunSuite

class DurationTrackerTest extends AnyFunSuite {

  test("Parse duration string with seconds") {
    val tracker = new DurationTracker("30s")
    tracker.start()
    assert(tracker.hasTimeRemaining)
  }

  test("Parse duration string with minutes") {
    val tracker = new DurationTracker("5m")
    tracker.start()
    assert(tracker.hasTimeRemaining)
  }

  test("Parse duration string with hours") {
    val tracker = new DurationTracker("1h")
    tracker.start()
    assert(tracker.hasTimeRemaining)
  }

  test("Parse complex duration string") {
    val tracker = new DurationTracker("1h30m45s")
    tracker.start()
    assert(tracker.hasTimeRemaining)
    val elapsedMs = tracker.getElapsedTimeMs
    assert(elapsedMs >= 0)
  }

  test("Fail on invalid duration format") {
    assertThrows[IllegalArgumentException] {
      new DurationTracker("invalid")
    }
  }

  test("Fail on invalid duration unit") {
    assertThrows[IllegalArgumentException] {
      new DurationTracker("5x")
    }
  }

  test("Track elapsed time") {
    val tracker = new DurationTracker("5s")
    tracker.start()
    Thread.sleep(100)
    val elapsedMs = tracker.getElapsedTimeMs
    assert(elapsedMs >= 100)
    assert(elapsedMs < 5000)
  }

  test("Get remaining time") {
    val tracker = new DurationTracker("5s")
    tracker.start()
    Thread.sleep(100)
    val remainingMs = tracker.getRemainingTimeMs
    assert(remainingMs > 0)
    assert(remainingMs < 5000)
  }

  test("Duration expires after time limit") {
    val tracker = new DurationTracker("100ms")
    tracker.start()
    Thread.sleep(150)
    assert(!tracker.hasTimeRemaining)
  }

  test("Remaining time is zero after expiration") {
    val tracker = new DurationTracker("50ms")
    tracker.start()
    Thread.sleep(100)
    val remainingMs = tracker.getRemainingTimeMs
    assert(remainingMs == 0)
  }
}
