package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Count, Step, Task, TaskSummary}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class DurationBasedExecutionStrategyTest extends AnyFunSuite with Matchers {

  private def createStrategy(
    duration: String = "10s",
    rate: Option[Int] = None,
    rateUnit: Option[String] = Some("1s")
  ): DurationBasedExecutionStrategy = {
    val count = Count(
      duration = Some(duration),
      rate = rate,
      rateUnit = rateUnit
    )

    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = count)
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
    new DurationBasedExecutionStrategy(executableTasks)
  }

  test("calculateNumBatches returns Int.MaxValue") {
    val strategy = createStrategy()
    strategy.calculateNumBatches shouldBe Int.MaxValue
  }

  test("shouldContinue returns true initially") {
    val strategy = createStrategy(duration = "1s")
    strategy.shouldContinue(1) shouldBe true
  }

  test("shouldContinue returns false after duration expires") {
    val strategy = createStrategy(duration = "50ms")

    strategy.shouldContinue(1) shouldBe true
    Thread.sleep(70)
    strategy.shouldContinue(2) shouldBe false
  }

  test("shouldContinue tracks elapsed time correctly") {
    val strategy = createStrategy(duration = "100ms")

    val startTime = System.currentTimeMillis()
    strategy.shouldContinue(1) shouldBe true

    Thread.sleep(120)
    val shouldContinue = strategy.shouldContinue(2)
    val elapsed = System.currentTimeMillis() - startTime

    shouldContinue shouldBe false
    elapsed should be >= 120L
  }

  test("getDurationSeconds returns correct duration for seconds") {
    val strategy = createStrategy(duration = "30s")
    strategy.getDurationSeconds shouldBe 30.0
  }

  test("getDurationSeconds returns correct duration for minutes") {
    val strategy = createStrategy(duration = "2m")
    strategy.getDurationSeconds shouldBe 120.0
  }

  test("getDurationSeconds returns correct duration for hours") {
    val strategy = createStrategy(duration = "1h")
    strategy.getDurationSeconds shouldBe 3600.0
  }

  test("getDurationSeconds returns correct duration for complex format") {
    val strategy = createStrategy(duration = "1h30m45s")
    strategy.getDurationSeconds shouldBe (3600.0 + 1800.0 + 45.0)
  }

  test("getTargetRate returns rate when configured") {
    val strategy = createStrategy(rate = Some(100))
    strategy.getTargetRate shouldBe Some(100)
  }

  test("getTargetRate returns None when not configured") {
    val strategy = createStrategy(rate = None)
    strategy.getTargetRate shouldBe None
  }

  test("getGenerationMode returns AllUpfront when rate is specified") {
    val strategy = createStrategy(rate = Some(50))
    strategy.getGenerationMode shouldBe GenerationMode.AllUpfront
  }

  test("getGenerationMode returns Batched when rate is not specified") {
    val strategy = createStrategy(rate = None)
    strategy.getGenerationMode shouldBe GenerationMode.Batched
  }

  test("onBatchStart records start time") {
    val strategy = createStrategy()

    strategy.shouldContinue(1) shouldBe true
    strategy.onBatchStart(1)

    // Should not throw exception
    succeed
  }

  test("onBatchEnd records metrics") {
    val strategy = createStrategy()

    strategy.shouldContinue(1) shouldBe true
    strategy.onBatchStart(1)
    strategy.onBatchEnd(1, 1000)

    val metrics = strategy.getMetrics
    metrics shouldBe defined
    metrics.get.totalRecords shouldBe 1000
    metrics.get.batchMetrics.size shouldBe 1
  }

  test("onBatchEnd applies rate limiting when configured") {
    val strategy = createStrategy(rate = Some(10000), rateUnit = Some("1s"))

    strategy.shouldContinue(1) shouldBe true
    strategy.onBatchStart(1)

    val startTime = System.currentTimeMillis()
    strategy.onBatchEnd(1, 20000) // 2x the rate, should throttle
    val elapsed = System.currentTimeMillis() - startTime

    // Should have throttled to maintain rate (at least 100ms for 2x rate)
    elapsed should be >= 100L
  }

  test("onBatchEnd does not throttle when rate not configured") {
    val strategy = createStrategy(rate = None)

    strategy.shouldContinue(1) shouldBe true
    strategy.onBatchStart(1)

    val startTime = System.currentTimeMillis()
    strategy.onBatchEnd(1, 10000)
    val elapsed = System.currentTimeMillis() - startTime

    // Should complete immediately without throttling
    elapsed should be < 100L
  }

  test("getMetrics returns performance metrics after multiple batches") {
    val strategy = createStrategy(duration = "500ms")

    strategy.shouldContinue(1) shouldBe true
    strategy.onBatchStart(1)
    strategy.onBatchEnd(1, 500)

    strategy.shouldContinue(2) shouldBe true
    strategy.onBatchStart(2)
    strategy.onBatchEnd(2, 700)

    strategy.shouldContinue(3) shouldBe true
    strategy.onBatchStart(3)
    strategy.onBatchEnd(3, 300)

    val metrics = strategy.getMetrics
    metrics shouldBe defined
    metrics.get.totalRecords shouldBe 1500
    metrics.get.batchMetrics.size shouldBe 3
  }

  test("rate unit of '1s' is parsed correctly") {
    val strategy = createStrategy(rate = Some(100), rateUnit = Some("1s"))
    strategy.getTargetRate shouldBe Some(100)
  }

  test("rate unit of 'second' is parsed correctly") {
    val strategy = createStrategy(rate = Some(100), rateUnit = Some("second"))
    strategy.getTargetRate shouldBe Some(100)
  }

  test("default rate unit is 1s when not specified") {
    val strategy = createStrategy(rate = Some(100), rateUnit = None)
    strategy.getTargetRate shouldBe Some(100)
  }

  test("multiple shouldContinue calls track time correctly") {
    val strategy = createStrategy(duration = "100ms")

    strategy.shouldContinue(1) shouldBe true
    Thread.sleep(30)
    strategy.shouldContinue(2) shouldBe true
    Thread.sleep(30)
    strategy.shouldContinue(3) shouldBe true
    Thread.sleep(60)
    strategy.shouldContinue(4) shouldBe false
  }

  test("execution completes within expected duration") {
    val strategy = createStrategy(duration = "200ms")

    val startTime = System.currentTimeMillis()
    strategy.shouldContinue(1) shouldBe true

    var batch = 1
    while (strategy.shouldContinue(batch)) {
      Thread.sleep(30)
      batch += 1
      if (batch > 20) fail("Too many batches, duration not enforced")
    }

    val elapsed = System.currentTimeMillis() - startTime
    elapsed should be >= 200L
    elapsed should be < 400L // Allow some overhead
  }

  test("throw exception when duration not specified") {
    assertThrows[IllegalArgumentException] {
      val task = Task(name = "test_task", steps = List(
        Step(name = "step1", count = Count())
      ))
      val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
      new DurationBasedExecutionStrategy(executableTasks)
    }
  }

  test("throw exception when no step with duration found") {
    assertThrows[IllegalArgumentException] {
      val task = Task(name = "test_task", steps = List(
        Step(name = "step1", count = Count(records = Some(1000)))
      ))
      val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
      new DurationBasedExecutionStrategy(executableTasks)
    }
  }

  test("batch counter increments correctly") {
    val strategy = createStrategy(duration = "500ms")

    strategy.shouldContinue(1) shouldBe true
    Thread.sleep(100)
    strategy.shouldContinue(2) shouldBe true
    Thread.sleep(100)
    strategy.shouldContinue(3) shouldBe true
  }

  test("logs completion message when duration expires") {
    val strategy = createStrategy(duration = "50ms")

    strategy.shouldContinue(1) shouldBe true
    Thread.sleep(70)
    val result = strategy.shouldContinue(2)

    result shouldBe false
  }
}
