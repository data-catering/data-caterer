package io.github.datacatering.datacaterer.core.generator.execution

import io.github.datacatering.datacaterer.api.model.{Count, Step, Task, TaskSummary}
import io.github.datacatering.datacaterer.core.generator.execution.pattern._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PatternBasedExecutionStrategyTest extends AnyFunSuite with Matchers {

  private def createStrategy(pattern: io.github.datacatering.datacaterer.api.model.LoadPattern, duration: String = "10s"): PatternBasedExecutionStrategy = {
    val task = Task(name = "test_task", steps = List(
      Step(name = "step1", count = Count(
        duration = Some(duration),
        pattern = Some(pattern),
        rateUnit = Some("second")
      ))
    ))
    val executableTasks = List((TaskSummary("test_task", "test_ds"), task))
    new PatternBasedExecutionStrategy(executableTasks)
  }

  test("getGenerationMode returns AllUpfront") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(10))
    val strategy = createStrategy(pattern)

    strategy.getGenerationMode shouldBe GenerationMode.AllUpfront
  }

  test("getDurationSeconds returns correct duration") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(10))
    val strategy = createStrategy(pattern, "30s")

    strategy.getDurationSeconds shouldBe 30.0
  }

  test("getInitialRate returns rate at time 0 for constant pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(15))
    val strategy = createStrategy(pattern)

    strategy.getInitialRate shouldBe 15
  }

  test("getInitialRate returns start rate for ramp pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("ramp",
      startRate = Some(5),
      endRate = Some(20)
    )
    val strategy = createStrategy(pattern)

    strategy.getInitialRate shouldBe 5
  }

  test("getRateUnit returns correct unit") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(10))
    val strategy = createStrategy(pattern)

    strategy.getRateUnit shouldBe "second"
  }

  test("getRateFunction returns function with correct rates for constant pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(10))
    val strategy = createStrategy(pattern, "10s")
    val rateFunction = strategy.getRateFunction

    // Constant pattern should return same rate at all times
    rateFunction(0.0) shouldBe 10
    rateFunction(5.0) shouldBe 10
    rateFunction(10.0) shouldBe 10
  }

  test("getRateFunction returns increasing rates for ramp pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("ramp",
      startRate = Some(2),
      endRate = Some(10)
    )
    val strategy = createStrategy(pattern, "10s")
    val rateFunction = strategy.getRateFunction

    // Ramp should start low and increase
    val rateAtStart = rateFunction(0.0)
    val rateAtMid = rateFunction(5.0)
    val rateAtEnd = rateFunction(10.0)

    rateAtStart shouldBe 2
    rateAtEnd shouldBe 10
    rateAtMid should be > rateAtStart
    rateAtMid should be < rateAtEnd
  }

  test("getRateFunction returns oscillating rates for wave pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("wave",
      baseRate = Some(10),
      amplitude = Some(5),
      frequency = Some(1.0)
    )
    val strategy = createStrategy(pattern, "10s")
    val rateFunction = strategy.getRateFunction

    // Wave should oscillate around base rate
    val rates = (0 until 10).map(i => rateFunction(i.toDouble))

    // Should have variation (not all same)
    rates.distinct.size should be > 1

    // Should be centered around base rate
    val avgRate = rates.sum.toDouble / rates.size
    avgRate should be (10.0 +- 2.0)
  }

  test("getAverageRate calculates correct average for constant pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(15))
    val strategy = createStrategy(pattern)

    strategy.getAverageRate shouldBe 15
  }

  test("getAverageRate calculates correct average for ramp pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("ramp",
      startRate = Some(2),
      endRate = Some(10)
    )
    val strategy = createStrategy(pattern)

    // Average of ramp from 2 to 10 should be around 6
    val avgRate = strategy.getAverageRate
    avgRate should be (6 +- 1) // Allow 1 unit tolerance for sampling
  }

  test("getAverageRate calculates correct average for wave pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("wave",
      baseRate = Some(10),
      amplitude = Some(3),
      frequency = Some(1.0)
    )
    val strategy = createStrategy(pattern)

    // Average of wave should be close to base rate
    val avgRate = strategy.getAverageRate
    avgRate should be (10 +- 1)
  }

  test("getAverageRate calculates correct average for spike pattern") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("spike",
      baseRate = Some(2),
      spikeRate = Some(20),
      spikeStart = Some(0.25),
      spikeDuration = Some(0.25)
    )
    val strategy = createStrategy(pattern, "4s")

    // Spike: 2/sec for 1s, 20/sec for 1s, 2/sec for 2s
    // Average = (2 + 20 + 2 + 2) / 4 = 6.5
    val avgRate = strategy.getAverageRate
    avgRate should be (6 +- 2) // Allow tolerance for sampling
  }

  test("getAverageRate calculates correct average for stepped pattern") {
    val steps = List(
      io.github.datacatering.datacaterer.api.model.LoadPatternStep(5, "3s"),
      io.github.datacatering.datacaterer.api.model.LoadPatternStep(10, "3s"),
      io.github.datacatering.datacaterer.api.model.LoadPatternStep(3, "3s")
    )
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("stepped",
      steps = Some(steps)
    )
    val strategy = createStrategy(pattern, "9s")

    // Stepped: 5/sec for 3s, 10/sec for 3s, 3/sec for 3s
    // Average = (5 + 10 + 3) / 3 = 6
    val avgRate = strategy.getAverageRate
    avgRate should be (6 +- 1)
  }

  test("calculateNumBatches returns Int.MaxValue") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(10))
    val strategy = createStrategy(pattern)

    strategy.calculateNumBatches shouldBe Int.MaxValue
  }

  test("shouldContinue returns true initially") {
    val pattern = io.github.datacatering.datacaterer.api.model.LoadPattern("constant", baseRate = Some(10))
    val strategy = createStrategy(pattern, "1s")

    strategy.shouldContinue(1) shouldBe true
  }
}
