package io.github.datacatering.datacaterer.core.generator.execution.pattern

import io.github.datacatering.datacaterer.api.model.LoadPatternStep
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoadPatternTest extends AnyFlatSpec with Matchers {

  "ConstantLoadPattern" should "maintain constant rate" in {
    val pattern = ConstantLoadPattern(100)

    pattern.getRateAt(0, 300) shouldBe 100
    pattern.getRateAt(150, 300) shouldBe 100
    pattern.getRateAt(299, 300) shouldBe 100
  }

  it should "validate positive rate" in {
    ConstantLoadPattern(100).validate() shouldBe empty
    ConstantLoadPattern(1).validate() shouldBe empty
    ConstantLoadPattern(0).validate() should not be empty
    ConstantLoadPattern(-10).validate() should not be empty
  }

  "RampLoadPattern" should "increase rate linearly" in {
    val pattern = RampLoadPattern(10, 100)

    pattern.getRateAt(0, 100) shouldBe 10
    pattern.getRateAt(50, 100) shouldBe 55 +- 1
    pattern.getRateAt(100, 100) shouldBe 100
  }

  it should "not exceed end rate" in {
    val pattern = RampLoadPattern(10, 100)

    pattern.getRateAt(150, 100) shouldBe 100
  }

  it should "validate configuration" in {
    RampLoadPattern(10, 100).validate() shouldBe empty
    RampLoadPattern(0, 100).validate() should not be empty
    RampLoadPattern(10, 0).validate() should not be empty
    RampLoadPattern(100, 10).validate() should not be empty
  }

  "SpikeLoadPattern" should "maintain base rate initially" in {
    val pattern = SpikeLoadPattern(50, 500, 0.5, 0.1)

    pattern.getRateAt(0, 100) shouldBe 50
    pattern.getRateAt(40, 100) shouldBe 50
  }

  it should "spike at configured time" in {
    val pattern = SpikeLoadPattern(50, 500, 0.5, 0.1)

    pattern.getRateAt(50, 100) shouldBe 500
    pattern.getRateAt(55, 100) shouldBe 500
  }

  it should "return to base rate after spike" in {
    val pattern = SpikeLoadPattern(50, 500, 0.5, 0.1)

    pattern.getRateAt(61, 100) shouldBe 50
    pattern.getRateAt(90, 100) shouldBe 50
  }

  it should "validate configuration" in {
    SpikeLoadPattern(50, 500, 0.5, 0.1).validate() shouldBe empty
    SpikeLoadPattern(0, 500, 0.5, 0.1).validate() should not be empty
    SpikeLoadPattern(50, 0, 0.5, 0.1).validate() should not be empty
    SpikeLoadPattern(50, 40, 0.5, 0.1).validate() should not be empty
    SpikeLoadPattern(50, 500, -0.1, 0.1).validate() should not be empty
    SpikeLoadPattern(50, 500, 1.5, 0.1).validate() should not be empty
    SpikeLoadPattern(50, 500, 0.5, 0.0).validate() should not be empty
    SpikeLoadPattern(50, 500, 0.5, 1.5).validate() should not be empty
    SpikeLoadPattern(50, 500, 0.9, 0.2).validate() should not be empty // spike end > 1.0
  }

  "SteppedLoadPattern" should "maintain rate within each step" in {
    val steps = List(
      LoadPatternStep(50, "30s"),
      LoadPatternStep(100, "30s"),
      LoadPatternStep(200, "30s")
    )
    val pattern = SteppedLoadPattern(steps)

    pattern.getRateAt(10, 90) shouldBe 50
    pattern.getRateAt(29, 90) shouldBe 50
    pattern.getRateAt(30, 90) shouldBe 100
    pattern.getRateAt(59, 90) shouldBe 100
    pattern.getRateAt(60, 90) shouldBe 200
    pattern.getRateAt(89, 90) shouldBe 200
  }

  it should "handle duration formats" in {
    val steps = List(
      LoadPatternStep(50, "1m"),
      LoadPatternStep(100, "2m"),
      LoadPatternStep(200, "1h")
    )
    val pattern = SteppedLoadPattern(steps)

    pattern.getRateAt(30, 3720) shouldBe 50
    pattern.getRateAt(61, 3720) shouldBe 100
    pattern.getRateAt(181, 3720) shouldBe 200
  }

  it should "validate configuration" in {
    val validSteps = List(LoadPatternStep(50, "30s"), LoadPatternStep(100, "30s"))
    SteppedLoadPattern(validSteps).validate() shouldBe empty

    SteppedLoadPattern(List()).validate() should not be empty

    val invalidRate = List(LoadPatternStep(0, "30s"))
    SteppedLoadPattern(invalidRate).validate() should not be empty

    val invalidDuration = List(LoadPatternStep(50, "0s"))
    SteppedLoadPattern(invalidDuration).validate() should not be empty
  }

  "WaveLoadPattern" should "oscillate around base rate" in {
    val pattern = WaveLoadPattern(100, 20, 1.0)

    val rate0 = pattern.getRateAt(0, 100)
    rate0 shouldBe 100 +- 1

    val rate25 = pattern.getRateAt(25, 100)
    rate25 shouldBe 120 +- 2 // Peak

    val rate50 = pattern.getRateAt(50, 100)
    rate50 shouldBe 100 +- 1 // Back to base

    val rate75 = pattern.getRateAt(75, 100)
    rate75 shouldBe 80 +- 2 // Trough
  }

  it should "complete multiple cycles" in {
    val pattern = WaveLoadPattern(100, 20, 2.0)

    pattern.getRateAt(0, 100) shouldBe 100 +- 1
    pattern.getRateAt(12.5, 100) shouldBe 120 +- 2
    pattern.getRateAt(25, 100) shouldBe 100 +- 1
    pattern.getRateAt(37.5, 100) shouldBe 80 +- 2
    pattern.getRateAt(50, 100) shouldBe 100 +- 1
  }

  it should "never go below 1" in {
    val pattern = WaveLoadPattern(100, 200, 1.0)

    val allRates = (0 to 100).map(t => pattern.getRateAt(t, 100))
    allRates.foreach(_ should be >= 1)
  }

  it should "validate configuration" in {
    WaveLoadPattern(100, 20, 1.0).validate() shouldBe empty
    WaveLoadPattern(0, 20, 1.0).validate() should not be empty
    WaveLoadPattern(100, -10, 1.0).validate() should not be empty
    WaveLoadPattern(100, 150, 1.0).validate() should not be empty // amplitude >= baseRate
    WaveLoadPattern(100, 20, 0).validate() should not be empty
  }

  "BreakingPointPattern" should "increase rate at intervals" in {
    val pattern = BreakingPointPattern(10, 10, 30.0, Some(100))

    pattern.getRateAt(0, 300) shouldBe 10
    pattern.getRateAt(29, 300) shouldBe 10
    pattern.getRateAt(30, 300) shouldBe 20
    pattern.getRateAt(59, 300) shouldBe 20
    pattern.getRateAt(60, 300) shouldBe 30
    pattern.getRateAt(90, 300) shouldBe 40
  }

  it should "respect max rate" in {
    val pattern = BreakingPointPattern(10, 10, 30.0, Some(50))

    pattern.getRateAt(0, 300) shouldBe 10
    pattern.getRateAt(30, 300) shouldBe 20
    pattern.getRateAt(60, 300) shouldBe 30
    pattern.getRateAt(90, 300) shouldBe 40
    pattern.getRateAt(120, 300) shouldBe 50
    pattern.getRateAt(150, 300) shouldBe 50 // capped
    pattern.getRateAt(180, 300) shouldBe 50 // capped
  }

  it should "increase indefinitely without max rate" in {
    val pattern = BreakingPointPattern(10, 10, 30.0, None)

    pattern.getRateAt(0, 300) shouldBe 10
    pattern.getRateAt(30, 300) shouldBe 20
    pattern.getRateAt(300, 300) shouldBe 110
    pattern.getRateAt(600, 300) shouldBe 210
  }

  it should "validate configuration" in {
    BreakingPointPattern(10, 10, 30.0, Some(100)).validate() shouldBe empty
    BreakingPointPattern(10, 10, 30.0, None).validate() shouldBe empty
    BreakingPointPattern(0, 10, 30.0, Some(100)).validate() should not be empty
    BreakingPointPattern(10, 0, 30.0, Some(100)).validate() should not be empty
    BreakingPointPattern(10, 10, 0, Some(100)).validate() should not be empty
    BreakingPointPattern(10, 10, 30.0, Some(5)).validate() should not be empty // maxRate <= startRate
  }
}
