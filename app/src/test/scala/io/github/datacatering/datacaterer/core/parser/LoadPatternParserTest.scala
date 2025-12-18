package io.github.datacatering.datacaterer.core.parser

import io.github.datacatering.datacaterer.api.model.{LoadPatternStep, LoadPattern => LoadPatternModel}
import io.github.datacatering.datacaterer.core.generator.execution.pattern._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoadPatternParserTest extends AnyFlatSpec with Matchers {

  "LoadPatternParser" should "parse constant pattern" in {
    val model = LoadPatternModel(
      `type` = "constant",
      baseRate = Some(100)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[ConstantLoadPattern]
    result.right.get.asInstanceOf[ConstantLoadPattern].rate shouldBe 100
  }

  it should "fail constant pattern without baseRate" in {
    val model = LoadPatternModel(`type` = "constant")

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
    result.left.get.head should include("baseRate")
  }

  it should "parse ramp pattern" in {
    val model = LoadPatternModel(
      `type` = "ramp",
      startRate = Some(10),
      endRate = Some(100)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[RampLoadPattern]
    val pattern = result.right.get.asInstanceOf[RampLoadPattern]
    pattern.startRate shouldBe 10
    pattern.endRate shouldBe 100
  }

  it should "fail ramp pattern without required fields" in {
    val model1 = LoadPatternModel(`type` = "ramp", endRate = Some(100))
    LoadPatternParser.parse(model1).isLeft shouldBe true

    val model2 = LoadPatternModel(`type` = "ramp", startRate = Some(10))
    LoadPatternParser.parse(model2).isLeft shouldBe true
  }

  it should "parse spike pattern" in {
    val model = LoadPatternModel(
      `type` = "spike",
      baseRate = Some(50),
      spikeRate = Some(500),
      spikeStart = Some(0.5),
      spikeDuration = Some(0.1)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[SpikeLoadPattern]
    val pattern = result.right.get.asInstanceOf[SpikeLoadPattern]
    pattern.baseRate shouldBe 50
    pattern.spikeRate shouldBe 500
    pattern.spikeStart shouldBe 0.5
    pattern.spikeDuration shouldBe 0.1
  }

  it should "fail spike pattern without required fields" in {
    val model = LoadPatternModel(
      `type` = "spike",
      baseRate = Some(50),
      spikeRate = Some(500)
      // Missing spikeStart and spikeDuration
    )

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
    result.left.get.head should include("spikeStart")
  }

  it should "parse stepped pattern" in {
    val steps = List(
      LoadPatternStep(50, "30s"),
      LoadPatternStep(100, "1m"),
      LoadPatternStep(200, "30s")
    )
    val model = LoadPatternModel(
      `type` = "stepped",
      steps = Some(steps)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[SteppedLoadPattern]
    val pattern = result.right.get.asInstanceOf[SteppedLoadPattern]
    pattern.steps shouldBe steps
  }

  it should "accept 'step' as alias for 'stepped'" in {
    val steps = List(LoadPatternStep(50, "30s"))
    val model = LoadPatternModel(`type` = "step", steps = Some(steps))

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[SteppedLoadPattern]
  }

  it should "fail stepped pattern without steps" in {
    val model = LoadPatternModel(`type` = "stepped")

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
    result.left.get.head should include("steps")
  }

  it should "fail stepped pattern with empty steps" in {
    val model = LoadPatternModel(`type` = "stepped", steps = Some(List()))

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
  }

  it should "parse wave pattern" in {
    val model = LoadPatternModel(
      `type` = "wave",
      baseRate = Some(100),
      amplitude = Some(20),
      frequency = Some(2.0)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[WaveLoadPattern]
    val pattern = result.right.get.asInstanceOf[WaveLoadPattern]
    pattern.baseRate shouldBe 100
    pattern.amplitude shouldBe 20
    pattern.frequency shouldBe 2.0
  }

  it should "accept 'sinusoidal' as alias for 'wave'" in {
    val model = LoadPatternModel(
      `type` = "sinusoidal",
      baseRate = Some(100),
      amplitude = Some(20),
      frequency = Some(2.0)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[WaveLoadPattern]
  }

  it should "fail wave pattern without required fields" in {
    val model = LoadPatternModel(
      `type` = "wave",
      baseRate = Some(100),
      amplitude = Some(20)
      // Missing frequency
    )

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
    result.left.get.head should include("frequency")
  }

  it should "parse breaking point pattern" in {
    val model = LoadPatternModel(
      `type` = "breakingPoint",
      startRate = Some(10),
      rateIncrement = Some(10),
      incrementInterval = Some("30s"),
      maxRate = Some(100)
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[BreakingPointPattern]
    val pattern = result.right.get.asInstanceOf[BreakingPointPattern]
    pattern.startRate shouldBe 10
    pattern.rateIncrement shouldBe 10
    pattern.incrementInterval shouldBe 30.0
    pattern.maxRate shouldBe Some(100)
  }

  it should "accept 'breaking_point' as alias for 'breakingPoint'" in {
    val model = LoadPatternModel(
      `type` = "breaking_point",
      startRate = Some(10),
      rateIncrement = Some(10),
      incrementInterval = Some("30s")
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get shouldBe a[BreakingPointPattern]
  }

  it should "parse breaking point pattern without maxRate" in {
    val model = LoadPatternModel(
      `type` = "breakingPoint",
      startRate = Some(10),
      rateIncrement = Some(10),
      incrementInterval = Some("30s")
    )

    val result = LoadPatternParser.parse(model)
    result.isRight shouldBe true
    result.right.get.asInstanceOf[BreakingPointPattern].maxRate shouldBe None
  }

  it should "fail breaking point pattern without required fields" in {
    val model = LoadPatternModel(
      `type` = "breakingPoint",
      startRate = Some(10),
      rateIncrement = Some(10)
      // Missing incrementInterval
    )

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
    result.left.get.head should include("incrementInterval")
  }

  it should "fail with unknown pattern type" in {
    val model = LoadPatternModel(`type` = "unknown")

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
    result.left.get.head should include("Unknown load pattern type")
  }

  it should "fail patterns that don't pass validation" in {
    val model = LoadPatternModel(
      `type` = "ramp",
      startRate = Some(100),
      endRate = Some(10) // Invalid: startRate > endRate
    )

    val result = LoadPatternParser.parse(model)
    result.isLeft shouldBe true
  }

  it should "parse duration formats correctly" in {
    val model1 = LoadPatternModel(
      `type` = "breakingPoint",
      startRate = Some(10),
      rateIncrement = Some(10),
      incrementInterval = Some("30s")
    )
    val result1 = LoadPatternParser.parse(model1)
    result1.right.get.asInstanceOf[BreakingPointPattern].incrementInterval shouldBe 30.0

    val model2 = LoadPatternModel(
      `type` = "breakingPoint",
      startRate = Some(10),
      rateIncrement = Some(10),
      incrementInterval = Some("2m")
    )
    val result2 = LoadPatternParser.parse(model2)
    result2.right.get.asInstanceOf[BreakingPointPattern].incrementInterval shouldBe 120.0

    val model3 = LoadPatternModel(
      `type` = "breakingPoint",
      startRate = Some(10),
      rateIncrement = Some(10),
      incrementInterval = Some("1h")
    )
    val result3 = LoadPatternParser.parse(model3)
    result3.right.get.asInstanceOf[BreakingPointPattern].incrementInterval shouldBe 3600.0
  }
}
