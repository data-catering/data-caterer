package io.github.datacatering.datacaterer.api.model

import org.scalatest.funsuite.AnyFunSuite

class NullabilityConfigTest extends AnyFunSuite {

  test("NullabilityConfig should be created with default values") {
    val config = NullabilityConfig()

    assert(config.nullPercentage == 0.0)
    assert(config.strategy == "random")
  }

  test("NullabilityConfig should accept valid null percentage") {
    val config = NullabilityConfig(nullPercentage = 0.3)

    assert(config.nullPercentage == 0.3)
    assert(config.strategy == "random")
  }

  test("NullabilityConfig should accept different strategies") {
    val strategies = List("random", "head", "tail")

    strategies.foreach { strategy =>
      val config = NullabilityConfig(strategy = strategy)
      assert(config.strategy == strategy)
    }
  }

  test("NullabilityConfig should reject null percentage below 0.0") {
    assertThrows[IllegalArgumentException] {
      NullabilityConfig(nullPercentage = -0.1)
    }
  }

  test("NullabilityConfig should reject null percentage above 1.0") {
    assertThrows[IllegalArgumentException] {
      NullabilityConfig(nullPercentage = 1.1)
    }
  }

  test("NullabilityConfig should accept null percentage at boundaries") {
    val config0 = NullabilityConfig(nullPercentage = 0.0)
    assert(config0.nullPercentage == 0.0)

    val config1 = NullabilityConfig(nullPercentage = 1.0)
    assert(config1.nullPercentage == 1.0)
  }

  test("NullabilityConfig parameterless constructor should work") {
    val config = new NullabilityConfig()

    assert(config.nullPercentage == 0.0)
    assert(config.strategy == "random")
  }

  test("NullabilityConfig should support custom percentage and strategy") {
    val config = NullabilityConfig(nullPercentage = 0.25, strategy = "head")

    assert(config.nullPercentage == 0.25)
    assert(config.strategy == "head")
  }
}
