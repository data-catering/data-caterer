package io.github.datacatering.datacaterer.api.model

import org.scalatest.funsuite.AnyFunSuite

class CardinalityConfigTest extends AnyFunSuite {

  test("CardinalityConfig should be created with default values") {
    val config = CardinalityConfig()

    assert(config.min.isEmpty)
    assert(config.max.isEmpty)
    assert(config.ratio.isEmpty)
    assert(config.distribution == "uniform")
  }

  test("CardinalityConfig should accept min and max values") {
    val config = CardinalityConfig(min = Some(1), max = Some(5))

    assert(config.min.contains(1))
    assert(config.max.contains(5))
    assert(config.ratio.isEmpty)
  }

  test("CardinalityConfig should accept ratio and distribution") {
    val config = CardinalityConfig(ratio = Some(3.5), distribution = "normal")

    assert(config.ratio.contains(3.5))
    assert(config.distribution == "normal")
    assert(config.min.isEmpty)
    assert(config.max.isEmpty)
  }

  test("CardinalityConfig should support all distribution types") {
    val distributions = List("uniform", "normal", "zipf", "power")

    distributions.foreach { dist =>
      val config = CardinalityConfig(distribution = dist)
      assert(config.distribution == dist)
    }
  }

  test("CardinalityConfig parameterless constructor should work") {
    val config = new CardinalityConfig()

    assert(config.min.isEmpty)
    assert(config.max.isEmpty)
    assert(config.ratio.isEmpty)
    assert(config.distribution == "uniform")
  }

  test("CardinalityConfig should support combined min, max, and ratio") {
    val config = CardinalityConfig(
      min = Some(2),
      max = Some(10),
      ratio = Some(5.0),
      distribution = "zipf"
    )

    assert(config.min.contains(2))
    assert(config.max.contains(10))
    assert(config.ratio.contains(5.0))
    assert(config.distribution == "zipf")
  }
}
