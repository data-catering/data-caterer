package io.github.datacatering.datacaterer.api

import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, NullabilityConfig}
import org.scalatest.funsuite.AnyFunSuite

class ForeignKeyConfigBuildersTest extends AnyFunSuite {

  // CardinalityConfigBuilder tests
  test("CardinalityConfigBuilder should create default config") {
    val builder = CardinalityConfigBuilder()

    assert(builder.config.min.isEmpty)
    assert(builder.config.max.isEmpty)
    assert(builder.config.ratio.isEmpty)
    assert(builder.config.distribution == "uniform")
  }

  test("CardinalityConfigBuilder should set min value") {
    val builder = CardinalityConfigBuilder().min(2)

    assert(builder.config.min.contains(2))
  }

  test("CardinalityConfigBuilder should set max value") {
    val builder = CardinalityConfigBuilder().max(10)

    assert(builder.config.max.contains(10))
  }

  test("CardinalityConfigBuilder should set ratio") {
    val builder = CardinalityConfigBuilder().ratio(3.5)

    assert(builder.config.ratio.contains(3.5))
  }

  test("CardinalityConfigBuilder should set distribution") {
    val builder = CardinalityConfigBuilder().distribution("normal")

    assert(builder.config.distribution == "normal")
  }

  test("CardinalityConfigBuilder should chain methods") {
    val builder = CardinalityConfigBuilder()
      .min(1)
      .max(5)
      .distribution("zipf")

    assert(builder.config.min.contains(1))
    assert(builder.config.max.contains(5))
    assert(builder.config.distribution == "zipf")
  }

  test("CardinalityConfigBuilder.oneToOne should create 1:1 config") {
    val builder = CardinalityConfigBuilder.oneToOne()

    assert(builder.config.min.contains(1))
    assert(builder.config.max.contains(1))
  }

  test("CardinalityConfigBuilder.oneToMany should create ratio-based config") {
    val builder = CardinalityConfigBuilder.oneToMany(2.5)

    assert(builder.config.ratio.contains(2.5))
    assert(builder.config.distribution == "uniform")
  }

  test("CardinalityConfigBuilder.oneToMany should accept custom distribution") {
    val builder = CardinalityConfigBuilder.oneToMany(3.0, "normal")

    assert(builder.config.ratio.contains(3.0))
    assert(builder.config.distribution == "normal")
  }

  test("CardinalityConfigBuilder.bounded should create min-max config") {
    val builder = CardinalityConfigBuilder.bounded(2, 5)

    assert(builder.config.min.contains(2))
    assert(builder.config.max.contains(5))
    assert(builder.config.distribution == "uniform")
  }

  test("CardinalityConfigBuilder.bounded should accept custom distribution") {
    val builder = CardinalityConfigBuilder.bounded(1, 10, "zipf")

    assert(builder.config.min.contains(1))
    assert(builder.config.max.contains(10))
    assert(builder.config.distribution == "zipf")
  }

  // NullabilityConfigBuilder tests
  test("NullabilityConfigBuilder should create default config") {
    val builder = NullabilityConfigBuilder()

    assert(builder.config.nullPercentage == 0.0)
    assert(builder.config.strategy == "random")
  }

  test("NullabilityConfigBuilder should set percentage") {
    val builder = NullabilityConfigBuilder().percentage(0.3)

    assert(builder.config.nullPercentage == 0.3)
  }

  test("NullabilityConfigBuilder should set strategy") {
    val builder = NullabilityConfigBuilder().strategy("head")

    assert(builder.config.strategy == "head")
  }

  test("NullabilityConfigBuilder should chain methods") {
    val builder = NullabilityConfigBuilder()
      .percentage(0.25)
      .strategy("tail")

    assert(builder.config.nullPercentage == 0.25)
    assert(builder.config.strategy == "tail")
  }

  test("NullabilityConfigBuilder should validate percentage bounds") {
    assertThrows[IllegalArgumentException] {
      NullabilityConfigBuilder().percentage(-0.1)
    }

    assertThrows[IllegalArgumentException] {
      NullabilityConfigBuilder().percentage(1.5)
    }
  }

  test("NullabilityConfigBuilder.partial should create config with percentage") {
    val builder = NullabilityConfigBuilder.partial(0.3)

    assert(builder.config.nullPercentage == 0.3)
  }

  test("NullabilityConfigBuilder.random should create random strategy config") {
    val builder = NullabilityConfigBuilder.random(0.2)

    assert(builder.config.nullPercentage == 0.2)
    assert(builder.config.strategy == "random")
  }

  test("CardinalityConfigBuilder should be immutable") {
    val builder1 = CardinalityConfigBuilder().min(1)
    val builder2 = builder1.max(5)

    assert(builder1.config.min.contains(1))
    assert(builder1.config.max.isEmpty)

    assert(builder2.config.min.contains(1))
    assert(builder2.config.max.contains(5))
  }

  test("NullabilityConfigBuilder should be immutable") {
    val builder1 = NullabilityConfigBuilder().percentage(0.1)
    val builder2 = builder1.strategy("head")

    assert(builder1.config.nullPercentage == 0.1)
    assert(builder1.config.strategy == "random")

    assert(builder2.config.nullPercentage == 0.1)
    assert(builder2.config.strategy == "head")
  }
}
