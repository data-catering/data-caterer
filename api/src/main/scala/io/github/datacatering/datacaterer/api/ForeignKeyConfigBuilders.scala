package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, NullabilityConfig}

/**
 * Builder for CardinalityConfig to control foreign key cardinality in relationships.
 *
 * Example usage:
 * {{{
 *   // One-to-one relationship
 *   cardinality.min(1).max(1)
 *
 *   // One-to-many with average of 3 children per parent
 *   cardinality.ratio(3.0).distribution("normal")
 *
 *   // Bounded one-to-many (2-5 children per parent)
 *   cardinality.min(2).max(5).distribution("uniform")
 * }}}
 */
case class CardinalityConfigBuilder(config: CardinalityConfig = CardinalityConfig()) {
  def this() = this(CardinalityConfig())

  /**
   * Set minimum number of related records per parent.
   * Useful for enforcing at least N children per parent.
   */
  def min(min: Int): CardinalityConfigBuilder =
    this.modify(_.config.min).setTo(Some(min))

  /**
   * Set maximum number of related records per parent.
   * Useful for capping the number of children per parent.
   */
  def max(max: Int): CardinalityConfigBuilder =
    this.modify(_.config.max).setTo(Some(max))

  /**
   * Set average ratio of child records per parent.
   * For example, ratio(2.5) means on average 2.5 orders per customer.
   */
  def ratio(ratio: Double): CardinalityConfigBuilder =
    this.modify(_.config.ratio).setTo(Some(ratio))

  /**
   * Set distribution pattern for cardinality.
   * Supported distributions:
   * - "uniform": All parents have similar number of children
   * - "normal": Normal distribution around the ratio
   * - "zipf": Power law distribution (few parents have many children)
   * - "power": Similar to zipf, power law distribution
   */
  def distribution(distribution: String): CardinalityConfigBuilder =
    this.modify(_.config.distribution).setTo(distribution)
}

/**
 * Companion object with factory methods for common cardinality patterns.
 */
object CardinalityConfigBuilder {
  /**
   * Create a one-to-one cardinality configuration.
   */
  def oneToOne(): CardinalityConfigBuilder =
    CardinalityConfigBuilder().min(1).max(1)

  /**
   * Create a one-to-many configuration with specified ratio.
   */
  def oneToMany(avgRatio: Double, distribution: String = CARDINALITY_DISTRIBUTION_UNIFORM): CardinalityConfigBuilder =
    CardinalityConfigBuilder().ratio(avgRatio).distribution(distribution)

  /**
   * Create a bounded one-to-many configuration.
   */
  def bounded(min: Int, max: Int, distribution: String = CARDINALITY_DISTRIBUTION_UNIFORM): CardinalityConfigBuilder =
    CardinalityConfigBuilder().min(min).max(max).distribution(distribution)
}

/**
 * Builder for NullabilityConfig to control nullable foreign keys (partial relationships).
 *
 * Example usage:
 * {{{
 *   // 20% of records will have null FK
 *   nullability.percentage(0.2)
 *
 *   // 30% null, selected randomly
 *   nullability.percentage(0.3).strategy("random")
 *
 *   // First 10% of records have null FK
 *   nullability.percentage(0.1).strategy("head")
 * }}}
 */
case class NullabilityConfigBuilder(config: NullabilityConfig = NullabilityConfig()) {
  def this() = this(NullabilityConfig())

  /**
   * Set percentage of records that should have null FK.
   * Must be between 0.0 (all have FK) and 1.0 (all null).
   */
  def percentage(percentage: Double): NullabilityConfigBuilder = {
    require(percentage >= 0.0 && percentage <= 1.0, "percentage must be between 0.0 and 1.0")
    this.modify(_.config.nullPercentage).setTo(percentage)
  }

  /**
   * Set strategy for selecting which records get null FK.
   * Supported strategies:
   * - "random": Randomly select records to have null FK
   * - "head": First N% of records have null FK
   * - "tail": Last N% of records have null FK
   */
  def strategy(strategy: String): NullabilityConfigBuilder =
    this.modify(_.config.strategy).setTo(strategy)
}

/**
 * Companion object with factory methods for common nullability patterns.
 */
object NullabilityConfigBuilder {
  /**
   * Create a configuration where specified percentage of records have null FK.
   */
  def partial(percentage: Double): NullabilityConfigBuilder =
    NullabilityConfigBuilder().percentage(percentage)

  /**
   * Create a configuration with random nulls.
   */
  def random(percentage: Double): NullabilityConfigBuilder =
    NullabilityConfigBuilder().percentage(percentage).strategy(NULLABILITY_STRATEGY_RANDOM)
}
