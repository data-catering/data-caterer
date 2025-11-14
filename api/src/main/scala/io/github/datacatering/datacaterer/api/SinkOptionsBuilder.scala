package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.model.{CardinalityConfig, ForeignKey, ForeignKeyRelation, ManyToManyRelation, NullabilityConfig, SinkOptions}

import scala.annotation.varargs

/**
 * Configurations that get applied across all generated data. This includes the random seed value, locale and foreign keys
 */
case class SinkOptionsBuilder(sinkOptions: SinkOptions = SinkOptions()) {

  /**
   * Random seed value to be used across all generated data
   *
   * @param seed Used as seed argument when creating Random instance
   * @return SinkOptionsBuilder
   */
  def seed(seed: Long): SinkOptionsBuilder = this.modify(_.sinkOptions.seed).setTo(Some(seed.toString))

  /**
   * Locale used when generating data via DataFaker expressions
   *
   * @param locale Locale for DataFaker data generated
   * @return SinkOptionsBuilder
   * @see <a href="https://data.catering/setup/generator/data-generator/#string">Docs</a> for details
   */
  def locale(locale: String): SinkOptionsBuilder = this.modify(_.sinkOptions.locale).setTo(Some(locale))

  /**
   * Define a foreign key relationship between fields across any data source for data generation.
   * To define which field to use, it is defined by the following:<br>
   * dataSourceName + stepName + fieldName
   *
   * @param foreignKey      Base foreign key
   * @param generationLinks Foreign key relations for data generation
   * @return SinkOptionsBuilder
   * @see <a href="https://data.catering/setup/foreign-key/">Docs</a> for details
   */
  @varargs def foreignKey(foreignKey: ForeignKeyRelation, generationLinks: ForeignKeyRelation*): SinkOptionsBuilder =
    this.modify(_.sinkOptions.foreignKeys)(_ ++ List(ForeignKey(foreignKey, generationLinks.toList, List())))

  /**
   * Define a foreign key relationship between fields across any data source for data generation and deletion.
   * Can be used for data generation and deletion.
   * To define which field to use, it is defined by the following:<br>
   * dataSourceName + stepName + fieldName
   *
   * @param foreignKey      Base foreign key
   * @param generationLinks Foreign key relations for data generation
   * @param deleteLinks     Foreign key relations for data deletion
   * @return SinkOptionsBuilder
   * @see <a href="https://data.catering/setup/foreign-key/">Docs</a> for details
   */
  def foreignKey(foreignKey: ForeignKeyRelation, generationLinks: List[ForeignKeyRelation], deleteLinks: List[ForeignKeyRelation]): SinkOptionsBuilder =
    this.modify(_.sinkOptions.foreignKeys)(_ ++ List(ForeignKey(foreignKey, generationLinks, deleteLinks)))

  /**
   * Define a foreign key relationship between fields across any data source for data generation.
   * To define which field to use, it is defined by the following:<br>
   * dataSourceName + stepName + fieldName
   *
   * @param foreignKey      Base foreign key
   * @param generationLinks Foreign key relations for data generation
   * @return SinkOptionsBuilder
   * @see <a href="https://data.catering/setup/foreign-key/">Docs</a> for details
   */
  def foreignKey(foreignKey: ForeignKeyRelation, generationLinks: List[ForeignKeyRelation]): SinkOptionsBuilder =
    this.foreignKey(foreignKey, generationLinks: _*)

  /**
   * Define a foreign key relationship with enhanced configuration options.
   *
   * @param foreignKey       Base foreign key
   * @param generationLinks  Foreign key relations for data generation
   * @param deleteLinks      Foreign key relations for data deletion
   * @param relationshipType Optional relationship type: "one-to-one", "one-to-many", "many-to-many"
   * @param cardinality      Optional cardinality configuration
   * @param nullability      Optional nullability configuration
   * @param generationMode   Optional generation mode: "all-exist", "all-combinations", "partial"
   * @return SinkOptionsBuilder
   */
  def foreignKey(foreignKey: ForeignKeyRelation,
                 generationLinks: List[ForeignKeyRelation],
                 deleteLinks: List[ForeignKeyRelation],
                 relationshipType: Option[String],
                 cardinality: Option[CardinalityConfig] = None,
                 nullability: Option[NullabilityConfig] = None,
                 generationMode: Option[String] = None): SinkOptionsBuilder =
    this.modify(_.sinkOptions.foreignKeys)(_ ++ List(ForeignKey(
      foreignKey, generationLinks, deleteLinks,
      relationshipType, cardinality, nullability, generationMode
    )))

  /**
   * Define a foreign key relationship with cardinality configuration.
   *
   * @param foreignKey       Base foreign key
   * @param generationLinks  Foreign key relations for data generation
   * @param cardinality      Cardinality configuration builder
   * @return SinkOptionsBuilder
   */
  def foreignKey(foreignKey: ForeignKeyRelation,
                 generationLinks: List[ForeignKeyRelation],
                 cardinality: CardinalityConfigBuilder): SinkOptionsBuilder =
    this.foreignKey(foreignKey, generationLinks, List(), None, Some(cardinality.config), None, None)

  /**
   * Define a foreign key relationship with nullability configuration.
   *
   * @param foreignKey       Base foreign key
   * @param generationLinks  Foreign key relations for data generation
   * @param nullability      Nullability configuration builder
   * @return SinkOptionsBuilder
   */
  def foreignKey(foreignKey: ForeignKeyRelation,
                 generationLinks: List[ForeignKeyRelation],
                 nullability: NullabilityConfigBuilder): SinkOptionsBuilder =
    this.foreignKey(foreignKey, generationLinks, List(), None, None, Some(nullability.config), None)

  /**
   * Define a foreign key relationship with cardinality and nullability configuration.
   *
   * @param foreignKey       Base foreign key
   * @param generationLinks  Foreign key relations for data generation
   * @param cardinality      Cardinality configuration builder
   * @param nullability      Nullability configuration builder
   * @return SinkOptionsBuilder
   */
  def foreignKey(foreignKey: ForeignKeyRelation,
                 generationLinks: List[ForeignKeyRelation],
                 cardinality: CardinalityConfigBuilder,
                 nullability: NullabilityConfigBuilder): SinkOptionsBuilder =
    this.foreignKey(foreignKey, generationLinks, List(), None, Some(cardinality.config), Some(nullability.config), None)

  /**
   * Define a many-to-many relationship using junction table pattern.
   *
   * @param leftSource       Left side of the relationship (e.g., students)
   * @param rightSource      Right side of the relationship (e.g., courses)
   * @param junctionTable    Junction/bridge table (e.g., enrollments)
   * @param leftCardinality  Optional cardinality config for left side
   * @param rightCardinality Optional cardinality config for right side
   * @return SinkOptionsBuilder
   */
  def manyToManyRelationship(leftSource: ForeignKeyRelation,
                             rightSource: ForeignKeyRelation,
                             junctionTable: ForeignKeyRelation,
                             leftCardinality: Option[CardinalityConfig] = None,
                             rightCardinality: Option[CardinalityConfig] = None): SinkOptionsBuilder = {
    // Create two foreign key relationships: left->junction and right->junction
    // Extract fields from junction table relation (assumes format like "junction_table_fields")
    this
      .foreignKey(leftSource, List(junctionTable), List(), None, leftCardinality, None, None)
      .foreignKey(rightSource, List(junctionTable), List(), None, rightCardinality, None, None)
  }
}
