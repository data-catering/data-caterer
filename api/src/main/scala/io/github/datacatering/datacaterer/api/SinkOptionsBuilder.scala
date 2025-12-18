package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.model.{ForeignKey, ForeignKeyRelation, SinkOptions}

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

}
