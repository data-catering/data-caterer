package io.github.datacatering.datacaterer.api

import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.model.{ForeignKeyRelation, SinkOptions}

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
   * Define a foreign key relationship between columns across any data source.
   * To define which column to use, it is defined by the following:<br>
   * dataSourceName + stepName + columnName
   *
   * @param foreignKey Base foreign key
   * @param relations  Foreign key relations
   * @return SinkOptionsBuilder
   * @see <a href="https://data.catering/setup/foreign-key/">Docs</a> for details
   */
  @varargs def foreignKey(foreignKey: ForeignKeyRelation, relations: ForeignKeyRelation*): SinkOptionsBuilder =
    this.modify(_.sinkOptions.foreignKeys)(_ ++ Map(foreignKey.toString -> relations.map(_.toString).toList))

  /**
   * Define a foreign key relationship between columns across any data source.
   * To define which column to use, it is defined by the following:<br>
   * dataSourceName + stepName + columnName
   *
   * @param foreignKey Base foreign key
   * @param relations  Foreign key relations
   * @return SinkOptionsBuilder
   * @see <a href="https://data.catering/setup/foreign-key/">Docs</a> for details
   */
  def foreignKey(foreignKey: ForeignKeyRelation, relations: List[ForeignKeyRelation]): SinkOptionsBuilder =
    this.foreignKey(foreignKey, relations: _*)
}
