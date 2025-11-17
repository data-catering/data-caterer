package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_COUNT_RECORDS, DEFAULT_DATA_SOURCE_NAME, DEFAULT_FIELD_NAME, DEFAULT_FIELD_NULLABLE, DEFAULT_FIELD_TYPE, DEFAULT_PER_FIELD_COUNT_RECORDS, DEFAULT_PLAN_NAME, DEFAULT_STEP_ENABLED, DEFAULT_STEP_NAME, DEFAULT_STEP_TYPE, DEFAULT_TASK_NAME, DEFAULT_TASK_SUMMARY_ENABLE, FOREIGN_KEY_DELIMITER}

import java.util.UUID
import scala.language.implicitConversions

case class Plan(
                 name: String = DEFAULT_PLAN_NAME,
                 description: String = "Data generation plan",
                 tasks: List[TaskSummary] = List(),
                 sinkOptions: Option[SinkOptions] = None,
                 validations: List[String] = List(),
                 runId: Option[String] = Some(UUID.randomUUID().toString),
                 runInterface: Option[String] = None,
                 testType: Option[String] = None,
                 testConfig: Option[TestConfig] = None,
                 // New unified YAML format fields
                 connections: Option[List[Connection]] = None,
                 configuration: Option[Map[String, Any]] = None
               )

case class SinkOptions(
                        seed: Option[String] = None,
                        locale: Option[String] = None,
                        foreignKeys: List[ForeignKey] = List()
                      )

case class ForeignKeyRelation(
                               dataSource: String = DEFAULT_DATA_SOURCE_NAME,
                               step: String = DEFAULT_STEP_NAME,
                               fields: List[String] = List(),
                               cardinality: Option[CardinalityConfig] = None,
                               nullability: Option[NullabilityConfig] = None,
                               generationMode: Option[String] = None
                             ) {

  def this(dataSource: String, step: String, field: String) = this(dataSource, step, List(field))

  override def toString: String = s"$dataSource$FOREIGN_KEY_DELIMITER$step$FOREIGN_KEY_DELIMITER${fields.mkString(",")}"
}

case class ForeignKey(
                       source: ForeignKeyRelation = ForeignKeyRelation(),
                       generate: List[ForeignKeyRelation] = List(),
                       delete: List[ForeignKeyRelation] = List(),
                       relationshipType: Option[String] = None
                     )

/**
 * Configuration for controlling the cardinality of foreign key relationships.
 * Useful for specifying one-to-one, one-to-many ratios, and distribution patterns.
 *
 * @param min          Minimum number of related records per parent (default: no minimum)
 * @param max          Maximum number of related records per parent (default: no maximum)
 * @param ratio        Average ratio of child records per parent (e.g., 2.5 orders per customer)
 * @param distribution Distribution pattern for cardinality: "uniform", "normal", "zipf", "power"
 */
case class CardinalityConfig(
                              min: Option[Int] = None,
                              max: Option[Int] = None,
                              ratio: Option[Double] = None,
                              distribution: String = "uniform"
                            ) {
}

/**
 * Configuration for controlling nullable foreign keys (partial relationships).
 * Allows generating records where some have FKs and others don't.
 *
 * @param nullPercentage Percentage of records that should have null FK (0.0 to 1.0)
 * @param strategy       Strategy for selecting which records get null: "random", "head", "tail"
 */
case class NullabilityConfig(
                              nullPercentage: Double = 0.0,
                              strategy: String = "random"
                            ) {
  require(nullPercentage >= 0.0 && nullPercentage <= 1.0, "nullPercentage must be between 0.0 and 1.0")
}

/**
 * Many-to-many relationship configuration using junction table pattern.
 *
 * @param leftSource     Left side of the relationship (e.g., students)
 * @param rightSource    Right side of the relationship (e.g., courses)
 * @param junctionTable  Junction/bridge table (e.g., enrollments)
 * @param leftCardinality  Cardinality config for left side (e.g., courses per student)
 * @param rightCardinality Cardinality config for right side (e.g., students per course)
 */
case class ManyToManyRelation(
                               leftSource: ForeignKeyRelation,
                               rightSource: ForeignKeyRelation,
                               junctionTable: ForeignKeyRelation,
                               leftCardinality: Option[CardinalityConfig] = None,
                               rightCardinality: Option[CardinalityConfig] = None
                             ) {
}

@JsonIgnoreProperties(ignoreUnknown = true)
case class TaskSummary(
                        name: String,
                        dataSourceName: String = "",
                        enabled: Boolean = DEFAULT_TASK_SUMMARY_ENABLE,
                        weight: Option[Int] = None,
                        stage: Option[String] = None,
                        // Inline task definition fields (unified YAML format)
                        steps: Option[List[Step]] = None,
                        transformation: Option[TransformationConfig] = None,
                        @JsonDeserialize(using = classOf[ConnectionDeserializer])
                        connection: Option[Either[String, Connection]] = None,
                        validations: Option[List[Any]] = None
                      )

@JsonIgnoreProperties(ignoreUnknown = true)
case class Task(
                 name: String = DEFAULT_TASK_NAME,
                 steps: List[Step] = List(),
                 transformation: Option[TransformationConfig] = None
               )

case class Step(
                 name: String = DEFAULT_STEP_NAME,
                 `type`: String = DEFAULT_STEP_TYPE,
                 count: Count = Count(),
                 options: Map[String, String] = Map(),
                 fields: List[Field] = List(),
                 enabled: Boolean = DEFAULT_STEP_ENABLED,
                 transformation: Option[TransformationConfig] = None,
                 // New unified YAML format fields
                 validations: Option[List[Any]] = None  // Validation objects (field, expression, groupBy, metric)
               )

case class Count(
                  @JsonDeserialize(contentAs = classOf[java.lang.Long]) records: Option[Long] = Some(DEFAULT_COUNT_RECORDS),
                  perField: Option[PerFieldCount] = None,
                  options: Map[String, Any] = Map(),
                  duration: Option[String] = None,
                  rate: Option[Int] = None,
                  rateUnit: Option[String] = None,
                  pattern: Option[LoadPattern] = None
                )

case class PerFieldCount(
                          fieldNames: List[String] = List(),
                          @JsonDeserialize(contentAs = classOf[java.lang.Long]) count: Option[Long] = Some(DEFAULT_PER_FIELD_COUNT_RECORDS),
                          options: Map[String, Any] = Map()
                        )

case class Field(
                  name: String = DEFAULT_FIELD_NAME,
                  `type`: Option[String] = Some(DEFAULT_FIELD_TYPE),
                  options: Map[String, Any] = Map(),
                  nullable: Boolean = DEFAULT_FIELD_NULLABLE,
                  static: Option[String] = None,
                  fields: List[Field] = List()
                )

case class TestConfig(
                       executionMode: Option[String] = None,
                       warmup: Option[String] = None,
                       cooldown: Option[String] = None
                     )

case class LoadPattern(
                        `type`: String,
                        startRate: Option[Int] = None,
                        endRate: Option[Int] = None,
                        baseRate: Option[Int] = None,
                        spikeRate: Option[Int] = None,
                        spikeStart: Option[Double] = None,
                        spikeDuration: Option[Double] = None,
                        steps: Option[List[LoadPatternStep]] = None,
                        amplitude: Option[Int] = None,
                        frequency: Option[Double] = None,
                        rateIncrement: Option[Int] = None,
                        incrementInterval: Option[String] = None,
                        maxRate: Option[Int] = None
                      )

case class LoadPatternStep(
                            rate: Int,
                            duration: String
                          )

/**
 * Connection configuration for unified YAML format.
 * Can be used inline in tasks or defined in the connections section of a plan.
 *
 * @param name       Optional connection name for reuse
 * @param `type`     Connection type (postgres, csv, json, kafka, http, etc.)
 * @param options    Connection options including url, credentials, format-specific settings, etc.
 *                   Common options: url, user, password, driver, path, header, delimiter, etc.
 */
case class Connection(
                       name: Option[String] = None,
                       `type`: String = "",
                       options: Map[String, String] = Map()
                     )

