package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import Constants.{DEFAULT_COUNT_RECORDS, DEFAULT_DATA_SOURCE_NAME, DEFAULT_FIELD_NAME, DEFAULT_FIELD_NULLABLE, DEFAULT_FIELD_TYPE, DEFAULT_GENERATOR_TYPE, DEFAULT_PER_COLUMN_COUNT_RECORDS, DEFAULT_STEP_ENABLED, DEFAULT_STEP_NAME, DEFAULT_STEP_TYPE, DEFAULT_TASK_NAME, DEFAULT_TASK_SUMMARY_ENABLE, FOREIGN_KEY_DELIMITER}

import scala.language.implicitConversions

case class Plan(
                 name: String = "Default plan",
                 description: String = "Data generation plan",
                 tasks: List[TaskSummary] = List(),
                 sinkOptions: Option[SinkOptions] = None,
                 validations: List[String] = List()
               )

case class SinkOptions(
                        seed: Option[String] = None,
                        locale: Option[String] = None,
                        foreignKeys: List[(String, List[String])] = List()
                      )

case class ForeignKeyRelation(
                               dataSource: String = DEFAULT_DATA_SOURCE_NAME,
                               step: String = DEFAULT_STEP_NAME,
                               columns: List[String] = List()
                             ) {

  def this(dataSource: String, step: String, column: String) = this(dataSource, step, List(column))

  override def toString: String = s"$dataSource$FOREIGN_KEY_DELIMITER$step$FOREIGN_KEY_DELIMITER${columns.mkString(",")}"
}

case class TaskSummary(
                        name: String,
                        dataSourceName: String,
                        enabled: Boolean = DEFAULT_TASK_SUMMARY_ENABLE
                      )

case class Task(
                 name: String = DEFAULT_TASK_NAME,
                 steps: List[Step] = List()
               )

case class Step(
                 name: String = DEFAULT_STEP_NAME,
                 `type`: String = DEFAULT_STEP_TYPE,
                 count: Count = Count(),
                 options: Map[String, String] = Map(),
                 schema: Schema = Schema(),
                 enabled: Boolean = DEFAULT_STEP_ENABLED
               )

case class Count(
                  @JsonDeserialize(contentAs = classOf[java.lang.Long]) records: Option[Long] = Some(DEFAULT_COUNT_RECORDS),
                  perColumn: Option[PerColumnCount] = None,
                  generator: Option[Generator] = None
                )

case class PerColumnCount(
                           columnNames: List[String] = List(),
                           @JsonDeserialize(contentAs = classOf[java.lang.Long]) count: Option[Long] = Some(DEFAULT_PER_COLUMN_COUNT_RECORDS),
                           generator: Option[Generator] = None
                         )

case class Schema(
                   fields: Option[List[Field]] = None
                 )

case class Field(
                  name: String = DEFAULT_FIELD_NAME,
                  `type`: Option[String] = Some(DEFAULT_FIELD_TYPE),
                  generator: Option[Generator] = Some(Generator()),
                  nullable: Boolean = DEFAULT_FIELD_NULLABLE,
                  static: Option[String] = None,
                  schema: Option[Schema] = None
                )

case class Generator(
                      `type`: String = DEFAULT_GENERATOR_TYPE,
                      options: Map[String, Any] = Map()
                    )
