package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.github.datacatering.datacaterer.api.model.Constants.{DEFAULT_COUNT_RECORDS, DEFAULT_DATA_SOURCE_NAME, DEFAULT_FIELD_NAME, DEFAULT_FIELD_NULLABLE, DEFAULT_FIELD_TYPE, DEFAULT_GENERATOR_TYPE, DEFAULT_PER_FIELD_COUNT_RECORDS, DEFAULT_STEP_ENABLED, DEFAULT_STEP_NAME, DEFAULT_STEP_TYPE, DEFAULT_TASK_NAME, DEFAULT_TASK_SUMMARY_ENABLE, FOREIGN_KEY_DELIMITER}

import scala.language.implicitConversions

case class Plan(
                 name: String = "default_plan",
                 description: String = "Data generation plan",
                 tasks: List[TaskSummary] = List(),
                 sinkOptions: Option[SinkOptions] = None,
                 validations: List[String] = List(),
                 runId: Option[String] = None
               )

case class SinkOptions(
                        seed: Option[String] = None,
                        locale: Option[String] = None,
                        foreignKeys: List[ForeignKey] = List()
                      )

case class ForeignKeyRelation(
                               dataSource: String = DEFAULT_DATA_SOURCE_NAME,
                               step: String = DEFAULT_STEP_NAME,
                               fields: List[String] = List()
                             ) {

  def this(dataSource: String, step: String, field: String) = this(dataSource, step, List(field))

  override def toString: String = s"$dataSource$FOREIGN_KEY_DELIMITER$step$FOREIGN_KEY_DELIMITER${fields.mkString(",")}"
}

case class ForeignKey(
                       source: ForeignKeyRelation = ForeignKeyRelation(),
                       generate: List[ForeignKeyRelation] = List(),
                       delete: List[ForeignKeyRelation] = List(),
                     )

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
                 fields: List[Field] = List(),
                 enabled: Boolean = DEFAULT_STEP_ENABLED
               )

case class Count(
                  @JsonDeserialize(contentAs = classOf[java.lang.Long]) records: Option[Long] = Some(DEFAULT_COUNT_RECORDS),
                  perField: Option[PerFieldCount] = None,
                  options: Map[String, Any] = Map()
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
