package com.github.pflooky.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonTypeIdResolver}
import com.github.pflooky.datacaterer.api.ValidationBuilder
import com.github.pflooky.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_SUM, DEFAULT_VALIDATION_COLUMN_NAME_TYPE, DEFAULT_VALIDATION_CONFIG_NAME, DEFAULT_VALIDATION_DESCRIPTION, DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD, DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES}
import com.github.pflooky.datacaterer.api.parser.ValidationIdResolver


@JsonTypeInfo(use = Id.CUSTOM, defaultImpl = classOf[ExpressionValidation])
@JsonTypeIdResolver(classOf[ValidationIdResolver])
@JsonIgnoreProperties(ignoreUnknown = true)
trait Validation {
  var description: Option[String] = None
  @JsonDeserialize(contentAs = classOf[java.lang.Double]) var errorThreshold: Option[Double] = None
}

case class ExpressionValidation(
                                 whereExpr: String = "true",
                                 selectExpr: List[String] = List("*")
                               ) extends Validation

case class GroupByValidation(
                              groupByCols: Seq[String] = Seq(),
                              aggCol: String = "",
                              aggType: String = AGGREGATION_SUM,
                              expr: String = "true"
                            ) extends Validation

case class UpstreamDataSourceValidation(
                                         validationBuilder: ValidationBuilder = ValidationBuilder(),
                                         upstreamDataSource: ConnectionTaskBuilder[_] = FileBuilder(),
                                         upstreamReadOptions: Map[String, String] = Map(),
                                         joinCols: List[String] = List(),
                                         joinType: String = DEFAULT_VALIDATION_JOIN_TYPE,
                                       ) extends Validation

case class ColumnNamesValidation(
                                  `type`: String = DEFAULT_VALIDATION_COLUMN_NAME_TYPE,
                                  count: Int = 0,
                                  minCount: Int = 0,
                                  maxCount: Int = 0,
                                  names: Array[String] = Array()
                                ) extends Validation

case class ValidationConfiguration(
                                    name: String = DEFAULT_VALIDATION_CONFIG_NAME,
                                    description: String = DEFAULT_VALIDATION_DESCRIPTION,
                                    dataSources: Map[String, List[DataSourceValidation]] = Map()
                                  )

case class DataSourceValidation(
                                 options: Map[String, String] = Map(),
                                 waitCondition: WaitCondition = PauseWaitCondition(),
                                 validations: List[ValidationBuilder] = List()
                               )

trait WaitCondition {
  val isRetryable: Boolean = true
  val maxRetries: Int = 10
  val waitBeforeRetrySeconds: Int = 2
}

case class PauseWaitCondition(
                               pauseInSeconds: Int = 0,
                             ) extends WaitCondition {
  override val isRetryable: Boolean = false
}

case class FileExistsWaitCondition(
                                    path: String,
                                  ) extends WaitCondition

case class DataExistsWaitCondition(
                                    dataSourceName: String,
                                    options: Map[String, String],
                                    expr: String,
                                  ) extends WaitCondition

case class WebhookWaitCondition(
                                 dataSourceName: String,
                                 url: String,
                                 method: String = DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD,
                                 statusCodes: List[Int] = DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES
                               ) extends WaitCondition