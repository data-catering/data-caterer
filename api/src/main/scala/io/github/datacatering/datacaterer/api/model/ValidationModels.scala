package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonTypeInfo}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonTypeIdResolver}
import Constants.{AGGREGATION_SUM, DEFAULT_VALIDATION_COLUMN_NAME_TYPE, DEFAULT_VALIDATION_CONFIG_NAME, DEFAULT_VALIDATION_DESCRIPTION, DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD, DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import io.github.datacatering.datacaterer.api.parser.ValidationIdResolver


@JsonTypeInfo(use = Id.CUSTOM, defaultImpl = classOf[ExpressionValidation])
@JsonTypeIdResolver(classOf[ValidationIdResolver])
@JsonIgnoreProperties(ignoreUnknown = true)
trait Validation {
  var description: Option[String] = None
  @JsonDeserialize(contentAs = classOf[java.lang.Double]) var errorThreshold: Option[Double] = None

  def toOptions: List[List[String]]
}

case class ExpressionValidation(
                                 whereExpr: String = "true",
                                 selectExpr: List[String] = List("*")
                               ) extends Validation {
  override def toOptions: List[List[String]] = List(
    List("selectExpr", selectExpr.mkString(", ")),
    List("whereExpr", whereExpr),
    List("errorThreshold", errorThreshold.getOrElse(0.0).toString)
  )
}

case class GroupByValidation(
                              groupByCols: Seq[String] = Seq(),
                              aggCol: String = "",
                              aggType: String = AGGREGATION_SUM,
                              expr: String = "true"
                            ) extends Validation {
  override def toOptions: List[List[String]] = List(
    List("expr", expr),
    List("groupByColumns", groupByCols.mkString(",")),
    List("aggregationColumn", aggCol),
    List("aggregationType", aggType),
    List("errorThreshold", errorThreshold.getOrElse(0.0).toString)
  )
}

case class UpstreamDataSourceValidation(
                                         validationBuilder: ValidationBuilder = ValidationBuilder(),
                                         upstreamDataSource: ConnectionTaskBuilder[_] = FileBuilder(),
                                         upstreamReadOptions: Map[String, String] = Map(),
                                         joinCols: List[String] = List(),
                                         joinType: String = DEFAULT_VALIDATION_JOIN_TYPE,
                                       ) extends Validation {
  override def toOptions: List[List[String]] = {
    val nestedValidation = validationBuilder.validation.toOptions
    List(
      List("upstreamDataSource", upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName),
      List("joinColumns", joinCols.mkString(",")),
      List("joinType", joinType),
    ) ++ nestedValidation
  }
}

case class ColumnNamesValidation(
                                  `type`: String = DEFAULT_VALIDATION_COLUMN_NAME_TYPE,
                                  count: Int = 0,
                                  minCount: Int = 0,
                                  maxCount: Int = 0,
                                  names: Array[String] = Array()
                                ) extends Validation {
  override def toOptions: List[List[String]] = {
    val baseAttributes = `type` match {
      case VALIDATION_COLUMN_NAME_COUNT_EQUAL => List(List("count", count.toString))
      case VALIDATION_COLUMN_NAME_COUNT_BETWEEN => List(List("min", minCount.toString), List("max", maxCount.toString))
      case VALIDATION_COLUMN_NAME_MATCH_ORDER => List(List("matchOrder", names.mkString(",")))
      case VALIDATION_COLUMN_NAME_MATCH_SET => List(List("matchSet", names.mkString(",")))
    }
    List(List("columnNameValidationType", `type`)) ++ baseAttributes
  }
}

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