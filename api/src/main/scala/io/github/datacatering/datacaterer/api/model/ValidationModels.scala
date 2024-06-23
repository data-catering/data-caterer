package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_SUM, DEFAULT_VALIDATION_COLUMN_NAME_TYPE, DEFAULT_VALIDATION_CONFIG_NAME, DEFAULT_VALIDATION_DESCRIPTION, DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_METHOD, DEFAULT_VALIDATION_WEBHOOK_HTTP_STATUS_CODES, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.{CombinationPreFilterBuilder, ValidationBuilder}


@JsonSubTypes(Array(
  new Type(value = classOf[YamlUpstreamDataSourceValidation]),
  new Type(value = classOf[GroupByValidation]),
  new Type(value = classOf[ColumnNamesValidation]),
  new Type(value = classOf[ExpressionValidation]),
))
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
@JsonIgnoreProperties(ignoreUnknown = true)
trait Validation {
  var description: Option[String] = None
  @JsonDeserialize(contentAs = classOf[java.lang.Double]) var errorThreshold: Option[Double] = None
  var preFilter: Option[CombinationPreFilterBuilder] = None
  var preFilterExpr: Option[String] = None

  def toOptions: List[List[String]]

  def baseOptions: List[List[String]] = {
    val descriptionOption = description.map(d => List(List("description", d))).getOrElse(List())
    val errorThresholdOption = errorThreshold.map(e => List(List("errorThreshold", e.toString))).getOrElse(List())
    val preFilterOptions = getPreFilterExpression match {
      case Some(preFilterStringExpr) => List(List("preFilterExpr", preFilterStringExpr))
      case _ => List()
    }
    descriptionOption ++ errorThresholdOption ++ preFilterOptions
  }

  def getPreFilterExpression: Option[String] = {
    (preFilterExpr, preFilter) match {
      case (Some(preFilterStringExpr), _) => Some(preFilterStringExpr)
      case (_, Some(preFilterBuilder)) =>
        if (preFilterBuilder.validate()) Some(preFilterBuilder.toExpression) else None
      case _ => None
    }
  }
}

case class ExpressionValidation(
                                 expr: String = "true",
                                 selectExpr: List[String] = List("*")
                               ) extends Validation {
  override def toOptions: List[List[String]] = List(
    List("selectExpr", selectExpr.mkString(", ")),
    List("expr", expr),
  ) ++ baseOptions
}

case class GroupByValidation(
                              groupByCols: Seq[String] = Seq(),
                              aggCol: String = "",
                              aggType: String = AGGREGATION_SUM,
                              aggExpr: String = "true"
                            ) extends Validation {
  override def toOptions: List[List[String]] = List(
    List("aggExpr", aggExpr),
    List("groupByColumns", groupByCols.mkString(",")),
    List("aggCol", aggCol),
    List("aggType", aggType),
  ) ++ baseOptions
}

case class UpstreamDataSourceValidation(
                                         validation: ValidationBuilder = ValidationBuilder(),
                                         upstreamDataSource: ConnectionTaskBuilder[_] = FileBuilder(),
                                         upstreamReadOptions: Map[String, String] = Map(),
                                         joinColumns: List[String] = List(),
                                         joinType: String = DEFAULT_VALIDATION_JOIN_TYPE,
                                       ) extends Validation {
  override def toOptions: List[List[String]] = {
    val nestedValidation = validation.validation.toOptions
    List(
      List("upstreamDataSource", upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName),
      List("upstreamReadOptions", upstreamReadOptions.mkString(", ")),
      List("joinColumns", joinColumns.mkString(",")),
      List("joinType", joinType),
    ) ++ nestedValidation ++ baseOptions
  }
}

case class YamlUpstreamDataSourceValidation(
                                             upstreamDataSource: String,
                                             validation: Validation = ExpressionValidation(),
                                             upstreamReadOptions: Map[String, String] = Map(),
                                             joinColumns: List[String] = List(),
                                             joinType: String = DEFAULT_VALIDATION_JOIN_TYPE,
                                           ) extends Validation {
  override def toOptions: List[List[String]] = List()
}

case class ColumnNamesValidation(
                                  columnNameType: String = DEFAULT_VALIDATION_COLUMN_NAME_TYPE,
                                  count: Int = 0,
                                  minCount: Int = 0,
                                  maxCount: Int = 0,
                                  names: Array[String] = Array()
                                ) extends Validation {
  override def toOptions: List[List[String]] = {
    val baseAttributes = columnNameType match {
      case VALIDATION_COLUMN_NAME_COUNT_EQUAL => List(List("count", count.toString))
      case VALIDATION_COLUMN_NAME_COUNT_BETWEEN => List(List("minCount", minCount.toString), List("maxCount", maxCount.toString))
      case VALIDATION_COLUMN_NAME_MATCH_ORDER => List(List("names", names.mkString(",")))
      case VALIDATION_COLUMN_NAME_MATCH_SET => List(List("names", names.mkString(",")))
    }
    List(List("columnNameValidationType", columnNameType)) ++ baseAttributes ++ baseOptions
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

case class YamlValidationConfiguration(
                                        name: String = DEFAULT_VALIDATION_CONFIG_NAME,
                                        description: String = DEFAULT_VALIDATION_DESCRIPTION,
                                        dataSources: Map[String, List[YamlDataSourceValidation]] = Map()
                                      )

case class YamlDataSourceValidation(
                                     options: Map[String, String] = Map(),
                                     waitCondition: WaitCondition = PauseWaitCondition(),
                                     validations: List[Validation] = List()
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

object ConditionType extends Enumeration {
  type ConditionType = Value
  val AND, OR = Value
}