package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import io.github.datacatering.datacaterer.api.model.Constants._
import io.github.datacatering.datacaterer.api.{CombinationPreFilterBuilder, ValidationBuilder}


@JsonSubTypes(Array(
  new Type(value = classOf[YamlUpstreamDataSourceValidation]),
  new Type(value = classOf[GroupByValidation]),
  new Type(value = classOf[FieldNamesValidation]),
  new Type(value = classOf[ExpressionValidation]),
  new Type(value = classOf[FieldValidations]),
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
                              groupByFields: Seq[String] = Seq(),
                              aggField: String = "",
                              aggType: String = AGGREGATION_SUM,
                              aggExpr: String = "true",
                              validation: List[FieldValidation] = List()
                            ) extends Validation {
  override def toOptions: List[List[String]] = {
    List(
      List("aggExpr", aggExpr),
      List("groupByFields", groupByFields.mkString(",")),
      List("aggField", aggField),
      List("aggType", aggType),
      List("validation", validation.map(_.toString).mkString(",")),
    ) ++ baseOptions
  }
}

case class UpstreamDataSourceValidation(
                                         validations: List[ValidationBuilder] = List(),
                                         upstreamDataSource: ConnectionTaskBuilder[_] = FileBuilder(),
                                         upstreamReadOptions: Map[String, String] = Map(),
                                         joinFields: List[String] = List(),
                                         joinType: String = DEFAULT_VALIDATION_JOIN_TYPE,
                                       ) extends Validation {
  override def toOptions: List[List[String]] = {
    val nestedValidation = validations.flatMap(_.validation.toOptions)
    List(
      List("upstreamDataSource", upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName),
      List("upstreamReadOptions", upstreamReadOptions.mkString(", ")),
      List("joinFields", joinFields.mkString(",")),
      List("joinType", joinType),
    ) ++ nestedValidation ++ baseOptions
  }
}

case class YamlUpstreamDataSourceValidation(
                                             upstreamDataSource: String = "",
                                             upstreamTaskName: String = "",
                                             validation: List[Validation] = List(),
                                             upstreamReadOptions: Map[String, String] = Map(),
                                             joinFields: List[String] = List(),
                                             joinType: String = DEFAULT_VALIDATION_JOIN_TYPE,
                                           ) extends Validation {
  override def toOptions: List[List[String]] = List()
}

case class FieldNamesValidation(
                                 fieldNameType: String = DEFAULT_VALIDATION_FIELD_NAME_TYPE,
                                 count: Int = 0,
                                 min: Int = 0,
                                 max: Int = 0,
                                 names: Array[String] = Array()
                               ) extends Validation {
  override def toOptions: List[List[String]] = {
    val baseAttributes = fieldNameType match {
      case VALIDATION_FIELD_NAME_COUNT_EQUAL => List(List("count", count.toString))
      case VALIDATION_FIELD_NAME_COUNT_BETWEEN => List(List("min", min.toString), List("max", max.toString))
      case VALIDATION_FIELD_NAME_MATCH_ORDER => List(List("names", names.mkString(",")))
      case VALIDATION_FIELD_NAME_MATCH_SET => List(List("names", names.mkString(",")))
    }
    List(List("fieldNameType", fieldNameType)) ++ baseAttributes ++ baseOptions
  }
}

case class FieldValidations(
                             field: String = "",
                             validation: List[FieldValidation] = List()
                           ) extends Validation {
  override def toOptions: List[List[String]] = List(
    List("field", field),
    List("validation", validation.map(_.toString).mkString(",")),
  ) ++ baseOptions
}

@JsonSubTypes(Array(
  new Type(value = classOf[EqualFieldValidation], name = "equal"),
  new Type(value = classOf[NullFieldValidation], name = "null"),
  new Type(value = classOf[ContainsFieldValidation], name = "contains"),
  new Type(value = classOf[UniqueFieldValidation], name = "unique"),
  new Type(value = classOf[LessThanFieldValidation], name = "lessThan"),
  new Type(value = classOf[GreaterThanFieldValidation], name = "greaterThan"),
  new Type(value = classOf[BetweenFieldValidation], name = "between"),
  new Type(value = classOf[InFieldValidation], name = "in"),
  new Type(value = classOf[MatchesFieldValidation], name = "matches"),
  new Type(value = classOf[MatchesListFieldValidation], name = "matchesList"),
  new Type(value = classOf[StartsWithFieldValidation], name = "startsWith"),
  new Type(value = classOf[EndsWithFieldValidation], name = "endsWith"),
  new Type(value = classOf[SizeFieldValidation], name = "size"),
  new Type(value = classOf[LessThanSizeFieldValidation], name = "lessThanSize"),
  new Type(value = classOf[GreaterThanSizeFieldValidation], name = "greaterThanSize"),
  new Type(value = classOf[LuhnCheckFieldValidation], name = "luhnCheck"),
  new Type(value = classOf[HasTypeFieldValidation], name = "hasType"),
  new Type(value = classOf[HasTypesFieldValidation], name = "hasTypes"),
  new Type(value = classOf[DistinctInSetFieldValidation], name = "distinctInSet"),
  new Type(value = classOf[DistinctContainsSetFieldValidation], name = "distinctContainsSet"),
  new Type(value = classOf[DistinctEqualFieldValidation], name = "distinctEqual"),
  new Type(value = classOf[MaxBetweenFieldValidation], name = "maxBetween"),
  new Type(value = classOf[MeanBetweenFieldValidation], name = "meanBetween"),
  new Type(value = classOf[MedianBetweenFieldValidation], name = "medianBetween"),
  new Type(value = classOf[MinBetweenFieldValidation], name = "minBetween"),
  new Type(value = classOf[StdDevBetweenFieldValidation], name = "stdDevBetween"),
  new Type(value = classOf[SumBetweenFieldValidation], name = "sumBetween"),
  new Type(value = classOf[LengthBetweenFieldValidation], name = "lengthBetween"),
  new Type(value = classOf[LengthEqualFieldValidation], name = "lengthEqual"),
  new Type(value = classOf[IsDecreasingFieldValidation], name = "isDecreasing"),
  new Type(value = classOf[IsIncreasingFieldValidation], name = "isIncreasing"),
  new Type(value = classOf[IsJsonParsableFieldValidation], name = "isJsonParsable"),
  new Type(value = classOf[MatchJsonSchemaFieldValidation], name = "matchJsonSchema"),
  new Type(value = classOf[MatchDateTimeFormatFieldValidation], name = "matchDateTimeFormat"),
  new Type(value = classOf[MostCommonValueInSetFieldValidation], name = "mostCommonValueInSet"),
  new Type(value = classOf[UniqueValuesProportionBetweenFieldValidation], name = "uniqueValuesProportionBetween"),
  new Type(value = classOf[QuantileValuesBetweenFieldValidation], name = "quantileValuesBetween"),
))
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type")
@JsonIgnoreProperties(ignoreUnknown = true)
trait FieldValidation extends Validation {

  val `type`: String

  override def toOptions: List[List[String]] = List(
    List("type", `type`)
  )
}

case class NullFieldValidation(negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_NULL
}

case class EqualFieldValidation(value: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_EQUAL
}

case class ContainsFieldValidation(value: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_CONTAINS
}

case class UniqueFieldValidation(negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_UNIQUE
}

case class BetweenFieldValidation(min: Double, max: Double, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_BETWEEN
}

case class LessThanFieldValidation(value: Any, strictly: Boolean = true) extends FieldValidation {
  override val `type`: String = VALIDATION_LESS_THAN
}

case class GreaterThanFieldValidation(value: Any, strictly: Boolean = true) extends FieldValidation {
  override val `type`: String = VALIDATION_GREATER_THAN
}

case class InFieldValidation(values: List[Any], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_IN
}

case class MatchesFieldValidation(regex: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MATCHES
}

case class MatchesListFieldValidation(regexes: List[String], matchAll: Boolean = true, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MATCHES_LIST
}

case class StartsWithFieldValidation(value: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_STARTS_WITH
}

case class EndsWithFieldValidation(value: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_ENDS_WITH
}

case class SizeFieldValidation(size: Int, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_SIZE
}

case class LessThanSizeFieldValidation(size: Int, strictly: Boolean = true) extends FieldValidation {
  override val `type`: String = VALIDATION_LESS_THAN_SIZE
}

case class GreaterThanSizeFieldValidation(size: Int, strictly: Boolean = true) extends FieldValidation {
  override val `type`: String = VALIDATION_GREATER_THAN_SIZE
}

case class LuhnCheckFieldValidation(negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_LUHN_CHECK
}

case class HasTypeFieldValidation(value: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_HAS_TYPE
}

case class HasTypesFieldValidation(values: List[String], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_HAS_TYPES
}

case class DistinctInSetFieldValidation(values: List[Any], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_DISTINCT_IN_SET
}

case class DistinctContainsSetFieldValidation(values: List[Any], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_DISTINCT_CONTAINS_SET
}

case class DistinctEqualFieldValidation(values: List[Any], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_DISTINCT_EQUAL
}

case class MaxBetweenFieldValidation(min: Any, max: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MAX_BETWEEN
}

case class MeanBetweenFieldValidation(min: Any, max: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MEAN_BETWEEN
}

case class MedianBetweenFieldValidation(min: Any, max: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MEDIAN_BETWEEN
}

case class MinBetweenFieldValidation(min: Any, max: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MIN_BETWEEN
}

case class StdDevBetweenFieldValidation(min: Any, max: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_STD_DEV_BETWEEN
}

case class SumBetweenFieldValidation(min: Any, max: Any, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_SUM_BETWEEN
}

case class LengthBetweenFieldValidation(min: Int, max: Int, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_LENGTH_BETWEEN
}

case class LengthEqualFieldValidation(value: Int, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_LENGTH_EQUAL
}

case class IsDecreasingFieldValidation(strictly: Boolean = true) extends FieldValidation {
  override val `type`: String = VALIDATION_IS_DECREASING
}

case class IsIncreasingFieldValidation(strictly: Boolean = true) extends FieldValidation {
  override val `type`: String = VALIDATION_IS_INCREASING
}

case class IsJsonParsableFieldValidation(negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_IS_JSON_PARSABLE
}

case class MatchJsonSchemaFieldValidation(schema: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MATCH_JSON_SCHEMA
}

case class MatchDateTimeFormatFieldValidation(format: String, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MATCH_DATE_TIME_FORMAT
}

case class MostCommonValueInSetFieldValidation(values: List[Any], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_MOST_COMMON_VALUE_IN_SET
}

case class UniqueValuesProportionBetweenFieldValidation(min: Double, max: Double, negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_UNIQUE_VALUES_PROPORTION_BETWEEN
}

case class QuantileValuesBetweenFieldValidation(quantileRanges: Map[Double, (Double, Double)], negate: Boolean = false) extends FieldValidation {
  override val `type`: String = VALIDATION_QUANTILE_VALUES_BETWEEN
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

@JsonSubTypes(Array(
  new Type(value = classOf[PauseWaitCondition]),
  new Type(value = classOf[FileExistsWaitCondition]),
  new Type(value = classOf[DataExistsWaitCondition]),
  new Type(value = classOf[WebhookWaitCondition]),
))
@JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
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