package io.github.datacatering.datacaterer.api

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.softwaremill.quicklens.ModifyPimp
import io.github.datacatering.datacaterer.api.ValidationHelper.cleanFieldName
import io.github.datacatering.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import io.github.datacatering.datacaterer.api.converter.Converters.{toScalaList, toScalaMap}
import io.github.datacatering.datacaterer.api.model.ConditionType.ConditionType
import io.github.datacatering.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM, DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, VALIDATION_FIELD_NAME_COUNT_BETWEEN, VALIDATION_FIELD_NAME_COUNT_EQUAL, VALIDATION_FIELD_NAME_MATCH_ORDER, VALIDATION_FIELD_NAME_MATCH_SET, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import io.github.datacatering.datacaterer.api.model.{ConditionType, DataExistsWaitCondition, DataSourceValidation, ExpressionValidation, FieldNamesValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import io.github.datacatering.datacaterer.api.parser.ValidationBuilderSerializer

import java.sql.{Date, Timestamp}
import scala.annotation.varargs
import scala.util.{Failure, Success, Try}


case class ValidationConfigurationBuilder(validationConfiguration: ValidationConfiguration = ValidationConfiguration()) {
  def this() = this(ValidationConfiguration())

  def name(name: String): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.name).setTo(name)

  def description(description: String): ValidationConfigurationBuilder =
    this.modify(_.validationConfiguration.description).setTo(description)

  def addDataSourceValidation(
                               dataSourceName: String,
                               validations: Seq[DataSourceValidation]
                             ): ValidationConfigurationBuilder = {
    val mergedDataSourceValidations = mergeDataSourceValidations(dataSourceName, validations)
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> mergedDataSourceValidations))
  }

  def addDataSourceValidation(
                               dataSourceName: String,
                               validation: DataSourceValidationBuilder
                             ): ValidationConfigurationBuilder = {
    val mergedDataSourceValidations = mergeDataSourceValidations(dataSourceName, validation)
    this.modify(_.validationConfiguration.dataSources)(_ ++ Map(dataSourceName -> mergedDataSourceValidations))
  }

  @varargs def addValidations(
                               dataSourceName: String,
                               options: Map[String, String],
                               validations: ValidationBuilder*
                             ): ValidationConfigurationBuilder =
    addValidations(dataSourceName, options, WaitConditionBuilder(), validations: _*)

  @varargs def addValidations(
                               dataSourceName: String,
                               options: Map[String, String],
                               waitCondition: WaitConditionBuilder,
                               validationBuilders: ValidationBuilder*
                             ): ValidationConfigurationBuilder = {
    val newDsValidation = DataSourceValidationBuilder().options(options).wait(waitCondition).validations(validationBuilders: _*)
    addDataSourceValidation(dataSourceName, newDsValidation)
  }

  private def mergeDataSourceValidations(dataSourceName: String, validation: DataSourceValidationBuilder): List[DataSourceValidation] = {
    validationConfiguration.dataSources.get(dataSourceName)
      .map(listDsValidations => listDsValidations ++ List(validation.dataSourceValidation))
      .getOrElse(List(validation.dataSourceValidation))
  }

  private def mergeDataSourceValidations(dataSourceName: String, validations: Seq[DataSourceValidation]): List[DataSourceValidation] = {
    validationConfiguration.dataSources.get(dataSourceName)
      .map(listDsValidations => listDsValidations ++ validations)
      .getOrElse(validations.toList)
  }
}

case class DataSourceValidationBuilder(dataSourceValidation: DataSourceValidation = DataSourceValidation()) {
  def this() = this(DataSourceValidation())

  def options(options: Map[String, String]): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ options)

  def option(option: (String, String)): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ Map(option))

  @varargs def validations(validations: ValidationBuilder*): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.validations)(_ ++ validations)

  def validations(metadataSourceBuilder: MetadataSourceBuilder): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.options)(_ ++ metadataSourceBuilder.metadataSource.allOptions)

  def wait(waitCondition: WaitConditionBuilder): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition.waitCondition)

  def wait(waitCondition: WaitCondition): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition)
}

@JsonSerialize(using = classOf[ValidationBuilderSerializer])
case class ValidationBuilder(validation: Validation = ExpressionValidation(), optCombinationPreFilterBuilder: Option[CombinationPreFilterBuilder] = None) {
  def this() = this(ExpressionValidation(), None)

  def description(description: String): ValidationBuilder = {
    this.validation.description = Some(description)
    this
  }

  /**
   * Define the number of records or percentage of records that do not meet the validation rule before marking the validation
   * as failed. If no error threshold is defined, any failures will mark the whole validation as failed.<br>
   * For example, if there are 10 records and 4 have failed:<br>
   * {{{errorThreshold(2) #marked as failed as more than 2 records have failed}}}
   * {{{errorThreshold(0.1) #marked as failed as more than 10% of records have failed}}}
   * {{{errorThreshold(4) #marked as success as less than or equal to 4 records have failed}}}
   * {{{errorThreshold(0.4) #marked as success as less than or equal to 40% of records have failed}}}
   *
   * @param threshold Number or percentage of failed records which is acceptable before marking as failed
   * @return ValidationBuilder
   */
  def errorThreshold(threshold: Double): ValidationBuilder = {
    this.validation.errorThreshold = Some(threshold)
    this
  }

  /**
   * SQL expression used to check if data is adhering to specified condition. Return result from SQL expression is
   * required to be boolean. Can use any fields in the validation logic.
   *
   * For example,
   * {{{validation.expr("CASE WHEN status == 'open' THEN balance > 0 ELSE balance == 0 END")}}}
   *
   * @param expr SQL expression which returns a boolean
   * @return ValidationBuilder
   * @see <a href="https://spark.apache.org/docs/latest/api/sql/">SQL expressions</a>
   */
  def expr(expr: String): ValidationBuilder = {
    validation match {
      case GroupByValidation(grpFields, aggField, aggType, _, _) =>
        val grpWithExpr = GroupByValidation(grpFields, aggField, aggType, expr)
        copyWithDescAndThreshold(grpWithExpr)
      case expressionValidation: ExpressionValidation =>
        val withExpr = expressionValidation.modify(_.expr).setTo(expr)
        copyWithDescAndThreshold(withExpr)
      case _ => copyWithDescAndThreshold(ExpressionValidation(expr))
    }
  }

  /**
   * SQL expression used to apply to fields before running validations.
   *
   * For example,
   * {{{validation.selectExpr("PERCENTILE(amount, 0.5) AS median_amount", "*")}}}
   *
   * @param expr SQL expressions
   * @return ValidationBuilder
   * @see <a href="https://spark.apache.org/docs/latest/api/sql/">SQL expressions</a>
   */
  @varargs def selectExpr(expr: String*): ValidationBuilder = {
    validation match {
      case expressionValidation: ExpressionValidation =>
        val withExpr = expressionValidation.modify(_.selectExpr).setTo(expr.toList)
        copyWithDescAndThreshold(withExpr)
      case _ => copyWithDescAndThreshold(ExpressionValidation(selectExpr = expr.toList))
    }
  }

  /**
   * Define a field validation that can cover validations for any type of data.
   *
   * @param field Name of the field to run validation against
   * @return FieldValidationBuilder
   */
  def field(field: String): FieldValidationBuilder = {
    FieldValidationBuilder(this, cleanFieldName(field))
  }

  /**
   * Define fields to group by, so that validation can be run on grouped by dataset
   *
   * @param fields Name of the field to run validation against
   * @return FieldValidationBuilder
   */
  @varargs def groupBy(fields: String*): GroupByValidationBuilder = {
    GroupByValidationBuilder(this, fields)
  }

  /**
   * Check row count of dataset
   *
   * @return FieldValidationBuilder to apply validation on row count
   */
  def count(): FieldValidationBuilder = {
    GroupByValidationBuilder().count()
  }

  /**
   * Check if field(s) values are unique
   *
   * @param fields One or more fields whose values will be checked for uniqueness
   * @return ValidationBuilder
   */
  @varargs def unique(fields: String*): ValidationBuilder = {
    this.modify(_.validation).setTo(GroupByValidation(fields, VALIDATION_UNIQUE, AGGREGATION_COUNT))
      .expr("count == 1")
  }

  /**
   * Define validations based on data in another data source.
   *
   * @param connectionTaskBuilder Other data source
   * @return UpstreamDataSourceValidationBuilder
   */
  def upstreamData(connectionTaskBuilder: ConnectionTaskBuilder[_]): UpstreamDataSourceValidationBuilder = {
    UpstreamDataSourceValidationBuilder(List(this), connectionTaskBuilder)
  }

  /**
   * Define validation for field names of dataset.
   *
   * @return FieldNamesValidationBuilder
   */
  def fieldNames: FieldNamesValidationBuilder = {
    FieldNamesValidationBuilder()
  }

  def preFilter(combinationPreFilterBuilder: CombinationPreFilterBuilder): ValidationBuilder = {
    this.modify(_.optCombinationPreFilterBuilder).setTo(Some(combinationPreFilterBuilder))
  }

  def preFilter(validationBuilder: ValidationBuilder): ValidationBuilder = {
    this.modify(_.optCombinationPreFilterBuilder).setTo(Some(PreFilterBuilder().filter(validationBuilder)))
  }

  private def copyWithDescAndThreshold(newValidation: Validation): ValidationBuilder = {
    newValidation.description = this.validation.description
    newValidation.errorThreshold = this.validation.errorThreshold
    newValidation.preFilter = this.optCombinationPreFilterBuilder
    this.modify(_.validation).setTo(newValidation)
  }
}

case class FieldValidationBuilder(validationBuilder: ValidationBuilder = ValidationBuilder(), field: String = "") {
  def this() = this(ValidationBuilder(), "")

  /**
   * Check if field values are equal to a certain value
   *
   * @param value Expected value for all field values
   * @param negate Check if not equal to when set to true
   * @return
   */
  def isEqual(value: Any, negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!=" else "=="
    validationBuilder.expr(s"$field $sign ${fieldValueToString(value)}")
  }

  def isEqual(value: Any): ValidationBuilder = {
    isEqual(value, false)
  }

  /**
   * Check if field values are equal to another field for each record
   *
   * @param value Other field name
   * @param negate Check if not equal to field when set to true
   * @return
   */
  def isEqualField(value: String, negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!=" else "=="
    validationBuilder.expr(s"$field $sign $value")
  }

  def isEqualField(value: String): ValidationBuilder = {
    isEqualField(value, false)
  }

  /**
   * Check if field values are null
   *
   * @param negate Check if not null when set to true
   * @return
   */
  def isNull(negate: Boolean = false): ValidationBuilder = {
    val nullExpr = if (negate) "ISNOTNULL" else "ISNULL"
    validationBuilder.expr(s"$nullExpr($field)")
  }

  def isNull: ValidationBuilder = {
    isNull(false)
  }

  /**
   * Check if field values contain particular string (only for string type fields)
   *
   * @param value Expected string that field values contain
   * @param negate Check if not contains when set to true
   * @return
   */
  def contains(value: String, negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!" else ""
    validationBuilder.expr(s"${sign}CONTAINS($field, '$value')")
  }

  def contains(value: String): ValidationBuilder = {
    contains(value, false)
  }

  /**
   * Check if field values are less than certain value
   *
   * @param value Less than value
   * @param strictly Check if less than or equal to when set to true
   * @return
   */
  def lessThan(value: Any, strictly: Boolean = true): ValidationBuilder = {
    val sign = if (strictly) "<" else "<="
    validationBuilder.expr(s"$field $sign ${fieldValueToString(value)}")
  }

  def lessThan(value: Any): ValidationBuilder = {
    lessThan(value, true)
  }

  /**
   * Check if field values are less than another field's values for each record
   *
   * @param value Other field name
   * @param strictly Check if less than or equal to field when set to true
   * @return
   */
  def lessThanField(value: String, strictly: Boolean = true): ValidationBuilder = {
    val sign = if (strictly) "<" else "<="
    validationBuilder.expr(s"$field $sign $value")
  }

  def lessThanField(value: String): ValidationBuilder = {
    lessThanField(value, true)
  }

  /**
   * Check if field is greater than a certain value
   *
   * @param value Greater than value
   * @param strictly Check if greater than or equal to when set to true
   * @return
   */
  def greaterThan(value: Any, strictly: Boolean = true): ValidationBuilder = {
    val sign = if (strictly) ">" else ">="
    validationBuilder.expr(s"$field $sign ${fieldValueToString(value)}")
  }

  def greaterThan(value: Any): ValidationBuilder = {
    greaterThan(value, true)
  }

  /**
   * Check if field is greater than another field's values for each record
   *
   * @param value Other field name
   * @param strictly Check if greater than or equal to field when set to true
   * @return
   */
  def greaterThanField(value: String, strictly: Boolean = true): ValidationBuilder = {
    val sign = if (strictly) ">" else ">="
    validationBuilder.expr(s"$field $sign $value")
  }

  def greaterThanField(value: String): ValidationBuilder = {
    greaterThanField(value, true)
  }

  /**
   * Check if field values are between two values (inclusive)
   *
   * @param min Minimum value (inclusive)
   * @param max Maximum value (inclusive)
   * @param negate Check if not between when set to true
   * @return
   */
  def between(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder.expr(s"$field ${prefix}BETWEEN ${fieldValueToString(min)} AND ${fieldValueToString(max)}")
  }

  def between(min: Any, max: Any): ValidationBuilder = {
    between(min, max, false)
  }

  /**
   * Check if field values are between values of other fields (inclusive)
   *
   * @param min Other field name determining minimum value (inclusive)
   * @param max Other field name determining maximum value (inclusive)
   * @param negate Check if not between fields when set to true
   * @return
   */
  def betweenFields(min: String, max: String, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder.expr(s"$field ${prefix}BETWEEN $min AND $max")
  }

  def betweenFields(min: String, max: String): ValidationBuilder = {
    betweenFields(min, max, false)
  }

  /**
   * Check if field values are in given set of expected values
   *
   * @param values Expected set of values
   * @return
   */
  @varargs def in(values: Any*): ValidationBuilder = {
    in(values.toList, false)
  }

  /**
   * Check if field values are in given set of expected values
   *
   * @param values Expected set of values
   * @param negate Check if not in set of values when set to true
   * @return
   */
  def in(values: List[Any], negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder.expr(s"$prefix$field IN (${values.map(fieldValueToString).mkString(",")})")
  }

  /**
   * Check if field values match certain regex pattern (Java regular expression)
   *
   * @param regex Java regular expression
   * @param negate Check if not matches regex when set to true
   * @return
   */
  def matches(regex: String, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "!" else ""
    validationBuilder.expr(s"${prefix}REGEXP($field, '$regex')")
  }

  def matches(regex: String): ValidationBuilder = {
    matches(regex, false)
  }

  /**
   * Check if field values match certain regex patterns (Java regular expression)
   *
   * @param regexes Java regular expressions
   * @param matchAll Check if matches all defined regex patterns if set to true, only match at least one regex pattern when set to false
   * @param negate Check if not matches regex patterns when set to true
   * @return
   */
  def matchesList(regexes: List[String], matchAll: Boolean = true, negate: Boolean = false): ValidationBuilder = {
    val mkStringValue = if (matchAll) " AND " else " OR "
    val prefix = if (negate) "NOT " else ""
    val checkAllPatterns = regexes
      .map(regex => s"REGEXP($field, '$regex')")
      .mkString(mkStringValue)
    validationBuilder.expr(s"$prefix($checkAllPatterns)")
  }

  def matchesList(regexes: List[String]): ValidationBuilder = {
    matchesList(regexes, true, false)
  }

  /**
   * Check if field values start with certain string (only for string fields)
   *
   * @param value Expected prefix for string values
   * @param negate Check if not starts with when set to true
   * @return
   */
  def startsWith(value: String, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "!" else ""
    validationBuilder.expr(s"${prefix}STARTSWITH($field, '$value')")
  }

  def startsWith(value: String): ValidationBuilder = {
    startsWith(value, false)
  }

  /**
   * Check if field values end with certain string (only for string fields)
   *
   * @param value Expected suffix for string
   * @param negate Check if not ends with when set to true
   * @return
   */
  def endsWith(value: String, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "!" else ""
    validationBuilder.expr(s"${prefix}ENDSWITH($field, '$value')")
  }

  def endsWith(value: String): ValidationBuilder = {
    endsWith(value, false)
  }

  /**
   * Check if field size is equal to certain amount (only for array or map fields)
   *
   * @param size Expected size
   * @param negate Check if not size when set to true
   * @return
   */
  def size(size: Int, negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!=" else "=="
    validationBuilder.expr(s"SIZE($field) $sign $size")
  }

  def size(s: Int): ValidationBuilder = {
    size(s, false)
  }

  /**
   * Check if field size is less than certain amount (only for array or map fields)
   *
   * @param size     Less than size
   * @param strictly Set to true to check less than or equal to size
   * @return
   */
  def lessThanSize(size: Int, strictly: Boolean = true): ValidationBuilder = {
    val sign = if (strictly) "<" else "<="
    validationBuilder.expr(s"SIZE($field) $sign $size")
  }

  def lessThanSize(size: Int): ValidationBuilder = {
    lessThanSize(size, true)
  }

  /**
   * Check if field size is greater than certain amount (only for array or map fields)
   *
   * @param size     Greater than size
   * @param strictly Set to true to check greater than or equal to size
   * @return
   */
  def greaterThanSize(size: Int, strictly: Boolean = true): ValidationBuilder = {
    val sign = if (strictly) ">" else ">="
    validationBuilder.expr(s"SIZE($field) $sign $size")
  }

  def greaterThanSize(size: Int): ValidationBuilder = {
    greaterThanSize(size, true)
  }

  /**
   * Check if field values adhere to Luhn algorithm. Usually used for credit card or identification numbers.
   *
   * @param negate Check if not adheres to Luhn algorithm when set to true
   * @return
   */
  def luhnCheck(negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "!" else ""
    validationBuilder.expr(s"${prefix}LUHN_CHECK($field)")
  }

  def luhnCheck(): ValidationBuilder = {
    luhnCheck(false)
  }

  /**
   * Check if field values adhere to expected type
   *
   * @param value Expected data type
   * @param negate Check if not has type when set to true
   * @return
   */
  def hasType(value: String, negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!=" else "=="
    validationBuilder.expr(s"TYPEOF($field) $sign '${value}'")
  }

  def hasType(value: String): ValidationBuilder = {
    hasType(value, false)
  }

  /**
   * Check if field values adhere to expected types
   *
   * @param types Expected data types
   * @return
   */
  @varargs def hasTypes(types: String*): ValidationBuilder = {
    hasTypes(types.toList)
  }

  /**
   * Check if field values adhere to expected types
   *
   * @param types Expected data types
   * @param negate Check if not has any data type when set to true
   * @return
   */
  def hasTypes(types: List[String], negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder.expr(s"TYPEOF($field) ${prefix}IN (${types.map(t => s"'$t'").mkString(",")})")
  }

  def hasTypes(types: java.util.List[String], negate: Boolean): ValidationBuilder = {
    hasTypes(toScalaList(types), negate)
  }

  /**
   * Check if distinct values of field exist in set
   *
   * @param values Expected set of distinct values
   * @return
   */
  @varargs def distinctInSet(values: Any*): ValidationBuilder = {
    distinctInSet(values.toList, false)
  }

  /**
   * Check if distinct values of field exist in set
   *
   * @param values Expected set of distinct values
   * @param negate Check if distinct values are not in set when set to true
   * @return
   */
  def distinctInSet(values: List[Any], negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!" else ""
    val removeTicksField = field.replaceAll("`", "")
    validationBuilder.selectExpr(s"COLLECT_SET($field) AS ${removeTicksField}_distinct")
      .expr(s"${sign}FORALL(${removeTicksField}_distinct, x -> ARRAY_CONTAINS(ARRAY(${seqToString(values)}), x))")
  }

  def distinctInSet(values: java.util.List[Any], negate: Boolean): ValidationBuilder = {
    distinctInSet(toScalaList(values), negate)
  }

  /**
   * Check if distinct values of field contains set
   *
   * @param values Expected contained set of distinct values
   * @return
   */
  @varargs def distinctContainsSet(values: Any*): ValidationBuilder = {
    distinctContainsSet(values.toList, false)
  }

  /**
   * Check if distinct values of field contains set
   *
   * @param values Expected contained set of distinct values
   * @param negate Check if distinct values does not contain set when set to true
   * @return
   */
  def distinctContainsSet(values: List[Any], negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!" else ""
    validationBuilder.selectExpr(s"COLLECT_SET($field) AS ${removeTicksField}_distinct")
      .expr(s"${sign}FORALL(ARRAY(${seqToString(values)}), x -> ARRAY_CONTAINS(${removeTicksField}_distinct, x))")
  }

  def distinctContainsSet(values: java.util.List[Any], negate: Boolean): ValidationBuilder = {
    distinctContainsSet(toScalaList(values), negate)
  }

  /**
   * Check if distinct values of field equals set
   *
   * @param values Expected set of distinct values
   * @return
   */
  @varargs def distinctEqual(values: Any*): ValidationBuilder = {
    distinctEqual(values.toList, false)
  }

  /**
   * Check if distinct values of field equals set
   *
   * @param values Expected set of distinct values
   * @param negate Check if distinct values does not equal set when set to true
   * @return
   */
  def distinctEqual(values: List[Any], negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!=" else "=="
    validationBuilder.selectExpr(s"COLLECT_SET($field) AS ${removeTicksField}_distinct")
      .expr(s"ARRAY_SIZE(ARRAY_EXCEPT(ARRAY(${seqToString(values)}), ${removeTicksField}_distinct)) $sign 0")
  }

  def distinctEqual(values: java.util.List[Any], negate: Boolean): ValidationBuilder = {
    distinctEqual(toScalaList(values), negate)
  }

  /**
   * Check if max field value is between two values
   *
   * @param min Minimum expected value for max
   * @param max Maximum expected value for max
   * @param negate Check if not between two values when set to true
   * @return
   */
  def maxBetween(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    validationBuilder.groupBy().max(field).between(min, max, negate)
  }

  def maxBetween(min: Any, max: Any): ValidationBuilder = {
    maxBetween(min, max, false)
  }

  /**
   * Check if mean field value is between two values
   *
   * @param min Minimum expected value for mean
   * @param max Maximum expected value for mean
   * @param negate Check if not between two values when set to true
   * @return
   */
  def meanBetween(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    validationBuilder.groupBy().avg(field).between(min, max, negate)
  }

  def meanBetween(min: Any, max: Any): ValidationBuilder = {
    meanBetween(min, max, false)
  }

  /**
   * Check if median field value is between two values
   *
   * @param min Minimum expected value for median
   * @param max Maximum expected value for median
   * @param negate Check if not between two values when set to true
   * @return
   */
  def medianBetween(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder
      .selectExpr(s"PERCENTILE($field, 0.5) AS ${removeTicksField}_median")
      .expr(s"${removeTicksField}_median ${prefix}BETWEEN $min AND $max")
  }

  def medianBetween(min: Any, max: Any): ValidationBuilder = {
    medianBetween(min, max, false)
  }

  /**
   * Check if min field value is between two values
   *
   * @param min Minimum expected value for min
   * @param max Maximum expected value for min
   * @param negate Check if not between two values when set to true
   * @return
   */
  def minBetween(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    validationBuilder.groupBy().min(field).between(min, max, negate)
  }

  def minBetween(min: Any, max: Any): ValidationBuilder = {
    minBetween(min, max, false)
  }

  /**
   * Check if standard deviation field value is between two values
   *
   * @param min Minimum expected value for standard deviation
   * @param max Maximum expected value for standard deviation
   * @param negate Check if not between two values when set to true
   * @return
   */
  def stdDevBetween(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    validationBuilder.groupBy().stddev(field).between(min, max, negate)
  }

  def stdDevBetween(min: Any, max: Any): ValidationBuilder = {
    stdDevBetween(min, max, false)
  }

  /**
   * Check if sum field values is between two values
   *
   * @param min Minimum expected value for sum
   * @param max Maximum expected value for sum
   * @param negate Check if not between two values when set to true
   * @return
   */
  def sumBetween(min: Any, max: Any, negate: Boolean = false): ValidationBuilder = {
    validationBuilder.groupBy().sum(field).between(min, max, negate)
  }

  def sumBetween(min: Any, max: Any): ValidationBuilder = {
    sumBetween(min, max, false)
  }

  /**
   * Check if length of field values is between two values
   *
   * @param min Minimum expected value for length
   * @param max Maximum expected value for length
   * @param negate Check if not between two values when set to true
   * @return
   */
  def lengthBetween(min: Int, max: Int, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder.expr(s"LENGTH($field) ${prefix}BETWEEN $min AND $max")
  }

  def lengthBetween(min: Int, max: Int): ValidationBuilder = {
    lengthBetween(min, max, false)
  }

  /**
   * Check if length of field values is equal to value
   *
   * @param value Expected length
   * @param negate Check if not length is not equal to value when set to true
   * @return
   */
  def lengthEqual(value: Int, negate: Boolean = false): ValidationBuilder = {
    val sign = if (negate) "!=" else "=="
    validationBuilder.expr(s"LENGTH($field) $sign $value")
  }

  def lengthEqual(value: Int): ValidationBuilder = {
    lengthEqual(value, false)
  }

  /**
   * Check if field values are decreasing
   *
   * @param strictly Check values are strictly decreasing when set to true
   * @return
   */
  def isDecreasing(strictly: Boolean = true): ValidationBuilder = {
    val lessSign = if (strictly) "<" else "<="
    validationBuilder
      .selectExpr(s"$field $lessSign LAG($field) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_${removeTicksField}_decreasing")
      .expr(s"is_${removeTicksField}_decreasing")
  }

  def isDecreasing: ValidationBuilder = {
    isDecreasing(true)
  }

  /**
   * Check if field values are increasing
   *
   * @param strictly Check values are strictly increasing when set to true
   * @return
   */
  def isIncreasing(strictly: Boolean = true): ValidationBuilder = {
    val greaterThan = if (strictly) ">" else ">="
    validationBuilder
      .selectExpr(s"$field $greaterThan LAG($field) OVER (ORDER BY MONOTONICALLY_INCREASING_ID()) AS is_${removeTicksField}_increasing")
      .expr(s"is_${removeTicksField}_increasing")
  }

  def isIncreasing: ValidationBuilder = {
    isIncreasing(true)
  }

  /**
   * Check if field values can be parsed as JSON
   *
   * @param negate Check values cannot be parsed as JSON when set to true
   * @return
   */
  def isJsonParsable(negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "" else "NOT "
    validationBuilder.expr(s"GET_JSON_OBJECT($field, '$$') IS ${prefix}NULL")
  }

  def isJsonParsable: ValidationBuilder = {
    isJsonParsable(false)
  }

  /**
   * Check if field values adhere to JSON schema
   *
   * @param schema Defined JSON schema
   * @param negate Check values do not adhere to JSON schema when set to true
   * @return
   */
  def matchJsonSchema(schema: String, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "" else "NOT "
    validationBuilder.expr(s"FROM_JSON($field, '$schema') IS ${prefix}NULL")
  }

  def matchJsonSchema(schema: String): ValidationBuilder = {
    matchJsonSchema(schema, false)
  }

  /**
   * Check if field values match date time format
   *
   * @param format Defined date time format ([defined formats](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html))
   * @param negate Check values do not adhere to date time format when set to true
   * @return
   */
  def matchDateTimeFormat(format: String, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "" else "NOT "
    validationBuilder.expr(s"TRY_TO_TIMESTAMP($field, '$format') IS ${prefix}NULL")
  }

  def matchDateTimeFormat(format: String): ValidationBuilder = {
    matchDateTimeFormat(format, false)
  }

  /**
   * Check if the most common field value exists in set of values
   *
   * @param values Expected set of values most common value to exist in
   * @param negate Check most common does not exist in set of values when set to true
   * @return
   */
  def mostCommonValueInSet(values: List[Any], negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "!" else ""
    validationBuilder
      .selectExpr(s"MODE($field) AS ${removeTicksField}_mode")
      .expr(s"${prefix}ARRAY_CONTAINS(ARRAY(${seqToString(values)}), ${removeTicksField}_mode)")
  }

  def mostCommonValueInSet(values: java.util.List[Any]): ValidationBuilder = {
    mostCommonValueInSet(toScalaList(values), false)
  }

  def mostCommonValueInSet(values: java.util.List[Any], negate: Boolean): ValidationBuilder = {
    mostCommonValueInSet(toScalaList(values), negate)
  }

  /**
   * Check if the fields proportion of unique values is between two values
   *
   * @param min Minimum proportion of unique values
   * @param max Maximum proportion of unique values
   * @param negate Check if proportion of unique values is not between two values when set to true
   * @return
   */
  def uniqueValuesProportionBetween(min: Double, max: Double, negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    validationBuilder
      .selectExpr(s"COUNT(DISTINCT $field) / COUNT(1) AS ${removeTicksField}_unique_proportion")
      .expr(s"${removeTicksField}_unique_proportion ${prefix}BETWEEN $min AND $max")
  }

  def uniqueValuesProportionBetween(min: Double, max: Double): ValidationBuilder = {
    uniqueValuesProportionBetween(min, max, false)
  }

  /**
   * Check if quantiles of field values is within range.
   *
   * For example,
   * `Map(0.1 -> (1.0, 2.0))` -> 10th percentile should be between 1 and 2
   *
   * @param quantileRanges Map of quantile to expected range
   * @param negate Check if quantile value is not between two values when set to true
   * @return
   */
  def quantileValuesBetween(quantileRanges: Map[Double, (Double, Double)], negate: Boolean = false): ValidationBuilder = {
    val prefix = if (negate) "NOT " else ""
    val quantileExprs = quantileRanges.zipWithIndex.map(quantileEntry => {
      //if coming from YAML file, quantile value is string
      val tryGetQuantile = Try(quantileEntry._1._1)
      val quantileWithDouble = tryGetQuantile match {
        case Success(_) => quantileEntry._1
        case Failure(_) =>
          val stringKeys = quantileEntry._1.asInstanceOf[(String, (Double, Double))]
          stringKeys._1.toDouble -> stringKeys._2
      }
      val quantile = quantileWithDouble._1
      val min = quantileWithDouble._2._1
      val max = quantileWithDouble._2._2
      val idx = quantileEntry._2
      val percentileColName = s"${removeTicksField}_percentile_$idx"
      val selectExpr = s"PERCENTILE($field, $quantile) AS $percentileColName"
      val whereExpr = s"$percentileColName ${prefix}BETWEEN $min AND $max"
      (selectExpr, whereExpr)
    })
    val selectExpr = quantileExprs.keys.toList
    val whereExpr = quantileExprs.values.mkString(" AND ")
    validationBuilder.selectExpr(selectExpr: _*).expr(whereExpr)
  }

  def quantileValuesBetween(quantileRanges: java.util.Map[Double, (Double, Double)], negate: Boolean): ValidationBuilder = {
    quantileValuesBetween(toScalaMap(quantileRanges), negate)
  }

  def quantileValuesBetween(quantileRanges: java.util.Map[Double, (Double, Double)]): ValidationBuilder = {
    quantileValuesBetween(toScalaMap(quantileRanges), false)
  }

  /**
   * Check if SQL expression is true or not. Can include reference to any other fields in the dataset.
   *
   * @param expr SQL expression
   * @return
   */
  def expr(expr: String): ValidationBuilder = {
    validationBuilder.expr(expr)
  }

  private def fieldValueToString(value: Any): String = {
    value match {
      case _: String => s"'$value'"
      case _: Date => s"DATE('$value')"
      case _: Timestamp => s"TIMESTAMP('$value')"
      case _ => s"$value"
    }
  }

  private def removeTicksField: String = field.replaceAll("`", "")

  private def seqToString(seq: Seq[Any]): String = {
    seq.head match {
      case _: String => seq.mkString("'", "','", "'")
      case _ => seq.mkString(",")
    }
  }
}

case class GroupByValidationBuilder(
                                     validationBuilder: ValidationBuilder = ValidationBuilder(),
                                     groupByFields: Seq[String] = Seq()
                                   ) {
  def this() = this(ValidationBuilder(), Seq())

  /**
   * Sum all values for field
   *
   * @param field Name of field to sum
   * @return
   */
  def sum(field: String): FieldValidationBuilder = {
    setGroupValidation(field, AGGREGATION_SUM)
  }

  /**
   * Count the number of records for a particular field
   *
   * @param field Name of field to count
   * @return
   */
  def count(field: String): FieldValidationBuilder = {
    setGroupValidation(field, AGGREGATION_COUNT)
  }

  /**
   * Count the number of records for the whole dataset
   *
   * @return
   */
  def count(): FieldValidationBuilder = {
    setGroupValidation("", AGGREGATION_COUNT)
  }

  /**
   * Get the minimum value for a particular field
   *
   * @param field Name of field
   * @return
   */
  def min(field: String): FieldValidationBuilder = {
    setGroupValidation(field, AGGREGATION_MIN)
  }

  /**
   * Get the maximum value for a particular field
   *
   * @param field Name of field
   * @return
   */
  def max(field: String): FieldValidationBuilder = {
    setGroupValidation(field, AGGREGATION_MAX)
  }

  /**
   * Get the average/mean for a particular field
   *
   * @param field Name of field
   * @return
   */
  def avg(field: String): FieldValidationBuilder = {
    setGroupValidation(field, AGGREGATION_AVG)
  }

  /**
   * Get the standard deviation for a particular field
   *
   * @param field Name of field
   * @return
   */
  def stddev(field: String): FieldValidationBuilder = {
    setGroupValidation(field, AGGREGATION_STDDEV)
  }

  private def setGroupValidation(field: String, aggType: String): FieldValidationBuilder = {
    val groupByValidation = GroupByValidation(groupByFields, field, aggType)
    groupByValidation.errorThreshold = validationBuilder.validation.errorThreshold
    groupByValidation.description = validationBuilder.validation.description
    val fieldName = if (field.isEmpty) aggType else s"$aggType($field)"
    FieldValidationBuilder(validationBuilder.modify(_.validation).setTo(groupByValidation), fieldName)
  }
}

case class UpstreamDataSourceValidationBuilder(
                                                validationBuilders: List[ValidationBuilder] = List(),
                                                connectionTaskBuilder: ConnectionTaskBuilder[_] = FileBuilder(),
                                                readOptions: Map[String, String] = Map(),
                                                joinFields: List[String] = List(),
                                                joinType: String = DEFAULT_VALIDATION_JOIN_TYPE
                                              ) {
  def this() = this(List(), FileBuilder(), Map(), List(), DEFAULT_VALIDATION_JOIN_TYPE)

  /**
   * Define any custom read options to control which dataset is the upstream dataset.
   * For example, if the upstream {{connectionTaskBuilder}} is a Postgres task with multiple tables, you can define
   * the exact table name in readOptions via {{readOptions(Map(JDBC_TABLE -> "my_schema.my_table"))}}
   *
   * @param readOptions
   * @return
   */
  def readOptions(readOptions: Map[String, String]): UpstreamDataSourceValidationBuilder = {
    this.modify(_.readOptions).setTo(readOptions)
  }

  /**
   * Define set of field names to use for join with upstream dataset
   *
   * @param fields field names used for join
   * @return
   */
  @varargs def joinFields(fields: String*): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinFields).setTo(fields.toList)
  }

  /**
   * Define SQL expression that determines the logic to join the current dataset with the upstream dataset. Must return
   * boolean value.
   *
   * @param expr SQL expression returning boolean
   * @return
   */
  def joinExpr(expr: String): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinFields).setTo(List(s"$VALIDATION_PREFIX_JOIN_EXPRESSION$expr"))
  }

  /**
   * Define type of join when joining with upstream dataset. Can be one of the following values:
   * - inner
   * - outer, full, fullouter, full_outer
   * - leftouter, left, left_outer
   * - rightouter, right, right_outer
   * - leftsemi, left_semi, semi
   * - leftanti, left_anti, anti
   * - cross
   *
   * @param joinType
   * @return
   */
  def joinType(joinType: String): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinType).setTo(joinType)
  }

  /**
   * Define validations to be used on joined dataset
   *
   * @param validations Validations to check on joined dataset
   * @return
   */
  @varargs def validations(validations: ValidationBuilder*): ValidationBuilder = {
    ValidationBuilder().modify(_.validation).setTo(UpstreamDataSourceValidation(validations.toList, connectionTaskBuilder, readOptions, joinFields, joinType))
  }
}

case class FieldNamesValidationBuilder(
                                        validationBuilder: ValidationBuilder = ValidationBuilder()
                                      ) {
  def this() = this(ValidationBuilder())

  /**
   * Check number of field is equal to certain value
   *
   * @param value Number of expected fields
   * @return ValidationBuilder
   */
  def countEqual(value: Int): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(FieldNamesValidation(VALIDATION_FIELD_NAME_COUNT_EQUAL, value))

  /**
   * Check number of fields is between two values
   *
   * @param min Minimum number of expected fields (inclusive)
   * @param max Maximum number of expected fields (inclusive)
   * @return ValidationBuilder
   */
  def countBetween(min: Int, max: Int): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(FieldNamesValidation(VALIDATION_FIELD_NAME_COUNT_BETWEEN, min = min, max = max))

  /**
   * Order of field names matches given order
   *
   * @param fieldNameOrder Expected field name ordering
   * @return ValidationBuilder
   */
  @varargs def matchOrder(fieldNameOrder: String*): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(FieldNamesValidation(VALIDATION_FIELD_NAME_MATCH_ORDER, names = fieldNameOrder.toArray))

  /**
   * Dataset field names contains set of field names
   *
   * @param fieldNames field names expected to exist within dataset
   * @return ValidationBuilder
   */
  @varargs def matchSet(fieldNames: String*): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(FieldNamesValidation(VALIDATION_FIELD_NAME_MATCH_SET, names = fieldNames.toArray))
}

case class WaitConditionBuilder(waitCondition: WaitCondition = PauseWaitCondition()) {
  def this() = this(PauseWaitCondition())

  /**
   * Pause for configurable number of seconds, before starting data validations.
   *
   * @param pauseInSeconds Seconds to pause
   * @return WaitConditionBuilder
   */
  def pause(pauseInSeconds: Int): WaitConditionBuilder = this.modify(_.waitCondition).setTo(PauseWaitCondition(pauseInSeconds))

  /**
   * Wait until file exists within path before starting data validations.
   *
   * @param path Path to file
   * @return WaitConditionBuilder
   */
  def file(path: String): WaitConditionBuilder = this.modify(_.waitCondition).setTo(FileExistsWaitCondition(path))

  /**
   * Wait until a specific data condition is met before starting data validations. Specific data condition to be defined
   * as a SQL expression that returns a boolean value. Need to use a data source that is already defined.
   *
   * @param dataSourceName Name of data source that is already defined
   * @param options        Additional data source connection options to use to get data
   * @param expr           SQL expression that returns a boolean
   * @return WaitConditionBuilder
   */
  def dataExists(dataSourceName: String, options: Map[String, String], expr: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(DataExistsWaitCondition(dataSourceName, options, expr))

  /**
   * Wait until GET request to URL returns back 200 status code, then will start data validations
   *
   * @param url URL for HTTP GET request
   * @return WaitConditionBuilder
   */
  def webhook(url: String): WaitConditionBuilder =
    webhook(DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, url)

  /**
   * Wait until URL returns back one of the status codes provided before starting data validations.
   *
   * @param url         URL for HTTP request
   * @param method      HTTP method (i.e. GET, PUT, POST)
   * @param statusCodes HTTP status codes that are treated as successful
   * @return WaitConditionBuilder
   */
  @varargs def webhook(url: String, method: String, statusCodes: Int*): WaitConditionBuilder =
    webhook(DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, url, method, statusCodes: _*)

  /**
   * Wait until pre-defined HTTP data source with URL, returns back 200 status code from GET request before starting
   * data validations.
   *
   * @param dataSourceName Name of data source already defined
   * @param url            URL for HTTP GET request
   * @return WaitConditionBuilder
   */
  def webhook(dataSourceName: String, url: String): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url))

  /**
   * Wait until pre-defined HTTP data source with URL, HTTP method and set of successful status codes, return back one
   * of the successful status codes before starting data validations.
   *
   * @param dataSourceName Name of data source already defined
   * @param url            URL for HTTP request
   * @param method         HTTP method (i.e. GET, PUT, POST)
   * @param statusCode     HTTP status codes that are treated as successful
   * @return WaitConditionBuilder
   */
  @varargs def webhook(dataSourceName: String, url: String, method: String, statusCode: Int*): WaitConditionBuilder =
    this.modify(_.waitCondition).setTo(WebhookWaitCondition(dataSourceName, url, method, statusCode.toList))
}

case class PreFilterBuilder(combinationPreFilterBuilder: CombinationPreFilterBuilder = CombinationPreFilterBuilder()) {
  def this() = this(CombinationPreFilterBuilder(None, List()))

  def filter(validationBuilder: ValidationBuilder): CombinationPreFilterBuilder =
    CombinationPreFilterBuilder(None, List(Left(validationBuilder)))
}

case class CombinationPreFilterBuilder(
                                        preFilterExpr: Option[String] = None,
                                        validationPreFilterBuilders: List[Either[ValidationBuilder, ConditionType]] = List()
                                      ) {

  def this() = this(None, List())

  def expr(expr: String): CombinationPreFilterBuilder = this.modify(_.preFilterExpr).setTo(Some(expr))

  def and(validationBuilder: ValidationBuilder): CombinationPreFilterBuilder =
    this.modify(_.validationPreFilterBuilders).setTo(validationPreFilterBuilders ++ List(Right(ConditionType.AND), Left(validationBuilder)))

  def or(validationBuilder: ValidationBuilder): CombinationPreFilterBuilder =
    this.modify(_.validationPreFilterBuilders).setTo(validationPreFilterBuilders ++ List(Right(ConditionType.OR), Left(validationBuilder)))

  def validate(): Boolean = {
    if (validationPreFilterBuilders.size < 3) {
      validationPreFilterBuilders.size == 1 && validationPreFilterBuilders.head.isLeft
    } else {
      validationPreFilterBuilders.sliding(3, 2)
        .forall(grp => grp.head.isLeft && grp(1).isRight && grp.last.isLeft)
    }
  }

  def toExpression: String = {
    preFilterExpr.getOrElse(
      validationPreFilterBuilders.map {
        case Left(validationBuilder) =>
          validationBuilder.validation match {
            case exprValidation: ExpressionValidation => exprValidation.expr
            case _ => "true"
          }
        case Right(conditionType) => conditionType.toString
      }.mkString(" ")
    )
  }
}

object ValidationHelper {
  def cleanFieldName(field: String): String = field.split("\\.").map(c => s"`$c`").mkString(".")
}
