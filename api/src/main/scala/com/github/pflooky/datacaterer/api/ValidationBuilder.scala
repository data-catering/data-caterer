package com.github.pflooky.datacaterer.api

import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.github.pflooky.datacaterer.api.connection.{ConnectionTaskBuilder, FileBuilder}
import com.github.pflooky.datacaterer.api.model.Constants.{AGGREGATION_AVG, AGGREGATION_COUNT, AGGREGATION_MAX, AGGREGATION_MIN, AGGREGATION_STDDEV, AGGREGATION_SUM, DEFAULT_VALIDATION_COLUMN_NAME_TYPE, DEFAULT_VALIDATION_JOIN_TYPE, DEFAULT_VALIDATION_WEBHOOK_HTTP_DATA_SOURCE_NAME, MAXIMUM, MINIMUM, VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET, VALIDATION_PREFIX_JOIN_EXPRESSION, VALIDATION_UNIQUE}
import com.github.pflooky.datacaterer.api.model.{ColumnNamesValidation, DataExistsWaitCondition, DataSourceValidation, ExpressionValidation, FileExistsWaitCondition, GroupByValidation, PauseWaitCondition, UpstreamDataSourceValidation, Validation, ValidationConfiguration, WaitCondition, WebhookWaitCondition}
import com.github.pflooky.datacaterer.api.parser.ValidationBuilderSerializer
import com.softwaremill.quicklens.ModifyPimp

import java.sql.{Date, Timestamp}
import scala.annotation.varargs


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

  def wait(waitCondition: WaitConditionBuilder): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition.waitCondition)

  def wait(waitCondition: WaitCondition): DataSourceValidationBuilder =
    this.modify(_.dataSourceValidation.waitCondition).setTo(waitCondition)
}

@JsonSerialize(using = classOf[ValidationBuilderSerializer])
case class ValidationBuilder(validation: Validation = ExpressionValidation()) {
  def this() = this(ExpressionValidation())

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
   * required to be boolean. Can use any columns in the validation logic.
   *
   * For example,
   * {{{validation.expr("CASE WHEN status == 'open' THEN balance > 0 ELSE balance == 0 END}}}
   *
   * @param expr SQL expression which returns a boolean
   * @return ValidationBuilder
   * @see <a href="https://spark.apache.org/docs/latest/api/sql/">SQL expressions</a>
   */
  def expr(expr: String): ValidationBuilder = {
    validation match {
      case GroupByValidation(grpCols, aggCol, aggType, _) =>
        val grpWithExpr = GroupByValidation(grpCols, aggCol, aggType, expr)
        grpWithExpr.description = this.validation.description
        grpWithExpr.errorThreshold = this.validation.errorThreshold
        this.modify(_.validation).setTo(grpWithExpr)
      case expressionValidation: ExpressionValidation =>
        val withExpr = expressionValidation.modify(_.expr).setTo(expr)
        withExpr.description = this.validation.description
        withExpr.errorThreshold = this.validation.errorThreshold
        this.modify(_.validation).setTo(withExpr)
    }
  }

  /**
   * Define a column validation that can cover validations for any type of data.
   *
   * @param column Name of the column to run validation against
   * @return ColumnValidationBuilder
   */
  def col(column: String): ColumnValidationBuilder = {
    ColumnValidationBuilder(this, column)
  }

  /**
   * Define columns to group by, so that validation can be run on grouped by dataset
   *
   * @param columns Name of the column to run validation against
   * @return ColumnValidationBuilder
   */
  @varargs def groupBy(columns: String*): GroupByValidationBuilder = {
    GroupByValidationBuilder(this, columns)
  }

  /**
   * Check row count of dataset
   *
   * @return ColumnValidationBuilder to apply validation on row count
   */
  def count(): ColumnValidationBuilder = {
    GroupByValidationBuilder().count()
  }

  /**
   * Check if column(s) values are unique
   *
   * @param columns One or more columns whose values will be checked for uniqueness
   * @return ValidationBuilder
   */
  @varargs def unique(columns: String*): ValidationBuilder = {
    this.modify(_.validation).setTo(GroupByValidation(columns, VALIDATION_UNIQUE, AGGREGATION_COUNT))
      .expr("count == 1")
  }

  /**
   * Define validations based on data in another data source.
   *
   * @param connectionTaskBuilder Other data source
   * @return UpstreamDataSourceValidationBuilder
   */
  def upstreamData(connectionTaskBuilder: ConnectionTaskBuilder[_]): UpstreamDataSourceValidationBuilder = {
    UpstreamDataSourceValidationBuilder(this, connectionTaskBuilder)
  }

  /**
   * Define validation for column names of dataset.
   *
   * @return ColumnNamesValidationBuilder
   */
  def columnNames(): ColumnNamesValidationBuilder = {
    ColumnNamesValidationBuilder()
  }
}

case class ColumnValidationBuilder(validationBuilder: ValidationBuilder = ValidationBuilder(), column: String = "") {
  def this() = this(ValidationBuilder(), "")

  /**
   * Check if column values are equal to a certain value
   * @param value Expected value for all column values
   * @return
   */
  def isEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column == ${colValueToString(value)}")
  }

  /**
   * Check if column values are equal to another column for each record
   * @param value Other column name
   * @return
   */
  def isEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column == $value")
  }

  /**
   * Check if column values are not equal to a certain value
   * @param value Value column should not equal to
   * @return
   */
  def isNotEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column != ${colValueToString(value)}")
  }

  /**
   * Check if column values are not equal to another column's value for each record
   * @param value Other column name not equal to
   * @return
   */
  def isNotEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column != $value")
  }

  /**
   * Check if column values are null
   * @return
   */
  def isNull: ValidationBuilder = {
    validationBuilder.expr(s"ISNULL($column)")
  }

  /**
   * Check if column values are not null
   * @return
   */
  def isNotNull: ValidationBuilder = {
    validationBuilder.expr(s"ISNOTNULL($column)")
  }

  /**
   * Check if column values contain particular string (only for string type columns)
   * @param value Expected string that column values contain
   * @return
   */
  def contains(value: String): ValidationBuilder = {
    validationBuilder.expr(s"CONTAINS($column, '$value')")
  }

  /**
   * Check if column values do not contain particular string (only for string type columns)
   * @param value String value not expected to contain in column values
   * @return
   */
  def notContains(value: String): ValidationBuilder = {
    validationBuilder.expr(s"!CONTAINS($column, '$value')")
  }

  /**
   * Check if column values are less than certain value
   * @param value Less than value
   * @return
   */
  def lessThan(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column < ${colValueToString(value)}")
  }

  /**
   * Check if column values are less than another column's values for each record
   * @param value Other column name
   * @return
   */
  def lessThanCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column < $value")
  }

  /**
   * Check if column values are less than or equal to certain value
   * @param value Less than or equal to value
   * @return
   */
  def lessThanOrEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column <= ${colValueToString(value)}")
  }

  /**
   * Check if column values are less than or equal to another column's values for each record
   * @param value Other column name
   * @return
   */
  def lessThanOrEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column <= $value")
  }

  /**
   * Check if column is greater than a certain value
   * @param value Greater than value
   * @return
   */
  def greaterThan(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column > ${colValueToString(value)}")
  }

  /**
   * Check if column is greater than another column's values for each record
   * @param value Other column name
   * @return
   */
  def greaterThanCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column > $value")
  }

  /**
   * Check if column is greater than or equal to a certain value
   * @param value Greater than or equal to value
   * @return
   */
  def greaterThanOrEqual(value: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column >= ${colValueToString(value)}")
  }

  /**
   * Check if column is greater than or equal to another column's values for each record
   * @param value Other column name
   * @return
   */
  def greaterThanOrEqualCol(value: String): ValidationBuilder = {
    validationBuilder.expr(s"$column >= $value")
  }

  /**
   * Check if column values are between two values (inclusive)
   * @param minValue Minimum value (inclusive)
   * @param maxValue Maximum value (inclusive)
   * @return
   */
  def between(minValue: Any, maxValue: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column BETWEEN ${colValueToString(minValue)} AND ${colValueToString(maxValue)}")
  }

  /**
   * Check if column values are between values of other columns (inclusive)
   * @param minValue Other column name determining minimum value (inclusive)
   * @param maxValue Other column name determining maximum value (inclusive)
   * @return
   */
  def betweenCol(minValue: String, maxValue: String): ValidationBuilder = {
    validationBuilder.expr(s"$column BETWEEN $minValue AND $maxValue")
  }

  /**
   * Check if column values are not between two values
   * @param minValue Minimum value
   * @param maxValue Maximum value
   * @return
   */
  def notBetween(minValue: Any, maxValue: Any): ValidationBuilder = {
    validationBuilder.expr(s"$column NOT BETWEEN ${colValueToString(minValue)} AND ${colValueToString(maxValue)}")
  }

  /**
   * Check if column values are not between values of other columns
   * @param minValue Other column name determining minimum value
   * @param maxValue Other column name determining maximum value
   * @return
   */
  def notBetweenCol(minValue: String, maxValue: String): ValidationBuilder = {
    validationBuilder.expr(s"$column NOT BETWEEN $minValue AND $maxValue")
  }

  /**
   * Check if column values are in given set of expected values
   * @param values Expected set of values
   * @return
   */
  @varargs def in(values: Any*): ValidationBuilder = {
    validationBuilder.expr(s"$column IN (${values.map(colValueToString).mkString(",")})")
  }

  /**
   * Check if column values are not in given set of values
   * @param values Set of unwanted values
   * @return
   */
  @varargs def notIn(values: Any*): ValidationBuilder = {
    validationBuilder.expr(s"NOT $column IN (${values.map(colValueToString).mkString(",")})")
  }

  /**
   * Check if column values match certain regex pattern (Java regular expression)
   * @param regex Java regular expression
   * @return
   */
  def matches(regex: String): ValidationBuilder = {
    validationBuilder.expr(s"REGEXP($column, '$regex')")
  }

  /**
   * Check if column values do not match certain regex (Java regular expression)
   * @param regex Java regular expression
   * @return
   */
  def notMatches(regex: String): ValidationBuilder = {
    validationBuilder.expr(s"!REGEXP($column, '$regex')")
  }

  /**
   * Check if column values start with certain string (only for string columns)
   * @param value Expected prefix for string values
   * @return
   */
  def startsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"STARTSWITH($column, '$value')")
  }

  /**
   * Check if column values do not start with certain string (only for string columns)
   * @param value Prefix string value should not start with
   * @return
   */
  def notStartsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"!STARTSWITH($column, '$value')")
  }

  /**
   * Check if column values end with certain string (only for string columns)
   * @param value Expected suffix for string
   * @return
   */
  def endsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"ENDSWITH($column, '$value')")
  }

  /**
   * Check if column values do not end with certain string (only for string columns)
   * @param value Suffix string value should not end with
   * @return
   */
  def notEndsWith(value: String): ValidationBuilder = {
    validationBuilder.expr(s"!ENDSWITH($column, '$value')")
  }

  /**
   * Check if column size is equal to certain amount (only for array or map columns)
   * @param size Expected size
   * @return
   */
  def size(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) == $size")
  }

  /**
   * Check if column size is not equal to certain amount (only for array or map columns)
   * @param size Array or map size should not equal
   * @return
   */
  def notSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) != $size")
  }

  /**
   * Check if column size is less than certain amount (only for array or map columns)
   * @param size Less than size
   * @return
   */
  def lessThanSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) < $size")
  }

  /**
   * Check if column size is less than or equal to certain amount (only for array or map columns)
   * @param size Less than or equal to size
   * @return
   */
  def lessThanOrEqualSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) <= $size")
  }

  /**
   * Check if column size is greater than certain amount (only for array or map columns)
   * @param size Greater than size
   * @return
   */
  def greaterThanSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) > $size")
  }

  /**
   * Check if column size is greater than or equal to certain amount (only for array or map columns)
   * @param size Greater than or equal to size
   * @return
   */
  def greaterThanOrEqualSize(size: Int): ValidationBuilder = {
    validationBuilder.expr(s"SIZE($column) >= $size")
  }

  /**
   * Check if column values adhere to Luhn algorithm. Usually used for credit card or identification numbers.
   * @return
   */
  def luhnCheck: ValidationBuilder = {
    validationBuilder.expr(s"LUHN_CHECK($column)")
  }

  /**
   * Check if column values adhere to expected type
   * @param `type` Expected data type
   * @return
   */
  def hasType(`type`: String): ValidationBuilder = {
    validationBuilder.expr(s"TYPEOF($column) == '${`type`}'")
  }

  /**
   * Check if SQL expression is true or not. Can include reference to any other columns in the dataset.
   * @param expr SQL expression
   * @return
   */
  def expr(expr: String): ValidationBuilder = {
    validationBuilder.expr(expr)
  }

  private def colValueToString(value: Any): String = {
    value match {
      case _: String => s"'$value'"
      case _: Date => s"DATE('$value')"
      case _: Timestamp => s"TIMESTAMP('$value')"
      case _ => s"$value"
    }
  }
}

case class GroupByValidationBuilder(
                                     validationBuilder: ValidationBuilder = ValidationBuilder(),
                                     groupByCols: Seq[String] = Seq()
                                   ) {
  def this() = this(ValidationBuilder(), Seq())

  /**
   * Sum all values for column
   * @param column Name of column to sum
   * @return
   */
  def sum(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_SUM)
  }

  /**
   * Count the number of records for a particular column
   * @param column Name of column to count
   * @return
   */
  def count(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_COUNT)
  }

  /**
   * Count the number of records for the whole dataset
   * @return
   */
  def count(): ColumnValidationBuilder = {
    setGroupValidation("", AGGREGATION_COUNT)
  }

  /**
   * Get the minimum value for a particular column
   * @param column Name of column
   * @return
   */
  def min(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_MIN)
  }

  /**
   * Get the maximum value for a particular column
   * @param column Name of column
   * @return
   */
  def max(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_MAX)
  }

  /**
   * Get the average/mean for a particular column
   * @param column Name of column
   * @return
   */
  def avg(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_AVG)
  }

  /**
   * Get the standard deviation for a particular column
   * @param column Name of column
   * @return
   */
  def stddev(column: String): ColumnValidationBuilder = {
    setGroupValidation(column, AGGREGATION_STDDEV)
  }

  private def setGroupValidation(column: String, aggType: String): ColumnValidationBuilder = {
    val groupByValidation = GroupByValidation(groupByCols, column, aggType)
    groupByValidation.errorThreshold = validationBuilder.validation.errorThreshold
    groupByValidation.description = validationBuilder.validation.description
    val colName = if (column.isEmpty) aggType else s"$aggType($column)"
    ColumnValidationBuilder(validationBuilder.modify(_.validation).setTo(groupByValidation), colName)
  }
}

case class UpstreamDataSourceValidationBuilder(
                                                validationBuilder: ValidationBuilder = ValidationBuilder(),
                                                connectionTaskBuilder: ConnectionTaskBuilder[_] = FileBuilder(),
                                                readOptions: Map[String, String] = Map(),
                                                joinColumns: List[String] = List(),
                                                joinType: String = DEFAULT_VALIDATION_JOIN_TYPE
                                              ) {
  def this() = this(ValidationBuilder(), FileBuilder(), Map(), List(), DEFAULT_VALIDATION_JOIN_TYPE)

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
   * Define set of column names to use for join with upstream dataset
   * @param joinCols Column names used for join
   * @return
   */
  @varargs def joinColumns(joinCols: String*): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinColumns).setTo(joinCols.toList)
  }

  /**
   * Define SQL expression that determines the logic to join the current dataset with the upstream dataset. Must return
   * boolean value.
   * @param expr SQL expression returning boolean
   * @return
   */
  def joinExpr(expr: String): UpstreamDataSourceValidationBuilder = {
    this.modify(_.joinColumns).setTo(List(s"$VALIDATION_PREFIX_JOIN_EXPRESSION$expr"))
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
   * Define validation to be used on joined dataset
   * @param validationBuilder Validation check on joined dataset
   * @return
   */
  def withValidation(validationBuilder: ValidationBuilder): ValidationBuilder = {
    validationBuilder.modify(_.validation).setTo(UpstreamDataSourceValidation(validationBuilder, connectionTaskBuilder, readOptions, joinColumns, joinType))
  }
}

case class ColumnNamesValidationBuilder(
                                         validationBuilder: ValidationBuilder = ValidationBuilder()
                                       ) {
  def this() = this(ValidationBuilder())

  /**
   * Check number of column is equal to certain value
   *
   * @param value Number of expected columns
   * @return ValidationBuilder
   */
  def countEqual(value: Int): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(ColumnNamesValidation(VALIDATION_COLUMN_NAME_COUNT_EQUAL, value))

  /**
   * Check number of columns is between two values
   * @param min Minimum number of expected columns (inclusive)
   * @param max Maximum number of expected columns (inclusive)
   * @return ValidationBuilder
   */
  def countBetween(min: Int, max: Int): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(ColumnNamesValidation(VALIDATION_COLUMN_NAME_COUNT_BETWEEN, minCount = min, maxCount = max))

  /**
   * Order of column names matches given order
   * @param columnNameOrder Expected column name ordering
   * @return ValidationBuilder
   */
  @varargs def matchOrder(columnNameOrder: String*): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(ColumnNamesValidation(VALIDATION_COLUMN_NAME_MATCH_ORDER, names = columnNameOrder.toArray))

  /**
   * Dataset column names contains set of column names
   * @param columnNames Column names expected to exist within dataset
   * @return ValidationBuilder
   */
  @varargs def matchSet(columnNames: String*): ValidationBuilder =
    validationBuilder.modify(_.validation).setTo(ColumnNamesValidation(VALIDATION_COLUMN_NAME_MATCH_SET, names = columnNames.toArray))
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