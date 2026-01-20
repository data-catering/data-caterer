package io.github.datacatering.datacaterer.api

/**
 * Type-safe builder for conditional value generation.
 * Allows referencing other fields and building CASE WHEN expressions.
 *
 * @example {{{
 *   field.name("discount").conditionalValue(
 *     when("total").greaterThan(1000) -> 100,
 *     when("total").greaterThan(500) -> 50,
 *     when("total").greaterThan(100) -> 10
 *   )(elseValue = 0)
 * }}}
 */
case class ConditionalBuilder(fieldName: String) {

  /**
   * Creates a condition where field is greater than a value.
   *
   * @param value the value to compare against
   * @return ConditionalBranch for chaining with then value
   */
  def greaterThan(value: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName > ${formatValue(value)}")

  /**
   * Creates a condition where field is less than a value.
   *
   * @param value the value to compare against
   * @return ConditionalBranch for chaining with then value
   */
  def lessThan(value: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName < ${formatValue(value)}")

  /**
   * Creates a condition where field equals a value.
   *
   * @param value the value to compare against
   * @return ConditionalBranch for chaining with then value
   */
  def equalTo(value: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName = ${formatValue(value)}")

  /**
   * Creates a condition where field is between two values (inclusive).
   *
   * @param min the minimum value
   * @param max the maximum value
   * @return ConditionalBranch for chaining with then value
   */
  def between(min: Any, max: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName BETWEEN ${formatValue(min)} AND ${formatValue(max)}")

  /**
   * Creates a condition where field is in a set of values.
   *
   * @param values the values to check against
   * @return ConditionalBranch for chaining with then value
   */
  def in(values: Any*): ConditionalBranch = {
    val valuesList = values.map(formatValue).mkString(", ")
    ConditionalBranch(s"$fieldName IN ($valuesList)")
  }

  /**
   * Creates a condition where field is greater than or equal to a value.
   *
   * @param value the value to compare against
   * @return ConditionalBranch for chaining with then value
   */
  def greaterThanOrEqual(value: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName >= ${formatValue(value)}")

  /**
   * Creates a condition where field is less than or equal to a value.
   *
   * @param value the value to compare against
   * @return ConditionalBranch for chaining with then value
   */
  def lessThanOrEqual(value: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName <= ${formatValue(value)}")

  /**
   * Creates a condition where field is not equal to a value.
   *
   * @param value the value to compare against
   * @return ConditionalBranch for chaining with then value
   */
  def notEqualTo(value: Any): ConditionalBranch =
    ConditionalBranch(s"$fieldName != ${formatValue(value)}")

  private def formatValue(value: Any): String = value match {
    case s: String => s"'${s.replace("'", "''")}'"  // Escape single quotes to prevent SQL injection
    case v => v.toString
  }
}

/**
 * Represents a conditional branch (the condition part of WHEN ... THEN).
 */
case class ConditionalBranch(condition: String) {

  /**
   * Completes the conditional with a constant value.
   * Usage: when("field").greaterThan(100) -> 50
   *
   * @param thenValue the value to return when condition is true
   * @return ConditionalCase for use in conditionalValue()
   */
  def ->(thenValue: Any): ConditionalCase = {
    val formattedValue = thenValue match {
      case s: String => s"'${s.replace("'", "''")}'"  // Escape single quotes to prevent SQL injection
      case v => v.toString
    }
    ConditionalCase(condition, formattedValue)
  }

  /**
   * Alternative syntax: value ->: condition
   * Completes the conditional with a constant value.
   *
   * @param thenValue the value to return when condition is true
   * @return ConditionalCase for use in conditionalValue()
   */
  def ->:(thenValue: Any): ConditionalCase = ->(thenValue)
}

/**
 * A complete conditional case (condition + value).
 * Represents one WHEN ... THEN clause in a SQL CASE expression.
 */
case class ConditionalCase(condition: String, thenValue: String) {
  /**
   * Converts this case to SQL WHEN ... THEN syntax.
   *
   * @return SQL string for this case
   */
  def toSql: String = s"WHEN $condition THEN $thenValue"
}
