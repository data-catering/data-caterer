package io.github.datacatering.datacaterer.core.generator.provider.regex

/**
 * Abstract Syntax Tree (AST) for regex patterns that can be converted to SQL.
 * Each node represents a part of a regex pattern and knows how to generate SQL.
 */
sealed trait RegexNode {
  /** Convert this node to a SQL expression */
  def toSql(randomExpr: String, randomExprWithIndex: String => String): String
  def toSql: String = toSql("RAND()", _ => "RAND()")

  /** Minimum length this pattern can generate */
  def minLength: Int

  /** Maximum length this pattern can generate */
  def maxLength: Int
}

/**
 * Literal string like "ACC" or "-"
 */
case class LiteralNode(value: String) extends RegexNode {
  override def toSql(randomExpr: String, randomExprWithIndex: String => String): String = s"'$value'"
  override def minLength: Int = value.length
  override def maxLength: Int = value.length
}

/**
 * Character class like [A-Z] or \d with quantifier
 */
case class CharacterClassNode(
  charType: CharacterType,
  min: Int = 1,
  max: Int = 1
) extends RegexNode {
  override def toSql(randomExpr: String, randomExprWithIndex: String => String): String =
    charType.generateSql(min, max, randomExpr, randomExprWithIndex)
  override def minLength: Int = min
  override def maxLength: Int = max
}

/**
 * Types of character classes that can be converted to SQL
 */
sealed trait CharacterType {
  def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String
}

case object Digit extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    if (min == max) {
      // Fixed length: LPAD(CAST(RAND() * 10^n AS BIGINT), n, '0')
      val maxVal = Math.pow(10, min).toLong
      s"LPAD(CAST($randomExpr * $maxVal AS BIGINT), $min, '0')"
    } else if (min == 0) {
      // Can be empty or up to max digits
      val maxVal = Math.pow(10, max).toLong
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE LPAD(CAST($randomExpr * $maxVal AS BIGINT), $len, '0') END"
    } else {
      // Variable length between min and max
      val maxVal = Math.pow(10, max).toLong
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"LPAD(CAST($randomExpr * $maxVal AS BIGINT), $len, '0')"
    }
  }
}

case object UppercaseLetter extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    if (min == max) {
      // CONCAT_WS('', TRANSFORM(SEQUENCE(1, n), i -> CHAR(65 + CAST(RAND() * 26 AS INT))))
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $min), i -> CHAR(65 + CAST(${randomExprWithIndex("i")} * 26 AS INT))))"
    } else if (min == 0) {
      // Can be empty
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> CHAR(65 + CAST(${randomExprWithIndex("i")} * 26 AS INT)))) END"
    } else {
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> CHAR(65 + CAST(${randomExprWithIndex("i")} * 26 AS INT))))"
    }
  }
}

case object LowercaseLetter extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    if (min == max) {
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $min), i -> CHAR(97 + CAST(${randomExprWithIndex("i")} * 26 AS INT))))"
    } else if (min == 0) {
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> CHAR(97 + CAST(${randomExprWithIndex("i")} * 26 AS INT)))) END"
    } else {
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> CHAR(97 + CAST(${randomExprWithIndex("i")} * 26 AS INT))))"
    }
  }
}

case object MixedLetter extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    if (min == max) {
      // Choose randomly between uppercase (65-90) and lowercase (97-122)
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $min), i -> CHAR(IF(${randomExprWithIndex("i")} < 0.5, 65, 97) + CAST(${randomExprWithIndex("i + 1")} * 26 AS INT))))"
    } else if (min == 0) {
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> CHAR(IF(${randomExprWithIndex("i")} < 0.5, 65, 97) + CAST(${randomExprWithIndex("i + 1")} * 26 AS INT)))) END"
    } else {
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> CHAR(IF(${randomExprWithIndex("i")} < 0.5, 65, 97) + CAST(${randomExprWithIndex("i + 1")} * 26 AS INT))))"
    }
  }
}

case object AlphanumericUpper extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    val charset = "'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'"
    if (min == max) {
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $min), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * 36 AS INT) + 1, 1)))"
    } else if (min == 0) {
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * 36 AS INT) + 1, 1))) END"
    } else {
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * 36 AS INT) + 1, 1)))"
    }
  }
}

case object AlphanumericMixed extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    val charset = "'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'"
    if (min == max) {
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $min), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * 62 AS INT) + 1, 1)))"
    } else if (min == 0) {
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * 62 AS INT) + 1, 1))) END"
    } else {
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * 62 AS INT) + 1, 1)))"
    }
  }
}

case class CustomCharSet(chars: String) extends CharacterType {
  override def generateSql(min: Int, max: Int, randomExpr: String, randomExprWithIndex: String => String): String = {
    // Escape single quotes in the character set
    val escapedChars = chars.replace("'", "''")
    val charset = s"'$escapedChars'"
    val charCount = chars.length
    if (min == max) {
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $min), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * $charCount AS INT) + 1, 1)))"
    } else if (min == 0) {
      val len = s"CAST($randomExpr * ${max + 1} AS INT)"
      s"CASE WHEN $len = 0 THEN '' ELSE CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * $charCount AS INT) + 1, 1))) END"
    } else {
      val len = s"CAST($min + $randomExpr * ${max - min + 1} AS INT)"
      s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $len), i -> SUBSTRING($charset, CAST(${randomExprWithIndex("i")} * $charCount AS INT) + 1, 1)))"
    }
  }
}

/**
 * Sequence of patterns like "ACC" + [0-9]{8}
 */
case class SequenceNode(parts: List[RegexNode]) extends RegexNode {
  override def toSql(randomExpr: String, randomExprWithIndex: String => String): String = {
    if (parts.isEmpty) "''"
    else if (parts.size == 1) parts.head.toSql(randomExpr, randomExprWithIndex)
    else s"CONCAT(${parts.map(_.toSql(randomExpr, randomExprWithIndex)).mkString(", ")})"
  }
  override def minLength: Int = parts.map(_.minLength).sum
  override def maxLength: Int = parts.map(_.maxLength).sum
}

/**
 * Alternation like (ACTIVE|INACTIVE|PENDING)
 */
case class AlternationNode(options: List[RegexNode]) extends RegexNode {
  override def toSql(randomExpr: String, randomExprWithIndex: String => String): String = {
    if (options.isEmpty) "''"
    else if (options.size == 1) options.head.toSql(randomExpr, randomExprWithIndex)
    else {
      // Build array and select random element using ELEMENT_AT (1-indexed in Spark)
      val optionsSql = options.map(_.toSql(randomExpr, randomExprWithIndex)).mkString(", ")
      val arraySize = options.size
      s"ELEMENT_AT(ARRAY($optionsSql), CAST($randomExpr * $arraySize AS INT) + 1)"
    }
  }
  override def minLength: Int = if (options.isEmpty) 0 else options.map(_.minLength).min
  override def maxLength: Int = if (options.isEmpty) 0 else options.map(_.maxLength).max
}

/**
 * Optional pattern (quantifier ?)
 */
case class OptionalNode(pattern: RegexNode) extends RegexNode {
  override def toSql(randomExpr: String, randomExprWithIndex: String => String): String = {
    s"IF($randomExpr < 0.5, ${pattern.toSql(randomExpr, randomExprWithIndex)}, '')"
  }
  override def minLength: Int = 0
  override def maxLength: Int = pattern.maxLength
}
