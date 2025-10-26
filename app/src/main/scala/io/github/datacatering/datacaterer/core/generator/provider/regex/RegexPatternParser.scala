package io.github.datacatering.datacaterer.core.generator.provider.regex

import scala.util.parsing.combinator.RegexParsers
import scala.util.{Failure, Success, Try}

/**
 * Parser for converting regex patterns to AST nodes that can generate SQL.
 * Supports common business regex patterns while falling back gracefully for unsupported features.
 */
class RegexPatternParser extends RegexParsers {

  override def skipWhitespace = false

  // Top-level pattern
  def pattern: Parser[RegexNode] = sequence

  // Sequence: consecutive elements
  def sequence: Parser[RegexNode] = rep1(element) ^^ {
    case single :: Nil => single
    case multiple =>
      // Merge consecutive literals
      val merged = multiple.foldLeft(List.empty[RegexNode]) {
        case (acc :+ LiteralNode(prev), LiteralNode(curr)) =>
          acc :+ LiteralNode(prev + curr)
        case (acc, node) =>
          acc :+ node
      }
      if (merged.size == 1) merged.head
      else SequenceNode(merged)
  }

  // Single element with optional quantifier
  def element: Parser[RegexNode] = (group | characterClass | literal) ~ quantifier.? ^^ {
    case node ~ Some(quant) => applyQuantifier(node, quant)
    case node ~ None => node
  }

  // Group: (pattern) or (?:pattern)
  // Groups can contain alternations
  def group: Parser[RegexNode] =
    ("(?:" ~> groupContent <~ ")") |
      ("(" ~> groupContent <~ ")")

  // Content inside a group (can have alternations)
  def groupContent: Parser[RegexNode] =
    repsep(sequence, "|") ^^ {
      case single :: Nil => single
      case options => AlternationNode(options)
    }

  // Character class: [A-Z], \d, etc.
  def characterClass: Parser[CharacterClassNode] =
    escapedClass | bracketClass

  def escapedClass: Parser[CharacterClassNode] =
    "\\d" ^^^ CharacterClassNode(Digit) |
      "\\D" ^^^ CharacterClassNode(AlphanumericMixed) | // Non-digit approximation
      "\\w" ^^^ CharacterClassNode(AlphanumericMixed) | // Word char
      "\\W" ^^^ CharacterClassNode(AlphanumericMixed) | // Non-word char approximation
      "\\s" ^^^ CharacterClassNode(CustomCharSet(" \t\n\r")) | // Whitespace
      "\\S" ^^^ CharacterClassNode(AlphanumericMixed) // Non-whitespace approximation

  def bracketClass: Parser[CharacterClassNode] =
    "[" ~> bracketContent <~ "]" ^^ { content =>
      CharacterClassNode(parseCharacterSet(content))
    }

  def bracketContent: Parser[String] = """[^\]]+""".r

  def parseCharacterSet(content: String): CharacterType = {
    // Handle negated character sets
    if (content.startsWith("^")) {
      // Negated sets are complex - use alphanumeric as safe fallback
      return AlphanumericMixed
    }

    // Match common patterns
    content match {
      case "0-9" => Digit
      case "A-Z" => UppercaseLetter
      case "a-z" => LowercaseLetter
      case "A-Za-z" | "a-zA-Z" => MixedLetter
      case "A-Z0-9" | "0-9A-Z" => AlphanumericUpper
      case "A-Za-z0-9" | "a-zA-Z0-9" | "0-9A-Za-z" | "0-9a-zA-Z" => AlphanumericMixed
      case other if !other.contains("-") || other.startsWith("-") || other.endsWith("-") =>
        // Custom character set like [abc123] or literal dash
        // Remove escape sequences for literal characters
        val cleaned = other.replace("\\-", "-").replace("\\]", "]")
        CustomCharSet(cleaned)
      case other =>
        // Complex range pattern - try to extract characters
        // For now, use alphanumeric as fallback
        // TODO: Could expand ranges like "a-zA-Z0-9" manually
        AlphanumericMixed
    }
  }

  // Literal characters (not special regex chars)
  def literal: Parser[LiteralNode] = (
    escapedChar |
      normalChar
    ) ^^ { char => LiteralNode(char) }

  def escapedChar: Parser[String] =
    "\\\\" ^^^ "\\" | // Backslash
      "\\-" ^^^ "-" | // Dash
      "\\." ^^^ "." | // Dot
      "\\(" ^^^ "(" |
      "\\)" ^^^ ")" |
      "\\[" ^^^ "[" |
      "\\]" ^^^ "]" |
      "\\{" ^^^ "{" |
      "\\}" ^^^ "}" |
      "\\|" ^^^ "|" |
      "\\*" ^^^ "*" |
      "\\+" ^^^ "+" |
      "\\?" ^^^ "?" |
      "\\^" ^^^ "^" |
      "\\$" ^^^ "$" |
      "\\" ~> ".".r ^^ { c => c } // Any other escaped char

  def normalChar: Parser[String] =
    """[^\\()\[\]{}|*+?^$]""".r // Regular chars (not special regex chars)

  // Quantifiers: {n}, {m,n}, +, *, ?
  def quantifier: Parser[Quantifier] =
    "{" ~> """[0-9]+,[0-9]+""".r <~ "}" ^^ { s =>
      val Array(min, max) = s.split(",")
      RangeQuantifier(min.toInt, max.toInt)
    } |
      "{" ~> """[0-9]+""".r <~ "}" ^^ { n => ExactQuantifier(n.toInt) } |
      "+" ^^^ PlusQuantifier |
      "*" ^^^ StarQuantifier |
      "?" ^^^ QuestionQuantifier

  /**
   * Apply a quantifier to a regex node
   */
  def applyQuantifier(node: RegexNode, quant: Quantifier): RegexNode = {
    quant match {
      case ExactQuantifier(n) =>
        node match {
          case CharacterClassNode(charType, _, _) =>
            // For character classes, update the quantifier
            CharacterClassNode(charType, n, n)
          case _ =>
            // For other nodes, repeat them n times
            if (n == 0) LiteralNode("")
            else if (n == 1) node
            else SequenceNode(List.fill(n)(node))
        }

      case RangeQuantifier(min, max) =>
        node match {
          case CharacterClassNode(charType, _, _) =>
            CharacterClassNode(charType, min, max)
          case _ if min == max =>
            if (min == 0) LiteralNode("")
            else if (min == 1) node
            else SequenceNode(List.fill(min)(node))
          case _ =>
            // For complex nodes with variable repetition, use min as safe default
            // Better would be to generate SQL that handles variable repetition
            if (min == 0) OptionalNode(node)
            else if (min == 1) node
            else SequenceNode(List.fill(min)(node))
        }

      case PlusQuantifier =>
        node match {
          case CharacterClassNode(charType, _, _) =>
            CharacterClassNode(charType, 1, 5) // Bounded default for +
          case _ =>
            // Repeat 1-3 times (bounded)
            SequenceNode(List.fill(3)(node))
        }

      case StarQuantifier =>
        node match {
          case CharacterClassNode(charType, _, _) =>
            CharacterClassNode(charType, 0, 3) // Bounded default for *
          case _ =>
            // 0-2 repetitions
            OptionalNode(SequenceNode(List.fill(2)(node)))
        }

      case QuestionQuantifier =>
        OptionalNode(node)
    }
  }

  /**
   * Parse a regex string into an AST
   */
  def parseRegex(input: String): Try[RegexNode] = {
    // Remove anchors (^ and $) as they don't affect generation
    val cleaned = input.stripPrefix("^").stripSuffix("$")

    if (cleaned.isEmpty) {
      return scala.util.Success(LiteralNode(""))
    }

    parseAll(pattern, cleaned) match {
      case parserSuccess: this.Success[_] => scala.util.Success(parserSuccess.get.asInstanceOf[RegexNode])
      case NoSuccess(msg, next) =>
        scala.util.Failure(new IllegalArgumentException(
          s"Failed to parse regex '$input' at position ${next.pos}: $msg"
        ))
    }
  }
}

/**
 * Quantifier representations
 */
sealed trait Quantifier

case class ExactQuantifier(n: Int) extends Quantifier

case class RangeQuantifier(min: Int, max: Int) extends Quantifier

case object PlusQuantifier extends Quantifier

case object StarQuantifier extends Quantifier

case object QuestionQuantifier extends Quantifier

/**
 * Companion object for easy access to parsing
 */
object RegexPatternParser {
  private val parser = new RegexPatternParser()

  /**
   * Parse a regex string into an AST node
   */
  def parse(regex: String): Try[RegexNode] = parser.parseRegex(regex)

  /**
   * Parse a regex and convert directly to SQL
   */
  def toSql(regex: String): Try[String] = {
    parse(regex).map(_.toSql)
  }
}
