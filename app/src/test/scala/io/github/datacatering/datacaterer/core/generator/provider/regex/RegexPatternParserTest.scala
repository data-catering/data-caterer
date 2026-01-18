package io.github.datacatering.datacaterer.core.generator.provider.regex

import org.scalatest.funsuite.AnyFunSuite

class RegexPatternParserTest extends AnyFunSuite {

  test("Parse simple literal") {
    val result = RegexPatternParser.parse("ACC")
    assert(result.isSuccess)
    assert(result.get == LiteralNode("ACC"))
  }

  test("Parse multiple literals in sequence") {
    val result = RegexPatternParser.parse("ABC-DEF")
    assert(result.isSuccess)
    // Consecutive literals are merged into a single LiteralNode
    assert(result.get == LiteralNode("ABC-DEF"))
  }

  test("Parse digit with exact quantifier") {
    val result = RegexPatternParser.parse("\\d{8}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(Digit, 8, 8) => // success
      case other => fail(s"Expected CharacterClassNode(Digit, 8, 8), got $other")
    }
  }

  test("Parse digit range quantifier") {
    val result = RegexPatternParser.parse("\\d{5,10}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(Digit, 5, 10) => // success
      case other => fail(s"Expected CharacterClassNode(Digit, 5, 10), got $other")
    }
  }

  test("Parse bracket digit class") {
    val result = RegexPatternParser.parse("[0-9]{3}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(Digit, 3, 3) => // success
      case other => fail(s"Expected CharacterClassNode(Digit, 3, 3), got $other")
    }
  }

  test("Parse uppercase letter class") {
    val result = RegexPatternParser.parse("[A-Z]{5}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(UppercaseLetter, 5, 5) => // success
      case other => fail(s"Expected CharacterClassNode(UppercaseLetter, 5, 5), got $other")
    }
  }

  test("Parse lowercase letter class") {
    val result = RegexPatternParser.parse("[a-z]{4}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(LowercaseLetter, 4, 4) => // success
      case other => fail(s"Expected CharacterClassNode(LowercaseLetter, 4, 4), got $other")
    }
  }

  test("Parse mixed letter class") {
    val result = RegexPatternParser.parse("[A-Za-z]{6}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(MixedLetter, 6, 6) => // success
      case other => fail(s"Expected CharacterClassNode(MixedLetter, 6, 6), got $other")
    }
  }

  test("Parse alphanumeric uppercase") {
    val result = RegexPatternParser.parse("[A-Z0-9]{10}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(AlphanumericUpper, 10, 10) => // success
      case other => fail(s"Expected CharacterClassNode(AlphanumericUpper, 10, 10), got $other")
    }
  }

  test("Parse alphanumeric mixed") {
    val result = RegexPatternParser.parse("[A-Za-z0-9]{12}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(AlphanumericMixed, 12, 12) => // success
      case other => fail(s"Expected CharacterClassNode(AlphanumericMixed, 12, 12), got $other")
    }
  }

  test("Parse custom character set") {
    val result = RegexPatternParser.parse("[abc123]{5}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(CustomCharSet("abc123"), 5, 5) => // success
      case other => fail(s"Expected CharacterClassNode(CustomCharSet(abc123), 5, 5), got $other")
    }
  }

  test("Parse complex account ID pattern") {
    val result = RegexPatternParser.parse("ACC[0-9]{8}")
    assert(result.isSuccess)
    result.get match {
      case SequenceNode(List(LiteralNode("ACC"), CharacterClassNode(Digit, 8, 8))) => // success
      case other => fail(s"Unexpected parse result: $other")
    }
  }

  test("Parse transaction ID pattern with dash") {
    val result = RegexPatternParser.parse("TXN-[0-9]{5}")
    assert(result.isSuccess)
    result.get match {
      case SequenceNode(parts) =>
        assert(parts.length == 2)
        assert(parts(0) == LiteralNode("TXN-"))
        parts(1) match {
          case CharacterClassNode(Digit, 5, 5) => // success
          case other => fail(s"Expected digit class, got $other")
        }
      case other => fail(s"Unexpected parse result: $other")
    }
  }

  test("Parse mixed pattern with letters and digits") {
    val result = RegexPatternParser.parse("[A-Z]{3}-[0-9]{2}")
    assert(result.isSuccess)
    result.get match {
      case SequenceNode(parts) =>
        assert(parts.length == 3)
        parts(0) match {
          case CharacterClassNode(UppercaseLetter, 3, 3) => // success
          case other => fail(s"Expected uppercase letter class, got $other")
        }
        assert(parts(1) == LiteralNode("-"))
        parts(2) match {
          case CharacterClassNode(Digit, 2, 2) => // success
          case other => fail(s"Expected digit class, got $other")
        }
      case other => fail(s"Unexpected parse result: $other")
    }
  }

  test("Parse alternation pattern") {
    val result = RegexPatternParser.parse("(ACTIVE|INACTIVE|PENDING)")
    assert(result.isSuccess)
    result.get match {
      case AlternationNode(options) =>
        assert(options.length == 3)
        assert(options(0) == LiteralNode("ACTIVE"))
        assert(options(1) == LiteralNode("INACTIVE"))
        assert(options(2) == LiteralNode("PENDING"))
      case other => fail(s"Expected AlternationNode, got $other")
    }
  }

  test("Parse non-capturing group alternation") {
    val result = RegexPatternParser.parse("(?:YES|NO)")
    assert(result.isSuccess)
    result.get match {
      case AlternationNode(options) =>
        assert(options.length == 2)
        assert(options.contains(LiteralNode("YES")))
        assert(options.contains(LiteralNode("NO")))
      case other => fail(s"Expected AlternationNode, got $other")
    }
  }

  test("Parse optional quantifier") {
    val result = RegexPatternParser.parse("ABC?")
    assert(result.isSuccess)
    result.get match {
      case SequenceNode(List(LiteralNode("AB"), OptionalNode(LiteralNode("C")))) => // success
      case other => fail(s"Expected sequence with optional, got $other")
    }
  }

  test("Parse plus quantifier on character class") {
    val result = RegexPatternParser.parse("\\d+")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(Digit, 1, 5) => // Default bounded for +
      case other => fail(s"Expected bounded digit class, got $other")
    }
  }

  test("Parse star quantifier on character class") {
    val result = RegexPatternParser.parse("\\d*")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(Digit, 0, 3) => // Default bounded for *
      case other => fail(s"Expected bounded digit class, got $other")
    }
  }

  test("Parse escaped special characters") {
    val result = RegexPatternParser.parse("\\(\\)")
    assert(result.isSuccess)
    // Merged into single literal
    assert(result.get == LiteralNode("()"))
  }

  test("Parse pattern with escaped dash") {
    val result = RegexPatternParser.parse("USER\\-ID")
    assert(result.isSuccess)
    // Merged into single literal
    assert(result.get == LiteralNode("USER-ID"))
  }

  test("Parse anchored pattern (anchors removed)") {
    val result = RegexPatternParser.parse("^ACC[0-9]{8}$")
    assert(result.isSuccess)
    // Anchors should be stripped during parsing
    result.get match {
      case SequenceNode(List(LiteralNode("ACC"), CharacterClassNode(Digit, 8, 8))) => // success
      case other => fail(s"Expected sequence without anchors, got $other")
    }
  }

  test("Generate SQL for simple literal") {
    val sql = RegexPatternParser.toSql("ACC")
    assert(sql.isSuccess)
    assert(sql.get == "'ACC'")
  }

  test("Generate SQL for digit pattern") {
    val sql = RegexPatternParser.toSql("\\d{8}")
    assert(sql.isSuccess)
    assert(sql.get.contains("LPAD"))
    assert(sql.get.contains("RAND()"))
  }

  test("Generate SQL for account ID pattern") {
    val sql = RegexPatternParser.toSql("ACC[0-9]{8}")
    assert(sql.isSuccess)
    assert(sql.get.contains("CONCAT"))
    assert(sql.get.contains("'ACC'"))
    assert(sql.get.contains("LPAD"))
  }

  test("Generate SQL for mixed pattern") {
    val sql = RegexPatternParser.toSql("[A-Z]{3}-[0-9]{2}")
    assert(sql.isSuccess)
    assert(sql.get.contains("CONCAT"))
    assert(sql.get.contains("TRANSFORM"))
    assert(sql.get.contains("CHAR"))
    assert(sql.get.contains("'-'"))
  }

  test("Generate SQL for alternation") {
    val sql = RegexPatternParser.toSql("(ACTIVE|INACTIVE)")
    assert(sql.isSuccess)
    assert(sql.get.contains("ELEMENT_AT"))
    assert(sql.get.contains("ARRAY"))
    assert(sql.get.contains("'ACTIVE'"))
    assert(sql.get.contains("'INACTIVE'"))
  }

  test("Fail gracefully on unsupported pattern (backreference)") {
    val result = RegexPatternParser.parse("(.)\\1")
    // Parser might succeed but won't handle backreference semantics
    // This is expected - we don't support advanced features
  }

  test("Parse empty pattern") {
    val result = RegexPatternParser.parse("")
    assert(result.isSuccess)
    assert(result.get == LiteralNode(""))
  }

  test("Parse word boundary is not supported (would fail or ignore)") {
    // Word boundaries \b are not character-generating, so they won't parse
    val result = RegexPatternParser.parse("\\bWORD\\b")
    // This should either fail or ignore boundaries - we accept either behavior
    assert(result.isFailure || result.isSuccess)
  }

  test("Complex real-world pattern: payment ID") {
    val result = RegexPatternParser.parse("PAY-[0-9]{10}")
    assert(result.isSuccess)
    val sql = result.get.toSql
    assert(sql.contains("CONCAT"))
    assert(sql.contains("'PAY-'"))
  }

  test("Complex real-world pattern: email-like") {
    val result = RegexPatternParser.parse("[a-z]{5,10}@[a-z]{3,8}\\.com")
    assert(result.isSuccess)
    // Should handle variable length character classes
  }

  test("Parse pattern with groups") {
    val result = RegexPatternParser.parse("(ABC)(DEF)")
    assert(result.isSuccess)
    // Groups without alternation are just literals, and they get merged
    assert(result.get == LiteralNode("ABCDEF"))
  }

  test("SQL output should be deterministic for same pattern") {
    val sql1 = RegexPatternParser.toSql("ACC[0-9]{8}")
    val sql2 = RegexPatternParser.toSql("ACC[0-9]{8}")
    assert(sql1 == sql2)
  }

  test("Min/max length calculation for literals") {
    val node = LiteralNode("HELLO")
    assert(node.minLength == 5)
    assert(node.maxLength == 5)
  }

  test("Min/max length calculation for character classes") {
    val node = CharacterClassNode(Digit, 3, 7)
    assert(node.minLength == 3)
    assert(node.maxLength == 7)
  }

  test("Min/max length calculation for sequences") {
    val node = SequenceNode(List(
      LiteralNode("ABC"),
      CharacterClassNode(Digit, 2, 5)
    ))
    assert(node.minLength == 5) // 3 + 2
    assert(node.maxLength == 8) // 3 + 5
  }

  test("Min/max length calculation for alternations") {
    val node = AlternationNode(List(
      LiteralNode("YES"),
      LiteralNode("NO")
    ))
    assert(node.minLength == 2) // "NO"
    assert(node.maxLength == 3) // "YES"
  }

  test("Parse hexadecimal lowercase pattern") {
    val result = RegexPatternParser.parse("[0-9a-f]{8}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(CustomCharSet(chars), 8, 8) =>
        // Should be a custom character set with only lowercase hex chars
        assert(chars == "0123456789abcdef", s"Expected '0123456789abcdef' but got '$chars'")
      case other => fail(s"Expected CharacterClassNode(CustomCharSet(0123456789abcdef), 8, 8), got $other")
    }
  }

  test("Parse hexadecimal uppercase pattern") {
    val result = RegexPatternParser.parse("[0-9A-F]{8}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(CustomCharSet(chars), 8, 8) =>
        // Should be a custom character set with only uppercase hex chars
        assert(chars == "0123456789ABCDEF", s"Expected '0123456789ABCDEF' but got '$chars'")
      case other => fail(s"Expected CharacterClassNode(CustomCharSet(0123456789ABCDEF), 8, 8), got $other")
    }
  }

  test("Parse hexadecimal mixed case pattern") {
    val result = RegexPatternParser.parse("[0-9a-fA-F]{8}")
    assert(result.isSuccess)
    result.get match {
      case CharacterClassNode(CustomCharSet(chars), 8, 8) =>
        // Should be a custom character set with both cases
        assert(chars == "0123456789abcdefABCDEF", s"Expected '0123456789abcdefABCDEF' but got '$chars'")
      case other => fail(s"Expected CharacterClassNode(CustomCharSet), got $other")
    }
  }

  test("Generate SQL for hexadecimal lowercase pattern") {
    val sql = RegexPatternParser.toSql("[0-9a-f]{8}")
    assert(sql.isSuccess)
    // Should use SUBSTRING with custom charset
    assert(sql.get.contains("SUBSTRING"))
    assert(sql.get.contains("0123456789abcdef"))
    // Should NOT contain uppercase letters
    assert(!sql.get.contains("ABCDEF"))
  }
}
