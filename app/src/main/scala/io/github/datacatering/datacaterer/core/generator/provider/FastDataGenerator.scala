package io.github.datacatering.datacaterer.core.generator.provider

import io.github.datacatering.datacaterer.api.model.Constants.{MAXIMUM_LENGTH, MINIMUM_LENGTH, REGEX_GENERATOR}
import io.github.datacatering.datacaterer.core.exception.InvalidDataGeneratorConfigurationException
import io.github.datacatering.datacaterer.core.generator.provider.RandomDataGenerator.tryGetValue
import net.datafaker.Faker
import org.apache.spark.sql.types.StructField

import scala.util.Try

/**
 * Fast mode data generators that replace UDF function calls with pure SQL expressions.
 * These generators prioritize speed over complex data patterns by using simple SQL instead of Faker/UDF calls.
 */
object FastDataGenerator {

  /**
   * Fast string data generator that replaces Faker expressions and UDF calls with simple SQL patterns.
   * Instead of using complex Faker expressions, it generates simple alphanumeric strings using pure SQL.
   */
  class FastStringDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private val minLength = tryGetValue(structField.metadata, MINIMUM_LENGTH, 1)
    private val maxLength = tryGetValue(structField.metadata, MAXIMUM_LENGTH, 20)
    assert(minLength <= maxLength, s"minLength has to be less than or equal to maxLength, field-name=${structField.name}, minLength=$minLength, maxLength=$maxLength")
    
    // Simple character set for fast generation (alphanumeric only)
    private val characterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    private val characterSetSize = characterSet.length

    override val edgeCases: List[String] = List("", " ", "A", "Z", "0", "9")

    override def generate: String = {
      // For fast mode, always use simple alphanumeric generation
      val stringLength = (random.nextDouble() * (maxLength - minLength) + minLength).toInt
      random.alphanumeric.take(stringLength).mkString
    }

    override def generateSqlExpression: String = {
      // Fast mode: Use simple SQL expression without UDF calls
      // Generate random length between min and max
      val lengthExpr = s"CAST($sqlRandom * ${maxLength - minLength} + $minLength AS INT)"
      
      // Generate random alphanumeric string using pure SQL
      s"UPPER(CONCAT_WS('', TRANSFORM(SEQUENCE(1, $lengthExpr), i -> SUBSTR('$characterSet', CAST($sqlRandom * $characterSetSize + 1 AS INT), 1))))"
    }
  }

  /**
   * Fast regex data generator that replaces complex regex patterns with simple SQL patterns.
   * Instead of using Faker's regexify function, it generates simple patterns that approximate common regex use cases.
   */
  class FastRegexDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    private val regex = Try(structField.metadata.getString(REGEX_GENERATOR))
      .getOrElse(throw InvalidDataGeneratorConfigurationException(structField, REGEX_GENERATOR))

    override val edgeCases: List[String] = List()

    override def generate: String = {
      // For fast mode, use simplified pattern generation
      generateSimplePattern(regex)
    }

    override def generateSqlExpression: String = {
      // Fast mode: Replace regex with simple SQL patterns
      generateSimpleSqlPattern(regex)
    }

    /**
     * Generate simple patterns for common regex use cases without using Faker.
     */
    private def generateSimplePattern(pattern: String): String = {
      pattern match {
        // Email patterns
        case p if p.contains("@") || p.contains("email") =>
          s"user${random.nextInt(1000)}@example.com"
        
        // Phone number patterns
        case p if p.contains("\\d{3}") && p.contains("\\d{4}") =>
          f"${random.nextInt(900) + 100}%03d-${random.nextInt(900) + 100}%03d-${random.nextInt(9000) + 1000}%04d"
        
        // UUID patterns
        case p if p.contains("uuid") || p.contains("UUID") =>
          java.util.UUID.randomUUID().toString
        
        // Alphanumeric patterns with specific length
        case p if p.contains("[A-Z0-9]") || p.contains("[a-zA-Z0-9]") =>
          val length = extractLengthFromPattern(p).getOrElse(8)
          random.alphanumeric.take(length).mkString.toUpperCase
        
        // Number patterns
        case p if p.contains("\\d") =>
          val length = extractLengthFromPattern(p).getOrElse(6)
          (1 to length).map(_ => random.nextInt(10)).mkString
        
        // Letter patterns
        case p if p.contains("[A-Z]") =>
          val length = extractLengthFromPattern(p).getOrElse(5)
          (1 to length).map(_ => ('A' + random.nextInt(26)).toChar).mkString
        
        // Default: simple alphanumeric string
        case _ =>
          random.alphanumeric.take(8).mkString
      }
    }

    /**
     * Generate simple SQL patterns for common regex use cases.
     */
    private def generateSimpleSqlPattern(pattern: String): String = {
      pattern match {
        // Email patterns
        case p if p.contains("@") || p.contains("email") =>
          s"CONCAT('user', CAST($sqlRandom * 1000 AS INT), '@example.com')"
        
        // Phone number patterns  
        case p if p.contains("\\d{3}") && p.contains("\\d{4}") =>
          s"CONCAT(LPAD(CAST($sqlRandom * 900 + 100 AS INT), 3, '0'), '-', LPAD(CAST($sqlRandom * 900 + 100 AS INT), 3, '0'), '-', LPAD(CAST($sqlRandom * 9000 + 1000 AS INT), 4, '0'))"
        
        // UUID patterns (using simple approach)
        case p if p.contains("uuid") || p.contains("UUID") =>
          s"CONCAT(LPAD(HEX(CAST($sqlRandom * 4294967295 AS BIGINT)), 8, '0'), '-', LPAD(HEX(CAST($sqlRandom * 65535 AS INT)), 4, '0'), '-', LPAD(HEX(CAST($sqlRandom * 65535 AS INT)), 4, '0'), '-', LPAD(HEX(CAST($sqlRandom * 65535 AS INT)), 4, '0'), '-', LPAD(HEX(CAST($sqlRandom * 281474976710655L AS BIGINT)), 12, '0'))"
        
        // Alphanumeric patterns
        case p if p.contains("[A-Z0-9]") || p.contains("[a-zA-Z0-9]") =>
          val length = extractLengthFromPattern(p).getOrElse(8)
          val charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
          s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $length), i -> SUBSTR('$charset', CAST($sqlRandom * ${charset.length} + 1 AS INT), 1)))"
        
        // Number patterns
        case p if p.contains("\\d") =>
          val length = extractLengthFromPattern(p).getOrElse(6)
          s"LPAD(CAST($sqlRandom * ${Math.pow(10, length).toLong} AS BIGINT), $length, '0')"
        
        // Letter patterns
        case p if p.contains("[A-Z]") =>
          val length = extractLengthFromPattern(p).getOrElse(5)
          s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, $length), i -> CHAR(CAST($sqlRandom * 26 + 65 AS INT))))"
        
        // Default: simple alphanumeric string
        case _ =>
          val charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
          s"CONCAT_WS('', TRANSFORM(SEQUENCE(1, 8), i -> SUBSTR('$charset', CAST($sqlRandom * ${charset.length} + 1 AS INT), 1)))"
      }
    }

    /**
     * Extract length specification from regex pattern (e.g., {5}, {3,8}).
     */
    private def extractLengthFromPattern(pattern: String): Option[Int] = {
      val lengthRegex = "\\{(\\d+)\\}".r
      val rangeRegex = "\\{(\\d+),(\\d+)\\}".r
      
      lengthRegex.findFirstMatchIn(pattern).map(_.group(1).toInt)
        .orElse(rangeRegex.findFirstMatchIn(pattern).map(m => (m.group(1).toInt + m.group(2).toInt) / 2))
        .orElse {
          // Look for repetition indicators like +, *, {n}
          if (pattern.contains("+")) Some(5)
          else if (pattern.contains("*")) Some(3)
          else None
        }
    }
  }
} 