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
      // Fast mode: Use MD5-based generation for maximum performance
      // MD5 produces 32 hex characters, which is much faster than TRANSFORM + SEQUENCE
      if (maxLength <= 32) {
        // For strings up to 32 chars, single MD5 hash is sufficient
        val seedExpr = s"CONCAT(CAST($sqlRandom * 1000000 AS BIGINT), '_', CAST(monotonically_increasing_id() AS BIGINT))"
        if (minLength == maxLength) {
          s"UPPER(SUBSTRING(MD5($seedExpr), 1, $maxLength))"
        } else {
          val lengthExpr = s"CAST($sqlRandom * (${maxLength - minLength}) + $minLength AS INT)"
          s"UPPER(SUBSTRING(MD5($seedExpr), 1, $lengthExpr))"
        }
      } else {
        // For longer strings, concatenate multiple MD5 hashes
        val numHashes = (maxLength / 32) + 1
        val hashes = (0 until numHashes).map { i =>
          val seedExpr = s"CONCAT(CAST($sqlRandom * 1000000 AS BIGINT), '_', $i, '_', CAST(monotonically_increasing_id() AS BIGINT))"
          s"MD5($seedExpr)"
        }.mkString(", ")

        if (minLength == maxLength) {
          s"UPPER(SUBSTRING(CONCAT($hashes), 1, $maxLength))"
        } else {
          val lengthExpr = s"CAST($sqlRandom * (${maxLength - minLength}) + $minLength AS INT)"
          s"UPPER(SUBSTRING(CONCAT($hashes), 1, $lengthExpr))"
        }
      }
    }
  }

  /**
   * Fast regex data generator that uses a parser to convert regex patterns to pure SQL expressions.
   * Uses RegexPatternParser to parse common regex patterns and generate efficient SQL.
   * Falls back to UDF-based generation for unsupported patterns.
   */
  class FastRegexDataGenerator(val structField: StructField, val faker: Faker = new Faker()) extends NullableDataGenerator[String] {
    import io.github.datacatering.datacaterer.core.generator.provider.regex.RegexPatternParser
    import io.github.datacatering.datacaterer.core.model.Constants.GENERATE_REGEX_UDF
    import org.apache.log4j.Logger

    private val LOGGER = Logger.getLogger(getClass.getName)
    private val regex = Try(structField.metadata.getString(REGEX_GENERATOR))
      .getOrElse(throw InvalidDataGeneratorConfigurationException(structField, REGEX_GENERATOR))

    // Parse the regex pattern once during initialization
    private val parsedPattern = RegexPatternParser.parse(regex)

    // Log parsing result for debugging
    parsedPattern match {
      case scala.util.Success(node) =>
        LOGGER.debug(s"Successfully parsed regex '$regex' for field ${structField.name}")
      case scala.util.Failure(ex) =>
        LOGGER.warn(s"Could not parse regex '$regex' for field ${structField.name}: ${ex.getMessage}. " +
          s"Falling back to UDF-based generation.")
    }

    override val edgeCases: List[String] = List()

    override def generate: String = {
      // For JVM generation, still use faker.regexify for accuracy
      faker.regexify(regex)
    }

    override def generateSqlExpression: String = {
      parsedPattern match {
        case scala.util.Success(node) =>
          // Successfully parsed - use pure SQL generation
          node.toSql

        case scala.util.Failure(_) =>
          // Parser failed - fall back to UDF (slower but correct)
          val escapedRegex = regex.replace("\\", "\\\\").replace("'", "\\'")
          s"$GENERATE_REGEX_UDF('$escapedRegex')"
      }
    }
  }
} 